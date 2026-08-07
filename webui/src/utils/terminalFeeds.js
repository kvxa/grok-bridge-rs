/** @typedef {{ session: string, reset: boolean, reset_cont?: boolean, cursor?: number, next_cursor?: number, data_base64: string, gap?: boolean }} TerminalEntry */

import {
  TERMINAL_REMOUNT_BUFFER_BYTES,
  TERMINAL_REMOUNT_BUFFER_MAX,
  TERMINAL_REMOUNT_GLOBAL_MAX_BYTES,
  TERMINAL_REMOUNT_GLOBAL_MAX_ENTRIES,
  TERMINAL_REMOUNT_SNAPSHOT_MAX_BYTES,
  isResetCont,
  isResetHead,
  isSnapshotPiece,
} from "./terminalBounds.js";

export {
  TERMINAL_REMOUNT_BUFFER_BYTES,
  TERMINAL_REMOUNT_BUFFER_MAX,
  TERMINAL_REMOUNT_GLOBAL_MAX_BYTES,
  TERMINAL_REMOUNT_GLOBAL_MAX_ENTRIES,
  TERMINAL_REMOUNT_SNAPSHOT_MAX_BYTES,
} from "./terminalBounds.js";

/**
 * @typedef {{
 *   entries: TerminalEntry[],
 *   bytes: number,
 *   snapshotBytes: number,
 *   gapped: boolean,
 *   inResetStream: boolean,
 * }} SessionBacklog
 */

/** @type {Map<string, SessionBacklog>} */
const buffers = new Map();
/** @type {Map<string, Set<(entry: TerminalEntry) => void>>} */
const listeners = new Map();
/**
 * After a gap (overflow or gap marker delivered), discard non-snapshot traffic
 * until an authoritative reset head arrives. Survives buffer delete across
 * collapse/remount.
 * @type {Set<string>}
 */
const awaitResetAfterGap = new Set();
/**
 * Live path: after a gap, only a reset head clears; orphan reset_cont is dropped.
 * @type {Set<string>}
 */
const liveInResetStream = new Set();

function entryByteLength(entry) {
  if (!entry || entry.gap) return 0;
  const raw = entry.data_base64;
  if (typeof raw !== "string" || raw.length === 0) return 0;
  // base64 length → approximate decoded bytes without allocating.
  const padding = raw.endsWith("==") ? 2 : raw.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((raw.length * 3) / 4) - padding);
}

function ensureBuffer(session) {
  let buffer = buffers.get(session);
  if (!buffer) {
    buffer = {
      entries: [],
      bytes: 0,
      snapshotBytes: 0,
      gapped: false,
      inResetStream: false,
    };
    buffers.set(session, buffer);
  }
  return buffer;
}

function hasLiveListeners(session) {
  const set = listeners.get(session);
  return Boolean(set && set.size > 0);
}

function clearBacklog(buffer) {
  buffer.entries.length = 0;
  buffer.bytes = 0;
  buffer.snapshotBytes = 0;
  buffer.inResetStream = false;
}

function markGapped(session, buffer) {
  buffer.gapped = true;
  clearBacklog(buffer);
  awaitResetAfterGap.add(session);
  liveInResetStream.delete(session);
}

function enforceGlobalBacklogBudget(currentSession) {
  const totals = () => {
    let bytes = 0;
    let entries = 0;
    for (const buffer of buffers.values()) {
      bytes += buffer.bytes;
      entries += buffer.entries.length;
    }
    return { bytes, entries };
  };
  let total = totals();
  if (
    total.bytes <= TERMINAL_REMOUNT_GLOBAL_MAX_BYTES &&
    total.entries <= TERMINAL_REMOUNT_GLOBAL_MAX_ENTRIES
  ) {
    return;
  }
  for (const [session, buffer] of buffers) {
    if (session === currentSession || buffer.entries.length === 0) continue;
    markGapped(session, buffer);
    total = totals();
    if (
      total.bytes <= TERMINAL_REMOUNT_GLOBAL_MAX_BYTES &&
      total.entries <= TERMINAL_REMOUNT_GLOBAL_MAX_ENTRIES
    ) {
      return;
    }
  }
  const current = buffers.get(currentSession);
  if (current) markGapped(currentSession, current);
}

/**
 * Append to remount backlog. Reset snapshot streams (head + reset_cont) use a
 * higher byte budget so multi-frame ANSI snapshots are never gap→resync looped.
 * Ordinary deltas still mark gap on overflow.
 */
function pushBounded(session, buffer, entry) {
  if (entry.gap) {
    markGapped(session, buffer);
    return;
  }

  if (isResetHead(entry)) {
    clearBacklog(buffer);
    buffer.gapped = false;
    awaitResetAfterGap.delete(session);
    buffer.inResetStream = true;
    const bytes = entryByteLength(entry);
    if (bytes > TERMINAL_REMOUNT_SNAPSHOT_MAX_BYTES) {
      // Pathological single piece — still keep head (authoritative) alone.
      buffer.entries.push(entry);
      buffer.bytes = bytes;
      buffer.snapshotBytes = bytes;
      return;
    }
    buffer.entries.push(entry);
    buffer.bytes = bytes;
    buffer.snapshotBytes = bytes;
    return;
  }

  if (isResetCont(entry)) {
    if (buffer.gapped || awaitResetAfterGap.has(session)) {
      // Waiting for a new reset head; orphan conts are not deltas to keep.
      return;
    }
    if (!buffer.inResetStream) {
      // Cont without head is discontinuous.
      markGapped(session, buffer);
      return;
    }
    const bytes = entryByteLength(entry);
    if (buffer.snapshotBytes + bytes > TERMINAL_REMOUNT_SNAPSHOT_MAX_BYTES) {
      markGapped(session, buffer);
      return;
    }
    buffer.entries.push(entry);
    buffer.bytes += bytes;
    buffer.snapshotBytes += bytes;
    return;
  }

  // Ordinary PTY delta.
  buffer.inResetStream = false;
  if (buffer.gapped || awaitResetAfterGap.has(session)) {
    return;
  }

  const bytes = entryByteLength(entry);
  const wouldExceedCount =
    buffer.entries.length >= TERMINAL_REMOUNT_BUFFER_MAX;
  // Count only non-snapshot tail pressure: snapshot stream already stored;
  // deltas use the smaller remount budget relative to post-snapshot growth.
  const deltaBytes = Math.max(0, buffer.bytes - buffer.snapshotBytes);
  const wouldExceedBytes =
    deltaBytes + bytes > TERMINAL_REMOUNT_BUFFER_BYTES ||
    buffer.bytes + bytes > TERMINAL_REMOUNT_SNAPSHOT_MAX_BYTES + TERMINAL_REMOUNT_BUFFER_BYTES;

  if (wouldExceedCount || wouldExceedBytes) {
    markGapped(session, buffer);
    return;
  }

  buffer.entries.push(entry);
  buffer.bytes += bytes;
}

/**
 * Push terminal stream entries in arrival order.
 *
 * - With live listeners: deliver immediately and do not retain.
 * - Without listeners: keep a bounded remount backlog (snapshot stream + deltas).
 *   Delta overflow sets gapped; snapshot stream uses a higher bound so legal
 *   multi-frame resets are not gap-looped.
 */
export function pushTerminalEntries(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return;
  for (const entry of entries) {
    if (!entry || typeof entry.session !== "string" || !entry.session) continue;

    if (hasLiveListeners(entry.session)) {
      if (awaitResetAfterGap.has(entry.session)) {
        if (isResetHead(entry)) {
          awaitResetAfterGap.delete(entry.session);
          liveInResetStream.add(entry.session);
        } else if (entry.gap) {
          // keep awaiting
        } else {
          // Drop orphan deltas/conts until a reset head.
          continue;
        }
      } else if (isResetHead(entry)) {
        liveInResetStream.add(entry.session);
      } else if (isResetCont(entry)) {
        if (!liveInResetStream.has(entry.session)) {
          // Orphan cont: force await-reset so consumer resyncs once.
          awaitResetAfterGap.add(entry.session);
          liveInResetStream.delete(entry.session);
          const set = listeners.get(entry.session);
          if (set) {
            for (const listener of set) {
              listener({
                session: entry.session,
                reset: false,
                reset_cont: false,
                gap: true,
                data_base64: "",
              });
            }
          }
          continue;
        }
      } else if (!entry.gap) {
        liveInResetStream.delete(entry.session);
      }
      const set = listeners.get(entry.session);
      for (const listener of set) listener(entry);
      continue;
    }

    const buffer = ensureBuffer(entry.session);
    // Map insertion order is the eviction order; current traffic becomes newest.
    buffers.delete(entry.session);
    buffers.set(entry.session, buffer);
    pushBounded(entry.session, buffer, entry);
    enforceGlobalBacklogBudget(entry.session);
  }
}

/**
 * Subscribe to a session's terminal feed.
 * Replays the pre-subscription backlog (snapshot stream + subsequent) in order,
 * then releases that backlog so live traffic is not retained.
 * If the backlog gapped under pressure, delivers a gap marker instead so the
 * terminal requests a resync snapshot (live stream then accepts multi-frame reset).
 */
export function subscribeTerminal(session, listener) {
  if (typeof session !== "string" || !session || typeof listener !== "function") {
    return () => {};
  }
  let set = listeners.get(session);
  if (!set) {
    set = new Set();
    listeners.set(session, set);
  }
  set.add(listener);

  const buffer = buffers.get(session);
  if (buffer) {
    buffers.delete(session);
    if (buffer.gapped || awaitResetAfterGap.has(session)) {
      awaitResetAfterGap.add(session);
      liveInResetStream.delete(session);
      listener({
        session,
        reset: false,
        reset_cont: false,
        gap: true,
        data_base64: "",
      });
    } else if (buffer.entries.length > 0) {
      const replay = buffer.entries.slice();
      // If replay ends mid-snapshot stream, keep liveInResetStream so conts attach.
      if (buffer.inResetStream) {
        liveInResetStream.add(session);
      }
      for (const entry of replay) listener(entry);
    }
  } else if (awaitResetAfterGap.has(session)) {
    listener({
      session,
      reset: false,
      reset_cont: false,
      gap: true,
      data_base64: "",
    });
  }

  return () => {
    set.delete(listener);
    if (set.size === 0) {
      listeners.delete(session);
      liveInResetStream.delete(session);
    }
  };
}

export function disposeTerminalSession(session) {
  buffers.delete(session);
  listeners.delete(session);
  awaitResetAfterGap.delete(session);
  liveInResetStream.delete(session);
}

/** Drop feeds for sessions that no longer exist in the pushed sessions list. */
export function reconcileTerminalSessions(activeSessionIds) {
  const active =
    activeSessionIds instanceof Set
      ? activeSessionIds
      : new Set(activeSessionIds || []);
  for (const session of [...buffers.keys()]) {
    if (!active.has(session)) disposeTerminalSession(session);
  }
  for (const session of [...listeners.keys()]) {
    if (!active.has(session)) {
      listeners.delete(session);
      awaitResetAfterGap.delete(session);
      liveInResetStream.delete(session);
    }
  }
}

/** Test helper: clear all buffered feeds and listeners. */
export function resetTerminalFeeds() {
  buffers.clear();
  listeners.clear();
  awaitResetAfterGap.clear();
  liveInResetStream.clear();
}

/** Test helper: inspect buffered entries for a session. */
export function peekTerminalBuffer(session) {
  const buffer = buffers.get(session);
  if (!buffer) return [];
  return buffer.entries.slice();
}

/** Test helper: whether remount backlog is marked gapped. */
export function peekTerminalBufferGapped(session) {
  return Boolean(buffers.get(session)?.gapped || awaitResetAfterGap.has(session));
}
