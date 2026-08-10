import {
  TERMINAL_BUFFER_MAX_BYTES,
  TERMINAL_BUFFER_MAX_ENTRIES,
} from "./constants.js";

/** @typedef {{ session: string, reset: boolean, cursor?: number, next_cursor?: number, data_base64: string }} TerminalEntry */

/** @type {Map<string, TerminalEntry[]>} */
const buffers = new Map();
/** @type {Map<string, number>} Total retained payload bytes per session. */
const bufferBytes = new Map();
/**
 * @type {Map<string, boolean>} Retained-stream integrity per session. Once a
 * trim (or an arrival-order jump) breaks the cursor chain, the retained
 * buffer can no longer be replayed faithfully: all subsequent deltas are
 * discarded until the next reset re-anchors a fresh snapshot.
 */
const gapInvalid = new Map();
/** @type {Map<string, Set<(entry: TerminalEntry) => void>>} */
const listeners = new Map();

function ensureBuffer(session) {
  let buffer = buffers.get(session);
  if (!buffer) {
    buffer = [];
    buffers.set(session, buffer);
    bufferBytes.set(session, 0);
  }
  return buffer;
}

function entryBytes(entry) {
  return typeof entry?.data_base64 === "string" ? entry.data_base64.length : 0;
}

function hasLiveListeners(session) {
  const set = listeners.get(session);
  return Boolean(set && set.size > 0);
}

function hasCursorRange(entry) {
  return (
    entry &&
    typeof entry.cursor === "number" &&
    typeof entry.next_cursor === "number"
  );
}

/** True when any adjacent retained pair breaks the cursor continuity. */
function hasCursorGap(buffer) {
  for (let i = 1; i < buffer.length; i += 1) {
    const prev = buffer[i - 1];
    const next = buffer[i];
    if (
      hasCursorRange(prev) &&
      hasCursorRange(next) &&
      prev.next_cursor !== next.cursor
    ) {
      return true;
    }
  }
  return false;
}

/** Drop the retained stream and mark the session gap-invalid until a reset. */
function invalidateRetainedStream(session) {
  const buffer = buffers.get(session);
  if (buffer) buffer.length = 0;
  bufferBytes.set(session, 0);
  gapInvalid.set(session, true);
}

/**
 * Bound a retained buffer after a push. A stream carrying a full-snapshot
 * anchor — buffer[0].reset === true, always at index 0 because a reset clears
 * the buffer before being pushed — is an atomic generation: the Runtime splits
 * a large snapshot into a reset anchor plus `reset=false` continuations, and
 * the anchor with every entry after it forms one replayable unit. Dropping
 * any member (byte budget, entry budget, or stale-delta trim) would replay a
 * partial snapshot into xterm, so the whole generation is invalidated instead
 * and the session stays gap-invalid until a reset that fits entirely arrives.
 *
 * Without an anchor (a pure delta stream) the oldest deltas are dropped as
 * before; a cursor chain broken by that trim still invalidates the stream.
 */
function trimRetainedBuffer(session, buffer) {
  let bytes = bufferBytes.get(session) ?? 0;
  const hasAnchor = buffer.length > 0 && buffer[0].reset === true;
  while (
    buffer.length > TERMINAL_BUFFER_MAX_ENTRIES ||
    bytes > TERMINAL_BUFFER_MAX_BYTES
  ) {
    if (hasAnchor) {
      // The generation cannot fit whole: keep no partial snapshot around.
      invalidateRetainedStream(session);
      return;
    }
    if (buffer.length <= 1) break;
    const dropped = buffer.splice(0, 1)[0];
    bytes -= entryBytes(dropped);
  }
  if (hasCursorGap(buffer)) {
    buffer.length = 0;
    bytes = 0;
    gapInvalid.set(session, true);
  }
  bufferBytes.set(session, bytes);
}

/**
 * Push terminal stream entries in arrival order.
 *
 * - With live listeners: deliver immediately and do not retain (unbounded growth).
 * - Without listeners: keep a bounded remount backlog (last reset + subsequent)
 *   capped by both entry count and bytes; stale deltas are dropped and the full
 *   snapshot (newest reset) remains the recovery anchor.
 */
export function pushTerminalEntries(entries) {
  if (!Array.isArray(entries) || entries.length === 0) return;
  for (const entry of entries) {
    if (!entry || typeof entry.session !== "string" || !entry.session) continue;

    if (hasLiveListeners(entry.session)) {
      if (entry.reset) {
        gapInvalid.delete(entry.session);
      } else if (gapInvalid.get(entry.session)) {
        // A newly mounted terminal has no faithful replay anchor after a
        // retained gap. Keep dropping deltas until the Runtime sends a reset.
        continue;
      }
      const set = listeners.get(entry.session);
      for (const listener of set) listener(entry);
      continue;
    }

    const buffer = ensureBuffer(entry.session);
    if (entry.reset) {
      // A fresh full snapshot re-anchors the retained stream.
      buffer.length = 0;
      bufferBytes.set(entry.session, 0);
      gapInvalid.delete(entry.session);
    } else if (gapInvalid.get(entry.session)) {
      // The retained chain is broken; deltas stay discarded until a reset.
      continue;
    }
    // A delta that does not continue the last retained range means data was
    // lost before it: the retained stream is no longer faithful.
    const prev = buffer.at(-1);
    if (
      prev &&
      hasCursorRange(prev) &&
      hasCursorRange(entry) &&
      prev.next_cursor !== entry.cursor
    ) {
      invalidateRetainedStream(entry.session);
      continue;
    }
    buffer.push(entry);
    bufferBytes.set(
      entry.session,
      (bufferBytes.get(entry.session) ?? 0) + entryBytes(entry),
    );
    trimRetainedBuffer(entry.session, buffer);
  }
}

/**
 * Subscribe to a session's terminal feed.
 * Replays the pre-subscription backlog (last reset + subsequent) in order, then
 * immediately releases that backlog so live traffic is not retained.
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
  // Subscribing releases the retained backlog/state even when it has drained
  // to an empty buffer: only the actual entries are replayed.
  if (buffers.has(session)) {
    buffers.delete(session);
    bufferBytes.delete(session);
  }
  if (buffer && buffer.length > 0) {
    for (const entry of buffer.slice()) listener(entry);
  }

  return () => {
    set.delete(listener);
    if (set.size === 0) listeners.delete(session);
  };
}

export function disposeTerminalSession(session) {
  buffers.delete(session);
  bufferBytes.delete(session);
  gapInvalid.delete(session);
  listeners.delete(session);
}

/** Drop feeds for sessions that no longer exist in the pushed sessions list. */
export function reconcileTerminalSessions(activeSessionIds) {
  const active =
    activeSessionIds instanceof Set
      ? activeSessionIds
      : new Set(activeSessionIds || []);
  // Reconcile every map a session can occupy: retained buffers, live
  // listeners, and the gap-invalid flag. Sessions that are only gap-invalid
  // (e.g. after their drained buffer was released) would otherwise leak that
  // flag even though they are gone from buffers and listeners.
  const sessions = new Set([
    ...buffers.keys(),
    ...listeners.keys(),
    ...gapInvalid.keys(),
  ]);
  for (const session of sessions) {
    if (!active.has(session)) disposeTerminalSession(session);
  }
}

/** Test helper: clear all buffered feeds and listeners. */
export function resetTerminalFeeds() {
  buffers.clear();
  bufferBytes.clear();
  gapInvalid.clear();
  listeners.clear();
}

/** Test helper: inspect buffered entries for a session. */
export function peekTerminalBuffer(session) {
  return buffers.get(session)?.slice() ?? [];
}
