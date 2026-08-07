import {
  TERMINAL_BUFFER_MAX_BYTES,
  TERMINAL_BUFFER_MAX_ENTRIES,
} from "./constants.js";

/** @typedef {{ session: string, reset: boolean, cursor?: number, next_cursor?: number, data_base64: string }} TerminalEntry */

/** @type {Map<string, TerminalEntry[]>} */
const buffers = new Map();
/** @type {Map<string, number>} Total retained payload bytes per session. */
const bufferBytes = new Map();
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

/**
 * Bound a retained buffer after a push: drop the oldest (stale) entries while
 * over either limit. The full-snapshot anchor — the newest reset entry, always
 * at index 0 because a reset clears the buffer before being pushed — is never
 * dropped: replay relies on it for recovery, so only deltas (or, when no
 * anchor exists yet, leading deltas) are trimmed.
 */
function trimRetainedBuffer(session, buffer) {
  let bytes = bufferBytes.get(session) ?? 0;
  const hasAnchor = buffer.length > 0 && buffer[0].reset === true;
  while (
    buffer.length > TERMINAL_BUFFER_MAX_ENTRIES ||
    bytes > TERMINAL_BUFFER_MAX_BYTES
  ) {
    if (buffer.length <= 1) break;
    const dropped = buffer.splice(hasAnchor ? 1 : 0, 1)[0];
    bytes -= entryBytes(dropped);
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
      const set = listeners.get(entry.session);
      for (const listener of set) listener(entry);
      continue;
    }

    const buffer = ensureBuffer(entry.session);
    if (entry.reset) {
      buffer.length = 0;
      bufferBytes.set(entry.session, 0);
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
  if (buffer && buffer.length > 0) {
    const replay = buffer.slice();
    buffers.delete(session);
    bufferBytes.delete(session);
    for (const entry of replay) listener(entry);
  }

  return () => {
    set.delete(listener);
    if (set.size === 0) listeners.delete(session);
  };
}

export function disposeTerminalSession(session) {
  buffers.delete(session);
  bufferBytes.delete(session);
  listeners.delete(session);
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
    if (!active.has(session)) listeners.delete(session);
  }
}

/** Test helper: clear all buffered feeds and listeners. */
export function resetTerminalFeeds() {
  buffers.clear();
  bufferBytes.clear();
  listeners.clear();
}

/** Test helper: inspect buffered entries for a session. */
export function peekTerminalBuffer(session) {
  return buffers.get(session)?.slice() ?? [];
}
