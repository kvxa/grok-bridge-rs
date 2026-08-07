/**
 * Shared WebUI terminal stream bounds.
 * Keep these aligned with server WEB_EVENTS_MAX_MESSAGE_BYTES (1 MiB frames).
 */

/** Server WebSocket text frame cap (JSON + base64). */
export const WEB_EVENTS_MAX_MESSAGE_BYTES = 1024 * 1024;

/**
 * Bound for ordinary PTY **delta** write-queue / remount backlog retention.
 * Exceeding this under a stalled consumer marks a gap and requests resync.
 */
export const TERMINAL_DELTA_QUEUE_MAX_BYTES = 256 * 1024;

/** Max delta entries retained in the xterm write queue. */
export const TERMINAL_DELTA_QUEUE_MAX_ENTRIES = 256;

/**
 * Max bytes of multi-frame **reset snapshot stream** that may be queued
 * (reset head + reset_cont pieces). Must exceed one full WS frame payload so a
 * legal split snapshot is never overflow-resynced into an infinite loop.
 */
export const TERMINAL_SNAPSHOT_STREAM_MAX_BYTES = 8 * 1024 * 1024;

/** Remount backlog entry count (last snapshot stream + subsequent deltas). */
export const TERMINAL_REMOUNT_BUFFER_MAX = 64;

/** Remount backlog delta budget (same as delta write queue). */
export const TERMINAL_REMOUNT_BUFFER_BYTES = TERMINAL_DELTA_QUEUE_MAX_BYTES;

/** Remount backlog budget for an in-progress reset snapshot stream. */
export const TERMINAL_REMOUNT_SNAPSHOT_MAX_BYTES = TERMINAL_SNAPSHOT_STREAM_MAX_BYTES;

/** Global retention across every hidden terminal without a live xterm listener. */
export const TERMINAL_REMOUNT_GLOBAL_MAX_BYTES = 16 * 1024 * 1024;
export const TERMINAL_REMOUNT_GLOBAL_MAX_ENTRIES = 2048;

/** True if this entry is the head of an authoritative ANSI snapshot. */
export function isResetHead(entry) {
  return Boolean(entry?.reset) && !entry?.reset_cont;
}

/** True if this entry continues a multi-frame reset snapshot (not a PTY delta). */
export function isResetCont(entry) {
  return Boolean(entry?.reset_cont);
}

/** Head or continuation of a split/unsplit reset snapshot. */
export function isSnapshotPiece(entry) {
  return isResetHead(entry) || isResetCont(entry);
}
