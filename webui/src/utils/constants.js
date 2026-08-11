import { MAX_WRITE_BYTES } from "./base64.js";

export const VERSION_POLL_MS = 60 * 60 * 1000;
/** Bounded exponential backoff delays for WebSocket reconnect (ms). */
export const WS_BACKOFF_MS = [1000, 2000, 4000, 8000, 15000, 30000];
export const GITHUB_URL = "https://github.com/luodaoyi/grok-bridge-rs";
export const DISMISS_UPDATE_KEY = "grok-bridge-dismissed-update";

/**
 * Bounds for terminal flow control. Every bound is enforced by BOTH entry
 * count and bytes so a full queue rejects an input as a whole instead of
 * admitting a partial write.
 */

/** Max in-flight WebUI commands awaiting a result per WebSocket connection. */
export const PENDING_COMMANDS_MAX = 64;
/**
 * Max pending terminal_input payload measured in base64 characters. Runtime's
 * writer budget measures decoded raw bytes, so this 256 KiB bound is more
 * conservative (about 192 KiB raw) than its like-named byte budget.
 */
export const PENDING_COMMANDS_MAX_BYTES = 256 * 1024;
/** Exact max raw bytes of one terminal_input; matches Runtime write_raw. */
export const MAX_INPUT_RAW_BYTES = MAX_WRITE_BYTES;
/**
 * Max base64 length of one terminal_input. Base64 length alone cannot separate
 * the last few raw sizes, so callers decode payloads at or below this fast-path
 * bound for an exact MAX_INPUT_RAW_BYTES check before sending any byte.
 */
export const MAX_INPUT_BASE64_LENGTH = 4 * Math.ceil(MAX_INPUT_RAW_BYTES / 3);
/** Max retained terminal feed entries per session (no live listener). */
export const TERMINAL_BUFFER_MAX_ENTRIES = 128;
/** Max retained terminal feed payload bytes per session. */
export const TERMINAL_BUFFER_MAX_BYTES = 1024 * 1024;
