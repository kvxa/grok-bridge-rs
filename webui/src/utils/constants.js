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
/** Max total payload bytes (base64) of pending commands. */
export const PENDING_COMMANDS_MAX_BYTES = 256 * 1024;
/**
 * Max base64 length of one terminal_input (4 × ceil(64 KiB / 3)). Fast path:
 * anything longer is definitely over the 64 KiB raw bound. Base64 length alone
 * cannot separate 65536/65537/65538 raw bytes (all encode to 87384 chars), so
 * sendTerminalInput decodes payloads at or below this bound for an exact check
 * against MAX_INPUT_RAW_BYTES before any byte is sent.
 */
export const MAX_INPUT_BASE64_LENGTH = 87384;
/** Exact max raw bytes of one terminal_input; matches Runtime write_raw. */
export const MAX_INPUT_RAW_BYTES = 64 * 1024;
/** Max retained terminal feed entries per session (no live listener). */
export const TERMINAL_BUFFER_MAX_ENTRIES = 128;
/** Max retained terminal feed payload bytes per session. */
export const TERMINAL_BUFFER_MAX_BYTES = 1024 * 1024;
