import { getWebUiClientIdentity } from "./utils/clientIdentity.js";
import { getWebUiCapability } from "./utils/webUiCapability.js";

/** Client marker only — not a secret. Capability is sent separately. */
const WEB_UI_MARKER = "1";

function webUiHeaders(extra = {}) {
  const headers = {
    "X-Grok-Bridge-WebUI": WEB_UI_MARKER,
    ...extra,
  };
  const capability = getWebUiCapability();
  if (capability) {
    headers["X-Grok-Bridge-Capability"] = capability;
  }
  return headers;
}
/** Single-session close and ordinary REST calls. */
const DEFAULT_TIMEOUT_MS = 8000;
/**
 * Mirrors server `CLOSE_BATCH_DEADLINE_MS` (src/session.rs): absolute wall
 * budget for one entire `close_owner` / `close_client` call (all rounds + final
 * scan share one Instant — not per-round 7.5s stacking). Frontend abort must
 * not fire before that server budget can finish.
 */
export const CLOSE_BATCH_DEADLINE_MS = 7_500;
/**
 * Response/scheduling overhead on top of the server absolute close budget.
 * `CLOSE_GROUP_TIMEOUT_MS` must stay above one server budget (never 2× batch)
 * so a late final-scan cannot outlive the client abort.
 */
export const CLOSE_GROUP_RESPONSE_OVERHEAD_MS = 4_500;
export const CLOSE_GROUP_TIMEOUT_MS =
  CLOSE_BATCH_DEADLINE_MS + CLOSE_GROUP_RESPONSE_OVERHEAD_MS;

async function responseError(response) {
  try {
    const message = await response.text();
    const body = (message || "").trim();
    if (response.status === 403) {
      // Stable code for i18n recovery copy (Runtime restart / lost cookie).
      return body === "forbidden" || !body
        ? "capability_forbidden"
        : body;
    }
    return body || `${response.status} ${response.statusText}`;
  } catch {
    if (response.status === 403) return "capability_forbidden";
    return `${response.status} ${response.statusText || "request failed"}`;
  }
}

function withTimeout(timeoutMs = DEFAULT_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  return {
    signal: controller.signal,
    clear: () => window.clearTimeout(timer),
  };
}

async function parseJson(response) {
  try {
    return await response.json();
  } catch (error) {
    throw new Error(`invalid JSON response: ${error?.message || error}`);
  }
}

export function normalizeSessions(data) {
  if (!Array.isArray(data)) {
    throw new Error("sessions payload is not an array");
  }
  return data.filter(
    (item) =>
      item &&
      typeof item === "object" &&
      typeof item.session === "string" &&
      item.session.length > 0,
  );
}

export async function getSessions({ timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const timeout = withTimeout(timeoutMs);
  try {
    const response = await fetch("/api/sessions", {
      cache: "no-store",
      credentials: "same-origin",
      headers: webUiHeaders(),
      signal: timeout.signal,
    });
    if (!response.ok) throw new Error(await responseError(response));
    return normalizeSessions(await parseJson(response));
  } finally {
    timeout.clear();
  }
}

/**
 * Same-origin WebSocket URL for /api/events.
 * Prefer HttpOnly bootstrap cookie (sent automatically on same-origin WS).
 * Optional `c=` when JS still holds an in-memory bootstrap value (dev / tests).
 */
export function eventsWebSocketUrl(
  location = window.location,
  clientIdentity = getWebUiClientIdentity(),
  capability = getWebUiCapability(),
) {
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const params = new URLSearchParams();
  // Cookie carries auth after server bootstrap; query is optional fallback only.
  if (capability) params.set("c", capability);
  if (clientIdentity) params.set("client", clientIdentity);
  const query = params.toString();
  return `${protocol}//${location.host}/api/events${query ? `?${query}` : ""}`;
}

export function normalizeTerminalEntries(data) {
  if (data == null) return [];
  if (!Array.isArray(data)) {
    throw new Error("terminals payload is not an array");
  }
  return data
    .filter(
      (item) =>
        item &&
        typeof item === "object" &&
        typeof item.session === "string" &&
        item.session.length > 0 &&
        typeof item.data_base64 === "string",
    )
    .map((item) => ({
      session: item.session,
      reset: Boolean(item.reset),
      // Multi-frame ANSI snapshot continuation (not a PTY delta).
      reset_cont: Boolean(item.reset_cont),
      cursor: typeof item.cursor === "number" ? item.cursor : 0,
      next_cursor:
        typeof item.next_cursor === "number" ? item.next_cursor : 0,
      data_base64: item.data_base64,
      gap: Boolean(item.gap),
    }));
}

/**
 * Normalize a pushed WebSocket JSON message.
 * Contract: { type: 'sessions', sessions: SessionState[], terminals: [...] }
 */
export function normalizeEventsMessage(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("events payload is not an object");
  }
  if (data.type !== "sessions") {
    throw new Error(`unsupported events type: ${String(data.type)}`);
  }
  return {
    type: "sessions",
    sessions: normalizeSessions(data.sessions),
    terminals: normalizeTerminalEntries(data.terminals),
  };
}

const WEB_COMMAND_RESULT_TYPES = new Set([
  "terminal_subscribe_result",
  "terminal_claim_result",
  "terminal_release_result",
  "terminal_resync_result",
  "input_result",
  "resize_result",
  "client_heartbeat_result",
]);

/** Normalize a WebSocket command acknowledgement without exposing raw frames. */
export function normalizeCommandResult(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("command result is not an object");
  }
  if (!WEB_COMMAND_RESULT_TYPES.has(data.type)) {
    throw new Error(`unsupported command result type: ${String(data.type)}`);
  }
  if (typeof data.ok !== "boolean") {
    throw new Error("command result is missing ok");
  }
  return {
    type: data.type,
    ok: data.ok,
    id: typeof data.id === "string" ? data.id : null,
    session: typeof data.session === "string" ? data.session : null,
    error_code:
      typeof data.error_code === "string" ? data.error_code : null,
    error: typeof data.error === "string" ? data.error : null,
  };
}

export function normalizeVersionStatus(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) {
    throw new Error("version payload is not an object");
  }
  if (typeof data.current !== "string" || data.current.length === 0) {
    throw new Error("version payload is missing current");
  }
  const latest =
    typeof data.latest === "string" && data.latest.length > 0
      ? data.latest
      : null;
  const releaseUrl =
    typeof data.release_url === "string" && data.release_url.length > 0
      ? data.release_url
      : "https://github.com/luodaoyi/grok-bridge-rs/releases/latest";
  return {
    current: data.current,
    latest,
    update_available: Boolean(data.update_available) && latest != null,
    release_url: releaseUrl,
    checked_at_ms:
      typeof data.checked_at_ms === "number" ? data.checked_at_ms : null,
  };
}

export async function getVersionStatus({ timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const timeout = withTimeout(timeoutMs);
  try {
    const response = await fetch("/api/version", {
      cache: "no-store",
      credentials: "same-origin",
      headers: webUiHeaders(),
      signal: timeout.signal,
    });
    if (!response.ok) throw new Error(await responseError(response));
    return normalizeVersionStatus(await parseJson(response));
  } finally {
    timeout.clear();
  }
}

export async function closeSessionRequest(id, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const timeout = withTimeout(timeoutMs);
  try {
    const response = await fetch(
      `/api/sessions/${encodeURIComponent(id)}/close`,
      {
        method: "POST",
        credentials: "same-origin",
        headers: webUiHeaders(),
        signal: timeout.signal,
      },
    );
    if (!response.ok) throw new Error(await responseError(response));
  } finally {
    timeout.clear();
  }
}

export async function closeOwnerRequest(
  owner,
  { timeoutMs = CLOSE_GROUP_TIMEOUT_MS } = {},
) {
  const timeout = withTimeout(timeoutMs);
  try {
    const response = await fetch(
      `/api/owners/${encodeURIComponent(owner)}/close`,
      {
        method: "POST",
        credentials: "same-origin",
        headers: webUiHeaders(),
        signal: timeout.signal,
      },
    );
    if (!response.ok) throw new Error(await responseError(response));
    return await parseJson(response);
  } finally {
    timeout.clear();
  }
}

export async function closeClientRequest(
  clientSessionId,
  { timeoutMs = CLOSE_GROUP_TIMEOUT_MS } = {},
) {
  const timeout = withTimeout(timeoutMs);
  try {
    const response = await fetch(
      `/api/clients/${encodeURIComponent(clientSessionId)}/close`,
      {
        method: "POST",
        credentials: "same-origin",
        headers: webUiHeaders(),
        signal: timeout.signal,
      },
    );
    if (!response.ok) throw new Error(await responseError(response));
    return await parseJson(response);
  } finally {
    timeout.clear();
  }
}
