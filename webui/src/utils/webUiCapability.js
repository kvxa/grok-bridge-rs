/**
 * Per-Runtime WebUI capability (bootstrap secret).
 *
 * Preferred path (production Runtime):
 * 1. `grok-bridge server ui` opens `/?c=<token>`.
 * 2. Server validates, sets HttpOnly SameSite=Strict cookie, 302 → `/`.
 * 3. Reload / Duplicate Tab reuse the cookie; fetch/WS send it automatically.
 *
 * In-memory value is optional (still filled when `?c=` is visible to JS, e.g.
 * vite dev). Never write the secret to localStorage. Runtime restart invalidates
 * the cookie → 403 until the user re-opens the bootstrap URL.
 */

/** @type {string | null} */
let capability = null;

const CAPABILITY_QUERY_KEYS = ["c", "capability"];

function parseCapabilityFromSearch(search) {
  if (typeof search !== "string" || !search) return null;
  const raw = search.startsWith("?") ? search.slice(1) : search;
  const params = new URLSearchParams(raw);
  for (const key of CAPABILITY_QUERY_KEYS) {
    const value = params.get(key);
    if (value && value.trim()) return value.trim();
  }
  return null;
}

function parseCapabilityFromHash(hash) {
  if (typeof hash !== "string" || !hash) return null;
  const body = hash.startsWith("#") ? hash.slice(1) : hash;
  // Support `#c=…` and `#/path?c=…`.
  const queryIndex = body.indexOf("?");
  const query = queryIndex >= 0 ? body.slice(queryIndex + 1) : body;
  return parseCapabilityFromSearch(query);
}

/**
 * Capture capability from the bootstrap URL (if still present) and scrub it
 * from the address bar. Production Runtime usually already 302-scrubbed and set
 * an HttpOnly cookie; this remains a client-side defense for dev servers.
 */
export function bootstrapWebUiCapability(
  search = typeof window !== "undefined" ? window.location.search : "",
  hash = typeof window !== "undefined" ? window.location.hash : "",
  { replaceUrl = true } = {},
) {
  const fromSearch = parseCapabilityFromSearch(search);
  const fromHash = parseCapabilityFromHash(hash);
  const found = fromSearch || fromHash;
  if (found) {
    capability = found;
  }
  if (
    replaceUrl &&
    found &&
    typeof window !== "undefined" &&
    typeof window.history?.replaceState === "function"
  ) {
    try {
      const url = new URL(window.location.href);
      for (const key of CAPABILITY_QUERY_KEYS) {
        url.searchParams.delete(key);
      }
      // Drop hash capability forms without leaving the secret in history.
      if (fromHash) {
        url.hash = "";
      }
      window.history.replaceState(
        window.history.state,
        "",
        `${url.pathname}${url.search}${url.hash}`,
      );
    } catch {
      /* ignore history scrub failures (file:// etc.) */
    }
  }
  return capability;
}

/**
 * In-memory capability for this document, if JS saw `?c=` / `#c=`.
 * May be null after cookie-only bootstrap; HTTP/WS still auth via cookie.
 */
export function getWebUiCapability() {
  return capability;
}

/** True when this document still holds an in-memory bootstrap token. */
export function hasInMemoryWebUiCapability() {
  return typeof capability === "string" && capability.length > 0;
}

/**
 * Stable error code for Runtime restart / missing cookie (403 forbidden).
 * Localize via i18n key `capability.forbidden` — never include the secret.
 */
export function capabilityRecoveryHint() {
  return "capability_forbidden";
}

/** Test helper: inject or clear capability without touching the real URL. */
export function setWebUiCapabilityForTests(value) {
  capability = typeof value === "string" && value.length > 0 ? value : null;
}
