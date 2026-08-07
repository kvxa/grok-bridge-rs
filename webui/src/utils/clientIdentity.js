/**
 * Per-Document WebUI client identity and request sequencing.
 *
 * Identity lives in module memory for the Document lifetime so:
 * - WebSocket reconnects within the same page reuse the same identity
 * - full page reload allocates a new identity (server cache TTL is fine)
 * - duplicated tabs / window.open get a distinct Document and thus a new
 *   identity (sessionStorage would be copied and collide)
 *
 * Request IDs are `${identityPrefix}-${seq}` with a module-local monotonic seq
 * reset only when the Document reloads (new module instance).
 */

let documentIdentity = null;
let documentRequestSeq = 0;

function randomId() {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID().replace(/-/g, "");
  }
  let out = "";
  for (let i = 0; i < 32; i += 1) {
    out += Math.floor(Math.random() * 16).toString(16);
  }
  return out;
}

/**
 * Stable identity for this Document. Format: [a-zA-Z0-9_-]{8,128}.
 */
export function getWebUiClientIdentity() {
  if (
    typeof documentIdentity === "string" &&
    documentIdentity.length >= 8 &&
    documentIdentity.length <= 128
  ) {
    return documentIdentity;
  }
  documentIdentity = `webui${randomId()}`;
  documentRequestSeq = 0;
  return documentIdentity;
}

/** Last issued sequence for this Document (0 if none). */
export function getWebUiRequestSeq() {
  return documentRequestSeq;
}

/** Next monotonic request sequence for this Document. */
export function nextWebUiRequestSeq() {
  getWebUiClientIdentity();
  documentRequestSeq += 1;
  return documentRequestSeq;
}

/** Test helper: reset module state (simulates a new Document). */
export function resetWebUiClientIdentityForTests() {
  documentIdentity = null;
  documentRequestSeq = 0;
}

/**
 * Test helper: simulate a second Document (new module bindings) by allocating
 * a fresh identity without clearing sessionStorage-like globals.
 */
export function allocateNewDocumentIdentityForTests() {
  documentIdentity = null;
  documentRequestSeq = 0;
  return getWebUiClientIdentity();
}
