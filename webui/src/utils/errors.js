import { createTranslator } from "../i18n/translate.js";

/** Stable code from api.js when Runtime rejects WebUI capability (403). */
export const CAPABILITY_FORBIDDEN = "capability_forbidden";

/**
 * True when the error is a WebUI capability rejection (missing/stale cookie).
 * @param {unknown} error
 */
export function isCapabilityForbidden(error) {
  if (!error) return false;
  const message =
    typeof error === "string"
      ? error
      : typeof error.message === "string"
        ? error.message
        : "";
  return (
    message === CAPABILITY_FORBIDDEN ||
    message === "forbidden" ||
    message.includes(CAPABILITY_FORBIDDEN)
  );
}

/**
 * Localize known client-side error wrappers; keep backend/detail messages as-is.
 * @param {unknown} error
 * @param {(key: string, params?: Record<string, string | number>) => string} [t]
 */
export function errorMessage(error, t = createTranslator("en")) {
  if (!error) return t("error.unknown");
  if (error.name === "AbortError") return t("error.timeout");
  if (isCapabilityForbidden(error)) return t("capability.forbidden");
  return error.message || String(error);
}
