import { describe, expect, it } from "vitest";
import { createTranslator } from "../i18n/translate.js";
import {
  CAPABILITY_FORBIDDEN,
  errorMessage,
  isCapabilityForbidden,
} from "./errors.js";

describe("capability forbidden mapping", () => {
  const t = createTranslator("en");

  it("detects stable capability_forbidden codes", () => {
    expect(isCapabilityForbidden(new Error(CAPABILITY_FORBIDDEN))).toBe(true);
    expect(isCapabilityForbidden("forbidden")).toBe(true);
    expect(isCapabilityForbidden(new Error("other"))).toBe(false);
  });

  it("localizes recovery copy without embedding secrets", () => {
    const text = errorMessage(new Error(CAPABILITY_FORBIDDEN), t);
    expect(text).toMatch(/grok-bridge server ui/i);
    expect(text).toMatch(/bootstrap/i);
    expect(text).not.toMatch(/[0-9a-f]{32}/i);
  });
});
