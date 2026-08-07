import { afterEach, describe, expect, it, vi } from "vitest";
import {
  bootstrapWebUiCapability,
  capabilityRecoveryHint,
  getWebUiCapability,
  hasInMemoryWebUiCapability,
  setWebUiCapabilityForTests,
} from "./webUiCapability.js";

afterEach(() => {
  setWebUiCapabilityForTests(null);
  vi.unstubAllGlobals();
});

describe("webUiCapability", () => {
  it("bootstraps from query and clears it from the address bar", () => {
    const replaceState = vi.fn();
    vi.stubGlobal("window", {
      location: {
        href: "http://127.0.0.1:47653/?c=deadbeef&client=x",
        search: "?c=deadbeef",
        hash: "",
        pathname: "/",
      },
      history: {
        state: null,
        replaceState,
      },
    });
    // Re-read after stub — pass search explicitly.
    const value = bootstrapWebUiCapability("?c=deadbeef", "", {
      replaceUrl: true,
    });
    expect(value).toBe("deadbeef");
    expect(getWebUiCapability()).toBe("deadbeef");
    expect(replaceState).toHaveBeenCalled();
    const nextUrl = replaceState.mock.calls[0][2];
    expect(String(nextUrl)).not.toContain("deadbeef");
    expect(String(nextUrl)).not.toContain("c=");
  });

  it("accepts hash bootstrap without persisting the secret", () => {
    setWebUiCapabilityForTests(null);
    const value = bootstrapWebUiCapability("", "#c=cafebabe", {
      replaceUrl: false,
    });
    expect(value).toBe("cafebabe");
    expect(getWebUiCapability()).toBe("cafebabe");
  });

  it("does not invent a capability when the URL has none", () => {
    setWebUiCapabilityForTests(null);
    expect(bootstrapWebUiCapability("", "", { replaceUrl: false })).toBeNull();
    expect(getWebUiCapability()).toBeNull();
  });

  /**
   * Production path after server 302 + Set-Cookie: address bar is scrubbed
   * before JS runs. Reload / Duplicate Tab start a new Document with no module
   * token — HTTP/WS auth is cookie-only (see server cookie tests + api.test).
   */
  it("reload and second-tab documents have no in-memory token after scrub", () => {
    // First document: saw ?c= (dev path or race before server redirect).
    bootstrapWebUiCapability("?c=deadbeefcafebabe", "", { replaceUrl: false });
    expect(hasInMemoryWebUiCapability()).toBe(true);

    // New Document (reload / Duplicate Tab / paste scrubbed URL): module reset.
    setWebUiCapabilityForTests(null);
    expect(
      bootstrapWebUiCapability("", "", { replaceUrl: false }),
    ).toBeNull();
    expect(getWebUiCapability()).toBeNull();
    expect(hasInMemoryWebUiCapability()).toBe(false);
    // Recovery code is stable for i18n; never embeds the secret.
    expect(capabilityRecoveryHint()).toBe("capability_forbidden");
    expect(capabilityRecoveryHint()).not.toContain("deadbeef");
  });
});
