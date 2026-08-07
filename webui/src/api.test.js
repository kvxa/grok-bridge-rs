import { afterEach, describe, expect, it, vi } from "vitest";
import {
  CLOSE_BATCH_DEADLINE_MS,
  CLOSE_GROUP_RESPONSE_OVERHEAD_MS,
  CLOSE_GROUP_TIMEOUT_MS,
  eventsWebSocketUrl,
  getSessions,
  getVersionStatus,
  normalizeEventsMessage,
  normalizeSessions,
  normalizeTerminalEntries,
  normalizeVersionStatus,
} from "./api.js";
import {
  getWebUiCapability,
  setWebUiCapabilityForTests,
} from "./utils/webUiCapability.js";

function jsonResponse(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("close group timeout budget", () => {
  it("frontend close budget covers one server absolute deadline plus overhead", () => {
    // Server close_owner/close_client share a single CLOSE_BATCH_DEADLINE_MS
    // Instant (not 2× stacked rounds). Client must exceed that once + margin.
    expect(CLOSE_BATCH_DEADLINE_MS).toBe(7_500);
    expect(CLOSE_GROUP_RESPONSE_OVERHEAD_MS).toBeGreaterThanOrEqual(2_000);
    expect(CLOSE_GROUP_TIMEOUT_MS).toBe(
      CLOSE_BATCH_DEADLINE_MS + CLOSE_GROUP_RESPONSE_OVERHEAD_MS,
    );
    expect(CLOSE_GROUP_TIMEOUT_MS).toBeGreaterThan(
      CLOSE_BATCH_DEADLINE_MS + 2_000,
    );
    // Must not be sized as if two full server budgets were required.
    expect(CLOSE_GROUP_TIMEOUT_MS).toBeLessThan(CLOSE_BATCH_DEADLINE_MS * 2);
  });
});

describe("normalizeSessions", () => {
  it("rejects non-array payloads", () => {
    expect(() => normalizeSessions({ sessions: [] })).toThrow(
      /not an array/i,
    );
  });

  it("drops invalid entries and keeps valid sessions", () => {
    expect(
      normalizeSessions([
        { session: "ok-1", phase: "running" },
        null,
        { session: "" },
        { phase: "idle" },
        { session: "ok-2" },
      ]),
    ).toEqual([
      { session: "ok-1", phase: "running" },
      { session: "ok-2" },
    ]);
  });
});

describe("getSessions", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns normalized sessions", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        jsonResponse([{ session: "a" }, { session: "" }, null]),
      ),
    );
    await expect(getSessions()).resolves.toEqual([{ session: "a" }]);
  });

  it("sends same-origin credentials so bootstrap cookie auth works after reload", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse([{ session: "a" }]));
    vi.stubGlobal("fetch", fetchMock);
    await getSessions();
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/sessions",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });

  it("maps 403 forbidden to capability_forbidden for recovery UX", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("forbidden", {
          status: 403,
          statusText: "Forbidden",
          headers: { "Content-Type": "text/plain" },
        }),
      ),
    );
    await expect(getSessions()).rejects.toThrow("capability_forbidden");
  });

  /**
   * Reload / Duplicate Tab: new Document has no in-memory token; browser still
   * sends the HttpOnly bootstrap cookie via credentials: same-origin.
   */
  it("cookie-only document (no in-memory token) still uses credentials and omits capability header", async () => {
    const previous = getWebUiCapability();
    setWebUiCapabilityForTests(null);
    try {
      const fetchMock = vi.fn().mockResolvedValue(jsonResponse([{ session: "a" }]));
      vi.stubGlobal("fetch", fetchMock);
      await expect(getSessions()).resolves.toEqual([{ session: "a" }]);
      const init = fetchMock.mock.calls[0][1];
      expect(init.credentials).toBe("same-origin");
      expect(init.headers["X-Grok-Bridge-Capability"]).toBeUndefined();
    } finally {
      setWebUiCapabilityForTests(previous);
    }
  });

  it("throws on invalid JSON without leaving hang state", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("not-json", {
          status: 200,
          headers: { "Content-Type": "text/plain" },
        }),
      ),
    );
    await expect(getSessions()).rejects.toThrow(/invalid JSON/i);
  });

  it("throws on non-array JSON", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(jsonResponse({ error: "boom" })),
    );
    await expect(getSessions()).rejects.toThrow(/not an array/i);
  });
});

describe("normalizeVersionStatus", () => {
  it("normalizes update payloads", () => {
    expect(
      normalizeVersionStatus({
        current: "0.6.1",
        latest: "0.6.2",
        update_available: true,
        release_url:
          "https://github.com/luodaoyi/grok-bridge-rs/releases/tag/v0.6.2",
        checked_at_ms: 42,
      }),
    ).toEqual({
      current: "0.6.1",
      latest: "0.6.2",
      update_available: true,
      release_url:
        "https://github.com/luodaoyi/grok-bridge-rs/releases/tag/v0.6.2",
      checked_at_ms: 42,
    });
  });

  it("rejects invalid payloads", () => {
    expect(() => normalizeVersionStatus([])).toThrow(/not an object/i);
    expect(() => normalizeVersionStatus({})).toThrow(/missing current/i);
  });
});

describe("getVersionStatus", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns normalized version status", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        jsonResponse({
          current: "0.6.1",
          latest: "0.6.2",
          update_available: true,
          release_url:
            "https://github.com/luodaoyi/grok-bridge-rs/releases/tag/v0.6.2",
        }),
      ),
    );
    await expect(getVersionStatus()).resolves.toMatchObject({
      current: "0.6.1",
      latest: "0.6.2",
      update_available: true,
    });
  });
});

describe("events stream helpers", () => {
  it("builds same-origin ws/wss events URL with capability query", () => {
    expect(
      eventsWebSocketUrl(
        { protocol: "http:", host: "127.0.0.1:47653" },
        "client-test-id",
        "cap-token",
      ),
    ).toBe(
      "ws://127.0.0.1:47653/api/events?c=cap-token&client=client-test-id",
    );
    expect(
      eventsWebSocketUrl(
        { protocol: "https:", host: "localhost:8443" },
        "client-test-id",
        "cap-token",
      ),
    ).toBe(
      "wss://localhost:8443/api/events?c=cap-token&client=client-test-id",
    );
  });

  it("omits c= for cookie-only second tab / reload (browser sends cookie)", () => {
    // Second document after scrub: no module token; cookie is the auth path.
    expect(
      eventsWebSocketUrl(
        { protocol: "http:", host: "127.0.0.1:47653" },
        "client-test-id",
        null,
      ),
    ).toBe("ws://127.0.0.1:47653/api/events?client=client-test-id");
  });

  it("normalizes sessions event frames and terminal entries", () => {
    const message = normalizeEventsMessage({
      type: "sessions",
      sessions: [{ session: "a" }, { session: "" }, null],
      terminals: [
        {
          session: "a",
          reset: true,
          cursor: 0,
          next_cursor: 3,
          data_base64: "YWI=",
        },
        { session: "", data_base64: "x" },
        null,
      ],
    });
    expect(message).toEqual({
      type: "sessions",
      sessions: [{ session: "a" }],
      terminals: [
        {
          session: "a",
          reset: true,
          reset_cont: false,
          gap: false,
          cursor: 0,
          next_cursor: 3,
          data_base64: "YWI=",
        },
      ],
    });
  });

  it("rejects invalid event frames", () => {
    expect(() => normalizeEventsMessage(null)).toThrow(/not an object/i);
    expect(() => normalizeEventsMessage({ type: "other" })).toThrow(
      /unsupported events type/i,
    );
    expect(() => normalizeTerminalEntries({})).toThrow(/not an array/i);
  });
});
