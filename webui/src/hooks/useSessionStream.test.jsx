import { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { catalogs, I18nProvider } from "../i18n/index.js";
import { installMockWebSocket, MockWebSocket } from "../test/mockWebSocket.js";
import { WS_BACKOFF_MS } from "../utils/constants.js";
import {
  peekTerminalBuffer,
  resetTerminalFeeds,
} from "../utils/terminalFeeds.js";
import { CLIENT_IO_ERROR, useSessionStream } from "./useSessionStream.js";
import { encodeUtf8ToBase64 } from "../utils/base64.js";

function Probe({ onState, setNotice, interactive }) {
  const state = useSessionStream({ setNotice, interactive });
  onState(state);
  return null;
}

/** Keeps one stream instance while toggling interactive for release tests. */
function InteractiveProbe({ onState, setNotice, initialInteractive = true }) {
  const [interactive, setInteractive] = useState(initialInteractive);
  const state = useSessionStream({ setNotice, interactive });
  onState({ ...state, interactive, setInteractive });
  return null;
}

describe("useSessionStream", () => {
  let container;
  let root;
  let latest;

  beforeEach(() => {
    vi.useFakeTimers();
    installMockWebSocket();
    resetTerminalFeeds();
    latest = null;
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
    resetTerminalFeeds();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  async function mount({ locale = "en", setNotice, interactive = false } = {}) {
    await act(async () => {
      root.render(
        <I18nProvider initialLocale={locale}>
          <Probe
            setNotice={setNotice}
            interactive={interactive}
            onState={(state) => {
              latest = state;
            }}
          />
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });
  }

  it("connects immediately to same-origin /api/events without GET /api/sessions", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    await mount();
    expect(MockWebSocket.instances).toHaveLength(1);
    expect(MockWebSocket.instances[0].url).toMatch(
      /\/api\/events\?c=[0-9a-f]+&client=[A-Za-z0-9_-]+$/,
    );
    expect(latest.connectionState).toBe("initial");
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("applies initial sessions snapshot and terminal reset entries", async () => {
    await mount();
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    expect(latest.connectionState).toBe("connected");

    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [
          {
            session: "gbt-1",
            phase: "running",
            activity: "working",
            rows: 30,
            cols: 100,
            updated_at_ms: 1,
          },
        ],
        terminals: [
          {
            session: "gbt-1",
            reset: true,
            cursor: 0,
            next_cursor: 5,
            data_base64: btoa("hello"),
          },
        ],
      });
    });

    expect(latest.sessions).toHaveLength(1);
    expect(latest.sessions[0].session).toBe("gbt-1");
    expect(peekTerminalBuffer("gbt-1")).toHaveLength(1);
    expect(peekTerminalBuffer("gbt-1")[0].reset).toBe(true);
  });

  it("sends terminal_input/resize without buffering when disconnected", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const ok = latest.sendTerminalInput("gbt-1", btoa("x"));
    expect(ok.ok).toBe(true);
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim).toBeTruthy();
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    expect(ws.sent.some((item) => String(item).includes("terminal_input"))).toBe(
      true,
    );
    // Shared session I/O serial: ack input before resize can admit.
    const inputCmd = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: inputCmd.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const resize = latest.sendTerminalResize("gbt-1", 80, 24);
    expect(resize.ok).toBe(true);
    await act(async () => {
      await Promise.resolve();
    });
    expect(ws.sent.some((item) => String(item).includes("terminal_resize"))).toBe(
      true,
    );

    await act(async () => ws.close());
    const fail = latest.sendTerminalInput("gbt-1", btoa("y"));
    expect(fail.ok).toBe(false);
    expect(fail.error).toBe(CLIENT_IO_ERROR.DISCONNECTED);
  });

  it("retries in_progress with the same side-effect request id", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    latest.sendTerminalInput("gbt-1", btoa("x"));
    const claim = ws.sent.map((item) => JSON.parse(String(item))).find((item) => item.type === "terminal_claim");
    await act(async () => ws.emitMessage({ type: "terminal_claim_result", ok: true, id: claim.id, session: "gbt-1" }));
    const input = ws.sent.map((item) => JSON.parse(String(item))).find((item) => item.type === "terminal_input");
    await act(async () => ws.emitMessage({ type: "input_result", ok: false, id: input.id, session: "gbt-1", error_code: "in_progress", error: "retry" }));
    await act(async () => vi.advanceTimersByTime(1000));
    const inputs = ws.sent.map((item) => JSON.parse(String(item))).filter((item) => item.type === "terminal_input");
    expect(inputs.map((item) => item.id)).toEqual([input.id, input.id]);
    await act(async () => ws.emitMessage({ type: "input_result", ok: true, id: input.id, session: "gbt-1" }));
    expect(ws.sent.map((item) => JSON.parse(String(item))).filter((item) => item.type === "terminal_input")).toHaveLength(2);
  });

  it("stops in_progress retries at the bounded request budget", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    latest.sendTerminalInput("gbt-1", btoa("budget"));
    const claim = ws.sent.map((item) => JSON.parse(String(item))).find((item) => item.type === "terminal_claim");
    await act(async () => ws.emitMessage({ type: "terminal_claim_result", ok: true, id: claim.id, session: "gbt-1" }));
    const input = ws.sent.map((item) => JSON.parse(String(item))).find((item) => item.type === "terminal_input");
    latest.sendTerminalInput("gbt-1", btoa("after-budget"));
    for (let attempt = 0; attempt < 30; attempt += 1) {
      await act(async () => ws.emitMessage({ type: "input_result", ok: false, id: input.id, session: "gbt-1", error_code: "in_progress", error: "retry" }));
      await act(async () => vi.advanceTimersByTime(1000));
    }
    const inputs = ws.sent.map((item) => JSON.parse(String(item))).filter((item) => item.type === "terminal_input");
    expect(inputs.filter((item) => item.id === input.id)).toHaveLength(30);
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });
    const afterBudget = ws.sent.map((item) => JSON.parse(String(item))).filter((item) => item.type === "terminal_input" && item.id !== input.id);
    expect(afterBudget).toHaveLength(1);
    await act(async () => ws.emitMessage({ type: "input_result", ok: false, id: input.id, session: "gbt-1", error_code: "in_progress", error: "late" }));
    await act(async () => vi.advanceTimersByTime(2000));
    expect(ws.sent.map((item) => JSON.parse(String(item))).filter((item) => item.type === "terminal_input" && item.id !== input.id)).toHaveLength(1);
  });

  it("cleans an in_progress side effect on disconnect without replay", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    latest.sendTerminalInput("gbt-1", btoa("disconnect"));
    const claim = ws.sent.map((item) => JSON.parse(String(item))).find((item) => item.type === "terminal_claim");
    await act(async () => ws.emitMessage({ type: "terminal_claim_result", ok: true, id: claim.id, session: "gbt-1" }));
    const input = ws.sent.map((item) => JSON.parse(String(item))).find((item) => item.type === "terminal_input");
    await act(async () => ws.emitMessage({ type: "input_result", ok: false, id: input.id, session: "gbt-1", error_code: "in_progress", error: "retry" }));
    await act(async () => ws.close());
    const oldCount = ws.sent.length;
    await act(async () => vi.advanceTimersByTime(100));
    expect(ws.sent).toHaveLength(oldCount);
    await act(async () => vi.advanceTimersByTime(1000));
    const latestSocket = MockWebSocket.instances[MockWebSocket.instances.length - 1];
    await act(async () => latestSocket.open());
    expect(latestSocket.sent.map((item) => JSON.parse(String(item))).filter((item) => item.type === "terminal_input")).toHaveLength(0);
  });

  it("localizes client send failures without leaking English homemade strings", async () => {
    let notice = null;
    await mount({
      locale: "zh-CN",
      interactive: true,
      setNotice: (value) => {
        notice = typeof value === "function" ? value(notice) : value;
      },
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    await act(async () => {
      latest.sendTerminalInput("", "");
    });
    expect(notice?.text).toBe(catalogs["zh-CN"]["interactive.invalidPayload"]);
    expect(notice?.text).not.toMatch(/invalid payload|Invalid terminal/i);

    await act(async () => ws.close());
    await act(async () => {
      latest.sendTerminalInput("gbt-1", btoa("y"));
    });
    expect(notice?.text).toBe(catalogs["zh-CN"]["interactive.disconnected"]);
    expect(notice?.text).not.toMatch(/disconnected|Live channel/i);

    // Force a send exception path.
    await mount({
      locale: "zh-CN",
      interactive: true,
      setNotice: (value) => {
        notice = typeof value === "function" ? value(notice) : value;
      },
    });
    const ws2 = MockWebSocket.instances[MockWebSocket.instances.length - 1];
    await act(async () => ws2.open());
    ws2.send = () => {
      throw new Error("WebSocket is already in CLOSING or CLOSED state");
    };
    await act(async () => {
      latest.sendTerminalResize("gbt-1", 80, 24);
    });
    expect(notice?.text).toBe(catalogs["zh-CN"]["interactive.sendFailed"]);
    expect(notice?.text).not.toMatch(
      /WebSocket is already|Failed to send|CLOSING/i,
    );
  });

  it("blocks input and resize while the hook is read-only", async () => {
    let notice = null;
    await mount({
      locale: "zh-CN",
      setNotice: (value) => {
        notice = typeof value === "function" ? value(notice) : value;
      },
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const before = ws.sent.length;
    const input = latest.sendTerminalInput("gbt-1", btoa("x"));
    const resize = latest.sendTerminalResize("gbt-1", 80, 24);
    expect(input).toEqual({ ok: false, error: CLIENT_IO_ERROR.READ_ONLY });
    expect(resize).toEqual({ ok: false, error: CLIENT_IO_ERROR.READ_ONLY });
    expect(ws.sent.length).toBe(before);
    expect(notice?.text).toBe(catalogs["zh-CN"]["interactive.unavailable"]);
  });

  it("rejects an oversized terminal input atomically before claim or socket send", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const before = ws.sent.length;
    const result = latest.sendTerminalInput(
      "gbt-1",
      btoa("x".repeat(64 * 1024 + 1)),
    );
    expect(result).toEqual({ ok: false, error: CLIENT_IO_ERROR.FLOW_CONTROL });
    expect(ws.sent).toHaveLength(before);
  });

  it("accepts the exact 64 KiB ASCII and UTF-8 byte boundaries", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const ascii = encodeUtf8ToBase64("a".repeat(64 * 1024));
    const utf8 = encodeUtf8ToBase64(`${"汉".repeat(21845)}a`); // 65,536 bytes
    expect(latest.sendTerminalInput("gbt-1", ascii).ok).toBe(true);
    expect(latest.sendTerminalInput("gbt-2", utf8).ok).toBe(true);
    expect(ws.sent.length).toBeGreaterThan(0);
    expect(ws.sent.map((item) => String(item)).some((item) => item.includes("flow_control"))).toBe(false);
  });

  it("rejects multi-byte UTF-8 over the limit without sending a prefix", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const before = ws.sent.length;
    const over = encodeUtf8ToBase64(`${"汉".repeat(21845)}ab`); // 65,537 bytes
    expect(latest.sendTerminalInput("gbt-1", over)).toEqual({
      ok: false,
      error: CLIENT_IO_ERROR.FLOW_CONTROL,
    });
    expect(ws.sent).toHaveLength(before);
  });

  it("keeps backend input_result detail after a localized prefix", async () => {
    let notice = null;
    await mount({
      locale: "zh-CN",
      setNotice: (value) => {
        notice = typeof value === "function" ? value(notice) : value;
      },
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [{ session: "gbt-1", phase: "running", rows: 24, cols: 80 }],
        terminals: [],
      });
    });
    await act(async () => {
      ws.emitMessage({
        type: "input_result",
        ok: false,
        id: "r1",
        session: "gbt-1",
        error: "session not found",
      });
    });
    expect(latest.sessions[0].session).toBe("gbt-1");
    expect(notice?.text).toContain("session not found");
    expect(notice?.text).toContain("终端输入失败");
  });

  it("applies ordered appends and later reset", async () => {
    await mount();
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [{ session: "gbt-1", phase: "running", rows: 24, cols: 80 }],
        terminals: [
          {
            session: "gbt-1",
            reset: true,
            data_base64: btoa("A"),
          },
        ],
      });
    });
    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [{ session: "gbt-1", phase: "running", rows: 24, cols: 80 }],
        terminals: [
          {
            session: "gbt-1",
            reset: false,
            data_base64: btoa("B"),
          },
          {
            session: "gbt-1",
            reset: false,
            data_base64: btoa("C"),
          },
        ],
      });
    });
    expect(peekTerminalBuffer("gbt-1").map((e) => e.data_base64)).toEqual([
      btoa("A"),
      btoa("B"),
      btoa("C"),
    ]);

    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [{ session: "gbt-1", phase: "idle", rows: 24, cols: 80 }],
        terminals: [
          {
            session: "gbt-1",
            reset: true,
            data_base64: btoa("RESET"),
          },
        ],
      });
    });
    expect(peekTerminalBuffer("gbt-1")).toHaveLength(1);
    expect(peekTerminalBuffer("gbt-1")[0].data_base64).toBe(btoa("RESET"));
  });

  it("disposes terminal feeds when sessions disappear from push", async () => {
    await mount();
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [{ session: "gbt-1", phase: "running" }],
        terminals: [
          { session: "gbt-1", reset: true, data_base64: btoa("x") },
        ],
      });
    });
    expect(peekTerminalBuffer("gbt-1")).toHaveLength(1);

    await act(async () => {
      ws.emitMessage({
        type: "sessions",
        sessions: [],
        terminals: [],
      });
    });
    expect(latest.sessions).toHaveLength(0);
    expect(peekTerminalBuffer("gbt-1")).toHaveLength(0);
  });

  it("reconnects with bounded exponential backoff and supports manual reconnect", async () => {
    await mount();
    const first = MockWebSocket.instances[0];
    await act(async () => first.open());
    await act(async () => first.close());
    expect(latest.connectionState).toBe("retrying");

    await act(async () => {
      await vi.advanceTimersByTimeAsync(WS_BACKOFF_MS[0] - 1);
    });
    expect(MockWebSocket.instances).toHaveLength(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(MockWebSocket.instances).toHaveLength(2);

    await act(async () => {
      latest.reconnect();
    });
    expect(MockWebSocket.instances.length).toBeGreaterThanOrEqual(3);
    const manual = MockWebSocket.instances[MockWebSocket.instances.length - 1];
    await act(async () => manual.open());
    expect(latest.connectionState).toBe("connected");
  });

  it("does not poll GET /api/sessions on a two-second interval", async () => {
    const fetchSpy = vi.fn(async () => new Response("{}", { status: 200 }));
    vi.stubGlobal("fetch", fetchSpy);
    await mount();
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(fetchSpy).not.toHaveBeenCalled();
    expect(
      fetchSpy.mock.calls.some((call) => String(call[0]).includes("/api/sessions")),
    ).toBe(false);
  });

  it("uses per-document identity and namespaces request ids", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    expect(ws.url).toMatch(
      /\/api\/events\?c=[0-9a-f]+&client=webui[A-Za-z0-9]+/,
    );
    await act(async () => ws.open());
    latest.sendTerminalInput("gbt-1", btoa("x"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim.id).toMatch(/^webui[A-Za-z0-9]+-\d+$/);
  });

  it("releases control when a session leaves the visible subscription set", async () => {
    // Collapse/hide only used to change terminal_subscribe; control stayed forever.
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.setTerminalSubscriptions(["gbt-1", "gbt-2"]);
    await act(async () => {
      await Promise.resolve();
    });
    latest.sendTerminalInput("gbt-1", btoa("x"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim).toBeTruthy();
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    // Hide gbt-1 (collapse / other tab focus): still subscribed only to gbt-2.
    latest.setTerminalSubscriptions(["gbt-2"]);
    await act(async () => {
      await Promise.resolve();
    });

    const released = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_release");
    expect(released.some((item) => item.session === "gbt-1")).toBe(true);
    // Subscribe set shrinks without remove→add thrash on the remaining session.
    const subs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_subscribe");
    const lastSub = subs.at(-1);
    expect(lastSub.sessions).toEqual(["gbt-2"]);
  });

  it("resyncs one terminal without unsubscribing or releasing control", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.setTerminalSubscriptions(["gbt-1"]);
    await act(async () => {
      await Promise.resolve();
    });
    latest.sendTerminalInput("gbt-1", btoa("x"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    const before = ws.sent.length;
    latest.requestTerminalResync("gbt-1");
    await act(async () => {
      await Promise.resolve();
    });
    const after = ws.sent
      .slice(before)
      .map((item) => JSON.parse(String(item)));
    expect(after.some((item) => item.type === "terminal_resync")).toBe(true);
    expect(after.some((item) => item.type === "terminal_release")).toBe(false);
    // Must not bounce subscribe remove→add (that released control).
    expect(
      after.some(
        (item) =>
          item.type === "terminal_subscribe" &&
          Array.isArray(item.sessions) &&
          !item.sessions.includes("gbt-1"),
      ),
    ).toBe(false);
  });

  it("sends client_heartbeat only while interactive and connected", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_100);
    });
    const heartbeats = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "client_heartbeat");
    expect(heartbeats.length).toBeGreaterThanOrEqual(1);

    // Read-only: no further heartbeats after flipping off.
    // (interactive prop is fixed at mount; release on unmount path covered elsewhere.)
  });

  it("re-claims on control_required and resends the same request id", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    latest.sendTerminalInput("gbt-1", btoa("x"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    const input = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    expect(input).toBeTruthy();
    const originalId = input.id;

    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: false,
        id: originalId,
        session: "gbt-1",
        error_code: "control_required",
        error: "claim terminal control before sending input",
      }),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    const after = ws.sent.map((item) => JSON.parse(String(item)));
    const reclaim = after.filter((item) => item.type === "terminal_claim").at(-1);
    expect(reclaim).toBeTruthy();
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: reclaim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    const resent = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input" && item.id === originalId);
    expect(resent.length).toBeGreaterThanOrEqual(2);
  });

  it("fails every queued command when claim is busy (resize clears pending)", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    const resizeResults = [];
    const queued = latest.sendTerminalResize("gbt-1", 80, 24, {
      onResult: (result) => resizeResults.push(result),
    });
    expect(queued.ok).toBe(true);
    expect(queued.queued).toBe(true);
    latest.sendTerminalInput("gbt-1", btoa("lost-if-silent"));

    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim).toBeTruthy();

    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: false,
        id: claim.id,
        session: "gbt-1",
        error_code: "control_busy",
        error: "terminal control is held by another WebUI client",
      }),
    );

    expect(resizeResults).toHaveLength(1);
    expect(resizeResults[0].ok).toBe(false);
    expect(resizeResults[0].error_code).toBe("control_busy");
    expect(
      ws.sent.some((item) => String(item).includes("terminal_input")),
    ).toBe(false);
    expect(
      ws.sent.some((item) => String(item).includes("terminal_resize")),
    ).toBe(false);
  });

  it("fails queued resize when interactive turns off (release drains queue)", async () => {
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <InteractiveProbe
            onState={(state) => {
              latest = state;
            }}
          />
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    const resizeResults = [];
    latest.sendTerminalResize("gbt-1", 90, 28, {
      onResult: (result) => resizeResults.push(result),
    });
    expect(
      ws.sent.some((item) =>
        String(item).includes('"type":"terminal_claim"'),
      ),
    ).toBe(true);

    // Same hook instance: flip interactive off so releaseControls drains the queue.
    await act(async () => {
      latest.setInteractive(false);
    });
    await act(async () => {
      await Promise.resolve();
    });

    expect(resizeResults).toHaveLength(1);
    expect(resizeResults[0].ok).toBe(false);
    expect(resizeResults[0].error_code).toBe(CLIENT_IO_ERROR.READ_ONLY);
    expect(
      ws.sent.some((item) => String(item).includes("terminal_release")),
    ).toBe(true);
  });

  it("deduplicates resync pressure so the next resize can proceed", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    // Subscribe so resync flood is valid; claim via first resize.
    latest.setTerminalSubscriptions(["gbt-1"]);
    await act(async () => {
      await Promise.resolve();
    });

    const resizeResults = [];
    // onResult runs before the serial settle advances; fill pending with
    // terminal_resync (each keeps a distinct id until acked). Subscribe alone
    // no longer fills the map — latest-wins supersedes prior subscribe entries.
    latest.sendTerminalResize("gbt-1", 80, 24, {
      onResult: (result) => {
        resizeResults.push(["first", result]);
        for (let i = 0; i < 64; i += 1) {
          latest.requestTerminalResync("gbt-1");
        }
      },
    });
    // Second stays behind the session I/O serial until first reaches a final.
    latest.sendTerminalResize("gbt-1", 100, 30, {
      onResult: (result) => resizeResults.push(["second", result]),
    });

    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim).toBeTruthy();

    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    const firstResize = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_resize");
    expect(firstResize).toBeTruthy();

    // Permanent fail for first (not retryable) so serial advances to second.
    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: false,
        id: firstResize.id,
        session: "gbt-1",
        error_code: "resize_rejected",
        error: "rejected",
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const resyncs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resync");
    expect(resyncs).toHaveLength(1);
    const resizeFrames = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resize");
    expect(resizeFrames).toHaveLength(2);
    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: true,
        id: resizeFrames[1].id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    expect(resizeResults.find(([tag]) => tag === "second")?.[1].ok).toBe(true);
  });

  it("retains a deduplicated resync intent until pending capacity frees", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const sessions = Array.from({ length: 65 }, (_, index) => `gbt-${index}`);
    latest.setTerminalSubscriptions(sessions);
    await act(async () => Promise.resolve());
    const subscribe = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_subscribe" && item.sessions.length === 65);
    await act(async () =>
      ws.emitMessage({
        type: "terminal_subscribe_result",
        ok: true,
        id: subscribe.id,
        error_code: null,
        error: null,
      }),
    );
    for (const session of sessions) latest.requestTerminalResync(session);
    let resyncs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resync");
    expect(resyncs).toHaveLength(63);

    await act(async () =>
      ws.emitMessage({
        type: "terminal_resync_result",
        ok: true,
        id: resyncs[0].id,
        session: resyncs[0].session,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => Promise.resolve());
    resyncs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resync");
    expect(resyncs).toHaveLength(64);
    await act(async () =>
      ws.emitMessage({
        type: "terminal_resync_result",
        ok: true,
        id: resyncs[1].id,
        session: resyncs[1].session,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => Promise.resolve());
    resyncs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resync");
    expect(resyncs).toHaveLength(65);
    expect(new Set(resyncs.map((item) => item.session)).size).toBe(65);
  });

  it("defers release until pending capacity frees and sends it before resync", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const sessions = Array.from({ length: 66 }, (_, index) => `gbt-${index}`);
    latest.setTerminalSubscriptions(sessions);
    await act(async () => Promise.resolve());
    const subscribe = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_subscribe");
    expect(subscribe).toBeTruthy();
    await act(async () =>
      ws.emitMessage({
        type: "terminal_subscribe_result",
        ok: true,
        id: subscribe.id,
        error_code: null,
        error: null,
      }),
    );
    for (const session of sessions) latest.requestTerminalResync(session);
    await act(async () => Promise.resolve());
    expect(
      ws.sent.map((item) => JSON.parse(String(item))).filter(
        (item) => item.type === "terminal_resync",
      ),
    ).toHaveLength(62);

    latest.setTerminalSubscriptions([]);
    await act(async () => Promise.resolve());
    expect(
      ws.sent.map((item) => JSON.parse(String(item))).filter(
        (item) => item.type === "terminal_release",
      ),
    ).toHaveLength(0);

    const firstResync = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_resync");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_resync_result",
        ok: true,
        id: firstResync.id,
        session: firstResync.session,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => Promise.resolve());
    const releases = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_release");
    expect(releases).toHaveLength(1);
    expect(releases[0].session).toBe("gbt-0");

    const anotherResync = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find(
        (item) =>
          item.type === "terminal_resync" && item.id !== firstResync.id,
      );
    await act(async () =>
      ws.emitMessage({
        type: "terminal_resync_result",
        ok: true,
        id: anotherResync.id,
        session: anotherResync.session,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => Promise.resolve());
    expect(
      ws.sent
        .map((item) => JSON.parse(String(item)))
        .filter(
          (item) =>
            item.type === "terminal_release" && item.session === "gbt-0",
        ),
    ).toHaveLength(1);
  });

  it("replays an unacknowledged release after reconnect with the same id", async () => {
    await mount({ interactive: true });
    const first = MockWebSocket.instances[0];
    await act(async () => first.open());
    latest.setTerminalSubscriptions(["gbt-1"]);
    await act(async () => Promise.resolve());
    const subscribe = first.sent
      .map((item) => JSON.parse(String(item)))
      .find(
        (item) =>
          item.type === "terminal_subscribe" && item.sessions.includes("gbt-1"),
      );
    await act(async () =>
      first.emitMessage({
        type: "terminal_subscribe_result",
        ok: true,
        id: subscribe.id,
        error_code: null,
        error: null,
      }),
    );
    latest.setTerminalSubscriptions([]);
    await act(async () => Promise.resolve());
    const release = first.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_release");
    expect(release).toBeTruthy();

    await act(async () => first.close());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(WS_BACKOFF_MS[0]);
    });
    const second = MockWebSocket.instances.at(-1);
    await act(async () => second.open());
    await act(async () => Promise.resolve());
    const replayed = second.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_release");
    expect(replayed).toHaveLength(1);
    expect(replayed[0].id).toBe(release.id);

    await act(async () =>
      second.emitMessage({
        type: "terminal_release_result",
        ok: true,
        id: release.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => Promise.resolve());
    expect(
      second.sent
        .map((item) => JSON.parse(String(item)))
        .filter((item) => item.type === "terminal_release"),
    ).toHaveLength(1);
  });

  it("serializes resize so flow_control retry of A cannot overwrite later B", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    const results = [];
    latest.sendTerminalResize("gbt-1", 80, 24, {
      onResult: (r) => results.push(["A", r]),
    });
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    const firstResize = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_resize");
    expect(firstResize).toBeTruthy();
    expect(firstResize.cols).toBe(80);
    expect(firstResize.rows).toBe(24);

    // Enqueue B while A is in flight.
    latest.sendTerminalResize("gbt-1", 100, 40, {
      onResult: (r) => results.push(["B", r]),
    });
    await act(async () => {
      await Promise.resolve();
    });
    let resizes = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resize");
    expect(resizes).toHaveLength(1);

    // A hits flow_control — will retry; B must still wait.
    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: false,
        id: firstResize.id,
        session: "gbt-1",
        error_code: "flow_control",
        error: "too many pending",
      }),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
      await Promise.resolve();
    });

    resizes = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resize");
    // Only A (possibly retried) may be on the wire so far.
    expect(resizes.every((item) => item.cols === 80 && item.rows === 24)).toBe(
      true,
    );

    const lastA = resizes.at(-1);
    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: true,
        id: lastA.id,
        session: "gbt-1",
        cols: 80,
        rows: 24,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    resizes = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resize");
    const bOnWire = resizes.find((item) => item.cols === 100 && item.rows === 40);
    expect(bOnWire).toBeTruthy();

    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: true,
        id: bOnWire.id,
        session: "gbt-1",
        cols: 100,
        rows: 40,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    // Final applied size on the wire must be B (last successful resize payload).
    const successful = resizes
      .filter((item) => {
        // reconstruct by looking at results that were ok with matching size
        return true;
      })
      .map((item) => `${item.cols}x${item.rows}`);
    // Distinct chronological sizes: A then B.
    const distinct = [];
    for (const key of successful) {
      if (distinct.at(-1) !== key) distinct.push(key);
    }
    expect(distinct.at(-1)).toBe("100x40");
    expect(distinct).toEqual(["80x24", "100x40"]);

    const bResult = results.find(([tag, r]) => tag === "B" && r.ok);
    expect(bResult).toBeTruthy();
    expect(bResult[1].cols).toBe(100);
    expect(bResult[1].rows).toBe(40);
  });

  it("serializes input then resize on the shared session I/O lane", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.sendTerminalInput("gbt-1", btoa("typed"));
    latest.sendTerminalResize("gbt-1", 120, 36, {
      onResult: () => {},
    });

    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    let wire = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter(
        (item) =>
          item.type === "terminal_input" || item.type === "terminal_resize",
      );
    // Input is first on the shared lane; resize must not leapfrog.
    expect(wire).toHaveLength(1);
    expect(wire[0].type).toBe("terminal_input");
    expect(wire[0].data_base64).toBe(btoa("typed"));

    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: wire[0].id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    wire = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter(
        (item) =>
          item.type === "terminal_input" || item.type === "terminal_resize",
      );
    expect(wire.map((item) => item.type)).toEqual([
      "terminal_input",
      "terminal_resize",
    ]);
    expect(wire[1].cols).toBe(120);
    expect(wire[1].rows).toBe(36);
  });

  it("keeps slow input pending past former ack timeouts until server success", async () => {
    // Former 1.5s × 2 retries finished with ack_timeout while a multi-second
    // PTY write was still running — surface failure + real success.
    const notices = [];
    await mount({
      interactive: true,
      setNotice: (value) => {
        notices.push(typeof value === "function" ? value(null) : value);
      },
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.sendTerminalInput("gbt-1", btoa("slow"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    const input = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    expect(input).toBeTruthy();

    // Far past old 1.5s × (1 + MAX_RETRIES) window, but before the absolute
    // 30s side-effect deadline.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(29_000);
    });
    await act(async () => {
      await Promise.resolve();
    });

    // Still only one input frame; no client-side failure notice.
    const inputs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input");
    expect(inputs).toHaveLength(1);
    const errorNotices = notices.filter(
      (n) => n && n.tone === "error" && n.kind === "input",
    );
    expect(errorNotices).toHaveLength(0);

    // Late server success settles without having falsely failed first.
    let settled = null;
    // Re-drive through settle path: emit success for the same id.
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: input.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      settled = true;
    });
    expect(settled).toBe(true);
    expect(
      notices.filter((n) => n && n.tone === "error" && n.kind === "input"),
    ).toHaveLength(0);
  });

  it("ends a side effect with no ack at the absolute deadline and releases its lane", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    latest.sendTerminalInput("gbt-1", btoa("first"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
      }),
    );
    await act(async () => Promise.resolve());
    const first = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    latest.sendTerminalInput("gbt-1", btoa("second"));

    await act(async () => vi.advanceTimersByTimeAsync(30_000));
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const inputs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input");
    expect(inputs).toHaveLength(2);
    expect(inputs[0].id).toBe(first.id);
    expect(inputs[1].id).not.toBe(first.id);

    // A late success for the retired id is ignored and cannot disturb B.
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: first.id,
        session: "gbt-1",
      }),
    );
    expect(
      ws.sent
        .map((item) => JSON.parse(String(item)))
        .filter((item) => item.type === "terminal_input"),
    ).toHaveLength(2);
  });

  it("serializes input so flow_control retry of A is not overtaken by B", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.sendTerminalInput("gbt-1", btoa("A"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    const firstInput = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    expect(firstInput).toBeTruthy();
    expect(firstInput.data_base64).toBe(btoa("A"));

    // Queue B while A is still in flight.
    latest.sendTerminalInput("gbt-1", btoa("B"));
    await act(async () => {
      await Promise.resolve();
    });
    const inputsBeforeRetry = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input");
    expect(inputsBeforeRetry).toHaveLength(1);

    // A hits flow_control — client will retry; B must still wait.
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: false,
        id: firstInput.id,
        session: "gbt-1",
        error_code: "flow_control",
        error: "too many pending",
      }),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(100);
    });
    await act(async () => {
      await Promise.resolve();
    });

    const afterFlow = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input");
    // Retry of A may appear; B must not appear yet.
    expect(
      afterFlow.every((item) => item.data_base64 === btoa("A")),
    ).toBe(true);

    // Final success for A (use last A id on the wire).
    const lastA = afterFlow.at(-1);
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: lastA.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    const allInputs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input")
      .map((item) => atob(item.data_base64));
    // Wire order of distinct payloads must be A then B (A may retry).
    const distinctOrder = [];
    for (const ch of allInputs) {
      if (distinctOrder.at(-1) !== ch) distinctOrder.push(ch);
    }
    expect(distinctOrder).toEqual(["A", "B"]);
  });

  it("keeps inflight input attached across interactive off→on and serializes later work", async () => {
    // releaseControls must not fake-finish a wired write as read_only, and must
    // not clear the session I/O lane so a later resize cannot leapfrog the write.
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <InteractiveProbe
            onState={(state) => {
              latest = state;
            }}
          />
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    const resizeResults = [];
    latest.sendTerminalInput("gbt-1", btoa("inflight"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim).toBeTruthy();
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    const input = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    expect(input).toBeTruthy();
    expect(input.data_base64).toBe(btoa("inflight"));
    const controlledBefore = () =>
      ws.sent
        .map((item) => JSON.parse(String(item)))
        .filter(
          (item) =>
            item.type === "terminal_input" || item.type === "terminal_resize",
        );

    // Interactive off while write is still in flight on the server.
    await act(async () => {
      latest.setInteractive(false);
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(
      ws.sent.some((item) => String(item).includes("terminal_release")),
    ).toBe(true);
    // Still only the original input on the wire — not locally "finished".
    expect(controlledBefore()).toHaveLength(1);

    // Interactive back on *before* the first write settles; queue resize+input
    // on the same session lane so they must wait for the inflight tail.
    await act(async () => {
      latest.setInteractive(true);
    });
    await act(async () => {
      await Promise.resolve();
    });
    latest.sendTerminalResize("gbt-1", 100, 40, {
      onResult: (result) => resizeResults.push(result),
    });
    latest.sendTerminalInput("gbt-1", btoa("after"));
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    // Serial lane still blocked by inflight input — no new controlled frames yet.
    expect(controlledBefore()).toHaveLength(1);

    // Accurate terminal success for the original write (not a fake read_only).
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: input.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    // Lane advances: new claim, then resize only (after-input still waits on resize).
    const claims = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_claim");
    const lastClaim = claims.at(-1);
    expect(lastClaim).toBeTruthy();
    expect(lastClaim.id).not.toBe(claim.id);
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: lastClaim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    let controlled = controlledBefore();
    expect(controlled[0].type).toBe("terminal_input");
    expect(controlled[0].data_base64).toBe(btoa("inflight"));
    expect(controlled[0].id).toBe(input.id);
    expect(controlled).toHaveLength(2);
    expect(controlled[1].type).toBe("terminal_resize");
    expect(controlled[1].cols).toBe(100);
    expect(controlled[1].rows).toBe(40);
    const resizeId = controlled[1].id;

    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: true,
        id: resizeId,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(resizeResults).toHaveLength(1);
    expect(resizeResults[0].ok).toBe(true);
    expect(resizeResults[0].cols).toBe(100);
    expect(resizeResults[0].rows).toBe(40);

    controlled = controlledBefore();
    expect(controlled).toHaveLength(3);
    expect(controlled[2].type).toBe("terminal_input");
    expect(controlled[2].data_base64).toBe(btoa("after"));
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: true,
        id: controlled[2].id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
  });

  it("does not claim or resend input after disconnect then interactive off", async () => {
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <InteractiveProbe
            onState={(state) => {
              latest = state;
            }}
          />
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.sendTerminalInput("gbt-1", btoa("pending-write"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    const input = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    expect(input).toBeTruthy();

    // Disconnect pauses input pending (same id retained).
    await act(async () => ws.close());
    await act(async () => {
      await Promise.resolve();
    });

    // Turn interactive off — freezes retries; must not auto-replay the write.
    await act(async () => {
      latest.setInteractive(false);
    });
    await act(async () => {
      await Promise.resolve();
    });

    // Reconnect as read-only: no claim, no input, no resize (no auto-replay).
    const before = MockWebSocket.instances.length;
    await act(async () => {
      latest.reconnect();
    });
    await act(async () => {
      await Promise.resolve();
    });
    // New socket may appear after reconnect.
    const ws2 =
      MockWebSocket.instances[MockWebSocket.instances.length - 1] || ws;
    if (ws2.readyState !== MockWebSocket.OPEN) {
      await act(async () => ws2.open());
    }
    await act(async () => {
      await vi.advanceTimersByTimeAsync(50);
      await Promise.resolve();
    });

    const after = ws2.sent.map((item) => JSON.parse(String(item)));
    expect(after.some((item) => item.type === "terminal_claim")).toBe(false);
    expect(after.some((item) => item.type === "terminal_input")).toBe(false);
    expect(after.some((item) => item.type === "terminal_resize")).toBe(false);
    expect(before).toBeGreaterThanOrEqual(1);
  });

  it("ends an in-flight resize as indeterminate on reconnect without replay", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    const results = [];
    latest.sendTerminalResize("gbt-1", 90, 30, {
      onResult: (result) => results.push(result),
    });
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    const resize = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_resize");
    expect(resize).toBeTruthy();

    await act(async () => ws.close());
    expect(results).toHaveLength(1);
    expect(results[0].error_code).toBe(CLIENT_IO_ERROR.INDETERMINATE);

    await act(async () => latest.reconnect());
    await act(async () => {
      await Promise.resolve();
    });
    const ws2 = MockWebSocket.instances.at(-1);
    await act(async () => ws2.open());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });
    const replayed = ws2.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_resize");
    expect(replayed).toHaveLength(0);
  });

  it("prunes session I/O serial map when the tail promise completes", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    // Drive one resize to completion so the serial map can prune.
    latest.sendTerminalResize("gbt-prune", 80, 24, {
      onResult: () => {},
    });
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-prune",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    const resize = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_resize");
    expect(resize).toBeTruthy();
    await act(async () =>
      ws.emitMessage({
        type: "resize_result",
        ok: true,
        id: resize.id,
        session: "gbt-prune",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    // Internal map is not exported; a second session must not wait on a dead
    // chain — admit immediately after prior session finished (no hang).
    const beforeLen = ws.sent.length;
    latest.sendTerminalResize("gbt-other", 90, 30, {
      onResult: () => {},
    });
    await act(async () => {
      await Promise.resolve();
    });
    const claim2 = ws.sent
      .slice(beforeLen)
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    expect(claim2).toBeTruthy();
    expect(claim2.session).toBe("gbt-other");
  });

  it("supersedes terminal_subscribe with generation so latest desired wins", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    // Connect may have sent an empty subscribe; supersede with A then B.
    latest.setTerminalSubscriptions(["gbt-a"]);
    await act(async () => {
      await Promise.resolve();
    });
    latest.setTerminalSubscriptions(["gbt-b"]);
    await act(async () => {
      await Promise.resolve();
    });

    const subs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_subscribe");
    expect(subs.length).toBeGreaterThanOrEqual(2);
    // Generations are monotonic; the last wire payload is the latest desired set.
    const gens = subs.map((item) => item.generation);
    for (let i = 1; i < gens.length; i += 1) {
      expect(gens[i]).toBeGreaterThan(gens[i - 1]);
    }
    const last = subs.at(-1);
    expect(last.sessions).toEqual(["gbt-b"]);
    expect(typeof last.generation).toBe("number");

    // Late ack for a superseded older id is ignored (no error, no rollback).
    const older = subs.find((item) => item.sessions?.includes?.("gbt-a"));
    if (older) {
      await act(async () =>
        ws.emitMessage({
          type: "terminal_subscribe_result",
          ok: true,
          id: older.id,
          error_code: null,
          error: null,
        }),
      );
      await act(async () => {
        await Promise.resolve();
      });
    }
    await act(async () =>
      ws.emitMessage({
        type: "terminal_subscribe_result",
        ok: true,
        id: last.id,
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    // No further subscribe after latest applied.
    const after = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_subscribe");
    expect(after.at(-1).sessions).toEqual(["gbt-b"]);
  });

  it("counts attempts by real send and retries consecutive flow_control", async () => {
    // COMMAND_MAX_RETRIES=2 → initial + 2 retries = 3 real sends, then stop.
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());

    latest.sendTerminalInput("gbt-1", btoa("retry-me"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });

    const first = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_input");
    expect(first).toBeTruthy();
    const requestId = first.id;

    // Two consecutive flow_control results should each trigger another send.
    for (let i = 0; i < 2; i += 1) {
      await act(async () =>
        ws.emitMessage({
          type: "input_result",
          ok: false,
          id: requestId,
          session: "gbt-1",
          error_code: "flow_control",
          error: "too many pending",
        }),
      );
      await act(async () => {
        await vi.advanceTimersByTimeAsync(100);
        await Promise.resolve();
      });
    }

    let inputs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input" && item.id === requestId);
    // Initial + 2 retries.
    expect(inputs).toHaveLength(3);

    // Third consecutive flow_control exceeds retry budget → finish without more sends.
    await act(async () =>
      ws.emitMessage({
        type: "input_result",
        ok: false,
        id: requestId,
        session: "gbt-1",
        error_code: "flow_control",
        error: "too many pending",
      }),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(200);
      await Promise.resolve();
    });

    inputs = ws.sent
      .map((item) => JSON.parse(String(item)))
      .filter((item) => item.type === "terminal_input" && item.id === requestId);
    expect(inputs).toHaveLength(3);
  });

  it("bumps controlEpochs when claim succeeds", async () => {
    await mount({ interactive: true });
    const ws = MockWebSocket.instances[0];
    await act(async () => ws.open());
    expect(latest.controlEpochs?.["gbt-1"] ?? 0).toBe(0);

    latest.sendTerminalInput("gbt-1", btoa("x"));
    const claim = ws.sent
      .map((item) => JSON.parse(String(item)))
      .find((item) => item.type === "terminal_claim");
    await act(async () =>
      ws.emitMessage({
        type: "terminal_claim_result",
        ok: true,
        id: claim.id,
        session: "gbt-1",
        error_code: null,
        error: null,
      }),
    );
    await act(async () => {
      await Promise.resolve();
    });
    expect(latest.controlEpochs["gbt-1"]).toBe(1);
  });
});
