import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TerminalIOContext } from "../context/TerminalIOContext.jsx";
import { I18nProvider } from "../i18n/index.js";
import { MockFitAddon } from "../test/mockFitAddon.js";
import { MockXTerm } from "../test/mockXterm.js";
import { encodeUtf8ToBase64 } from "../utils/base64.js";
import {
  pushTerminalEntries,
  resetTerminalFeeds,
} from "../utils/terminalFeeds.js";
import {
  TERMINAL_HEIGHT_DEFAULT,
  TERMINAL_HEIGHT_MAX,
  TERMINAL_HEIGHT_MIN,
  clampTerminalHeight,
  maxTerminalHeight,
  terminalHeightStorageKey,
} from "../utils/terminalHeight.js";
import { TERMINAL_FONT_FAMILY } from "../utils/terminalTheme.js";
import {
  coalesceTerminalEntries,
  createTerminalWriteQueue,
  fitTerminalHost,
  TERMINAL_WRITE_QUEUE_MAX,
  TERMINAL_WRITE_QUEUE_MAX_BYTES,
} from "./Terminal.jsx";

vi.mock("@xterm/xterm", () => ({
  Terminal: MockXTerm,
}));

vi.mock("@xterm/addon-fit", () => ({
  FitAddon: MockFitAddon,
}));

import { Terminal } from "./Terminal.jsx";

function decodeChunk(chunk) {
  return typeof chunk === "string"
    ? chunk
    : new TextDecoder().decode(chunk);
}

/** jsdom ResizeObserver shim that tests can trigger. */
class TestResizeObserver {
  static instances = [];

  static reset() {
    TestResizeObserver.instances = [];
  }

  constructor(callback) {
    this.callback = callback;
    this.targets = [];
    this.disconnected = false;
    TestResizeObserver.instances.push(this);
  }

  observe(target) {
    this.targets.push(target);
  }

  unobserve() {}

  disconnect() {
    this.disconnected = true;
  }

  trigger() {
    this.callback(this.targets.map((target) => ({ target })));
  }
}

function sizeElement(el, width, height) {
  if (!el) return;
  Object.defineProperty(el, "clientWidth", {
    configurable: true,
    get: () => width,
  });
  Object.defineProperty(el, "clientHeight", {
    configurable: true,
    get: () => height,
  });
  Object.defineProperty(el, "offsetWidth", {
    configurable: true,
    get: () => width,
  });
  Object.defineProperty(el, "offsetHeight", {
    configurable: true,
    get: () => height,
  });
}

describe("Terminal (xterm read-only)", () => {
  let container;
  let root;
  let rafQueue;
  let ioState;
  let sendInput;
  let sendResize;

  beforeEach(() => {
    MockXTerm.reset();
    MockFitAddon.reset();
    TestResizeObserver.reset();
    resetTerminalFeeds();
    localStorage.clear();
    rafQueue = [];
    sendInput = vi.fn(() => ({ ok: true, id: "t1" }));
    // Auto-ack resize with the exact request id + cols/rows so dedupe commits
    // only after a matching backend result (never on queue/send alone).
    let resizeSeq = 0;
    sendResize = vi.fn((session, cols, rows, { onResult } = {}) => {
      resizeSeq += 1;
      const id = `resize-${resizeSeq}`;
      queueMicrotask(() => {
        onResult?.({
          ok: true,
          id,
          session,
          cols,
          rows,
          error_code: null,
          error: null,
        });
      });
      return { ok: true, id };
    });
    ioState = {
      interactive: false,
      setInteractive: vi.fn(),
      connectionState: "connected",
      sendTerminalInput: (...args) => sendInput(...args),
      sendTerminalResize: (...args) => sendResize(...args),
      setTerminalSubscriptions: vi.fn(() => ({ ok: true })),
      requestTerminalResync: vi.fn(() => ({ ok: true })),
      controlEpochs: {},
    };
    vi.stubGlobal(
      "requestAnimationFrame",
      (cb) => {
        rafQueue.push(cb);
        return rafQueue.length;
      },
    );
    vi.stubGlobal("cancelAnimationFrame", (id) => {
      rafQueue[id - 1] = null;
    });
    vi.stubGlobal("ResizeObserver", TestResizeObserver);
    // Tall viewport so default 560px is below the 0.7vh cap during height tests.
    Object.defineProperty(window, "innerHeight", {
      configurable: true,
      writable: true,
      value: 1200,
    });
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    if (root) {
      await act(async () => {
        try {
          root.unmount();
        } catch {
          /* already unmounted by a test */
        }
      });
      root = null;
    }
    container?.remove();
    container = null;
    resetTerminalFeeds();
    MockXTerm.reset();
    MockFitAddon.reset();
    TestResizeObserver.reset();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  async function flushRaf() {
    await act(async () => {
      const pending = rafQueue.filter(Boolean);
      rafQueue = [];
      for (const cb of pending) cb(0);
      await Promise.resolve();
    });
  }

  async function renderTerminal(props = {}) {
    if (props.interactive != null) ioState.interactive = props.interactive;
    if (props.connectionState != null) {
      ioState.connectionState = props.connectionState;
    }
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <TerminalIOContext.Provider value={ioState}>
            <Terminal
              id={props.id ?? "gbt-1"}
              heightKey={props.heightKey ?? "client:test-group"}
              rows={props.rows ?? 24}
              cols={props.cols ?? 80}
              label={props.label ?? "term"}
            />
          </TerminalIOContext.Provider>
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });
    const host = container.querySelector("[data-terminal-host]");
    sizeElement(host, props.hostWidth ?? 720, props.hostHeight ?? 280);
    await flushRaf();
    return host;
  }

  async function setInteractive(value) {
    ioState = { ...ioState, interactive: value };
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <TerminalIOContext.Provider value={ioState}>
            <Terminal
              id="gbt-1"
              heightKey="client:test-group"
              rows={24}
              cols={80}
              label="term"
            />
          </TerminalIOContext.Provider>
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });
    await flushRaf();
  }

  it("configures disableStdin and never registers input handlers", async () => {
    await renderTerminal();
    expect(MockXTerm.instances).toHaveLength(1);
    const term = MockXTerm.instances[0];
    expect(term.options.disableStdin).toBe(true);
    expect(term.options.cursorBlink).toBe(false);
    expect(term.options.lineHeight).toBe(1);
    expect(term.options.fontFamily).toBe(TERMINAL_FONT_FAMILY);
    expect(term.options.fontFamily).toMatch(/^Consolas,/);
    expect(term.options.fontFamily).toContain(
      '"SauceCodePro Nerd Font Mono", "Source Code Pro", Monaco, "Liberation Mono", "Courier New", monospace',
    );
    expect(term.options.fontFamily).not.toContain("var(");
    expect(
      container.querySelector("[data-terminal-host]").style.fontFamily,
    ).toBe(TERMINAL_FONT_FAMILY);
    expect(term.handlers.data).toHaveLength(0);
    expect(term.handlers.key).toHaveLength(0);
    expect(term.opened).toBe(true);
  });

  it("applies initial reset snapshot then ordered appends", async () => {
    pushTerminalEntries([
      {
        session: "gbt-1",
        reset: true,
        data_base64: btoa("SNAP"),
      },
    ]);
    await renderTerminal();
    const term = MockXTerm.instances[0];
    expect(term.resetCount).toBe(1);
    expect(term.written).toHaveLength(1);
    expect(Array.from(term.written[0])).toEqual(
      Array.from(new TextEncoder().encode("SNAP")),
    );

    await act(async () => {
      pushTerminalEntries([
        {
          session: "gbt-1",
          reset: false,
          data_base64: btoa("A"),
        },
        {
          session: "gbt-1",
          reset: false,
          data_base64: btoa("B"),
        },
      ]);
    });
    expect(term.written).toHaveLength(3);
    expect(Array.from(term.written[1])).toEqual([65]);
    expect(Array.from(term.written[2])).toEqual([66]);
  });

  it("resets on reset=true without disposing the instance", async () => {
    await renderTerminal();
    const term = MockXTerm.instances[0];
    await act(async () => {
      pushTerminalEntries([
        { session: "gbt-1", reset: true, data_base64: btoa("one") },
      ]);
    });
    await act(async () => {
      pushTerminalEntries([
        { session: "gbt-1", reset: false, data_base64: btoa("two") },
      ]);
    });
    await act(async () => {
      pushTerminalEntries([
        { session: "gbt-1", reset: true, data_base64: btoa("three") },
      ]);
    });
    expect(term.resetCount).toBe(2);
    expect(term.disposed).toBe(false);
    expect(term.written).toHaveLength(1);
    expect(Array.from(term.written[0])).toEqual(
      Array.from(new TextEncoder().encode("three")),
    );
  });

  it("FIFO: reset waits for prior write callback and completes snapshot before later append", async () => {
    await renderTerminal();
    const term = MockXTerm.instances[0];
    term.holdWriteCallbacks = true;

    await act(async () => {
      pushTerminalEntries([
        { session: "gbt-1", reset: false, data_base64: btoa("A") },
        { session: "gbt-1", reset: true, data_base64: btoa("SNAP") },
        { session: "gbt-1", reset: false, data_base64: btoa("B") },
      ]);
    });

    // Only append A has entered write(); reset must not have run yet.
    expect(term.ops.map((op) => op[0]).filter((op) => op !== "resize")).toEqual(
      ["write"],
    );
    expect(decodeChunk(term.ops.find((op) => op[0] === "write")[1])).toBe("A");
    expect(term.resetCount).toBe(0);
    expect(term.pendingCallbacks).toHaveLength(1);

    await act(async () => {
      term.flushWriteCallback();
    });
    const afterReset = term.ops.map((op) => op[0]).filter((op) => op !== "resize");
    expect(afterReset).toEqual(["write", "reset", "write"]);
    expect(term.resetCount).toBe(1);
    expect(term.pendingCallbacks).toHaveLength(1);
    expect(
      term.ops.some((op) => op[0] === "write" && decodeChunk(op[1]) === "B"),
    ).toBe(false);

    await act(async () => {
      term.flushWriteCallback();
    });
    const ops = term.ops.map((op) => op[0]).filter((op) => op !== "resize");
    expect(ops).toEqual(["write", "reset", "write", "write"]);
    expect(
      decodeChunk(term.ops.filter((op) => op[0] === "write").at(-1)[1]),
    ).toBe("B");

    await act(async () => {
      term.flushWriteCallback();
    });
    expect(term.pendingCallbacks).toHaveLength(0);
  });

  it("FIFO unit: append -> reset -> append order with held callbacks", () => {
    const term = new MockXTerm();
    term.holdWriteCallbacks = true;
    const queue = createTerminalWriteQueue(term);

    queue.enqueue({ reset: false, data_base64: btoa("A") });
    queue.enqueue({ reset: true, data_base64: btoa("SNAP") });
    queue.enqueue({ reset: false, data_base64: btoa("B") });

    expect(term.ops.map((op) => op[0])).toEqual(["write"]);
    expect(decodeChunk(term.ops[0][1])).toBe("A");

    term.flushWriteCallback();
    expect(term.ops.map((op) => op[0])).toEqual(["write", "reset", "write"]);
    expect(decodeChunk(term.ops[2][1])).toBe("SNAP");

    term.flushWriteCallback();
    expect(term.ops.map((op) => op[0])).toEqual([
      "write",
      "reset",
      "write",
      "write",
    ]);
    expect(decodeChunk(term.ops[3][1])).toBe("B");

    term.flushWriteCallback();
    expect(term.pendingCallbacks).toHaveLength(0);
    queue.dispose();
  });

  it("dispose drops pending queue entries even if prior write callback later fires", async () => {
    await renderTerminal();
    const term = MockXTerm.instances[0];
    term.holdWriteCallbacks = true;

    await act(async () => {
      pushTerminalEntries([
        { session: "gbt-1", reset: false, data_base64: btoa("A") },
        { session: "gbt-1", reset: false, data_base64: btoa("B") },
        { session: "gbt-1", reset: true, data_base64: btoa("LATE") },
      ]);
    });
    expect(decodeChunk(term.ops.find((op) => op[0] === "write")[1])).toBe("A");

    await act(async () => root.unmount());
    expect(term.disposed).toBe(true);

    await act(async () => {
      term.flushAllWriteCallbacks();
    });
    // No further writes or resets after dispose.
    expect(
      term.ops.map((op) => op[0]).filter((op) => op === "write" || op === "reset"),
    ).toEqual(["write"]);
    expect(term.resetCount).toBe(0);
  });

  it("loads FitAddon, fits on visible host, and disposes on unmount", async () => {
    await renderTerminal({ rows: 20, cols: 90 });
    const term = MockXTerm.instances[0];
    const fit = MockFitAddon.instances[0];
    expect(fit).toBeTruthy();
    expect(term.addons).toContain(fit);
    expect(fit.fitCount).toBeGreaterThanOrEqual(1);
    expect(term.disposed).toBe(false);

    await act(async () => root.unmount());
    expect(term.disposed).toBe(true);
    expect(fit.disposed).toBe(true);
    expect(
      TestResizeObserver.instances.every((observer) => observer.disconnected),
    ).toBe(true);
  });

  it("applies default/min/max height constraints on the host", async () => {
    await renderTerminal();
    const shell = container.querySelector("[data-terminal]");
    const host = container.querySelector("[data-terminal-host]");
    expect(shell).not.toBeNull();
    expect(host).not.toBeNull();
    expect(Number(shell.dataset.terminalHeight)).toBe(TERMINAL_HEIGHT_DEFAULT);
    expect(host.style.height).toBe(`${TERMINAL_HEIGHT_DEFAULT}px`);
    expect(host.style.minHeight).toBe(`${TERMINAL_HEIGHT_MIN}px`);
    expect(Number.parseInt(host.style.maxHeight, 10)).toBeLessThanOrEqual(
      TERMINAL_HEIGHT_MAX,
    );
    expect(container.querySelector("[data-terminal-resize]")).not.toBeNull();
  });

  it("fits when ResizeObserver fires and skips zero-size hosts", async () => {
    const host = await renderTerminal();
    const fit = MockFitAddon.instances[0];
    const before = fit.fitCount;

    sizeElement(host, 800, 360);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    expect(fit.fitCount).toBeGreaterThan(before);

    const zeroFit = fitTerminalHost(fit, {
      clientWidth: 0,
      clientHeight: 0,
      offsetWidth: 0,
      offsetHeight: 0,
    });
    expect(zeroFit).toBe(false);
    const afterZero = fit.fitCount;
    // zero host must not increment fit
    expect(fit.fitCount).toBe(afterZero);
  });

  it("re-fits after height keyboard resize without recreating Terminal", async () => {
    await renderTerminal();
    const term = MockXTerm.instances[0];
    const fit = MockFitAddon.instances[0];
    const handle = container.querySelector("[data-terminal-resize]");
    const beforeFits = fit.fitCount;

    await act(async () => {
      handle.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
      );
    });
    await flushRaf();

    const shell = container.querySelector("[data-terminal]");
    expect(Number(shell.dataset.terminalHeight)).toBe(
      TERMINAL_HEIGHT_DEFAULT + 24,
    );
    expect(
      localStorage.getItem(terminalHeightStorageKey("client:test-group")),
    ).toBe(String(TERMINAL_HEIGHT_DEFAULT + 24));
    expect(MockXTerm.instances).toHaveLength(1);
    expect(MockXTerm.instances[0]).toBe(term);
    expect(fit.fitCount).toBeGreaterThan(beforeFits);
  });

  it("shares height within a group and isolates other groups", async () => {
    localStorage.setItem(terminalHeightStorageKey("client:group-a"), "200");
    localStorage.setItem(terminalHeightStorageKey("client:group-b"), "400");

    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <TerminalIOContext.Provider value={ioState}>
            <div>
              <Terminal
                id="gbt-a1"
                heightKey="client:group-a"
                rows={24}
                cols={80}
                label="a1"
              />
              <Terminal
                id="gbt-a2"
                heightKey="client:group-a"
                rows={24}
                cols={80}
                label="a2"
              />
              <Terminal
                id="gbt-b1"
                heightKey="client:group-b"
                rows={24}
                cols={80}
                label="b1"
              />
            </div>
          </TerminalIOContext.Provider>
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
    });

    const shells = [...container.querySelectorAll("[data-terminal]")];
    expect(shells).toHaveLength(3);
    expect(Number(shells[0].dataset.terminalHeight)).toBe(200);
    expect(Number(shells[1].dataset.terminalHeight)).toBe(200);
    expect(Number(shells[2].dataset.terminalHeight)).toBe(400);

    // Resize one terminal in group-a; sibling in same group follows immediately.
    const handleA1 = container
      .querySelector('[data-terminal="gbt-a1"]')
      .querySelector("[data-terminal-resize]");
    await act(async () => {
      handleA1.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
      );
    });
    await flushRaf();
    expect(Number(shells[0].dataset.terminalHeight)).toBe(224);
    expect(Number(shells[1].dataset.terminalHeight)).toBe(224);
    expect(Number(shells[2].dataset.terminalHeight)).toBe(400);
    expect(localStorage.getItem(terminalHeightStorageKey("client:group-a"))).toBe(
      "224",
    );
    expect(localStorage.getItem(terminalHeightStorageKey("client:group-b"))).toBe(
      "400",
    );
  });

  it("clamps oversized stored heights so the page cannot be infinitely stretched", () => {
    expect(clampTerminalHeight(50_000, 1000)).toBeLessThanOrEqual(
      TERMINAL_HEIGHT_MAX,
    );
    expect(clampTerminalHeight(50_000, 1000)).toBe(
      Math.min(TERMINAL_HEIGHT_MAX, Math.floor(1000 * 0.7)),
    );
  });

  it("fits again after hidden zero-size then expand", async () => {
    const host = await renderTerminal();
    const fit = MockFitAddon.instances[0];

    sizeElement(host, 0, 0);
    const before = fit.fitCount;
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    // zero-size must not fit
    expect(fit.fitCount).toBe(before);

    sizeElement(host, 640, 240);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    expect(fit.fitCount).toBeGreaterThan(before);
  });

  it("defaults to read-only: disableStdin and no onData traffic", async () => {
    await renderTerminal();
    const term = MockXTerm.instances[0];
    expect(term.options.disableStdin).toBe(true);
    expect(container.querySelector("[data-readonly]").dataset.readonly).toBe(
      "true",
    );
    term.emitData("should-not-send");
    expect(sendInput).not.toHaveBeenCalled();
  });

  it("suppresses terminal_resize while read-only and never sends terminal_input", async () => {
    vi.useFakeTimers();
    await renderTerminal({ interactive: false, hostWidth: 900, hostHeight: 400 });
    sendResize.mockClear();
    sendInput.mockClear();
    const host = container.querySelector("[data-terminal-host]");
    sizeElement(host, 720, 340);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    expect(sendResize).not.toHaveBeenCalled();
    expect(sendInput).not.toHaveBeenCalled();
    MockXTerm.instances[0].emitData("typed-while-readonly");
    expect(sendInput).not.toHaveBeenCalled();
    vi.useRealTimers();
  });

  it("publishes the fitted grid when interactive turns on after read-only layout", async () => {
    vi.useFakeTimers();
    await renderTerminal({ interactive: false, hostWidth: 900, hostHeight: 400 });
    sendResize.mockClear();
    const host = container.querySelector("[data-terminal-host]");
    sizeElement(host, 720, 340);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    expect(sendResize).not.toHaveBeenCalled();

    await setInteractive(true);
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    expect(sendResize).toHaveBeenCalled();
    const [session, cols, rows] = sendResize.mock.calls.at(-1);
    expect(session).toBe("gbt-1");
    expect(cols).toBeGreaterThanOrEqual(20);
    expect(rows).toBeGreaterThanOrEqual(5);
    vi.useRealTimers();
  });

  it("clamps extreme fit grids and retries resize after send failure", async () => {
    vi.useFakeTimers();
    sendResize.mockImplementation(() => ({ ok: false, error: "disconnected" }));
    await renderTerminal({ interactive: true, hostWidth: 20, hostHeight: 20 });
    const term = MockXTerm.instances[0];
    // Force a sub-minimum fitted grid before clamp.
    term.cols = 2;
    term.rows = 1;
    const host = container.querySelector("[data-terminal-host]");
    sizeElement(host, 20, 20);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    expect(sendResize).toHaveBeenCalled();
    const [, cols, rows] = sendResize.mock.calls.at(-1);
    expect(cols).toBe(20);
    expect(rows).toBe(5);

    // Failure must leave lastSent unset so the same size retries.
    sendResize.mockClear();
    sendResize.mockImplementation((session, nextCols, nextRows, { onResult } = {}) => {
      const id = "ok-retry";
      queueMicrotask(() => {
        onResult?.({
          ok: true,
          id,
          session,
          cols: nextCols,
          rows: nextRows,
        });
      });
      return { ok: true, id };
    });
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(sendResize).toHaveBeenCalledWith(
      "gbt-1",
      20,
      5,
      expect.objectContaining({ onResult: expect.any(Function) }),
    );
    vi.useRealTimers();
  });

  it("does not clip header/host/handle when host is at max height", async () => {
    const maxH = maxTerminalHeight();
    await renderTerminal();
    const handle = container.querySelector("[data-terminal-resize]");
    await act(async () => {
      handle.dispatchEvent(
        new KeyboardEvent("keydown", { key: "End", bubbles: true }),
      );
    });
    await flushRaf();
    const shell = container.querySelector("[data-terminal]");
    const header = container.querySelector("[data-terminal-header]");
    const host = container.querySelector("[data-terminal-host]");
    expect(Number(shell.dataset.terminalHeight)).toBe(maxH);
    // Shell must not share the host hard max (would clip chrome).
    expect(shell.style.maxHeight).toBe("");
    expect(getComputedStyle(shell).maxHeight === "none" || !shell.style.maxHeight).toBe(
      true,
    );
    expect(host.style.maxHeight).toBe(`${maxH}px`);
    expect(host.style.height).toBe(`${maxH}px`);
    expect(header).not.toBeNull();
    expect(handle).not.toBeNull();
    // All three chrome pieces stay present as distinct flex children.
    expect(shell.children.length).toBeGreaterThanOrEqual(3);
    expect(shell.contains(header)).toBe(true);
    expect(shell.contains(host)).toBe(true);
    expect(shell.contains(handle)).toBe(true);
  });

  it("enables onData UTF-8/control/paste when interactive without rebuilding", async () => {
    await renderTerminal();
    const term = MockXTerm.instances[0];
    await setInteractive(true);
    expect(MockXTerm.instances).toHaveLength(1);
    expect(MockXTerm.instances[0]).toBe(term);
    expect(term.options.disableStdin).toBe(false);
    expect(container.querySelector("[data-readonly]").dataset.readonly).toBe(
      "false",
    );

    term.emitData("hi");
    term.emitData("\u001b[A");
    term.emitData("中文paste");
    expect(sendInput).toHaveBeenCalled();
    const payloads = sendInput.mock.calls.map((call) => call[1]);
    expect(payloads).toContain(encodeUtf8ToBase64("hi"));
    expect(payloads).toContain(encodeUtf8ToBase64("\u001b[A"));
    expect(payloads).toContain(encodeUtf8ToBase64("中文paste"));
    expect(sendInput.mock.calls.every((call) => call[0] === "gbt-1")).toBe(
      true,
    );
  });

  it("stops sending immediately after interactive is turned off", async () => {
    await renderTerminal({ interactive: true });
    const term = MockXTerm.instances[0];
    term.emitData("a");
    expect(sendInput).toHaveBeenCalledTimes(1);
    await setInteractive(false);
    expect(term.options.disableStdin).toBe(true);
    term.emitData("b");
    expect(sendInput).toHaveBeenCalledTimes(1);
  });

  it("sends debounced resize when fit changes cols/rows and dedupes successes", async () => {
    vi.useFakeTimers();
    await renderTerminal({ interactive: true, hostWidth: 900, hostHeight: 400 });
    sendResize.mockClear();
    const host = container.querySelector("[data-terminal-host]");
    // New host size → FitAddon derives a new grid; Terminal should publish it.
    sizeElement(host, 720, 340);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    // Flush microtask onResult acks.
    await act(async () => {
      await Promise.resolve();
    });
    expect(sendResize).toHaveBeenCalled();
    const [session, cols, rows] = sendResize.mock.calls.at(-1);
    expect(session).toBe("gbt-1");
    expect(cols).toBeGreaterThanOrEqual(20);
    expect(rows).toBeGreaterThanOrEqual(5);
    const calls = sendResize.mock.calls.length;
    // Same fitted size should not resend after a matching request-id result.
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(sendResize.mock.calls.length).toBe(calls);
    vi.useRealTimers();
  });

  it("coalesces overflowed write-queue deltas without dropping bytes", () => {
    const term = new MockXTerm();
    term.holdWriteCallbacks = true;
    const queue = createTerminalWriteQueue(term, {
      maxPending: 3,
      maxBytes: 1024,
    });
    // Fill up to capacity with distinct deltas (1 in-flight + pending ≤ 3).
    queue.enqueue({ reset: false, data_base64: btoa("A") });
    queue.enqueue({ reset: false, data_base64: btoa("B") });
    queue.enqueue({ reset: false, data_base64: btoa("C") });
    queue.enqueue({ reset: false, data_base64: btoa("D") });
    // Pending queue is bounded via coalescing (one item is in-flight/busy).
    expect(queue.pending).toBeLessThanOrEqual(3);
    // Drain everything: reconstructed output must contain A,B,C,D in order.
    term.flushAllWriteCallbacks();
    // Pump remaining after first flush cycle.
    for (let i = 0; i < 8; i += 1) {
      term.flushAllWriteCallbacks();
    }
    const text = term.written
      .map((chunk) =>
        typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk),
      )
      .join("");
    expect(text).toBe("ABCD");
    queue.dispose();
  });

  it("bounds write-queue by bytes under sustained producer and resyncs via reset", () => {
    const term = new MockXTerm();
    term.holdWriteCallbacks = true;
    let resyncCalls = 0;
    const maxPending = 4;
    const maxBytes = 128;
    const queue = createTerminalWriteQueue(term, {
      maxPending,
      maxBytes,
      onResync: () => {
        resyncCalls += 1;
      },
    });
    // ~48 decoded bytes per entry; continuous deltas under a stalled writer.
    const chunk = btoa("X".repeat(48));
    for (let i = 0; i < 80; i += 1) {
      queue.enqueue({ reset: false, data_base64: chunk, session: "gbt-bound" });
      // True memory bound: never retain past budgets (gap marker is 0 bytes).
      expect(queue.pendingBytes).toBeLessThanOrEqual(maxBytes);
      expect(queue.pending).toBeLessThanOrEqual(maxPending + 1);
    }
    expect(resyncCalls).toBeGreaterThanOrEqual(1);
    expect(queue.needsResync).toBe(true);
    // Interim deltas after the gap must not accumulate.
    queue.enqueue({ reset: false, data_base64: btoa("stale") });
    expect(queue.pendingBytes).toBeLessThanOrEqual(maxBytes);

    // Authoritative snapshot restores correct content deterministically.
    queue.enqueue({
      reset: true,
      data_base64: btoa("SNAPSHOT-OK"),
      session: "gbt-bound",
    });
    for (let i = 0; i < 16; i += 1) {
      term.flushAllWriteCallbacks();
    }
    const text = term.written
      .map((chunk) =>
        typeof chunk === "string" ? chunk : new TextDecoder().decode(chunk),
      )
      .join("");
    expect(text).toBe("SNAPSHOT-OK");
    expect(queue.needsResync).toBe(false);
    expect(queue.pendingBytes).toBe(0);
    expect(queue.pending).toBe(0);
    // Default production budget remains finite (regression against unbounded coalesce).
    expect(TERMINAL_WRITE_QUEUE_MAX_BYTES).toBe(256 * 1024);
    queue.dispose();
  });

  it("coalesceTerminalEntries concatenates binary-safe deltas", () => {
    const merged = coalesceTerminalEntries(
      { reset: false, data_base64: btoa("xy") },
      { reset: false, data_base64: btoa("z") },
    );
    expect(merged.reset).toBe(false);
    expect(atob(merged.data_base64)).toBe("xyz");
    expect(
      coalesceTerminalEntries(
        { reset: true, data_base64: btoa("a") },
        { reset: false, data_base64: btoa("b") },
      ),
    ).toBeNull();
  });

  it("streams multi-frame reset snapshot >1MiB without resync loop", () => {
    const term = new MockXTerm();
    term.holdWriteCallbacks = true;
    let resyncCalls = 0;
    const queue = createTerminalWriteQueue(term, {
      maxPending: 4,
      maxBytes: 256 * 1024,
      snapshotMaxBytes: 8 * 1024 * 1024,
      onResync: () => {
        resyncCalls += 1;
      },
    });
    // Head + three conts: ~1.2 MiB total, each cont > 256 KiB delta budget.
    const head = btoa("H".repeat(200 * 1024));
    const cont = btoa("C".repeat(350 * 1024));
    queue.enqueue({
      reset: true,
      reset_cont: false,
      data_base64: head,
      session: "gbt-big",
    });
    for (let i = 0; i < 3; i += 1) {
      queue.enqueue({
        reset: false,
        reset_cont: true,
        data_base64: cont,
        session: "gbt-big",
      });
    }
    // Must not treat conts as delta overflow.
    expect(resyncCalls).toBe(0);
    expect(queue.needsResync).toBe(false);
    expect(queue.pending).toBeGreaterThanOrEqual(1);

    // Drain slow xterm: only one write at a time.
    for (let i = 0; i < 32; i += 1) {
      term.flushAllWriteCallbacks();
    }
    expect(resyncCalls).toBe(0);
    expect(queue.needsResync).toBe(false);
    expect(queue.pending).toBe(0);
    expect(term.resetCount).toBe(1);
    const totalWritten = term.written.reduce(
      (n, chunk) => n + (chunk?.length ?? 0),
      0,
    );
    expect(totalWritten).toBe(200 * 1024 + 3 * 350 * 1024);
    // Re-enqueue same split snapshot (as if resync returned) — still no loop.
    queue.enqueue({
      reset: true,
      reset_cont: false,
      data_base64: head,
      session: "gbt-big",
    });
    queue.enqueue({
      reset: false,
      reset_cont: true,
      data_base64: cont,
      session: "gbt-big",
    });
    for (let i = 0; i < 16; i += 1) {
      term.flushAllWriteCallbacks();
    }
    expect(resyncCalls).toBe(0);
    queue.dispose();
  });

  it("decode failure and term.write throw enter gap/resync not stuck busy", () => {
    const term = new MockXTerm();
    let resyncCalls = 0;
    const queue = createTerminalWriteQueue(term, {
      onResync: () => {
        resyncCalls += 1;
      },
    });
    // Invalid base64 (non-empty) → decode empty → resync.
    queue.enqueue({
      reset: false,
      data_base64: "!!!not-valid-base64!!!",
      session: "gbt-err",
    });
    expect(resyncCalls).toBe(1);
    expect(queue.needsResync).toBe(true);
    expect(queue.isBusy).toBe(false);

    // Reset head clears gap; write throw also resyncs.
    term.write = () => {
      throw new Error("xterm write failed");
    };
    queue.enqueue({
      reset: true,
      data_base64: btoa("SNAP"),
      session: "gbt-err",
    });
    expect(queue.isBusy).toBe(false);
    expect(resyncCalls).toBeGreaterThanOrEqual(2);
    queue.dispose();
  });

  it("orphan reset_cont without head requests resync once", () => {
    const term = new MockXTerm();
    let resyncCalls = 0;
    const queue = createTerminalWriteQueue(term, {
      onResync: () => {
        resyncCalls += 1;
      },
    });
    queue.enqueue({
      reset: false,
      reset_cont: true,
      data_base64: btoa("orphan"),
      session: "gbt-orphan",
    });
    expect(resyncCalls).toBe(1);
    queue.enqueue({
      reset: false,
      reset_cont: true,
      data_base64: btoa("again"),
      session: "gbt-orphan",
    });
    // While awaiting head, further conts are dropped (no extra resync storms).
    expect(resyncCalls).toBe(1);
    queue.dispose();
  });

  it("does not commit resize dedupe on session-only acks or mismatched request ids", async () => {
    vi.useFakeTimers();
    sendResize.mockImplementation((session, cols, rows, { onResult } = {}) => {
      const id = "resize-race-1";
      queueMicrotask(() => {
        // Session-only success without cols/rows must not commit.
        onResult?.({ ok: true, id: "other-id", session });
        // Matching id but wrong size must not commit; clears inflight so we retry.
        onResult?.({
          ok: true,
          id,
          session,
          cols: cols + 1,
          rows,
        });
      });
      return { ok: true, id };
    });
    await renderTerminal({ interactive: true, hostWidth: 900, hostHeight: 400 });
    sendResize.mockClear();
    const host = container.querySelector("[data-terminal-host]");
    sizeElement(host, 720, 340);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const callsAfterFirst = sendResize.mock.calls.length;
    expect(callsAfterFirst).toBeGreaterThanOrEqual(1);
    // Without a matching id+size ack, the same size must still be retryable.
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    expect(sendResize.mock.calls.length).toBeGreaterThan(callsAfterFirst);
    vi.useRealTimers();
  });

  it("forces grid resize resend when controlEpoch bumps after reclaim", async () => {
    vi.useFakeTimers();
    // Commit lastSentSize on matching acks so a second same-size send would
    // normally be suppressed — epoch must clear that cache.
    sendResize.mockImplementation((session, cols, rows, { onResult } = {}) => {
      const id = `rz-epoch-${cols}x${rows}-${sendResize.mock.calls.length}`;
      queueMicrotask(() => {
        onResult?.({
          ok: true,
          id,
          session,
          cols,
          rows,
          error_code: null,
          error: null,
        });
      });
      return { ok: true, id };
    });
    await renderTerminal({ interactive: true, hostWidth: 900, hostHeight: 400 });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const callsAfterMount = sendResize.mock.calls.length;
    expect(callsAfterMount).toBeGreaterThanOrEqual(1);

    // Same size again without epoch change: dedupe holds (no extra send).
    sendResize.mockClear();
    const host = container.querySelector("[data-terminal-host]");
    sizeElement(host, 900, 400);
    await act(async () => {
      for (const observer of TestResizeObserver.instances) observer.trigger();
    });
    await flushRaf();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(150);
    });
    expect(sendResize.mock.calls.length).toBe(0);

    // Reclaim bumps controlEpoch for this session → must re-send current grid.
    ioState = {
      ...ioState,
      interactive: true,
      controlEpochs: { "gbt-1": 1 },
    };
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <TerminalIOContext.Provider value={ioState}>
            <Terminal
              id="gbt-1"
              heightKey="client:test-group"
              rows={24}
              cols={80}
              label="term"
            />
          </TerminalIOContext.Provider>
        </I18nProvider>,
      );
    });
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(sendResize.mock.calls.length).toBeGreaterThanOrEqual(1);
    vi.useRealTimers();
  });
});
