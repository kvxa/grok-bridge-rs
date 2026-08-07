import { afterEach, describe, expect, it, vi } from "vitest";
import {
  TERMINAL_REMOUNT_BUFFER_BYTES,
  TERMINAL_REMOUNT_BUFFER_MAX,
  disposeTerminalSession,
  peekTerminalBuffer,
  peekTerminalBufferGapped,
  pushTerminalEntries,
  reconcileTerminalSessions,
  resetTerminalFeeds,
  subscribeTerminal,
} from "./terminalFeeds.js";

afterEach(() => {
  resetTerminalFeeds();
});

describe("terminalFeeds", () => {
  it("replays bounded pre-subscription backlog then releases it", () => {
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        data_base64: btoa("SNAP"),
        cursor: 0,
        next_cursor: 4,
      },
      {
        session: "s1",
        reset: false,
        data_base64: btoa("A"),
        cursor: 4,
        next_cursor: 5,
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(2);

    const received = [];
    const unsubscribe = subscribeTerminal("s1", (entry) => received.push(entry));
    expect(received).toHaveLength(2);
    expect(received[0].reset).toBe(true);
    expect(received[1].data_base64).toBe(btoa("A"));
    // Backlog released immediately after replay.
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        data_base64: btoa("B"),
        cursor: 5,
        next_cursor: 6,
      },
    ]);
    expect(received).toHaveLength(3);
    // Live traffic is not retained.
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
    unsubscribe();
  });

  it("does not accumulate entries while live subscribers exist", () => {
    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry));

    for (let i = 0; i < 200; i += 1) {
      pushTerminalEntries([
        {
          session: "s1",
          reset: i === 0,
          data_base64: btoa(String(i)),
        },
      ]);
    }

    expect(received).toHaveLength(200);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
  });

  it("late initial mount still replays last reset plus subsequent in order", () => {
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("old") },
      { session: "s1", reset: false, data_base64: btoa("x") },
      { session: "s1", reset: true, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, data_base64: btoa("A") },
      { session: "s1", reset: false, data_base64: btoa("B") },
    ]);
    // Bounded: last reset + subsequent only.
    expect(peekTerminalBuffer("s1").map((e) => e.data_base64)).toEqual([
      btoa("SNAP"),
      btoa("A"),
      btoa("B"),
    ]);

    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry.data_base64));
    expect(received).toEqual([btoa("SNAP"), btoa("A"), btoa("B")]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
  });

  it("clears backlog on reset=true and disposes removed sessions", () => {
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("old") },
      { session: "s1", reset: false, data_base64: btoa("x") },
    ]);
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("new") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(1);
    expect(peekTerminalBuffer("s1")[0].data_base64).toBe(btoa("new"));

    reconcileTerminalSessions(new Set());
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    const listener = vi.fn();
    subscribeTerminal("gone", listener);
    disposeTerminalSession("gone");
    pushTerminalEntries([
      { session: "gone", reset: true, data_base64: btoa("z") },
    ]);
    expect(listener).not.toHaveBeenCalled();
  });

  it("bounds hidden terminal backlog globally across many sessions", () => {
    const entries = Array.from({ length: 2050 }, (_, index) => ({
      session: `hidden-${index}`,
      reset: false,
      data_base64: btoa("x"),
    }));
    pushTerminalEntries(entries);
    expect(peekTerminalBufferGapped("hidden-0")).toBe(true);
    expect(peekTerminalBuffer("hidden-2049")).toHaveLength(1);
  });

  it("rebuilds remount backlog only after all listeners leave", () => {
    const first = [];
    const unsubscribe = subscribeTerminal("s1", (entry) => first.push(entry));
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("live") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
    unsubscribe();

    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("backlog") },
      { session: "s1", reset: false, data_base64: btoa("delta") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(2);

    const second = [];
    subscribeTerminal("s1", (entry) => second.push(entry.data_base64));
    expect(second).toEqual([btoa("backlog"), btoa("delta")]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
  });

  it("count overflow marks gap instead of silent middle drop", () => {
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("SNAP") },
    ]);
    for (let i = 0; i < TERMINAL_REMOUNT_BUFFER_MAX + 8; i += 1) {
      pushTerminalEntries([
        {
          session: "s1",
          reset: false,
          data_base64: btoa(`d${i}`),
        },
      ]);
    }
    expect(peekTerminalBufferGapped("s1")).toBe(true);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry));
    expect(received).toHaveLength(1);
    expect(received[0].gap).toBe(true);
    // No discontinuous deltas after gap.
    expect(received.every((e) => e.gap || e.reset)).toBe(true);
  });

  it("byte overflow marks gap and remount gets gap then reset path", () => {
    // One large chunk under the byte budget, then enough more to exceed.
    const chunk = btoa("x".repeat(8 * 1024));
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("SNAP") },
    ]);
    let pushed = 0;
    while (pushed < TERMINAL_REMOUNT_BUFFER_BYTES + 4096) {
      pushTerminalEntries([
        { session: "s1", reset: false, data_base64: chunk },
      ]);
      pushed += 8 * 1024;
    }
    expect(peekTerminalBufferGapped("s1")).toBe(true);

    const received = [];
    const unsub = subscribeTerminal("s1", (entry) => received.push(entry));
    expect(received.some((e) => e.gap)).toBe(true);
    expect(received.some((e) => !e.gap && !e.reset && e.data_base64)).toBe(
      false,
    );
    unsub();

    // After gap, only a reset rebuilds a clean backlog (orphan deltas dropped).
    pushTerminalEntries([
      { session: "s1", reset: false, data_base64: btoa("orphan") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("FRESH") },
      { session: "s1", reset: false, data_base64: btoa("ok") },
    ]);
    const remount = [];
    subscribeTerminal("s1", (entry) => remount.push(entry));
    expect(remount[0].reset).toBe(true);
    expect(remount[0].data_base64).toBe(btoa("FRESH"));
    expect(remount.map((e) => e.data_base64)).toEqual([
      btoa("FRESH"),
      btoa("ok"),
    ]);
    expect(remount.some((e) => e.gap)).toBe(false);
  });

  it("rapid collapse remount does not replay gapped stream", () => {
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: btoa("SNAP") },
    ]);
    for (let i = 0; i < TERMINAL_REMOUNT_BUFFER_MAX + 2; i += 1) {
      pushTerminalEntries([
        { session: "s1", reset: false, data_base64: btoa(String(i)) },
      ]);
    }
    // StrictMode-style double subscribe: both see gap, never a hole.
    const a = [];
    const unsubA = subscribeTerminal("s1", (e) => a.push(e));
    expect(a[0]?.gap).toBe(true);
    unsubA();

    pushTerminalEntries([
      { session: "s1", reset: false, data_base64: btoa("orphan-delta") },
    ]);
    // Still await-reset — orphan delta discarded; remount gets gap again.
    const b = [];
    subscribeTerminal("s1", (e) => b.push(e));
    expect(b.length).toBeGreaterThan(0);
    expect(b.every((e) => e.gap || e.reset)).toBe(true);
    expect(b.some((e) => e.data_base64 === btoa("orphan-delta"))).toBe(false);
  });

  it("keeps multi-frame reset snapshot (head + reset_cont) without gap loop", () => {
    // Simulate server split: first ~300 KiB head, then large conts totaling >1 MiB.
    const piece = (n) => btoa("R".repeat(n));
    const headBytes = 300 * 1024;
    const contBytes = 400 * 1024;
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        reset_cont: false,
        data_base64: piece(headBytes),
      },
      {
        session: "s1",
        reset: false,
        reset_cont: true,
        data_base64: piece(contBytes),
      },
      {
        session: "s1",
        reset: false,
        reset_cont: true,
        data_base64: piece(contBytes),
      },
      {
        session: "s1",
        reset: false,
        reset_cont: true,
        data_base64: piece(contBytes),
      },
    ]);
    expect(peekTerminalBufferGapped("s1")).toBe(false);
    const buf = peekTerminalBuffer("s1");
    expect(buf).toHaveLength(4);
    expect(buf[0].reset).toBe(true);
    expect(buf.slice(1).every((e) => e.reset_cont)).toBe(true);

    const received = [];
    subscribeTerminal("s1", (e) => received.push(e));
    expect(received.some((e) => e.gap)).toBe(false);
    expect(received).toHaveLength(4);
    expect(received[0].reset).toBe(true);
    expect(received.filter((e) => e.reset_cont)).toHaveLength(3);
  });

  it("hide then expand replays multi-frame reset without gap after large cont", () => {
    const piece = (n) => btoa("X".repeat(n));
    // Unmounted backlog (no listeners).
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: piece(200 * 1024) },
      { session: "s1", reset: false, reset_cont: true, data_base64: piece(300 * 1024) },
      { session: "s1", reset: false, reset_cont: true, data_base64: piece(300 * 1024) },
    ]);
    expect(peekTerminalBufferGapped("s1")).toBe(false);

    const first = [];
    const unsub = subscribeTerminal("s1", (e) => first.push(e));
    expect(first.some((e) => e.gap)).toBe(false);
    expect(first[0].reset).toBe(true);
    expect(first.filter((e) => e.reset_cont).length).toBe(2);
    unsub();

    // Collapse: backlog rebuilds on next push while unmounted.
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: piece(250 * 1024) },
      { session: "s1", reset: false, reset_cont: true, data_base64: piece(500 * 1024) },
    ]);
    const second = [];
    subscribeTerminal("s1", (e) => second.push(e));
    expect(second.some((e) => e.gap)).toBe(false);
    expect(second[0].reset).toBe(true);
    expect(second[1].reset_cont).toBe(true);
  });
});
