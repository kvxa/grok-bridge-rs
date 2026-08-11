import { afterEach, describe, expect, it, vi } from "vitest";
import {
  TERMINAL_BUFFER_MAX_BYTES,
  TERMINAL_BUFFER_MAX_ENTRIES,
} from "./constants.js";
import {
  disposeTerminalSession,
  peekTerminalBuffer,
  pushTerminalEntries,
  reconcileTerminalSessions,
  resetTerminalFeeds,
  subscribeTerminal,
} from "./terminalFeeds.js";

afterEach(() => {
  resetTerminalFeeds();
});

// Raw delta sized from the byte budget rather than a hard-coded 1 MiB
// constant: base64 of n raw bytes is about 4n/3 chars, so 3/4 of the budget
// minus a small margin keeps a single delta's base64 form under the budget
// while two of them stably overflow it.
const DELTA_RAW_BYTES = Math.floor((TERMINAL_BUFFER_MAX_BYTES * 3) / 4) - 64;

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

  it("reconcile keeps active sessions and clears orphaned gap-invalid state", () => {
    // Two budget-derived deltas gap the cursor chain: the bridging delta is
    // trimmed away and the retained stream turns gap-invalid (emptied).
    const big = btoa("x".repeat(DELTA_RAW_BYTES));
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: 10,
        data_base64: btoa("SNAP"),
      },
      {
        session: "s1",
        reset: false,
        cursor: 10,
        next_cursor: 20,
        data_base64: big,
      },
      {
        session: "s1",
        reset: false,
        cursor: 20,
        next_cursor: 30,
        data_base64: big,
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
    // While the gap-invalid flag lives on, subsequent deltas stay discarded.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 30, next_cursor: 40, data_base64: btoa("C") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    // A subscribe/unsubscribe cycle releases the (now empty) retained buffer:
    // s1 disappears from both buffers and listeners, leaving only the
    // orphaned gap-invalid flag behind for reconcile to clear.
    const unsubscribe = subscribeTerminal("s1", () => {});
    unsubscribe();
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    // A healthy active session keeps its anchored backlog untouched.
    pushTerminalEntries([
      { session: "s2", reset: true, cursor: 0, next_cursor: 50, data_base64: btoa("SNAP2") },
      { session: "s2", reset: false, cursor: 50, next_cursor: 55, data_base64: btoa("A") },
    ]);
    reconcileTerminalSessions(new Set(["s2"]));
    expect(peekTerminalBuffer("s2").map((entry) => entry.data_base64)).toEqual([
      btoa("SNAP2"),
      btoa("A"),
    ]);

    // The inactive session's leftover gap-invalid flag was cleared: a fresh
    // delta is retained again instead of being dropped by the stale flag.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 30, next_cursor: 40, data_base64: btoa("C") },
    ]);
    expect(peekTerminalBuffer("s1").map((entry) => entry.data_base64)).toEqual([
      btoa("C"),
    ]);
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

  it("drops stale deltas when the retained buffer exceeds the entry cap", () => {
    const entries = [];
    for (let i = 0; i < TERMINAL_BUFFER_MAX_ENTRIES + 40; i += 1) {
      entries.push({
        session: "s1",
        reset: false,
        data_base64: btoa(String(i)),
      });
    }
    pushTerminalEntries(entries);
    const kept = peekTerminalBuffer("s1");
    expect(kept).toHaveLength(TERMINAL_BUFFER_MAX_ENTRIES);
    // Oldest (stale) deltas are dropped; the newest entries survive in order.
    expect(kept[0].data_base64).toBe(btoa(String(40)));
    expect(kept.at(-1).data_base64).toBe(
      btoa(String(TERMINAL_BUFFER_MAX_ENTRIES + 39)),
    );
  });

  it("drops stale deltas when the retained buffer exceeds the byte cap", () => {
    const big = btoa("x".repeat(24 * 1024)); // 32768-char base64 payload
    const bigBytes = big.length;
    const capacity = Math.floor(TERMINAL_BUFFER_MAX_BYTES / bigBytes);
    const entries = [];
    for (let i = 0; i < capacity + 2; i += 1) {
      entries.push({
        session: "s1",
        reset: false,
        data_base64: big,
      });
    }
    pushTerminalEntries(entries);
    const kept = peekTerminalBuffer("s1");
    expect(kept).toHaveLength(capacity);
    expect(kept.length * bigBytes).toBeLessThanOrEqual(
      TERMINAL_BUFFER_MAX_BYTES,
    );
  });

  it("keeps a fitting reset generation whole and invalidates an over-budget one", () => {
    // Snapshot sized so the anchor fits the budget together with a few deltas.
    const raw = DELTA_RAW_BYTES;
    const snapshot = btoa("S".repeat(raw));
    pushTerminalEntries([
      { session: "s1", reset: true, data_base64: snapshot },
      { session: "s1", reset: false, data_base64: btoa("A") },
      { session: "s1", reset: false, data_base64: btoa("B") },
    ]);
    const kept = peekTerminalBuffer("s1");
    expect(kept[0].reset).toBe(true);
    expect(kept[0].data_base64).toBe(snapshot);
    // Anchor + deltas fit: nothing trimmed.
    expect(kept.map((entry) => entry.data_base64)).toEqual([
      snapshot,
      btoa("A"),
      btoa("B"),
    ]);

    // A snapshot whose base64 payload alone exceeds the byte budget cannot be
    // retained whole, and a partial snapshot must never be replayed: the
    // whole generation is dropped and the session goes gap-invalid.
    const huge = btoa("H".repeat(TERMINAL_BUFFER_MAX_BYTES));
    pushTerminalEntries([{ session: "s2", reset: true, data_base64: huge }]);
    expect(peekTerminalBuffer("s2")).toHaveLength(0);

    // Deltas keep being discarded until a reset that fits arrives.
    pushTerminalEntries([
      { session: "s2", reset: false, data_base64: btoa("A") },
      { session: "s2", reset: false, data_base64: btoa("B") },
    ]);
    expect(peekTerminalBuffer("s2")).toHaveLength(0);
    pushTerminalEntries([
      { session: "s2", reset: true, data_base64: btoa("FRESH") },
    ]);
    expect(peekTerminalBuffer("s2").map((entry) => entry.data_base64)).toEqual([
      btoa("FRESH"),
    ]);
  });

  it("retains contiguous cursor ranges across reset and deltas", () => {
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 100, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, cursor: 100, next_cursor: 105, data_base64: btoa("A") },
      { session: "s1", reset: false, cursor: 105, next_cursor: 110, data_base64: btoa("B") },
    ]);
    const kept = peekTerminalBuffer("s1");
    expect(kept.map((entry) => entry.data_base64)).toEqual([
      btoa("SNAP"),
      btoa("A"),
      btoa("B"),
    ]);
    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry.data_base64));
    expect(received).toEqual([btoa("SNAP"), btoa("A"), btoa("B")]);
  });

  it("discards deltas once a trim creates a cursor gap until a reset re-anchors", () => {
    // Two budget-derived deltas force the byte cap to drop the bridging delta.
    const big = btoa("x".repeat(DELTA_RAW_BYTES));
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: 10,
        data_base64: btoa("SNAP"),
      },
      {
        session: "s1",
        reset: false,
        cursor: 10,
        next_cursor: 20,
        data_base64: big,
      },
    ]);
    // Anchor + first big delta still fit.
    expect(peekTerminalBuffer("s1")).toHaveLength(2);

    // The second big delta overflows: the trim drops delta 10→20, so the
    // retained chain jumps 10 → 20 and the whole stream turns gap-invalid.
    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        cursor: 20,
        next_cursor: 30,
        data_base64: big,
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    // Subsequent deltas keep being discarded, even contiguous-looking ones.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 30, next_cursor: 40, data_base64: btoa("C") },
      { session: "s1", reset: false, cursor: 40, next_cursor: 50, data_base64: btoa("D") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    // A reset re-anchors the stream and delta retention resumes.
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 50, data_base64: btoa("FRESH") },
      { session: "s1", reset: false, cursor: 50, next_cursor: 55, data_base64: btoa("E") },
    ]);
    const recovered = peekTerminalBuffer("s1");
    expect(recovered.map((entry) => entry.data_base64)).toEqual([
      btoa("FRESH"),
      btoa("E"),
    ]);
  });

  it("keeps a trimmed gap invalid after subscribe until a live reset", () => {
    const big = btoa("x".repeat(DELTA_RAW_BYTES));
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: 10,
        data_base64: btoa("SNAP"),
      },
      {
        session: "s1",
        reset: false,
        cursor: 10,
        next_cursor: 20,
        data_base64: big,
      },
      {
        session: "s1",
        reset: false,
        cursor: 20,
        next_cursor: 30,
        data_base64: big,
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry));
    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        cursor: 30,
        next_cursor: 40,
        data_base64: btoa("DROP"),
      },
    ]);
    expect(received).toHaveLength(0);

    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: 40,
        data_base64: btoa("FRESH"),
      },
      {
        session: "s1",
        reset: false,
        cursor: 40,
        next_cursor: 41,
        data_base64: btoa("A"),
      },
    ]);
    expect(received.map((entry) => entry.data_base64)).toEqual([
      btoa("FRESH"),
      btoa("A"),
    ]);
  });

  it("invalidates the retained stream on an arrival-order cursor gap", () => {
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: 100,
        data_base64: btoa("SNAP"),
      },
      // Jumps 100 → 200: data was lost before this delta arrived.
      {
        session: "s1",
        reset: false,
        cursor: 200,
        next_cursor: 205,
        data_base64: btoa("A"),
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 205, next_cursor: 210, data_base64: btoa("B") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);
  });

  it("treats a multi-frame reset snapshot as one indivisible generation", () => {
    // Runtime splits an oversized snapshot into a reset anchor plus
    // continuations; two budget-sized pieces together overflow the byte cap.
    const piece = btoa("P".repeat(DELTA_RAW_BYTES));
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: DELTA_RAW_BYTES,
        data_base64: piece,
      },
    ]);
    // Anchor alone fits the budget and is kept.
    expect(peekTerminalBuffer("s1")).toHaveLength(1);

    // The continuation overflows: the generation cannot be kept whole, so no
    // partial snapshot is retained and the stream turns gap-invalid.
    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        cursor: DELTA_RAW_BYTES,
        next_cursor: DELTA_RAW_BYTES * 2,
        data_base64: piece,
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    // Subsequent deltas stay discarded until the next full reset.
    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        cursor: DELTA_RAW_BYTES * 2,
        next_cursor: DELTA_RAW_BYTES * 2 + 5,
        data_base64: btoa("C"),
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    // A reset that fits entirely re-anchors the stream.
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 6, data_base64: btoa("FRESH") },
      { session: "s1", reset: false, cursor: 6, next_cursor: 7, data_base64: btoa("D") },
    ]);
    expect(peekTerminalBuffer("s1").map((entry) => entry.data_base64)).toEqual([
      btoa("FRESH"),
      btoa("D"),
    ]);
  });

  it("invalidates a multi-frame reset that exceeds the entry budget", () => {
    // Many small continuation frames push the retained generation over the
    // entry cap; the generation is dropped whole instead of keeping a
    // contiguous slice of the split snapshot.
    const frames = [];
    for (let i = 0; i < TERMINAL_BUFFER_MAX_ENTRIES + 16; i += 1) {
      frames.push({
        session: "s1",
        reset: i === 0,
        cursor: i,
        next_cursor: i + 1,
        data_base64: btoa("x"),
      });
    }
    pushTerminalEntries(frames);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        cursor: frames.length + 1,
        next_cursor: frames.length + 2,
        data_base64: btoa("C"),
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 4, data_base64: btoa("OK") },
      { session: "s1", reset: false, cursor: 4, next_cursor: 5, data_base64: btoa("D") },
    ]);
    expect(peekTerminalBuffer("s1").map((entry) => entry.data_base64)).toEqual([
      btoa("OK"),
      btoa("D"),
    ]);
  });

  it("never replays a partial snapshot into a late subscriber", () => {
    // The generation was dropped whole for budget, so the buffer is empty:
    // subscribing must not surface a half-snapshot into xterm.
    const piece = btoa("P".repeat(DELTA_RAW_BYTES));
    pushTerminalEntries([
      {
        session: "s1",
        reset: true,
        cursor: 0,
        next_cursor: DELTA_RAW_BYTES,
        data_base64: piece,
      },
      {
        session: "s1",
        reset: false,
        cursor: DELTA_RAW_BYTES,
        next_cursor: DELTA_RAW_BYTES * 2,
        data_base64: piece,
      },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry.data_base64));
    expect(received).toEqual([]);

    // Live deltas stay dropped while the gap is invalid; a reset restores.
    pushTerminalEntries([
      {
        session: "s1",
        reset: false,
        cursor: DELTA_RAW_BYTES * 2,
        next_cursor: DELTA_RAW_BYTES * 2 + 2,
        data_base64: btoa("A"),
      },
    ]);
    expect(received).toEqual([]);
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 5, data_base64: btoa("FRESH") },
    ]);
    expect(received).toEqual([btoa("FRESH")]);
  });

  it("delivers a contiguous live stream and drops deltas after a live cursor gap until a reset", () => {
    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry.data_base64));

    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 100, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, cursor: 100, next_cursor: 105, data_base64: btoa("A") },
      { session: "s1", reset: false, cursor: 105, next_cursor: 110, data_base64: btoa("B") },
    ]);
    // Contiguous live entries are delivered in order.
    expect(received).toEqual([btoa("SNAP"), btoa("A"), btoa("B")]);

    // A delta that jumps the last delivered cursor is dropped and poisons
    // the live stream.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 200, next_cursor: 210, data_base64: btoa("J") },
    ]);
    expect(received).toEqual([btoa("SNAP"), btoa("A"), btoa("B")]);

    // Later deltas stay suppressed, even contiguous-looking ones.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 210, next_cursor: 220, data_base64: btoa("C") },
      { session: "s1", reset: false, cursor: 220, next_cursor: 230, data_base64: btoa("D") },
    ]);
    expect(received).toEqual([btoa("SNAP"), btoa("A"), btoa("B")]);

    // A reset re-anchors the stream and live delivery resumes.
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 300, data_base64: btoa("FRESH") },
      { session: "s1", reset: false, cursor: 300, next_cursor: 305, data_base64: btoa("E") },
    ]);
    expect(received).toEqual([
      btoa("SNAP"),
      btoa("A"),
      btoa("B"),
      btoa("FRESH"),
      btoa("E"),
    ]);
  });

  it("keeps live cursor continuity across unsubscribe and resubscribe", () => {
    const first = [];
    const unsubscribe = subscribeTerminal("s1", (entry) => first.push(entry.data_base64));
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 100, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, cursor: 100, next_cursor: 110, data_base64: btoa("A") },
    ]);
    expect(first).toEqual([btoa("SNAP"), btoa("A")]);
    unsubscribe();

    // The retained stream continues from the last live next_cursor.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 110, next_cursor: 120, data_base64: btoa("B") },
      { session: "s1", reset: false, cursor: 120, next_cursor: 130, data_base64: btoa("C") },
    ]);
    expect(peekTerminalBuffer("s1").map((entry) => entry.data_base64)).toEqual([
      btoa("B"),
      btoa("C"),
    ]);

    // A fresh subscriber replays the backlog; the live delta continues it
    // instead of being gapped against the stale pre-unsubscribe anchor.
    const second = [];
    subscribeTerminal("s1", (entry) => second.push(entry.data_base64));
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 130, next_cursor: 140, data_base64: btoa("D") },
    ]);
    expect(second).toEqual([btoa("B"), btoa("C"), btoa("D")]);
  });

  it("does not retain a partial sequence after unsubscribe until a reset re-anchors", () => {
    const unsubscribe = subscribeTerminal("s1", () => {});
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 100, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, cursor: 100, next_cursor: 110, data_base64: btoa("A") },
    ]);
    unsubscribe();

    // The first retained delta must continue the last live anchor; a jump
    // would later replay a partial sequence to a subscriber.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 500, next_cursor: 510, data_base64: btoa("J") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 510, next_cursor: 520, data_base64: btoa("K") },
    ]);
    expect(peekTerminalBuffer("s1")).toHaveLength(0);

    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 520, data_base64: btoa("FRESH") },
    ]);
    expect(peekTerminalBuffer("s1").map((entry) => entry.data_base64)).toEqual([
      btoa("FRESH"),
    ]);
  });

  it("preserves the live cursor anchor across an immediate resubscribe without backlog", () => {
    const first = [];
    const unsubscribe = subscribeTerminal("s1", (entry) => first.push(entry.data_base64));
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 100, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, cursor: 100, next_cursor: 110, data_base64: btoa("A") },
    ]);
    expect(first).toEqual([btoa("SNAP"), btoa("A")]);
    unsubscribe();

    // No retained entry arrives before the immediate resubscribe: the live
    // anchor delivered to the previous listener must survive the transition.
    const second = [];
    subscribeTerminal("s1", (entry) => second.push(entry.data_base64));

    // A jumped delta must not be silently accepted as a fresh anchor.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 500, next_cursor: 510, data_base64: btoa("J") },
    ]);
    expect(second).toEqual([]);

    // Later deltas stay suppressed until a reset re-anchors delivery.
    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 510, next_cursor: 520, data_base64: btoa("K") },
    ]);
    expect(second).toEqual([]);

    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 520, data_base64: btoa("FRESH") },
      { session: "s1", reset: false, cursor: 520, next_cursor: 525, data_base64: btoa("L") },
    ]);
    expect(second).toEqual([btoa("FRESH"), btoa("L")]);
  });

  it("re-anchors the retained stream on a reset after a live phase", () => {
    const unsubscribe = subscribeTerminal("s1", () => {});
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 100, data_base64: btoa("SNAP") },
      { session: "s1", reset: false, cursor: 100, next_cursor: 110, data_base64: btoa("A") },
    ]);
    unsubscribe();

    // A retained reset starts a fresh 0-based coordinate system: it must not
    // be compared against the old live anchor (110) and rejected.
    pushTerminalEntries([
      { session: "s1", reset: true, cursor: 0, next_cursor: 200, data_base64: btoa("FRESH") },
    ]);
    expect(peekTerminalBuffer("s1").map((entry) => entry.data_base64)).toEqual([
      btoa("FRESH"),
    ]);

    // The reset is replayed to the next subscriber, and live delivery
    // resumes from its next_cursor instead of the pre-reset live anchor.
    const received = [];
    subscribeTerminal("s1", (entry) => received.push(entry.data_base64));
    expect(received).toEqual([btoa("FRESH")]);

    pushTerminalEntries([
      { session: "s1", reset: false, cursor: 200, next_cursor: 210, data_base64: btoa("B") },
    ]);
    expect(received).toEqual([btoa("FRESH"), btoa("B")]);
  });
});
