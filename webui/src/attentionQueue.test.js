import { describe, expect, test } from "vitest";
import {
  GROUP_CLASS,
  attentionQueueReducer,
  createInitialAttentionQueueState,
  groupClass,
  orderGroups,
} from "./attentionQueue.js";

const { ATTENTION, CLEAN_DONE } = GROUP_CLASS;

// Default session is clean-done: idle phase, no activity, connected client.
function session(overrides) {
  return {
    session: "s",
    owner: "alice",
    phase: "idle",
    activity: "unknown",
    client_state: "connected",
    created_at_ms: 100,
    last_output_at_ms: 100,
    hook_at_ms: 0,
    updated_at_ms: 100,
    ...overrides,
  };
}
const done = (o) => session(o);
const working = (o) => session({ phase: "running", ...o });

const initial = () => createInitialAttentionQueueState();
const sync = (state, groups) =>
  attentionQueueReducer(state, { type: "GROUP_SYNC", groups });
const focus = (state, key) =>
  attentionQueueReducer(state, { type: "GROUP_FOCUS", key });
const blur = (state, key) =>
  attentionQueueReducer(state, { type: "GROUP_BLUR", key });
const toggle = (state, key, open) =>
  attentionQueueReducer(state, { type: "GROUP_TOGGLE", key, open });
const order = (state, groups) => orderGroups(groups, state).map(([key]) => key);

describe("groupClass", () => {
  test("every child clean, connected and idle/done is CLEAN_DONE", () => {
    expect(groupClass([done()])).toBe(CLEAN_DONE);
    expect(groupClass([done(), done()])).toBe(CLEAN_DONE);
  });
  test("unmanaged (known non-risk lease state) is CLEAN_DONE when done", () => {
    expect(groupClass([done({ client_state: "unmanaged" })])).toBe(CLEAN_DONE);
  });
  test.each([
    ["error", { error: "boom" }],
    ["stopped phase", { phase: "failed" }],
    ["waiting activity", { phase: "idle", activity: "waiting" }],
    ["unknown activity", { phase: "unknown", activity: "unknown" }],
    ["disconnected", { client_state: "disconnected" }],
    ["orphaned", { client_state: "orphaned" }],
    ["closing", { client_state: "closing" }],
    ["missing client_state", { client_state: undefined }],
    ["unknown client_state", { client_state: "suspicious-lease-state" }],
    ["working", { phase: "running" }],
    ["empty group", null],
  ])("%s -> ATTENTION", (_, o) => {
    expect(groupClass(o ? [session(o)] : [])).toBe(ATTENTION);
  });
  test("regression: a child with missing client_state forces the whole group to ATTENTION", () => {
    expect(groupClass([done({ client_state: undefined }), done()])).toBe(
      ATTENTION,
    );
  });
  test("regression: a child with unknown client_state forces the whole group to ATTENTION", () => {
    expect(groupClass([done({ client_state: "pending-lease" }), done()])).toBe(
      ATTENTION,
    );
  });
});

describe("orderGroups", () => {
  test("attention first; same class by semantic time desc, key asc", () => {
    const gs = [
      ["a", [done({ last_output_at_ms: 100 })]],
      ["b", [working({ last_output_at_ms: 300 })]],
      ["c", [done({ last_output_at_ms: 300 })]],
      ["d", [done({ last_output_at_ms: 300 })]],
      ["e", [working({ last_output_at_ms: 500 })]],
    ];
    expect(order(initial(), gs)).toEqual(["e", "b", "c", "d", "a"]);
  });

  test("clean-done cycle: semantic advances reorder, updated_at_ms noise does not", () => {
    const a = { last_output_at_ms: 100, updated_at_ms: 100 };
    const b = { last_output_at_ms: 200, updated_at_ms: 100 };
    const gs = (x, y) => [["a", [done(x)]], ["b", [done(y)]]];
    let s = sync(initial(), gs(a, b));
    expect(order(s, gs(a, b))).toEqual(["b", "a"]);
    // updated_at_ms noise on the older group cannot reorder the pinned snapshot
    s = sync(s, gs({ ...a, updated_at_ms: 999 }, b));
    expect(order(s, gs({ ...a, updated_at_ms: 999 }, b))).toEqual(["b", "a"]);
    // created_at_ms / last_output_at_ms / hook_at_ms advances do reorder
    s = sync(s, gs({ ...a, hook_at_ms: 999 }, b));
    expect(order(s, gs({ ...a, hook_at_ms: 999 }, b))).toEqual(["a", "b"]);
    s = sync(s, gs({ ...a, last_output_at_ms: 800 }, b));
    expect(order(s, gs({ ...a, last_output_at_ms: 800 }, b))).toEqual(["a", "b"]);
    s = sync(s, gs({ ...a, created_at_ms: 900 }, b));
    expect(order(s, gs({ ...a, created_at_ms: 900 }, b))).toEqual(["a", "b"]);
  });
});

describe("auto-fold", () => {
  test("attention folds clean-done; when attention clears the most recent opens", () => {
    let s = sync(initial(), [["a", [working()]], ["b", [done({ last_output_at_ms: 500 })]]]);
    expect(s.collapsed.has("b")).toBe(true);
    s = sync(s, [["a", [done()]], ["b", [done({ last_output_at_ms: 500 })]]]);
    expect(s.collapsed.has("b")).toBe(false);
    expect(s.collapsed.has("a")).toBe(true);
  });

  test("regression: a folded clean-done group re-opens when it becomes attention", () => {
    let s = sync(initial(), [["a", [working()]], ["b", [done()]]]);
    expect(s.collapsed.has("b")).toBe(true);
    s = sync(s, [["a", [working()]], ["b", [session({ error: "boom" })]]]);
    expect(s.collapsed.has("b")).toBe(false);
  });

  test("all done: only the most recent real supervisor opens; unowned never exempt", () => {
    const real = (id, t) => done({ session: id, last_output_at_ms: t });
    const unowned = (id, t) =>
      done({ session: id, owner: null, client_session_id: null, last_output_at_ms: t });
    const s = sync(initial(), [
      ["u", [unowned("1", 4000)]],
      ["r", [real("2", 3000)]],
      ["o", [real("3", 1000)]],
    ]);
    expect(s.collapsed.has("r")).toBe(false);
    expect([...s.collapsed].sort()).toEqual(["o", "u"]);
  });

  test("all-done unowned groups receive no open exemption", () => {
    const unowned = (id, t) =>
      done({ session: id, owner: null, client_session_id: null, last_output_at_ms: t });
    const s = sync(initial(), [
      ["u1", [unowned("1", 100)]],
      ["u2", [unowned("2", 200)]],
    ]);
    expect([...s.collapsed].sort()).toEqual(["u1", "u2"]);
  });

  test("new clean group re-evaluates the unique exemption", () => {
    let s = sync(initial(), [["a", [done({ last_output_at_ms: 100 })]], ["b", [done({ last_output_at_ms: 200 })]]]);
    expect(s.collapsed.has("b")).toBe(false);
    s = sync(s, [["a", [done({ last_output_at_ms: 100 })]], ["b", [done({ last_output_at_ms: 200 })]], ["c", [done({ last_output_at_ms: 300 })]]]);
    expect(s.collapsed.has("c")).toBe(false);
    expect(s.collapsed.has("b")).toBe(true);
  });

  test("child-set change starts a new cycle and re-evaluates the exemption", () => {
    let s = sync(initial(), [["a", [done({ session: "1", last_output_at_ms: 100 })]], ["b", [done({ session: "2", last_output_at_ms: 200 })]]]);
    expect(s.collapsed.has("b")).toBe(false);
    s = sync(s, [
      ["a", [done({ session: "1", last_output_at_ms: 100 }), done({ session: "3", last_output_at_ms: 400 })]],
      ["b", [done({ session: "2", last_output_at_ms: 200 })]],
    ]);
    expect(s.collapsed.has("a")).toBe(false);
    expect(s.collapsed.has("b")).toBe(true);
  });

  test("regression: plain sync after a blur fold does not re-open the group", () => {
    let s = sync(initial(), [["a", [done({ last_output_at_ms: 100 })]], ["b", [working({ last_output_at_ms: 200 })]]]);
    s = focus(s, "b");
    s = sync(s, [["a", [done({ last_output_at_ms: 100 })]], ["b", [done({ last_output_at_ms: 200 })]]]);
    s = blur(s, "b");
    expect(s.collapsed.has("b")).toBe(true);
    s = sync(s, [["a", [done({ last_output_at_ms: 100 })]], ["b", [done({ last_output_at_ms: 200 })]]]);
    expect(s.collapsed.has("b")).toBe(true);
  });
});

describe("manual intent", () => {
  test("manual open survives noise but is cleared by a new cycle", () => {
    let s = sync(initial(), [["a", [done()]], ["b", [working()]]]);
    s = toggle(s, "a", true);
    s = sync(s, [["a", [done({ updated_at_ms: 999 })]], ["b", [working()]]]);
    expect(s.collapsed.has("a")).toBe(false);
    s = sync(s, [
      ["a", [done({ session: "1" }), done({ session: "2" })]],
      ["b", [working()]],
    ]);
    expect(s.collapsed.has("a")).toBe(true);
    expect(s.manualOpen.has("a")).toBe(false);
  });

  test("expand-all/collapse-all are explicit and protected across noise syncs", () => {
    let s = sync(initial(), [["a", [done()]], ["b", [working()]]]);
    s = attentionQueueReducer(s, { type: "GROUP_COLLAPSE_ALL", keys: ["a", "b"] });
    s = sync(s, [["a", [done({ updated_at_ms: 999 })]], ["b", [working()]]]);
    expect(s.collapsed.has("a") && s.collapsed.has("b")).toBe(true);
    s = attentionQueueReducer(s, { type: "GROUP_EXPAND_ALL", keys: ["a", "b"] });
    expect(s.collapsed.size).toBe(0);
  });
});

describe("focus deferral", () => {
  test("completion while focused stays open until blur", () => {
    let s = sync(initial(), [["a", [working()]]]);
    s = focus(s, "a");
    s = sync(s, [["a", [done()]]]);
    expect(s.collapsed.has("a")).toBe(false);
    expect(s.deferred.has("a")).toBe(true);
    s = sync(s, [["a", [done({ updated_at_ms: 999 })]]]);
    expect(s.deferred.has("a")).toBe(true);
    s = blur(s, "a");
    expect(s.collapsed.has("a")).toBe(true);
  });

  test("blur does not fold once the group left clean-done", () => {
    let s = sync(initial(), [["a", [working()]]]);
    s = focus(s, "a");
    s = sync(s, [["a", [done()]]]);
    s = sync(s, [["a", [session({ error: "boom" })]]]);
    s = blur(s, "a");
    expect(s.collapsed.has("a")).toBe(false);
  });

  test("blur does not fold a manually opened group", () => {
    let s = sync(initial(), [["a", [working()]]]);
    s = focus(s, "a");
    s = sync(s, [["a", [done()]]]);
    s = toggle(s, "a", true);
    s = blur(s, "a");
    expect(s.collapsed.has("a")).toBe(false);
  });
});

test("reducer owns group folds only; per-child folding is untouched", () => {
  const s = sync(initial(), [["a", [done(), working()]]]);
  expect(s).not.toHaveProperty("collapsedSessions");
});
