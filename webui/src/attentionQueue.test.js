import { describe, expect, it } from "vitest";
import {
  ATTENTION_ACTION,
  attentionReducer,
  createAttentionState,
  isGroupOpen,
} from "./attentionQueue.js";
import { groupSessions } from "./sessions.js";

function session(id, overrides = {}) {
  return {
    session: id,
    owner: overrides.owner ?? id,
    client_session_id: overrides.client_session_id ?? id,
    phase: "running",
    activity: "working",
    created_at_ms: 1,
    updated_at_ms: 1,
    hook_at_ms: 1,
    last_output_at_ms: null,
    client_last_seen_at_ms: 1,
    ...overrides,
  };
}

function sync(state, sessions) {
  return attentionReducer(state, {
    type: ATTENTION_ACTION.SYNC_GROUPS,
    groups: groupSessions(sessions),
    locale: "en",
  });
}

function syncFresh(sessions) {
  return sync(createAttentionState(), sessions);
}

describe("attention queue state machine", () => {
  it("keeps manual intent through heartbeats and unrelated group changes", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      hook_at_ms: 100,
    });
    const second = session("second", {
      owner: "Second",
      client_session_id: "client-second",
      hook_at_ms: 90,
    });
    let state = syncFresh([first, second]);

    state = attentionReducer(state, {
      type: ATTENTION_ACTION.TOGGLE_OWNER,
      key: "client:client-first",
      open: false,
    });
    expect(isGroupOpen(state, "client:client-first")).toBe(false);

    state = sync(state, [
      { ...first, client_last_seen_at_ms: 10_000 },
      { ...second, hook_at_ms: 110 },
    ]);
    expect(isGroupOpen(state, "client:client-first")).toBe(false);
    expect(state.order[0]).toBe("client:client-second");
  });

  it("orders a later Hook while both sessions remain working", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      hook_at_ms: 100,
    });
    const second = session("second", {
      owner: "Second",
      client_session_id: "client-second",
      hook_at_ms: 200,
    });
    let state = syncFresh([first, second]);
    expect(state.order).toEqual(["client:client-second", "client:client-first"]);

    state = sync(state, [
      { ...first, hook_at_ms: 300 },
      second,
    ]);
    expect(state.order).toEqual(["client:client-first", "client:client-second"]);
  });

  it("uses terminal output as semantic activity but ignores resize noise", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      hook_at_ms: 300,
    });
    const second = session("second", {
      owner: "Second",
      client_session_id: "client-second",
      hook_at_ms: 200,
    });
    let state = syncFresh([first, second]);
    expect(state.order[0]).toBe("client:client-first");

    state = sync(state, [
      first,
      { ...second, updated_at_ms: 900, last_output_at_ms: null },
    ]);
    expect(state.order[0]).toBe("client:client-first");

    state = sync(state, [
      first,
      { ...second, updated_at_ms: 901, last_output_at_ms: 1_000 },
    ]);
    expect(state.order[0]).toBe("client:client-second");
  });

  it("uses backend semantic and completion times instead of lease timestamps", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      semantic_active_at_ms: 300,
      completed_at_ms: 300,
      phase: "idle",
      activity: "done",
      updated_at_ms: 10_000,
    });
    const second = session("second", {
      owner: "Second",
      client_session_id: "client-second",
      semantic_active_at_ms: 200,
      completed_at_ms: 200,
      phase: "idle",
      activity: "done",
      updated_at_ms: 1,
    });
    let state = syncFresh([first, second]);
    expect(state.order[0]).toBe("client:client-first");

    state = sync(state, [
      first,
      {
        ...second,
        semantic_active_at_ms: 400,
        completed_at_ms: 400,
        phase: "idle",
        activity: "done",
        updated_at_ms: 99_999,
      },
    ]);
    expect(state.order[0]).toBe("client:client-second");
  });

  it("resets only after the group crosses attention or changes session set", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      hook_at_ms: 100,
    });
    const second = session("second", {
      owner: "Second",
      client_session_id: "client-second",
      hook_at_ms: 90,
    });
    let state = syncFresh([first, second]);

    state = attentionReducer(state, {
      type: ATTENTION_ACTION.TOGGLE_OWNER,
      key: "client:client-first",
      open: false,
    });
    state = sync(state, [
      { ...first, hook_at_ms: 120 },
      second,
    ]);
    expect(isGroupOpen(state, "client:client-first")).toBe(false);

    state = sync(state, [
      {
        ...first,
        activity: "done",
        phase: "idle",
        updated_at_ms: 200,
      },
      second,
    ]);
    expect(isGroupOpen(state, "client:client-first")).toBe(false);

    state = sync(state, [
      {
        ...first,
        activity: "working",
        phase: "running",
        hook_at_ms: 300,
      },
      second,
    ]);
    expect(isGroupOpen(state, "client:client-first")).toBe(true);

    state = sync(state, [
      {
        ...first,
        session: "first-new-child",
        hook_at_ms: 310,
      },
      second,
    ]);
    expect(isGroupOpen(state, "client:client-first")).toBe(true);
  });

  it("resets manual intent when a completed cycle changes without observing running", () => {
    const done = session("done", {
      owner: "Done",
      client_session_id: "client-done",
      phase: "idle",
      activity: "done",
      completed_at_ms: 100,
      semantic_active_at_ms: 100,
    });
    let state = syncFresh([done]);
    state = attentionReducer(state, {
      type: ATTENTION_ACTION.TOGGLE_OWNER,
      key: "client:client-done",
      open: false,
    });
    expect(isGroupOpen(state, "client:client-done")).toBe(false);

    state = sync(state, [
      {
        ...done,
        completed_at_ms: 200,
        semantic_active_at_ms: 200,
      },
    ]);
    expect(state.intents.has("client:client-done")).toBe(false);
  });

  it("resets child collapse intent with a new completed group cycle", () => {
    const parent = session("parent", {
      owner: "Owner",
      client_session_id: "client-owner",
      phase: "idle",
      activity: "done",
      completed_at_ms: 100,
    });
    const child = session("child", {
      owner: "Owner",
      client_session_id: "client-owner",
      phase: "idle",
      activity: "done",
      completed_at_ms: 100,
    });
    let state = syncFresh([parent, child]);
    state = attentionReducer(state, {
      type: ATTENTION_ACTION.TOGGLE_SESSION,
      sessionId: "child",
      open: false,
    });
    expect(state.collapsedSessions.has("child")).toBe(true);
    state = sync(state, [
      { ...parent, completed_at_ms: 200 },
      { ...child, completed_at_ms: 200 },
    ]);
    expect(state.collapsedSessions.has("child")).toBe(false);
  });

  it("defers automatic collapse while focused and applies it after focus leaves", () => {
    const focused = session("focused", {
      owner: "Focused",
      client_session_id: "client-focused",
      hook_at_ms: 200,
    });
    const other = session("other", {
      owner: "Other",
      client_session_id: "client-other",
      hook_at_ms: 100,
    });
    let state = syncFresh([focused, other]);

    state = attentionReducer(state, {
      type: ATTENTION_ACTION.FOCUS_GROUP,
      key: "client:client-focused",
    });
    state = sync(state, [
      {
        ...focused,
        activity: "done",
        phase: "idle",
        updated_at_ms: 300,
      },
      other,
    ]);
    expect(isGroupOpen(state, "client:client-focused")).toBe(true);

    state = attentionReducer(state, {
      type: ATTENTION_ACTION.FOCUS_GROUP,
      key: null,
    });
    expect(isGroupOpen(state, "client:client-focused")).toBe(false);
    expect(isGroupOpen(state, "client:client-other")).toBe(true);
  });

  it("never adds child collapse state when an owner group is auto-collapsed", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      hook_at_ms: 200,
    });
    const other = session("other", {
      owner: "Other",
      client_session_id: "client-other",
      hook_at_ms: 100,
    });
    let state = syncFresh([first, other]);
    state = sync(state, [
      {
        ...first,
        activity: "done",
        phase: "idle",
        updated_at_ms: 300,
      },
      other,
    ]);

    expect(isGroupOpen(state, "client:client-first")).toBe(false);
    expect(state.collapsedSessions).toEqual(new Set());
  });

  it("opens only the latest real supervisor by default when every group is done", () => {
    const older = session("older", {
      owner: "Older",
      client_session_id: "client-older",
      activity: "done",
      phase: "idle",
      updated_at_ms: 100,
    });
    const newer = session("newer", {
      owner: "Newer",
      client_session_id: "client-newer",
      activity: "done",
      phase: "idle",
      updated_at_ms: 200,
    });
    const unowned = session("unowned", {
      owner: null,
      client_session_id: null,
      activity: "done",
      phase: "idle",
      updated_at_ms: 300,
    });
    const state = syncFresh([older, newer, unowned]);

    expect(isGroupOpen(state, "client:client-older")).toBe(false);
    expect(isGroupOpen(state, "client:client-newer")).toBe(true);
    expect(isGroupOpen(state, "missing-owner")).toBe(false);
  });

  it("reopens the group that becomes the latest completion after reactivation", () => {
    const first = session("first", {
      owner: "First",
      client_session_id: "client-first",
      activity: "done",
      phase: "idle",
      updated_at_ms: 100,
    });
    const second = session("second", {
      owner: "Second",
      client_session_id: "client-second",
      activity: "done",
      phase: "idle",
      updated_at_ms: 200,
    });
    let state = syncFresh([first, second]);
    expect(isGroupOpen(state, "client:client-second")).toBe(true);

    state = sync(state, [
      first,
      { ...second, activity: "working", phase: "running", hook_at_ms: 300 },
    ]);
    expect(isGroupOpen(state, "client:client-second")).toBe(true);

    state = sync(state, [
      first,
      {
        ...second,
        activity: "done",
        phase: "idle",
        updated_at_ms: 400,
      },
    ]);
    expect(isGroupOpen(state, "client:client-first")).toBe(false);
    expect(isGroupOpen(state, "client:client-second")).toBe(true);
  });
});
