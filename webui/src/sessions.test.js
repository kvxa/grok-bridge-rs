import { describe, expect, it } from "vitest";
import {
  activityOf,
  groupSessions,
  groupSummary,
  ownerKey,
  sessionStats,
} from "./sessions.js";

describe("activityOf", () => {
  it("lets terminal PTY phases override stale hook activity", () => {
    for (const phase of ["exited", "failed", "stopped"]) {
      expect(activityOf({ phase, activity: "working" })).toBe("stopped");
      expect(activityOf({ phase, activity: "waiting" })).toBe("stopped");
    }
  });

  it("uses hooks before live phase fallbacks", () => {
    expect(activityOf({ phase: "running", activity: "waiting" })).toBe(
      "waiting",
    );
    expect(activityOf({ phase: "idle", activity: "unknown" })).toBe("done");
    expect(activityOf({ phase: "starting" })).toBe("working");
  });
});

describe("session grouping", () => {
  const sessions = [
    { session: "b", owner: "Codex B", phase: "idle", activity: "done" },
    { session: "missing", owner: null, phase: "running", activity: "working" },
    { session: "a", owner: "Codex A", phase: "running", activity: "waiting" },
    { session: "a2", owner: "Codex A", phase: "exited", activity: "done" },
  ];

  it("sorts owner groups and keeps a distinct missing-owner key", () => {
    const groups = groupSessions(sessions);
    expect(groups.map(([owner]) => owner)).toEqual([null, "Codex A", "Codex B"]);
    expect(groups[1][1]).toHaveLength(2);
    expect(ownerKey(null)).not.toBe(ownerKey("missing-owner"));
  });

  it("computes headline and per-group status counts", () => {
    expect(sessionStats(sessions)).toEqual({
      owners: 3,
      sessions: 4,
      working: 1,
      waiting: 1,
      done: 1,
    });
    expect(groupSummary(groupSessions(sessions)[1][1])).toBe("1 个等待输入");
  });
});
