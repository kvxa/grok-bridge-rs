export const STOPPED_PHASES = new Set(["exited", "failed", "stopped"]);

export function activityOf(session) {
  if (STOPPED_PHASES.has(session.phase)) return "stopped";
  if (session.activity && session.activity !== "unknown") {
    return session.activity;
  }
  if (session.phase === "idle") return "done";
  if (["starting", "running"].includes(session.phase)) return "working";
  return "unknown";
}

export function activityLabel(activity) {
  return (
    {
      working: "工作中",
      waiting: "等待输入",
      done: "已完成",
      stopped: "已退出",
      unknown: "状态未知",
    }[activity] ?? activity
  );
}

export function ownerKey(owner) {
  return owner == null ? "missing-owner" : `owner:${owner}`;
}

export function groupSessions(sessions) {
  const grouped = new Map();
  for (const session of sessions) {
    const owner = session.owner ?? null;
    if (!grouped.has(owner)) grouped.set(owner, []);
    grouped.get(owner).push(session);
  }
  return [...grouped.entries()].sort(([left], [right]) =>
    String(left ?? "").localeCompare(String(right ?? ""), "zh-CN"),
  );
}

export function sessionStats(sessions) {
  const activities = sessions.map(activityOf);
  return {
    owners: new Set(sessions.map((session) => session.owner ?? null)).size,
    sessions: sessions.length,
    working: activities.filter((activity) => activity === "working").length,
    waiting: activities.filter((activity) => activity === "waiting").length,
    done: activities.filter((activity) => activity === "done").length,
  };
}

export function groupSummary(sessions) {
  const counts = { working: 0, waiting: 0, done: 0 };
  for (const session of sessions) {
    const activity = activityOf(session);
    if (activity in counts) counts[activity] += 1;
  }
  return [
    counts.working && `${counts.working} 个工作中`,
    counts.waiting && `${counts.waiting} 个等待输入`,
    counts.done && `${counts.done} 个完成/空闲`,
  ]
    .filter(Boolean)
    .join(" · ") || "无可用状态";
}

export function ageLabel(updatedAt, now = Date.now()) {
  const seconds = Math.max(0, Math.floor((now - updatedAt) / 1000));
  if (seconds < 60) return `${seconds} 秒前`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分钟前`;
  return `${Math.floor(seconds / 3600)} 小时前`;
}

export function sessionsSignature(sessions) {
  return JSON.stringify(
    sessions.map((session) => [
      session.session,
      session.owner,
      session.phase,
      session.title,
      session.cwd,
      session.process_id,
      session.updated_at_ms,
      session.activity,
      session.hook_event,
      session.hook_at_ms,
      session.tool_name,
      session.waiting_reason,
      session.screen,
    ]),
  );
}
