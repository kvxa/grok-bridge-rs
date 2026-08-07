import {
  formatAge,
  formatCountdown,
  formatDurationMs,
  formatNumber,
  formatRemaining,
} from "./i18n/format.js";
import { createTranslator } from "./i18n/translate.js";

export const STOPPED_PHASES = new Set(["exited", "failed", "stopped"]);
export const GROUP_PHASE = Object.freeze({
  ATTENTION: "attention",
  CLEAN_DONE: "clean-done",
});

const CLEAN_DONE_ACTIVITIES = new Set(["done", "stopped"]);

export function activityOf(session) {
  if (STOPPED_PHASES.has(session.phase)) return "stopped";
  // Cancelling is not clean-done; keep it out of the done bucket.
  if (session.activity === "cancelling") return "working";
  if (session.activity && session.activity !== "unknown") {
    return session.activity;
  }
  if (session.phase === "idle") return "done";
  if (["starting", "running"].includes(session.phase)) return "working";
  return "unknown";
}

export function activityLabel(activity, t = createTranslator("en")) {
  const key = {
    working: "activity.working",
    waiting: "activity.waiting",
    done: "activity.done",
    stopped: "activity.stopped",
    unknown: "activity.unknown",
  }[activity];
  return key ? t(key) : activity;
}

export function ownerKey(owner) {
  return owner == null ? "missing-owner" : `owner:${owner}`;
}

export function sessionGroupKey(session) {
  return session.client_session_id
    ? `client:${session.client_session_id}`
    : ownerKey(session.owner ?? null);
}

export function isCleanDoneActivity(activity) {
  return CLEAN_DONE_ACTIVITIES.has(activity);
}

export function groupPhase(sessions) {
  return sessions.length > 0 &&
    sessions.every((session) => isCleanDoneActivity(activityOf(session)))
    ? GROUP_PHASE.CLEAN_DONE
    : GROUP_PHASE.ATTENTION;
}

/** Session timestamps that represent work, output, or completion, not lease noise. */
export function sessionSemanticActiveAt(session) {
  // New runtimes publish a timestamp that excludes lease/control/resize noise.
  // completed_at_ms orders clean-done cycles without treating later bookkeeping
  // as a new completion. Keep legacy field fallbacks for older Runtime binaries.
  if (
    typeof session.semantic_active_at_ms === "number" &&
    Number.isFinite(session.semantic_active_at_ms)
  ) {
    const values = [session.semantic_active_at_ms, session.created_at_ms];
    if (isCleanDoneActivity(activityOf(session))) {
      values.push(session.completed_at_ms);
    }
    return values.reduce((latest, value) => {
      if (typeof value !== "number" || !Number.isFinite(value)) return latest;
      return Math.max(latest, value);
    }, 0);
  }

  const values = [session.hook_at_ms, session.last_output_at_ms];
  if (isCleanDoneActivity(activityOf(session))) values.push(session.updated_at_ms);
  values.push(session.created_at_ms);

  return values.reduce((latest, value) => {
    if (typeof value !== "number" || !Number.isFinite(value)) return latest;
    return Math.max(latest, value);
  }, 0);
}

export function groupSemanticActiveAt(sessions) {
  return sessions.reduce(
    (latest, session) => Math.max(latest, sessionSemanticActiveAt(session)),
    0,
  );
}

export function sessionSetSignature(sessions) {
  return JSON.stringify(
    sessions
      .map((session) => session.session)
      .filter((session) => typeof session === "string")
      .sort(),
  );
}

export function isRealSupervisorGroup(sessions) {
  return sessions.some(
    (session) =>
      typeof session.client_session_id === "string" &&
      session.client_session_id.length > 0,
  );
}

export function compareGroupEntries(left, right, locale = "en") {
  const leftSessions = left[1];
  const rightSessions = right[1];
  const leftPhase = groupPhase(leftSessions);
  const rightPhase = groupPhase(rightSessions);
  if (leftPhase !== rightPhase) {
    return leftPhase === GROUP_PHASE.ATTENTION ? -1 : 1;
  }

  const leftActiveAt = groupSemanticActiveAt(leftSessions);
  const rightActiveAt = groupSemanticActiveAt(rightSessions);
  if (leftActiveAt !== rightActiveAt) {
    return rightActiveAt > leftActiveAt ? 1 : -1;
  }

  const ownerOrder = String(leftSessions[0]?.owner ?? "").localeCompare(
    String(rightSessions[0]?.owner ?? ""),
    locale,
  );
  if (ownerOrder !== 0) return ownerOrder;
  return String(left[0]).localeCompare(String(right[0]), locale);
}

export function groupSessions(sessions, locale = "en") {
  const grouped = new Map();
  for (const session of sessions) {
    const key = sessionGroupKey(session);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push(session);
  }
  return [...grouped.entries()].sort((left, right) =>
    compareGroupEntries(left, right, locale),
  );
}

export function sessionStats(sessions) {
  const activities = sessions.map(activityOf);
  return {
    owners: new Set(sessions.map(sessionGroupKey)).size,
    sessions: sessions.length,
    working: activities.filter((activity) => activity === "working").length,
    waiting: activities.filter((activity) => activity === "waiting").length,
    done: activities.filter((activity) => activity === "done").length,
  };
}

export function clientStateLabel(state, t = createTranslator("en")) {
  const key = {
    unmanaged: "client.unmanaged",
    connected: "client.connected",
    disconnected: "client.disconnected",
    orphaned: "client.orphaned",
    web_controlled: "client.webControlled",
    closing: "client.closing",
  }[state];
  return key ? t(key) : t("client.unknown");
}

/** Visual lifecycle for Codex lease / cleanup pipeline. */
export function clientLifecycle(state) {
  return (
    {
      unmanaged: "unmanaged",
      connected: "connected",
      disconnected: "disconnected",
      web_controlled: "web_held",
      orphaned: "cleanup",
      closing: "cleanup",
    }[state] ?? "unknown"
  );
}

export function clientLifecycleLabel(state, t = createTranslator("en")) {
  const key = {
    unmanaged: "lifecycle.unmanaged",
    connected: "lifecycle.connected",
    disconnected: "lifecycle.disconnected",
    web_controlled: "lifecycle.webControlled",
    orphaned: "lifecycle.orphaned",
    closing: "lifecycle.closing",
  }[state];
  return key ? t(key) : t("lifecycle.unknown");
}

export function dominantClientState(sessions) {
  const priority = [
    "closing",
    "orphaned",
    "web_controlled",
    "disconnected",
    "connected",
    "unmanaged",
  ];
  for (const state of priority) {
    if (sessions.some((session) => session.client_state === state)) {
      return state;
    }
  }
  return sessions[0]?.client_state ?? "unmanaged";
}

export function groupSummary(
  sessions,
  t = createTranslator("en"),
  locale = "en",
) {
  const counts = { working: 0, waiting: 0, done: 0 };
  for (const session of sessions) {
    const activity = activityOf(session);
    if (activity in counts) counts[activity] += 1;
  }
  const n = (value) => formatNumber(value, locale);
  return (
    [
      counts.working &&
        t("group.summary.working", { n: n(counts.working) }),
      counts.waiting &&
        t("group.summary.waiting", { n: n(counts.waiting) }),
      counts.done && t("group.summary.done", { n: n(counts.done) }),
    ]
      .filter(Boolean)
      .join(t("group.summary.sep")) || t("group.summary.none")
  );
}

export function ageLabel(updatedAt, now = Date.now(), locale = "en") {
  return formatAge(updatedAt, now, locale);
}

export function remainingLabel(deadline, now = Date.now(), locale = "en") {
  return formatRemaining(deadline, now, locale);
}

export function countdownLabel(deadline, now = Date.now(), locale = "en") {
  return formatCountdown(deadline, now, locale);
}

export function durationMsLabel(ms, locale = "en") {
  return formatDurationMs(ms, locale);
}

/** Optional wire ms field: accept only finite non-negative numbers. */
export function optionalDurationMs(value) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    return null;
  }
  return value;
}

/**
 * Pure lifecycle model for managed-session auto-close UI.
 * Unmanaged and connected return kind "none" (no keep-alive / lease prose).
 * Disconnected keeps a compact offline notice; orphaned/closing are risk states.
 * Missing deadline fields stay null — never invent defaults.
 */
export function lifecycleHintModel(session) {
  const state = session?.client_state;
  if (!state || state === "unmanaged" || state === "connected") {
    return { kind: "none" };
  }
  const deadlineMs = optionalDurationMs(session.auto_close_at_ms);

  if (state === "disconnected") {
    return { kind: "disconnected" };
  }
  // Codex offline but interactive WebUI write control is temporarily holding.
  if (state === "web_controlled") {
    return { kind: "web_controlled" };
  }
  if (state === "orphaned") {
    return { kind: "orphaned", deadlineMs };
  }
  if (state === "closing") {
    return { kind: "closing" };
  }
  return { kind: "none" };
}

/**
 * Short risk line for the collapsed summary row.
 * Only orphaned/closing produce text so the fold still shows auto-close risk.
 */
export function lifecycleCollapsedSummary(
  session,
  t = createTranslator("en"),
  locale = "en",
  now = Date.now(),
) {
  const model = lifecycleHintModel(session);
  if (model.kind === "closing") {
    return t("session.lifecycle.collapsedClosing");
  }
  if (model.kind === "orphaned") {
    if (model.deadlineMs == null) {
      return t("session.lifecycle.collapsedOrphanedUnknown");
    }
    if (now >= model.deadlineMs) {
      return t("session.lifecycle.collapsedOrphanedDue");
    }
    return t("session.lifecycle.collapsedOrphaned", {
      remaining: formatCountdown(model.deadlineMs, now, locale),
    });
  }
  return null;
}

export function sessionsSignature(sessions) {
  // Terminal bytes stream separately; signature tracks metadata only.
  return JSON.stringify(
    sessions.map((session) => [
      session.session,
      session.owner,
      session.client_session_id,
      session.client_state,
      session.client_lease_ms,
      session.orphan_grace_ms,
      session.client_last_seen_at_ms,
      session.orphaned_at_ms,
      session.auto_close_at_ms,
      session.phase,
      session.title,
      session.cwd,
      session.process_id,
      session.updated_at_ms,
      session.semantic_active_at_ms,
      session.completed_at_ms,
      session.last_output_at_ms,
      session.activity,
      session.hook_event,
      session.hook_at_ms,
      session.tool_name,
      session.waiting_reason,
      session.rows,
      session.cols,
    ]),
  );
}
