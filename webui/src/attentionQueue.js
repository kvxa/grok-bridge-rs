import {
  GROUP_PHASE,
  groupPhase,
  groupSemanticActiveAt,
  isRealSupervisorGroup,
  sessionSetSignature,
} from "./sessions.js";

export const ATTENTION_ACTION = Object.freeze({
  SYNC_GROUPS: "sync_groups",
  TOGGLE_OWNER: "toggle_owner",
  TOGGLE_SESSION: "toggle_session",
  SET_ALL_EXPANDED: "set_all_expanded",
  FOCUS_GROUP: "focus_group",
});

function groupEntries(groups) {
  if (groups instanceof Map) return [...groups.entries()];
  return Array.isArray(groups) ? groups : [];
}

function toGroupRecord(entry) {
  const [key, sessions] = entry;
  const normalizedSessions = Array.isArray(sessions) ? sessions : [];
  return {
    key,
    sessions: normalizedSessions,
    phase: groupPhase(normalizedSessions),
    sessionSet: sessionSetSignature(normalizedSessions),
    cycleSignature: normalizedSessions
      .map(
        (session) =>
          `${session.session}:${Number(session.completed_at_ms ?? 0)}`,
      )
      .sort()
      .join("|"),
    lastSemanticActiveAt: groupSemanticActiveAt(normalizedSessions),
    realSupervisor: isRealSupervisorGroup(normalizedSessions),
  };
}

export function attentionGroups(groups) {
  return groupEntries(groups).map(toGroupRecord);
}

export function compareAttentionGroups(left, right, locale = "en") {
  if (left.phase !== right.phase) {
    return left.phase === GROUP_PHASE.ATTENTION ? -1 : 1;
  }
  if (left.lastSemanticActiveAt !== right.lastSemanticActiveAt) {
    return right.lastSemanticActiveAt > left.lastSemanticActiveAt ? 1 : -1;
  }

  const leftOwner = String(left.sessions[0]?.owner ?? "");
  const rightOwner = String(right.sessions[0]?.owner ?? "");
  const ownerOrder = leftOwner.localeCompare(rightOwner, locale);
  if (ownerOrder !== 0) return ownerOrder;
  return String(left.key).localeCompare(String(right.key), locale);
}

export function sortAttentionGroups(groups, locale = "en") {
  return [...groups].sort((left, right) =>
    compareAttentionGroups(left, right, locale),
  );
}

function latestRealCleanDoneGroup(groups, locale = "en") {
  return sortAttentionGroups(
    groups.filter(
      (group) =>
        group.phase === GROUP_PHASE.CLEAN_DONE && group.realSupervisor,
    ),
    locale,
  )[0]?.key ?? null;
}

export function defaultGroupOpen(group, groups, locale = "en") {
  if (group.phase === GROUP_PHASE.ATTENTION) return true;
  if (groups.some((candidate) => candidate.phase === GROUP_PHASE.ATTENTION)) {
    return false;
  }
  return group.key === latestRealCleanDoneGroup(groups, locale);
}

export function createAttentionState() {
  return {
    groups: new Map(),
    order: [],
    intents: new Map(),
    collapsedSessions: new Set(),
    focusedGroupKey: null,
    locale: "en",
  };
}

function syncGroups(state, groups, locale) {
  const records = sortAttentionGroups(attentionGroups(groups), locale);
  const nextGroups = new Map(records.map((group) => [group.key, group]));
  const nextIntents = new Map();
  const resetCollapsedSessions = new Set();

  for (const group of records) {
    const previous = state.groups.get(group.key);
    const intent = state.intents.get(group.key);
    if (
      previous &&
      previous.phase === group.phase &&
      previous.sessionSet === group.sessionSet &&
      previous.cycleSignature === group.cycleSignature
    ) {
      if (intent) nextIntents.set(group.key, intent);
    } else if (previous) {
      for (const session of group.sessions) {
        resetCollapsedSessions.add(session.session);
      }
    }
  }

  const sessionIds = new Set(
    records.flatMap((group) => group.sessions.map((session) => session.session)),
  );
  const nextCollapsedSessions = new Set(
    [...state.collapsedSessions].filter(
      (session) =>
        sessionIds.has(session) && !resetCollapsedSessions.has(session),
    ),
  );

  return {
    ...state,
    groups: nextGroups,
    order: records.map((group) => group.key),
    intents: nextIntents,
    collapsedSessions: nextCollapsedSessions,
    focusedGroupKey: nextGroups.has(state.focusedGroupKey)
      ? state.focusedGroupKey
      : null,
    locale,
  };
}

export function attentionReducer(state, action) {
  switch (action.type) {
    case ATTENTION_ACTION.SYNC_GROUPS:
      return syncGroups(state, action.groups, action.locale ?? state.locale);

    case ATTENTION_ACTION.TOGGLE_OWNER: {
      if (!state.groups.has(action.key)) return state;
      const intents = new Map(state.intents);
      intents.set(action.key, { open: Boolean(action.open) });
      return { ...state, intents };
    }

    case ATTENTION_ACTION.TOGGLE_SESSION: {
      const collapsedSessions = new Set(state.collapsedSessions);
      if (action.open) collapsedSessions.delete(action.sessionId);
      else collapsedSessions.add(action.sessionId);
      return { ...state, collapsedSessions };
    }

    case ATTENTION_ACTION.SET_ALL_EXPANDED: {
      const intents = new Map(state.intents);
      for (const key of action.groupKeys ?? state.order) {
        if (state.groups.has(key)) {
          intents.set(key, { open: Boolean(action.open) });
        }
      }
      const collapsedSessions = action.open
        ? new Set()
        : new Set(action.sessionIds ?? []);
      return { ...state, intents, collapsedSessions };
    }

    case ATTENTION_ACTION.FOCUS_GROUP: {
      const focusedGroupKey =
        action.key && state.groups.has(action.key) ? action.key : null;
      if (focusedGroupKey === state.focusedGroupKey) return state;
      return { ...state, focusedGroupKey };
    }

    default:
      return state;
  }
}

export function isGroupOpen(state, key) {
  const group = state.groups.get(key);
  if (!group) return false;

  const intent = state.intents.get(key);
  if (intent) return intent.open;
  if (state.focusedGroupKey === key) return true;
  return defaultGroupOpen(group, [...state.groups.values()], state.locale);
}
