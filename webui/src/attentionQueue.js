import { activityOf } from "./sessions.js";

export const GROUP_CLASS = Object.freeze({
  ATTENTION: "attention",
  CLEAN_DONE: "clean-done",
});

// Allowlist of client states that are clearly safe for folding. Unknown or
// missing client_state must NOT be treated as safe: only explicit known-safe
// states count, everything else (disconnected, orphaned, closing, typos,
// future values, absent field) lands in attention.
const SAFE_CLIENT_STATES = new Set(["connected", "unmanaged"]);

// clean-done requires every child: no error, known-safe client state, done activity.
export function groupClass(sessions) {
  return (
    sessions.length > 0 &&
    sessions.every(
      (s) =>
        !s.error &&
        SAFE_CLIENT_STATES.has(s.client_state) &&
        activityOf(s) === "done",
    )
  )
    ? GROUP_CLASS.CLEAN_DONE
    : GROUP_CLASS.ATTENTION;
}

function ms(value) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : 0;
}

// Peak semantic activity (creation/output/hook); updated_at_ms only for the
// completion snapshot at cycle start.
function peak(sessions, withUpdated) {
  return sessions.reduce(
    (max, s) =>
      Math.max(
        max, ms(s.created_at_ms), ms(s.last_output_at_ms),
        ms(s.hook_at_ms), withUpdated ? ms(s.updated_at_ms) : 0,
      ),
    0,
  );
}
const semantic = (sessions) => peak(sessions, false);
const snapshot = (sessions) => peak(sessions, true);

function childSetId(sessions) {
  return sessions.map((s) => s.session).sort().join("\u0000");
}

function isRealSupervisor(sessions) {
  return sessions.some((s) => s.owner != null || s.client_session_id != null);
}

export function createInitialAttentionQueueState() {
  return {
    groups: {}, // key -> { childIds, class, completedAtMs }
    collapsed: new Set(),
    manualOpen: new Set(),
    manualClosed: new Set(),
    deferred: new Set(), // completion-while-focused folds, applied on blur
    focusedKey: null,
    hadAttention: false,
    hadGroups: false,
  };
}

export function attentionQueueReducer(state, action) {
  switch (action.type) {
    case "GROUP_SYNC":
      return syncGroups(state, action.groups);
    case "GROUP_FOCUS":
      return state.focusedKey === action.key ? state : { ...state, focusedKey: action.key };
    case "GROUP_BLUR":
      return blurGroup(state, action.key);
    case "GROUP_TOGGLE":
      return setAllGroups(state, [action.key], action.open);
    case "GROUP_EXPAND_ALL":
    case "GROUP_COLLAPSE_ALL":
      return setAllGroups(
        state,
        action.keys ?? [],
        action.type === "GROUP_EXPAND_ALL",
      );
    default:
      return state;
  }
}

function syncGroups(state, groups) {
  const groupsByKey = new Map(groups);
  const nextGroups = {};
  const collapsed = new Set(state.collapsed);
  const manualOpen = new Set(state.manualOpen);
  const manualClosed = new Set(state.manualClosed);
  const deferred = new Set(state.deferred);
  let focusedKey = state.focusedKey;
  let hasAttention = false;
  let structureChanged = false;

  for (const [key, sessions] of groups) {
    const childIds = childSetId(sessions);
    const klass = groupClass(sessions);
    const prev = state.groups[key];
    const newCycle =
      prev == null || prev.childIds !== childIds || prev.class !== klass;
    if (klass === GROUP_CLASS.ATTENTION) hasAttention = true;
    if (newCycle) {
      // New cycle drops cycle-scoped manual intent/deferral; actionable groups re-open.
      manualOpen.delete(key);
      manualClosed.delete(key);
      deferred.delete(key);
      structureChanged = true;
      if (klass === GROUP_CLASS.ATTENTION) collapsed.delete(key);
    }
    let completedAtMs =
      klass === GROUP_CLASS.CLEAN_DONE ? prev?.completedAtMs ?? null : null;
    if (klass === GROUP_CLASS.CLEAN_DONE && newCycle) {
      completedAtMs = snapshot(sessions); // pinned for the whole cycle
      if (focusedKey === key) deferred.add(key);
    }
    nextGroups[key] = { childIds, class: klass, completedAtMs };
  }

  // Removed groups drop their fold and manual state.
  for (const key of Object.keys(state.groups)) {
    if (nextGroups[key]) continue;
    collapsed.delete(key);
    manualOpen.delete(key);
    manualClosed.delete(key);
    deferred.delete(key);
    if (focusedKey === key) focusedKey = null;
    structureChanged = true;
  }

  const cleanDoneKeys = Object.keys(nextGroups).filter(
    (key) => nextGroups[key].class === GROUP_CLASS.CLEAN_DONE,
  );

  if (hasAttention) {
    // Attention wins: fold clean-done groups without manual intent or focus.
    for (const key of cleanDoneKeys) {
      if (!manualOpen.has(key) && focusedKey !== key) collapsed.add(key);
    }
  } else if (!state.hadGroups || state.hadAttention || structureChanged) {
    // All done: open only the most recent real supervisor group (unowned can
    // never be the exemption). Re-evaluate on initial load, when attention
    // vanishes, or on structural changes, so a blur fold is not undone by
    // plain non-semantic syncs.
    const realDone = cleanDoneKeys.filter((key) =>
      isRealSupervisor(groupsByKey.get(key) ?? []),
    );
    if (realDone.length > 0) {
      const exempt = mostRecent(realDone, nextGroups);
      if (!manualClosed.has(exempt)) collapsed.delete(exempt);
      for (const key of cleanDoneKeys) {
        if (key !== exempt && !manualOpen.has(key) && focusedKey !== key) collapsed.add(key);
      }
    } else {
      // Unowned groups never become the sole all-done exemption.
      for (const key of cleanDoneKeys) {
        if (!manualOpen.has(key) && focusedKey !== key) collapsed.add(key);
      }
    }
  }

  return {
    groups: nextGroups,
    collapsed,
    manualOpen,
    manualClosed,
    deferred,
    focusedKey,
    hadAttention: hasAttention,
    hadGroups: groups.length > 0,
  };
}

function mostRecent(keys, groups) {
  return keys.slice().sort(
    (a, b) =>
      (groups[b].completedAtMs ?? 0) - (groups[a].completedAtMs ?? 0) ||
      (a < b ? -1 : a > b ? 1 : 0),
  )[0];
}

function blurGroup(state, key) {
  if (state.focusedKey !== key) return state;
  const deferred = new Set(state.deferred);
  const pending = deferred.delete(key);
  // Deferred fold applies only while still clean-done and without manual-open.
  if (
    pending &&
    state.groups[key]?.class === GROUP_CLASS.CLEAN_DONE &&
    !state.manualOpen.has(key)
  ) {
    const collapsed = new Set(state.collapsed);
    collapsed.add(key);
    return { ...state, focusedKey: null, deferred, collapsed };
  }
  return { ...state, focusedKey: null, deferred };
}

function setAllGroups(state, keys, open) {
  const collapsed = new Set(state.collapsed);
  const manualOpen = new Set(state.manualOpen);
  const manualClosed = new Set(state.manualClosed);
  const deferred = new Set(state.deferred);
  for (const key of keys) {
    if (open) {
      collapsed.delete(key);
      manualOpen.add(key);
      manualClosed.delete(key);
    } else {
      collapsed.add(key);
      manualClosed.add(key);
      manualOpen.delete(key);
    }
    deferred.delete(key);
  }
  return { ...state, collapsed, manualOpen, manualClosed, deferred };
}

export function orderGroups(groups, state) {
  return groups
    .map(([key, sessions]) => {
      const mem = state.groups[key];
      const klass = groupClass(sessions);
      const time =
        klass === GROUP_CLASS.CLEAN_DONE
          ? mem
            ? Math.max(mem.completedAtMs ?? 0, semantic(sessions))
            : snapshot(sessions)
          : semantic(sessions);
      return { key, sessions, class: klass, time };
    })
    .sort(compareGroups)
    .map(({ key, sessions }) => [key, sessions]);
}

function compareGroups(a, b) {
  if (a.class !== b.class) return a.class === GROUP_CLASS.ATTENTION ? -1 : 1;
  if (a.time !== b.time) return b.time - a.time;
  return a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
}
