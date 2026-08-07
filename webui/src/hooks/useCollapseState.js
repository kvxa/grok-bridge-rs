import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useReducer,
} from "react";
import {
  ATTENTION_ACTION,
  attentionReducer,
  createAttentionState,
  isGroupOpen,
} from "../attentionQueue.js";

export {
  ATTENTION_ACTION,
  attentionReducer,
  createAttentionState,
  isGroupOpen,
};

/**
 * Derive the attention view for the current groups/focus without waiting for
 * an effect. First non-empty groups render must apply default open policy —
 * never paint one frame with empty state (all groups expanded).
 */
function deriveViewState(state, groups, locale, focusedGroupKey) {
  let next = attentionReducer(state, {
    type: ATTENTION_ACTION.SYNC_GROUPS,
    groups,
    locale,
  });
  next = attentionReducer(next, {
    type: ATTENTION_ACTION.FOCUS_GROUP,
    key: focusedGroupKey,
  });
  return next;
}

export function useCollapseState(
  groups = [],
  sessions = [],
  locale = "en",
  focusedGroupKey = null,
) {
  const [state, dispatch] = useReducer(
    attentionReducer,
    undefined,
    createAttentionState,
  );

  // Persist groups into reducer before paint so toggle/setAll see real keys.
  // Render path still uses viewState so even the first commit is correct.
  useLayoutEffect(() => {
    dispatch({
      type: ATTENTION_ACTION.SYNC_GROUPS,
      groups,
      locale,
    });
  }, [groups, locale]);

  useEffect(() => {
    dispatch({
      type: ATTENTION_ACTION.FOCUS_GROUP,
      key: focusedGroupKey,
    });
  }, [focusedGroupKey]);
  const viewState = useMemo(
    () => deriveViewState(state, groups, locale, focusedGroupKey),
    [state, groups, locale, focusedGroupKey],
  );

  const collapsedOwners = useMemo(() => {
    const collapsed = new Set();
    for (const key of viewState.order) {
      if (!isGroupOpen(viewState, key)) collapsed.add(key);
    }
    return collapsed;
  }, [viewState]);

  const toggleOwner = useCallback((key, open) => {
    dispatch({
      type: ATTENTION_ACTION.TOGGLE_OWNER,
      key,
      open,
    });
  }, []);

  const toggleSession = useCallback((sessionId, open) => {
    dispatch({
      type: ATTENTION_ACTION.TOGGLE_SESSION,
      sessionId,
      open,
    });
  }, []);

  const setAllExpanded = useCallback(
    (open, currentGroups = groups, currentSessions = sessions) => {
      dispatch({
        type: ATTENTION_ACTION.SET_ALL_EXPANDED,
        open,
        groupKeys: currentGroups.map(([key]) => key),
        sessionIds: currentSessions.map((session) => session.session),
      });
    },
    [groups, sessions],
  );

  return {
    collapsedOwners,
    collapsedSessions: viewState.collapsedSessions,
    toggleOwner,
    toggleSession,
    setAllExpanded,
    attentionState: viewState,
  };
}
