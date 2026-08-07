import { useCallback, useState } from "react";

/**
 * Manual collapse state for individual child sessions only.
 * Group folding is owned by the attention-queue reducer so automatic folds,
 * manual group intent, and focus deferral live in one place.
 */
export function useCollapseState() {
  const [collapsedSessions, setCollapsedSessions] = useState(() => new Set());

  const toggleSession = useCallback((sessionId, open) => {
    setCollapsedSessions((current) => {
      const next = new Set(current);
      if (open) next.delete(sessionId);
      else next.add(sessionId);
      return next;
    });
  }, []);

  /** Explicit global expand/collapse of every child session. */
  const setAllSessionsExpanded = useCallback((open, sessions) => {
    setCollapsedSessions(
      open ? new Set() : new Set(sessions.map((session) => session.session)),
    );
  }, []);

  return {
    collapsedSessions,
    toggleSession,
    setAllSessionsExpanded,
  };
}
