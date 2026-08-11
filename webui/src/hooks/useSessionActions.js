import { useCallback } from "react";
import {
  closeClientRequest,
  closeOwnerRequest,
  closeSessionRequest,
} from "../api.js";
import { useI18n } from "../i18n/index.js";
import { errorMessage } from "../utils/errors.js";

/**
 * Close actions POST to the Runtime and rely on the WebSocket push for state.
 * They intentionally do not force a GET /api/sessions refresh.
 */
export function useSessionActions({ loadingRef, setLoading, setNotice }) {
  const { t, formatNumber } = useI18n();

  const closeSession = useCallback(
    async (id) => {
      if (loadingRef.current) return;
      if (!window.confirm(t("action.confirmCloseSession", { id }))) return;
      loadingRef.current = true;
      setLoading(true);
      try {
        await closeSessionRequest(id);
        setNotice({
          tone: "info",
          text: t("action.closedSession", { id }),
        });
      } catch (error) {
        setNotice({
          tone: "error",
          text: t("action.closeFailed", {
            detail: errorMessage(error, t),
          }),
        });
      } finally {
        loadingRef.current = false;
        setLoading(false);
      }
    },
    [loadingRef, setLoading, setNotice, t],
  );

  const closeGroup = useCallback(
    async (owner, clientSessionId, count) => {
      if (loadingRef.current) return;
      const displayOwner = owner ?? clientSessionId;
      if (
        !window.confirm(
          t("action.confirmCloseGroup", {
            owner: displayOwner,
            count: formatNumber(count),
          }),
        )
      ) {
        return;
      }
      loadingRef.current = true;
      setLoading(true);
      try {
        const result = clientSessionId
          ? await closeClientRequest(clientSessionId)
          : await closeOwnerRequest(owner);
        const timedOut = result.timed_out || [];
        const failures = result.failures || [];
        if (result.matched === 0) {
          setNotice({
            tone: "info",
            text: t("action.groupEmpty"),
          });
        } else if (
          failures.length ||
          timedOut.length ||
          result.closed !== result.matched
        ) {
          // Partial group close: surface both timed-out and failed handles so
          // the operation is never presented as fully successful.
          setNotice({
            tone: "error",
            text: t("action.groupPartial", {
              closed: formatNumber(result.closed),
              matched: formatNumber(result.matched),
              failures: [...timedOut, ...failures].join(t("action.failureJoin")),
            }),
          });
        } else {
          setNotice({
            tone: "info",
            text: t("action.groupClosed", {
              owner: displayOwner,
              count: formatNumber(result.closed),
            }),
          });
        }
      } catch (error) {
        setNotice({
          tone: "error",
          text: t("action.closeFailed", {
            detail: errorMessage(error, t),
          }),
        });
      } finally {
        loadingRef.current = false;
        setLoading(false);
      }
    },
    [formatNumber, loadingRef, setLoading, setNotice, t],
  );

  return { closeSession, closeGroup };
}
