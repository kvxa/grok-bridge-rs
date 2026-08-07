import { useCallback, useEffect, useRef, useState } from "react";
import {
  eventsWebSocketUrl,
  normalizeEventsMessage,
} from "../api.js";
import { useI18n } from "../i18n/index.js";
import { sessionsSignature } from "../sessions.js";
import { decodeBase64ToUint8Array } from "../utils/base64.js";
import {
  MAX_INPUT_BASE64_LENGTH,
  MAX_INPUT_RAW_BYTES,
  PENDING_COMMANDS_MAX,
  PENDING_COMMANDS_MAX_BYTES,
  WS_BACKOFF_MS,
} from "../utils/constants.js";
import { errorMessage } from "../utils/errors.js";
import {
  pushTerminalEntries,
  reconcileTerminalSessions,
} from "../utils/terminalFeeds.js";

/** @typedef {'initial' | 'connected' | 'disconnected' | 'retrying'} ConnectionState */

/** Stable client-side I/O error codes (never shown raw in the UI). */
export const CLIENT_IO_ERROR = Object.freeze({
  DISCONNECTED: "disconnected",
  INVALID_PAYLOAD: "invalid_payload",
  SEND_FAILED: "send_failed",
  TOO_LARGE: "too_large",
  QUEUE_FULL: "queue_full",
});

const CLIENT_IO_ERROR_KEYS = Object.freeze({
  [CLIENT_IO_ERROR.DISCONNECTED]: "interactive.disconnected",
  [CLIENT_IO_ERROR.INVALID_PAYLOAD]: "interactive.invalidPayload",
  [CLIENT_IO_ERROR.SEND_FAILED]: "interactive.sendFailed",
  [CLIENT_IO_ERROR.TOO_LARGE]: "interactive.tooLarge",
  [CLIENT_IO_ERROR.QUEUE_FULL]: "interactive.queueFull",
});

/**
 * In-flight command bookkeeping. `bytes` is the base64 payload length, so the
 * pending window is bounded by both entry count and bytes.
 * @typedef {{ kind: "input" | "resize", session: string, bytes: number }} PendingEntry
 */

export function useSessionStream({ setNotice } = {}) {
  const { t } = useI18n();
  const tRef = useRef(t);
  tRef.current = t;

  const [sessions, setSessions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [connectionState, setConnectionState] = useState(
    /** @type {ConnectionState} */ ("initial"),
  );
  const [lastUpdated, setLastUpdated] = useState(null);
  /** Inputs whose single result was lost (disconnect): delivery is unknown and
   *  they are never replayed. Exposed so the UI can show indeterminate. */
  const [unconfirmedInputs, setUnconfirmedInputs] = useState(0);
  const unconfirmedInputsRef = useRef(0);
  const signatureRef = useRef(null);
  const loadingRef = useRef(false);
  const mountedRef = useRef(true);
  const reconnectRef = useRef(() => {});
  /** Request ids are unique within the current connection; reset on connect. */
  const requestSeqRef = useRef(0);
  /** @type {import('react').MutableRefObject<Map<string, PendingEntry>>} */
  const pendingRef = useRef(new Map());
  const pendingBytesRef = useRef(0);
  /** @type {import('react').MutableRefObject<WebSocket | null>} */
  const socketRef = useRef(null);

  function nextRequestId() {
    requestSeqRef.current += 1;
    return `webui-${requestSeqRef.current}`;
  }

  const clearStreamError = useCallback(() => {
    if (!setNotice) return;
    setNotice((current) =>
      current?.tone === "error" && current?.kind === "stream" ? null : current,
    );
  }, [setNotice]);

  const reportStreamError = useCallback(
    (error) => {
      if (!setNotice) return;
      const translate = tRef.current;
      setNotice({
        tone: "error",
        kind: "stream",
        text: translate("stream.error", {
          detail: errorMessage(error, translate),
        }),
      });
    },
    [setNotice],
  );

  /** Map a stable client error code to a fully localized Notice (no English leak). */
  const reportClientIoError = useCallback(
    (code) => {
      if (!setNotice) return;
      const translate = tRef.current;
      const key =
        CLIENT_IO_ERROR_KEYS[code] ?? "interactive.unavailable";
      setNotice({
        tone: "error",
        kind: "input",
        text: translate(key),
      });
    },
    [setNotice],
  );

  /**
   * Backend input_result / resize_result detail may stay after the localized
   * prefix from interactive.error.
   */
  const reportBackendIoError = useCallback(
    (detail) => {
      if (!setNotice) return;
      const translate = tRef.current;
      setNotice({
        tone: "error",
        kind: "input",
        text: translate("interactive.error", {
          detail: detail || translate("error.unknown"),
        }),
      });
    },
    [setNotice],
  );

  /**
   * Send a JSON command on the live events socket.
   * Never buffers: if the socket is not OPEN, fails immediately. Every
   * terminal_input / terminal_resize gets a fresh connection-unique request id
   * and is tracked as pending until its single result arrives. Admission is
   * bounded by both entry count and bytes; when the window is full the whole
   * command is rejected (never partially queued).
   * @returns {{ ok: true, id: string } | { ok: false, error: string }}
   *   `error` is a stable CLIENT_IO_ERROR code, never a free-form English string.
   */
  const sendClientCommand = useCallback((message) => {
    const ws = socketRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      return { ok: false, error: CLIENT_IO_ERROR.DISCONNECTED };
    }
    const id = nextRequestId();
    const payload = { ...message, id };
    const kind = payload.type === "terminal_input" ? "input" : "resize";
    const bytes =
      typeof payload.data_base64 === "string" ? payload.data_base64.length : 0;
    if (
      pendingRef.current.size >= PENDING_COMMANDS_MAX ||
      pendingBytesRef.current + bytes > PENDING_COMMANDS_MAX_BYTES
    ) {
      return { ok: false, error: CLIENT_IO_ERROR.QUEUE_FULL };
    }
    // Admit before send so a synchronous result (or a close race) can settle
    // the command without observing a missing pending entry.
    pendingRef.current.set(id, { kind, session: payload.session, bytes });
    pendingBytesRef.current += bytes;
    try {
      ws.send(JSON.stringify(payload));
    } catch {
      pendingRef.current.delete(id);
      pendingBytesRef.current = Math.max(0, pendingBytesRef.current - bytes);
      return { ok: false, error: CLIENT_IO_ERROR.SEND_FAILED };
    }
    return { ok: true, id };
  }, []);

  const settlePending = useCallback((id, resultType, session) => {
    const entry = pendingRef.current.get(id);
    if (!entry) return null;
    const expectedType =
      entry.kind === "input" ? "input_result" : "resize_result";
    if (resultType !== expectedType || entry.session !== session) return null;
    pendingRef.current.delete(id);
    pendingBytesRef.current = Math.max(0, pendingBytesRef.current - entry.bytes);
    return entry;
  }, []);

  /** Resize ack listeners keyed by (session, request id) so terminals can
   *  commit their resize dedupe only after the server confirms the apply. */
  const resizeAckListenersRef = useRef(new Set());
  const subscribeResizeAck = useCallback((listener) => {
    resizeAckListenersRef.current.add(listener);
    return () => {
      resizeAckListenersRef.current.delete(listener);
    };
  }, []);

  /**
   * A lost connection can never confirm non-idempotent input. Pending inputs
   * become indeterminate (delivery unknown, never replayed); pending resizes
   * are idempotent and are simply dropped — the Terminal clears its dedupe on
   * disconnect and re-publishes the visible size after reconnect.
   */
  const abandonPending = useCallback(() => {
    let lost = 0;
    for (const entry of pendingRef.current.values()) {
      if (entry.kind === "input") lost += 1;
    }
    pendingRef.current.clear();
    pendingBytesRef.current = 0;
    if (lost > 0) {
      unconfirmedInputsRef.current += lost;
      if (mountedRef.current) {
        setUnconfirmedInputs(unconfirmedInputsRef.current);
      }
    }
  }, []);

  const sendTerminalInput = useCallback(
    (session, dataBase64) => {
      if (!session || !dataBase64) {
        reportClientIoError(CLIENT_IO_ERROR.INVALID_PAYLOAD);
        return { ok: false, error: CLIENT_IO_ERROR.INVALID_PAYLOAD };
      }
      // Reject an oversized input/paste as a whole BEFORE any byte is sent,
      // so nothing can partially enter the runtime writer. The base64-length
      // fast path catches anything clearly over 64 KiB; the decode then makes
      // the boundary exact (base64 length alone cannot separate 65536/65537/
      // 65538 raw bytes, which all encode to the same 87384 characters).
      if (dataBase64.length > MAX_INPUT_BASE64_LENGTH) {
        reportClientIoError(CLIENT_IO_ERROR.TOO_LARGE);
        return { ok: false, error: CLIENT_IO_ERROR.TOO_LARGE };
      }
      if (decodeBase64ToUint8Array(dataBase64).length > MAX_INPUT_RAW_BYTES) {
        reportClientIoError(CLIENT_IO_ERROR.TOO_LARGE);
        return { ok: false, error: CLIENT_IO_ERROR.TOO_LARGE };
      }
      const result = sendClientCommand({
        type: "terminal_input",
        session,
        data_base64: dataBase64,
      });
      if (!result.ok) {
        reportClientIoError(result.error);
      }
      return result;
    },
    [reportClientIoError, sendClientCommand],
  );

  const sendTerminalResize = useCallback(
    (session, cols, rows) => {
      if (!session) {
        reportClientIoError(CLIENT_IO_ERROR.INVALID_PAYLOAD);
        return { ok: false, error: CLIENT_IO_ERROR.INVALID_PAYLOAD };
      }
      const result = sendClientCommand({
        type: "terminal_resize",
        session,
        cols,
        rows,
      });
      if (!result.ok) {
        reportClientIoError(result.error);
      }
      return result;
    },
    [reportClientIoError, sendClientCommand],
  );

  useEffect(() => {
    mountedRef.current = true;
    let cancelled = false;
    let socket = null;
    let retryTimer = 0;
    let attempt = 0;
    let everConnected = false;

    const clearRetry = () => {
      if (retryTimer) {
        window.clearTimeout(retryTimer);
        retryTimer = 0;
      }
    };

    const applySessionsMessage = (parsed) => {
      const message = normalizeEventsMessage(parsed);
      if (!mountedRef.current || cancelled) return;

      const signature = sessionsSignature(message.sessions);
      if (signature !== signatureRef.current) {
        signatureRef.current = signature;
        setSessions(message.sessions);
      }

      pushTerminalEntries(message.terminals);
      reconcileTerminalSessions(
        new Set(message.sessions.map((session) => session.session)),
      );
      setLastUpdated(new Date());
      clearStreamError();
    };

    const applyMessage = (rawText) => {
      let parsed;
      try {
        parsed = JSON.parse(rawText);
      } catch (error) {
        throw new Error(`invalid events JSON: ${error?.message || error}`);
      }

      // Command results must not go through sessions normalization.
      if (
        parsed &&
        typeof parsed === "object" &&
        (parsed.type === "input_result" || parsed.type === "resize_result")
      ) {
        // Exactly one result per command: settle only a matching pending
        // entry. Stale, cross-kind, and cross-session results are ignored.
        const entry =
          typeof parsed.id === "string"
            ? settlePending(parsed.id, parsed.type, parsed.session)
            : null;
        if (!entry) return;
        if (entry.kind === "resize") {
          // Both positive and negative resize results resolve the in-flight
          // dedupe state; the terminal decides whether to commit or retry.
          for (const listener of resizeAckListenersRef.current) {
            listener(parsed.session, parsed.id, parsed.ok === true);
          }
        }
        if (parsed.reconnect === true) {
          // The server rejected this command before applying it because this
          // connection exhausted its bounded request-id set. Rotate only after
          // settling the explicit result; pending input on the old connection
          // remains indeterminate and is never replayed.
          reportClientIoError(CLIENT_IO_ERROR.DISCONNECTED);
          reconnectRef.current();
          return;
        }
        if (parsed.ok === false) {
          reportBackendIoError(
            typeof parsed.error === "string" ? parsed.error : null,
          );
        }
        return;
      }

      applySessionsMessage(parsed);
    };

    const scheduleReconnect = () => {
      if (cancelled) return;
      clearRetry();
      const delay =
        WS_BACKOFF_MS[Math.min(attempt, WS_BACKOFF_MS.length - 1)] ?? 30000;
      attempt += 1;
      if (mountedRef.current) setConnectionState("retrying");
      retryTimer = window.setTimeout(() => {
        retryTimer = 0;
        connect();
      }, delay);
    };

    const connect = () => {
      if (cancelled) return;
      clearRetry();

      if (socket) {
        // Explicit reconnect detaches the old socket's onclose handler below;
        // abandon its pending commands first so input delivery is not silently
        // forgotten and is never replayed on the replacement connection.
        abandonPending();
        try {
          socket.onopen = null;
          socket.onmessage = null;
          socket.onerror = null;
          socket.onclose = null;
          if (
            socket.readyState === WebSocket.OPEN ||
            socket.readyState === WebSocket.CONNECTING
          ) {
            socket.close();
          }
        } catch {
          /* ignore close races */
        }
        socket = null;
        socketRef.current = null;
      }

      if (mountedRef.current) {
        setConnectionState(everConnected ? "retrying" : "initial");
      }

      let ws;
      try {
        ws = new WebSocket(eventsWebSocketUrl());
      } catch (error) {
        reportStreamError(error);
        scheduleReconnect();
        return;
      }
      socket = ws;
      socketRef.current = ws;
      // Request ids are scoped to the current connection; the server rejects
      // duplicates within one connection, so a fresh connection restarts them.
      requestSeqRef.current = 0;

      ws.onopen = () => {
        if (cancelled || socket !== ws) return;
        everConnected = true;
        attempt = 0;
        socketRef.current = ws;
        if (mountedRef.current) setConnectionState("connected");
        clearStreamError();
      };

      ws.onmessage = (event) => {
        if (cancelled || socket !== ws) return;
        try {
          applyMessage(String(event.data ?? ""));
        } catch (error) {
          reportStreamError(error);
        }
      };

      ws.onerror = () => {
        // Browsers surface details via onclose; keep a soft signal only.
        if (cancelled || socket !== ws) return;
      };

      ws.onclose = () => {
        if (cancelled || socket !== ws) return;
        socket = null;
        if (socketRef.current === ws) socketRef.current = null;
        // Ack for in-flight commands is lost: inputs become indeterminate and
        // are never replayed; idempotent resizes are dropped for re-publish.
        abandonPending();
        if (mountedRef.current) {
          setConnectionState("disconnected");
        }
        scheduleReconnect();
      };
    };

    reconnectRef.current = () => {
      if (cancelled) return;
      attempt = 0;
      clearRetry();
      if (mountedRef.current) {
        setConnectionState(everConnected ? "retrying" : "initial");
      }
      connect();
    };

    connect();

    return () => {
      cancelled = true;
      mountedRef.current = false;
      clearRetry();
      reconnectRef.current = () => {};
      socketRef.current = null;
      if (socket) {
        try {
          socket.onopen = null;
          socket.onmessage = null;
          socket.onerror = null;
          socket.onclose = null;
          socket.close();
        } catch {
          /* ignore */
        }
        socket = null;
      }
    };
  }, [
    clearStreamError,
    reportClientIoError,
    reportBackendIoError,
    reportStreamError,
    settlePending,
    abandonPending,
  ]);

  const reconnect = useCallback(() => {
    reconnectRef.current();
  }, []);

  return {
    sessions,
    loading,
    connectionState,
    connected: connectionState === "connected",
    lastUpdated,
    reconnect,
    loadingRef,
    setLoading,
    sendTerminalInput,
    sendTerminalResize,
    subscribeResizeAck,
    unconfirmedInputs,
  };
}
