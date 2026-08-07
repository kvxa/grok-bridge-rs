import { useCallback, useEffect, useRef, useState } from "react";
import {
  eventsWebSocketUrl,
  getSessions,
  normalizeCommandResult,
  normalizeEventsMessage,
} from "../api.js";
import { useI18n } from "../i18n/index.js";
import { sessionsSignature } from "../sessions.js";
import {
  getWebUiClientIdentity,
  nextWebUiRequestSeq,
} from "../utils/clientIdentity.js";
import { WS_BACKOFF_MS } from "../utils/constants.js";
import { decodeBase64ToUint8Array } from "../utils/base64.js";
import {
  CAPABILITY_FORBIDDEN,
  errorMessage,
  isCapabilityForbidden,
} from "../utils/errors.js";
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
  READ_ONLY: "read_only",
  FLOW_CONTROL: "flow_control",
  ACK_TIMEOUT: "ack_timeout",
  /** Submitted side effect may have run; no auto-replay (reconnect while RO). */
  INDETERMINATE: "indeterminate",
});

const CLIENT_IO_ERROR_KEYS = Object.freeze({
  [CLIENT_IO_ERROR.DISCONNECTED]: "interactive.disconnected",
  [CLIENT_IO_ERROR.INVALID_PAYLOAD]: "interactive.invalidPayload",
  [CLIENT_IO_ERROR.SEND_FAILED]: "interactive.sendFailed",
});

const COMMAND_RESULT_TYPES = new Set([
  "terminal_subscribe_result",
  "terminal_claim_result",
  "terminal_release_result",
  "terminal_resync_result",
  "input_result",
  "resize_result",
  "client_heartbeat_result",
]);
const MAX_PENDING_COMMANDS = 64;
const MAX_CONTROL_QUEUE = 32;
const MAX_SESSION_IO_ENTRIES = 64;
const MAX_SESSION_IO_BYTES = 1024 * 1024;
/** Non-side-effect commands only (claim/subscribe/heartbeat). */
const COMMAND_ACK_TIMEOUT_MS = 1500;
const COMMAND_RETRY_DELAY_MS = 80;
const COMMAND_MAX_RETRIES = 2;
const RETIRED_REQUEST_TOMBSTONE_CAP = 256;

function retireRequestId(retired, id) {
  if (!id) return;
  retired.delete(id);
  retired.set(id, Date.now());
  while (retired.size > RETIRED_REQUEST_TOMBSTONE_CAP) {
    retired.delete(retired.keys().next().value);
  }
}
const IN_PROGRESS_RETRY_DELAY_MS = 1000;
const IN_PROGRESS_DEADLINE_MS = 30_000;
/** Renew control for still-subscribed (visible) terminals while interactive. */
const CLIENT_HEARTBEAT_MS = 4_000;

function commandSession(command) {
  return command.session;
}

/** PTY side effects: never finish via client timer (server is source of truth). */
function isSideEffectCommand(payload) {
  const type = payload?.type;
  return type === "terminal_input" || type === "terminal_resize";
}

function isRetryableResult(result) {
  return (
    !result.ok &&
    (result.error_code === "flow_control" ||
      result.error_code === "control_required" ||
      result.error_code === "in_progress")
  );
}

export function useSessionStream({ setNotice, interactive = false } = {}) {
  const { t } = useI18n();
  const tRef = useRef(t);
  tRef.current = t;

  const [sessions, setSessions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [connectionState, setConnectionState] = useState(
    /** @type {ConnectionState} */ ("initial"),
  );
  const [lastUpdated, setLastUpdated] = useState(null);
  const signatureRef = useRef(null);
  const loadingRef = useRef(false);
  const mountedRef = useRef(true);
  const reconnectRef = useRef(() => {});
  /** @type {import('react').MutableRefObject<WebSocket | null>} */
  const socketRef = useRef(null);
  const interactiveRef = useRef(Boolean(interactive));
  const pendingCommandsRef = useRef(new Map());
  const retiredRequestIdsRef = useRef(new Map());
  const controlSessionsRef = useRef(new Map());
  const flushControlQueueRef = useRef(() => {});
  const sessionIdsRef = useRef([]);
  /** Visible expanded/selected terminals only; default empty (no auto all-session). */
  const desiredSubscriptionsRef = useRef([]);
  /** Last successfully acked subscribe set (JSON key). */
  const appliedSubscriptionsKeyRef = useRef(null);
  /** Monotonic generation for latest-wins subscribe (client + server). */
  const subscribeGenerationRef = useRef(0);
  /** Request id of the single in-flight terminal_subscribe, or null. */
  const subscribeInFlightIdRef = useRef(null);
  const syncSubscriptionsRef = useRef(() => {});
  const deferredResyncsRef = useRef(new Set());
  const resyncInFlightRef = useRef(new Set());
  const deferredReleaseRef = useRef(new Set());
  /** session -> { id, inFlight }; preserves one id across disconnect replay. */
  const releaseInFlightRef = useRef(new Map());
  const deferredSubscribeRef = useRef(false);
  const flushDeferredReleasesRef = useRef(() => {});
  const flushDeferredResyncsRef = useRef(() => {});
  const clientIdentityRef = useRef(getWebUiClientIdentity());
  const heartbeatTimerRef = useRef(0);
  const sendClientCommandRef = useRef(() => ({ ok: false }));
  const reclaimSessionRef = useRef(() => {});
  /**
   * Per-session PTY I/O serialization for terminal_input and terminal_resize.
   * One command at a time (including flow_control retries) so a delayed retry of
   * an older resize cannot overwrite a newer size, and input/resize share order.
   * @type {import('react').MutableRefObject<Map<string, Promise<unknown>>>}
   */
  const sessionIoSerialRef = useRef(new Map());
  /**
   * Bumped when write control is (re)acquired for a session so Terminal forces
   * a resize of the current grid (another tab may have changed PTY size).
   * @type {[Record<string, number>, Function]}
   */
  const [controlEpochs, setControlEpochs] = useState(
    /** @type {Record<string, number>} */ ({}),
  );
  interactiveRef.current = Boolean(interactive);

  const bumpControlEpoch = useCallback((session) => {
    if (!session) return;
    setControlEpochs((prev) => ({
      ...prev,
      [session]: (prev[session] || 0) + 1,
    }));
  }, []);

  useEffect(() => {
    const active = new Set(
      sessions
        .map((session) => session?.session)
        .filter((session) => typeof session === "string" && session),
    );
    setControlEpochs((previous) => {
      const next = Object.fromEntries(
        Object.entries(previous).filter(([session]) => active.has(session)),
      );
      return Object.keys(next).length === Object.keys(previous).length
        ? previous
        : next;
    });
  }, [sessions]);

  const nextRequestId = useCallback(() => {
    // Persist sequence in sessionStorage so a full page reload does not restart
    // at 1 and collide with the server's 60s identity result cache (id_conflict).
    const seq = nextWebUiRequestSeq();
    // Namespace by tab identity so two tabs never collide on webui-1, webui-2…
    const prefix = String(clientIdentityRef.current || "webui").slice(0, 12);
    return `${prefix}-${seq}`;
  }, []);

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
      // Capability 403 has a dedicated recovery path (re-open bootstrap URL).
      if (isCapabilityForbidden(error)) {
        setNotice({
          tone: "error",
          kind: "stream",
          text: translate("capability.forbidden"),
        });
        return;
      }
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

  /** Map a stable client error code to a fully localized Notice. */
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
   * Send one command and retain it until the backend acknowledges it. The
   * same request id is reused only for bounded, pre-side-effect retries. Once
   * input/resize bytes were written to a socket, a disconnect is indeterminate;
   * replaying later could duplicate the PTY effect.
   *
   * Side-effect commands (input/resize) stay attached until a real server
   * result or disconnect — client timers never declare write failure (a slow
   * PTY write would otherwise surface ack_timeout while the write still runs).
   */
  const sendClientCommand = useCallback(
    (message, { onResult, silent = false } = {}) => {
      const websocket = socketRef.current;
      if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        return { ok: false, error: CLIENT_IO_ERROR.DISCONNECTED };
      }
      if (pendingCommandsRef.current.size >= MAX_PENDING_COMMANDS) {
        reportClientIoError(CLIENT_IO_ERROR.FLOW_CONTROL);
        return { ok: false, error: CLIENT_IO_ERROR.FLOW_CONTROL };
      }

      const id = message.id || nextRequestId();
      const payload = { ...message, id };
      const sideEffect = isSideEffectCommand(payload);
      const entry = {
        id,
        payload,
        /** Number of times this payload was successfully written to the socket. */
        attempts: 0,
        timer: 0,
        deadlineTimer: 0,
        onResult,
        silent,
        sideEffect,
        /** Cleared by releaseControls so submitted writes never auto-retry. */
        allowRetry: true,
        inProgressDeadline: 0,
        retry: null,
        finish: null,
      };

      const finish = (result) => {
        if (pendingCommandsRef.current.get(id) !== entry) return;
        if (entry.timer) window.clearTimeout(entry.timer);
        if (entry.deadlineTimer) window.clearTimeout(entry.deadlineTimer);
        pendingCommandsRef.current.delete(id);
        if (entry.sideEffect) {
          retireRequestId(retiredRequestIdsRef.current, id);
        }
        entry.onResult?.(result);
        flushDeferredResyncsRef.current();
        if (!result.ok && !entry.onResult && !entry.silent) {
          reportBackendIoError(result.error);
        }
      };
      const retry = () => {
        if (pendingCommandsRef.current.get(id) !== entry) return;
        const current = socketRef.current;
        if (!current || current.readyState !== WebSocket.OPEN) {
          finish({
            ok: false,
            id,
            error_code: CLIENT_IO_ERROR.DISCONNECTED,
            error: "live channel disconnected",
          });
          return;
        }
        // Cap by real sends: initial send counts as 1; allow COMMAND_MAX_RETRIES
        // additional sends (attempts is only incremented after a successful send).
        if (entry.inProgressDeadline && Date.now() >= entry.inProgressDeadline) {
          finish({
            ok: false,
            id,
            error_code: CLIENT_IO_ERROR.INDETERMINATE,
            error: "command may have completed; result was not confirmed before the deadline",
          });
          return;
        }
        if (!entry.inProgressDeadline && entry.attempts >= 1 + COMMAND_MAX_RETRIES) {
          finish({
            ok: false,
            id,
            error_code: CLIENT_IO_ERROR.ACK_TIMEOUT,
            error: "command acknowledgement timed out",
          });
          if (!entry.onResult && !entry.silent) {
            reportClientIoError(CLIENT_IO_ERROR.ACK_TIMEOUT);
          }
          return;
        }
        try {
          current.send(JSON.stringify(entry.payload));
          entry.attempts += 1;
        } catch {
          finish({
            ok: false,
            id,
            error_code: CLIENT_IO_ERROR.SEND_FAILED,
            error: "failed to send command",
          });
          if (!entry.onResult && !entry.silent) {
            reportClientIoError(CLIENT_IO_ERROR.SEND_FAILED);
          }
          return;
        }
        // Side effects: stay attached without client deadline after a real send.
        if (entry.sideEffect) {
          entry.timer = 0;
          return;
        }
        entry.timer = window.setTimeout(retry, COMMAND_ACK_TIMEOUT_MS);
      };
      entry.retry = retry;
      entry.finish = finish;
      pendingCommandsRef.current.set(id, entry);
      try {
        websocket.send(JSON.stringify(payload));
        entry.attempts = 1;
        if (entry.sideEffect) {
          entry.inProgressDeadline = Date.now() + IN_PROGRESS_DEADLINE_MS;
          entry.deadlineTimer = window.setTimeout(() => {
            finish({
              ok: false,
              id,
              error_code: CLIENT_IO_ERROR.INDETERMINATE,
              error: "command may have completed; result was not confirmed before the deadline",
            });
          }, IN_PROGRESS_DEADLINE_MS);
        }
      } catch {
        pendingCommandsRef.current.delete(id);
        return { ok: false, error: CLIENT_IO_ERROR.SEND_FAILED };
      }
      // Side effects wait for the server terminal result only.
      if (!sideEffect) {
        entry.timer = window.setTimeout(retry, COMMAND_ACK_TIMEOUT_MS);
      }
      return { ok: true, id };
    },
    [nextRequestId, reportBackendIoError, reportClientIoError],
  );
  sendClientCommandRef.current = sendClientCommand;

  const settleCommandResult = useCallback(
    (rawResult) => {
      let result;
      try {
        result = normalizeCommandResult(rawResult);
      } catch (error) {
        reportStreamError(error);
        return;
      }
      const id = result.id;
      if (!id) {
        if (!result.ok) reportBackendIoError(result.error);
        return;
      }
      const entry = pendingCommandsRef.current.get(id);
      if (!entry) {
        // A bounded tombstone absorbs late replay/ack frames after timeout,
        // disconnect, or unmount without polluting the next command's notice.
        if (retiredRequestIdsRef.current.has(id)) {
          return;
        }
        if (!result.ok) {
          reportBackendIoError(result.error);
        }
        return;
      }
      if (result.type === "client_heartbeat_result") {
        entry.finish(result);
        return;
      }
      if (result.error_code === "in_progress" && entry.sideEffect) {
        if (Date.now() >= entry.inProgressDeadline) {
          entry.finish({
            ...result,
            error_code: CLIENT_IO_ERROR.INDETERMINATE,
            error: "command may have completed; result was not confirmed before the deadline",
          });
          return;
        }
        if (entry.allowRetry === false) return;
        if (entry.timer) window.clearTimeout(entry.timer);
        entry.timer = window.setTimeout(entry.retry, IN_PROGRESS_RETRY_DELAY_MS);
        return;
      }
      // Interactive release freezes submitted side effects: wait for the server
      // terminal result only — never auto-retry (would need a new claim/write).
      // attempts counts real socket sends only; never increment here.
      if (
        isRetryableResult(result) &&
        entry.attempts < 1 + COMMAND_MAX_RETRIES &&
        entry.allowRetry !== false
      ) {
        if (entry.timer) window.clearTimeout(entry.timer);
        // Subscribe is serial + latest-wins: if desired set moved on, drop this
        // payload and flush the current desired set instead of retrying stale A.
        if (entry.payload?.type === "terminal_subscribe") {
          const desiredKey = JSON.stringify(
            Array.isArray(desiredSubscriptionsRef.current)
              ? desiredSubscriptionsRef.current
              : [],
          );
          const payloadKey = JSON.stringify(entry.payload.sessions || []);
          if (
            desiredKey !== payloadKey ||
            entry.payload.generation !== subscribeGenerationRef.current
          ) {
            subscribeInFlightIdRef.current = null;
            entry.finish({
              ok: false,
              id: entry.id,
              error_code: "superseded",
              error: "subscribe superseded by a newer desired set",
            });
            syncSubscriptionsRef.current();
            return;
          }
        }
        // control_required: lease expired under us. Drop local ownership, re-claim,
        // then resend the *same* request id (server fingerprint cache → exactly once).
        if (
          result.error_code === "control_required" &&
          entry.payload?.session &&
          entry.payload?.type !== "terminal_claim"
        ) {
          controlSessionsRef.current.delete(entry.payload.session);
          entry.timer = window.setTimeout(() => {
            reclaimSessionRef.current(entry.payload.session, () => {
              entry.retry?.();
            }, (fail) => {
              entry.finish?.(fail);
            });
          }, COMMAND_RETRY_DELAY_MS);
          return;
        }
        entry.timer = window.setTimeout(entry.retry, COMMAND_RETRY_DELAY_MS);
        return;
      }
      entry.finish(result);
    },
    [reportBackendIoError, reportStreamError],
  );

  /** Pause reconnect state and finish submitted side effects as indeterminate. */
  const pausePendingCommandsForReconnect = useCallback(() => {
    const dropTypes = new Set([
      "terminal_claim",
      "terminal_release",
      "terminal_subscribe",
      "client_heartbeat",
      "terminal_resync",
    ]);
    for (const entry of [...pendingCommandsRef.current.values()]) {
      if (entry.timer) {
        window.clearTimeout(entry.timer);
        entry.timer = 0;
      }
      if (entry.deadlineTimer) {
        window.clearTimeout(entry.deadlineTimer);
        entry.deadlineTimer = 0;
      }
      const type = entry.payload?.type;
      if (type === "terminal_input" || type === "terminal_resize") {
        retireRequestId(retiredRequestIdsRef.current, entry.id);
        pendingCommandsRef.current.delete(entry.id);
        const result = {
          ok: false,
          id: entry.id,
          error_code: CLIENT_IO_ERROR.INDETERMINATE,
          error:
            "command may have executed before disconnect; result unknown (not auto-replayed)",
        };
        entry.onResult?.(result);
        if (!entry.onResult && !entry.silent) {
          reportBackendIoError(result.error);
        }
      } else if (dropTypes.has(type)) {
        pendingCommandsRef.current.delete(entry.id);
        entry.onResult?.({
          ok: false,
          id: entry.id,
          error_code: CLIENT_IO_ERROR.DISCONNECTED,
          error: "live channel disconnected",
        });
      }
    }
    subscribeInFlightIdRef.current = null;
    appliedSubscriptionsKeyRef.current = null;
    controlSessionsRef.current.clear();
  }, [reportBackendIoError]);

  const failAllPendingCommands = useCallback((errorCode) => {
    const entries = [...pendingCommandsRef.current.values()];
    pendingCommandsRef.current.clear();
    for (const entry of entries) {
      if (entry.timer) window.clearTimeout(entry.timer);
      if (entry.deadlineTimer) window.clearTimeout(entry.deadlineTimer);
      if (entry.sideEffect) {
        retireRequestId(retiredRequestIdsRef.current, entry.id);
      }
      entry.onResult?.({
        ok: false,
        id: entry.id,
        error_code: errorCode,
        error: "live channel disconnected",
      });
    }
    controlSessionsRef.current.clear();
  }, []);

  const reportCommandResult = useCallback(
    (result) => {
      if (!result.ok) {
        reportBackendIoError(result.error || result.error_code);
      }
    },
    [reportBackendIoError],
  );

  /**
   * Fail every queued control command with one accurate result so resize
   * __onResult callbacks clear pending state and input is not silently lost.
   */
  const failControlQueue = useCallback((control, failure) => {
    if (!control?.queue?.length) return;
    const queued = control.queue.splice(0, control.queue.length);
    for (const command of queued) {
      if (typeof command.__onResult === "function") {
        command.__onResult({
          ok: false,
          id: command.id ?? null,
          session: command.session ?? null,
          error_code: failure.error_code ?? failure.error ?? CLIENT_IO_ERROR.SEND_FAILED,
          error: failure.error ?? failure.error_code ?? "command failed",
        });
      }
    }
  }, []);

  const flushControlQueue = useCallback(
    (session) => {
      const control = controlSessionsRef.current.get(session);
      if (!control || control.state !== "owned") return;
      while (control.queue.length > 0) {
        const command = control.queue.shift();
        const onResult = command.__onResult || reportCommandResult;
        const { __onResult: _drop, ...wire } = command;
        const sent = sendClientCommand(wire, { onResult });
        if (!sent.ok) {
          reportClientIoError(sent.error);
          // Fail the command we just popped and every remaining queued command.
          onResult({
            ok: false,
            id: wire.id ?? null,
            session: wire.session ?? session,
            error_code: sent.error,
            error: "failed to send command",
          });
          failControlQueue(control, {
            error_code: sent.error,
            error: "failed to send command",
          });
          return;
        }
      }
    },
    [failControlQueue, reportClientIoError, reportCommandResult, sendClientCommand],
  );
  flushControlQueueRef.current = flushControlQueue;

  const startControlClaim = useCallback(
    (session) => {
      const control = controlSessionsRef.current.get(session);
      if (!control || control.state !== "claiming") return null;
      const sent = sendClientCommand(
        { type: "terminal_claim", session },
        {
          onResult: (result) => {
            const current = controlSessionsRef.current.get(session);
            if (!current || current.claimId !== result.id) return;
            if (result.ok) {
              current.state = "owned";
              // Another tab may have resized while we were away: force grid resend.
              bumpControlEpoch(session);
              flushControlQueueRef.current(session);
              return;
            }
            // Claim busy/timeout/error: every queued command must get a failure.
            failControlQueue(current, {
              error_code: result.error_code || "control_busy",
              error: result.error || result.error_code || "claim failed",
            });
            controlSessionsRef.current.delete(session);
            reportBackendIoError(result.error || result.error_code);
          },
        },
      );
      if (!sent.ok) {
        failControlQueue(control, {
          error_code: sent.error,
          error: "failed to send claim",
        });
        controlSessionsRef.current.delete(session);
        reportClientIoError(sent.error);
        return sent;
      }
      control.claimId = sent.id;
      return sent;
    },
    [
      bumpControlEpoch,
      failControlQueue,
      reportBackendIoError,
      reportClientIoError,
      sendClientCommand,
    ],
  );

  /** Re-claim a session after control_required; then run onOwned. */
  reclaimSessionRef.current = (session, onOwned, onFail) => {
    if (!interactiveRef.current) {
      onFail?.({
        ok: false,
        error_code: CLIENT_IO_ERROR.READ_ONLY,
        error: "read only",
      });
      return;
    }
    let control = controlSessionsRef.current.get(session);
    if (control?.state === "owned") {
      onOwned?.();
      return;
    }
    if (!control) {
      control = { state: "claiming", claimId: null, queue: [] };
      controlSessionsRef.current.set(session, control);
    } else {
      control.state = "claiming";
      control.claimId = null;
    }
    const sent = sendClientCommandRef.current(
      { type: "terminal_claim", session },
      {
        onResult: (result) => {
          const current = controlSessionsRef.current.get(session);
          if (!current) {
            onFail?.(result);
            return;
          }
          if (result.ok) {
            current.state = "owned";
            // Reclaim must not trust lastSentSize from a prior ownership epoch.
            bumpControlEpoch(session);
            onOwned?.();
            return;
          }
          failControlQueue(current, {
            error_code: result.error_code || "control_busy",
            error: result.error || "claim failed",
          });
          controlSessionsRef.current.delete(session);
          onFail?.(result);
        },
      },
    );
    if (!sent.ok) {
      failControlQueue(control, {
        error_code: sent.error,
        error: "failed to re-claim control",
      });
      controlSessionsRef.current.delete(session);
      onFail?.({
        ok: false,
        error_code: sent.error,
        error: "failed to re-claim control",
      });
      return;
    }
    control.claimId = sent.id;
  };

  const sendControlledCommand = useCallback(
    (command) => {
      if (!interactiveRef.current) {
        reportClientIoError(CLIENT_IO_ERROR.READ_ONLY);
        return { ok: false, error: CLIENT_IO_ERROR.READ_ONLY };
      }
      const websocket = socketRef.current;
      if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        reportClientIoError(CLIENT_IO_ERROR.DISCONNECTED);
        return { ok: false, error: CLIENT_IO_ERROR.DISCONNECTED };
      }
      const session = commandSession(command);
      let control = controlSessionsRef.current.get(session);
      let startedClaim = false;
      if (!control) {
        control = { state: "claiming", claimId: null, queue: [] };
        controlSessionsRef.current.set(session, control);
        startedClaim = true;
      }
      if (control.state === "owned") {
        return sendClientCommand(command, { onResult: reportCommandResult });
      }
      if (control.queue.length >= MAX_CONTROL_QUEUE) {
        reportClientIoError(CLIENT_IO_ERROR.FLOW_CONTROL);
        return { ok: false, error: CLIENT_IO_ERROR.FLOW_CONTROL };
      }
      control.queue.push(command);
      if (startedClaim) {
        const claim = startControlClaim(session);
        if (!claim || !claim.ok) {
          // startControlClaim already failed the queue when send failed.
          return {
            ok: false,
            error: claim?.error || CLIENT_IO_ERROR.DISCONNECTED,
          };
        }
      }
      return { ok: true, queued: true, id: control.claimId };
    },
    [
      reportClientIoError,
      reportCommandResult,
      sendClientCommand,
      startControlClaim,
    ],
  );

  const releaseControls = useCallback(() => {
    // Fail only *unsubmitted* claim queues. Already-wired terminal_input/resize
    // stay in pendingCommandsRef with the same id until the server publishes a
    // terminal result — never fake read_only while a PTY job may still succeed.
    const entries = [...controlSessionsRef.current.entries()];
    controlSessionsRef.current.clear();
    for (const [session, control] of entries) {
      failControlQueue(control, {
        error_code: CLIENT_IO_ERROR.READ_ONLY,
        error: "interactive mode disabled",
      });
      deferredReleaseRef.current.add(session);
    }
    flushDeferredReleasesRef.current();
    // Freeze submitted side effects: stop retry/reclaim timers; keep attachment
    // for settleCommandResult. Do **not** clear sessionIoSerialRef — new work
    // after interactive returns must chain after in-flight tails.
    for (const entry of pendingCommandsRef.current.values()) {
      const type = entry.payload?.type;
      if (type !== "terminal_input" && type !== "terminal_resize") continue;
      entry.allowRetry = false;
      if (entry.timer) {
        window.clearTimeout(entry.timer);
        entry.timer = 0;
      }
    }
  }, [failControlQueue]);

  /**
   * Latest-wins terminal_subscribe with monotonic generation.
   * Desired-set changes drop the previous local pending entry and send a new
   * generation immediately (no wait on stale empty/remove→add). Server ignores
   * older generations so reverse order cannot roll the subscription set back.
   */
  const syncTerminalSubscriptions = useCallback(() => {
    const requested = Array.isArray(desiredSubscriptionsRef.current)
      ? desiredSubscriptionsRef.current
      : [];
    const key = JSON.stringify(requested);
    if (appliedSubscriptionsKeyRef.current === key) {
      return { ok: true, queued: true };
    }
    // Same payload already in flight for the latest generation — wait for it.
    if (subscribeInFlightIdRef.current) {
      const inflight = pendingCommandsRef.current.get(
        subscribeInFlightIdRef.current,
      );
      if (
        inflight &&
        JSON.stringify(inflight.payload?.sessions || []) === key &&
        inflight.payload?.generation === subscribeGenerationRef.current
      ) {
        return { ok: true, queued: true };
      }
      // Supersede: drop local tracking so retries cannot re-send stale A, and a
      // late ack is ignored (no pending entry). Server generation still rejects
      // stale applies if they race past the newer set.
      const oldId = subscribeInFlightIdRef.current;
      const old = pendingCommandsRef.current.get(oldId);
      if (old) {
        if (old.timer) window.clearTimeout(old.timer);
        pendingCommandsRef.current.delete(oldId);
      }
      subscribeInFlightIdRef.current = null;
    }
    const generation = subscribeGenerationRef.current + 1;
    subscribeGenerationRef.current = generation;
    const sent = sendClientCommand(
      {
        type: "terminal_subscribe",
        sessions: requested,
        generation,
      },
      {
        silent: true,
        onResult: (result) => {
          if (subscribeInFlightIdRef.current === result?.id) {
            subscribeInFlightIdRef.current = null;
          }
          // Stale generation (superseded after this callback was bound).
          if (generation !== subscribeGenerationRef.current) {
            return;
          }
          if (result.ok) {
            const stillDesired = JSON.stringify(
              Array.isArray(desiredSubscriptionsRef.current)
                ? desiredSubscriptionsRef.current
                : [],
            );
            if (stillDesired === key) {
              appliedSubscriptionsKeyRef.current = key;
            }
          } else if (
            result.error_code !== "superseded" &&
            result.error_code !== CLIENT_IO_ERROR.DISCONNECTED &&
            result.error_code !== CLIENT_IO_ERROR.ACK_TIMEOUT
          ) {
            reportBackendIoError(result.error || result.error_code);
          }
          // Flush if desired moved during the round-trip.
          syncSubscriptionsRef.current();
        },
      },
    );
    if (sent.ok) {
      subscribeInFlightIdRef.current = sent.id;
      deferredSubscribeRef.current = false;
    } else if (
      sent.error === CLIENT_IO_ERROR.FLOW_CONTROL ||
      sent.error === CLIENT_IO_ERROR.DISCONNECTED
    ) {
      deferredSubscribeRef.current = true;
    }
    return sent;
  }, [reportBackendIoError, sendClientCommand]);
  syncSubscriptionsRef.current = syncTerminalSubscriptions;

  const flushDeferredReleases = useCallback(() => {
    for (const session of [...deferredReleaseRef.current]) {
      const existing = releaseInFlightRef.current.get(session);
      if (existing?.inFlight) continue;
      const request = existing ?? { id: nextRequestId(), inFlight: false };
      releaseInFlightRef.current.set(session, request);
      const sent = sendClientCommand(
        { type: "terminal_release", session, id: request.id },
        {
          silent: true,
          onResult: (result) => {
            const current = releaseInFlightRef.current.get(session);
            if (!current || current.id !== request.id) return;
            current.inFlight = false;
            if (result.error_code === CLIENT_IO_ERROR.DISCONNECTED) {
              deferredReleaseRef.current.add(session);
              return;
            }
            deferredReleaseRef.current.delete(session);
            releaseInFlightRef.current.delete(session);
          },
        },
      );
      if (sent.ok) {
        request.inFlight = true;
        deferredReleaseRef.current.delete(session);
      } else {
        request.inFlight = false;
        if (
          sent.error !== CLIENT_IO_ERROR.DISCONNECTED &&
          sent.error !== CLIENT_IO_ERROR.FLOW_CONTROL
        ) {
          deferredReleaseRef.current.delete(session);
          releaseInFlightRef.current.delete(session);
        }
        break;
      }
    }
  }, [nextRequestId, sendClientCommand]);
  flushDeferredReleasesRef.current = flushDeferredReleases;

  const flushDeferredResyncs = useCallback(() => {
    flushDeferredReleasesRef.current();
    if (deferredSubscribeRef.current) syncSubscriptionsRef.current();
    const current = Array.isArray(desiredSubscriptionsRef.current)
      ? desiredSubscriptionsRef.current
      : [];
    for (const session of [...deferredResyncsRef.current]) {
      if (!current.includes(session)) {
        deferredResyncsRef.current.delete(session);
        resyncInFlightRef.current.delete(session);
        continue;
      }
      if (resyncInFlightRef.current.has(session)) continue;
      const sent = sendClientCommand(
        { type: "terminal_resync", session },
        {
          silent: true,
          onResult: (result) => {
            resyncInFlightRef.current.delete(session);
            if (result.ok) {
              deferredResyncsRef.current.delete(session);
              flushDeferredResyncsRef.current();
            } else if (result.error_code === CLIENT_IO_ERROR.DISCONNECTED) {
              // Reconnect onopen flushes the retained intent.
            } else if (
              result.error_code === CLIENT_IO_ERROR.FLOW_CONTROL ||
              result.error_code === CLIENT_IO_ERROR.ACK_TIMEOUT
            ) {
              window.setTimeout(
                () => flushDeferredResyncsRef.current(),
                COMMAND_RETRY_DELAY_MS,
              );
            } else {
              deferredResyncsRef.current.delete(session);
              flushDeferredResyncsRef.current();
            }
          },
        },
      );
      if (!sent.ok) break;
      resyncInFlightRef.current.add(session);
    }
  }, [sendClientCommand]);
  flushDeferredResyncsRef.current = flushDeferredResyncs;

  /**
   * Release write control for sessions that left the visible subscription set.
   * Server Subscribe also releases; this keeps controlSessionsRef honest so a
   * later re-open re-claims instead of assuming ownership.
   */
  const releaseControlsForSessions = useCallback(
    (sessionIds) => {
      if (!Array.isArray(sessionIds) || sessionIds.length === 0) return;
      for (const session of sessionIds) {
        deferredReleaseRef.current.add(session);
        const control = controlSessionsRef.current.get(session);
        if (control) {
          failControlQueue(control, {
            error_code: CLIENT_IO_ERROR.READ_ONLY,
            error: "terminal no longer visible",
          });
          controlSessionsRef.current.delete(session);
        }
      }
      flushDeferredReleasesRef.current();
    },
    [failControlQueue],
  );

  const setTerminalSubscriptions = useCallback(
    (sessionIds) => {
      if (!Array.isArray(sessionIds)) {
        reportClientIoError(CLIENT_IO_ERROR.INVALID_PAYLOAD);
        return { ok: false, error: CLIENT_IO_ERROR.INVALID_PAYLOAD };
      }
      const next = [
        ...new Set(sessionIds.filter((session) => typeof session === "string")),
      ].slice(0, 256);
      const prev = Array.isArray(desiredSubscriptionsRef.current)
        ? desiredSubscriptionsRef.current
        : [];
      const nextSet = new Set(next);
      const removed = prev.filter((session) => !nextSet.has(session));
      if (removed.length > 0) {
        // Visible set shrank (collapse / hide / tab change): drop control now.
        releaseControlsForSessions(removed);
      }
      const key = JSON.stringify(next);
      desiredSubscriptionsRef.current = next;
      if (appliedSubscriptionsKeyRef.current === key) {
        return { ok: true, queued: true };
      }
      // Do not clear applied key to null here: that would allow a concurrent
      // in-flight older ack to look "fresh". Serial flush + generation handle it.
      return syncTerminalSubscriptions();
    },
    [
      reportClientIoError,
      releaseControlsForSessions,
      syncTerminalSubscriptions,
    ],
  );

  /**
   * Force a server ANSI snapshot for one session after a local write-queue
   * overflow. Uses terminal_resync so we never remove→add subscriptions (that
   * briefly released control and allowed other tabs to steal mid-resync).
   */
  const requestTerminalResync = useCallback(
    (sessionId) => {
      if (typeof sessionId !== "string" || !sessionId) {
        return { ok: false, error: CLIENT_IO_ERROR.INVALID_PAYLOAD };
      }
      const current = Array.isArray(desiredSubscriptionsRef.current)
        ? desiredSubscriptionsRef.current
        : [];
      if (!current.includes(sessionId)) {
        return { ok: true, queued: false };
      }
      deferredResyncsRef.current.add(sessionId);
      flushDeferredResyncs();
      return { ok: true, queued: true };
    },
    [flushDeferredResyncs],
  );

  /**
   * Run one controlled PTY command (input/resize) on the session I/O serial lane.
   * `execute` must call `settle` exactly once when the command reaches a final
   * result (after flow_control / control_required retries).
   *
   * The map entry is removed only when it is still this command's tail promise,
   * so completed sessions do not leak keys and a newer chain is never truncated.
   */
  const enqueueSessionIo = useCallback((session, execute, cost = 1) => {
    const previous = sessionIoSerialRef.current.get(session);
    const count = previous?.count ?? 0;
    const bytes = previous?.bytes ?? 0;
    if (count >= MAX_SESSION_IO_ENTRIES || bytes + cost > MAX_SESSION_IO_BYTES) {
      reportClientIoError(CLIENT_IO_ERROR.FLOW_CONTROL);
      return { ok: false, error: CLIENT_IO_ERROR.FLOW_CONTROL };
    }
    let settle;
    const finished = new Promise((resolve) => {
      settle = resolve;
    });
    if (!previous) {
      execute(settle);
    } else {
      previous.tail.then(() => {
        execute(settle);
      });
    }
    const tail = finished.then(
      () => undefined,
      () => undefined,
    );
    // `finally` yields a distinct promise; store that identity for safe prune.
    const tracked = tail.finally(() => {
      const current = sessionIoSerialRef.current.get(session);
      if (current?.tail === tracked) {
        sessionIoSerialRef.current.delete(session);
      } else if (current) {
        current.count -= 1;
        current.bytes -= cost;
      }
    });
    sessionIoSerialRef.current.set(session, {
      tail: tracked,
      count: count + 1,
      bytes: bytes + cost,
    });
    return { ok: true, promise: finished };
  }, [reportClientIoError]);

  /** Admit one controlled command (owned send or claim+queue) and finish on settle. */
  const admitControlledCommand = useCallback(
    (command, onFinal) => {
      const session = commandSession(command);
      let control = controlSessionsRef.current.get(session);
      let startedClaim = false;
      if (!control) {
        control = { state: "claiming", claimId: null, queue: [] };
        controlSessionsRef.current.set(session, control);
        startedClaim = true;
      }
      if (control.state === "owned") {
        const sent = sendClientCommand(command, { onResult: onFinal });
        if (!sent.ok) {
          onFinal({
            ok: false,
            error_code: sent.error,
            error: "failed to send command",
          });
          return { ok: false, error: sent.error };
        }
        return { ok: true, id: sent.id };
      }
      if (control.queue.length >= MAX_CONTROL_QUEUE) {
        reportClientIoError(CLIENT_IO_ERROR.FLOW_CONTROL);
        onFinal({
          ok: false,
          error_code: CLIENT_IO_ERROR.FLOW_CONTROL,
          error: "flow control",
        });
        return { ok: false, error: CLIENT_IO_ERROR.FLOW_CONTROL };
      }
      control.queue.push({
        ...command,
        __onResult: onFinal,
      });
      if (startedClaim) {
        const claim = startControlClaim(session);
        if (!claim || !claim.ok) {
          // failControlQueue invokes __onResult (onFinal) for each queued cmd.
          if (control.queue.length > 0) {
            failControlQueue(control, {
              error_code: claim?.error || CLIENT_IO_ERROR.DISCONNECTED,
              error: "failed to send claim",
            });
            controlSessionsRef.current.delete(session);
          } else {
            onFinal({
              ok: false,
              error_code: claim?.error || CLIENT_IO_ERROR.DISCONNECTED,
              error: "failed to send claim",
            });
          }
          return {
            ok: false,
            error: claim?.error || CLIENT_IO_ERROR.DISCONNECTED,
          };
        }
      }
      return { ok: true, queued: true, id: null };
    },
    [failControlQueue, reportClientIoError, sendClientCommand, startControlClaim],
  );

  const sendTerminalInput = useCallback(
    (session, dataBase64) => {
      if (!session || !dataBase64) {
        reportClientIoError(CLIENT_IO_ERROR.INVALID_PAYLOAD);
        return { ok: false, error: CLIENT_IO_ERROR.INVALID_PAYLOAD };
      }
      // Admission is all-or-nothing at the WebUI boundary. Reject oversized
      // raw payloads before the session lane or socket sees any prefix.
      if (decodeBase64ToUint8Array(dataBase64).length > 64 * 1024) {
        reportClientIoError(CLIENT_IO_ERROR.FLOW_CONTROL);
        return { ok: false, error: CLIENT_IO_ERROR.FLOW_CONTROL };
      }
      if (!interactiveRef.current) {
        reportClientIoError(CLIENT_IO_ERROR.READ_ONLY);
        return { ok: false, error: CLIENT_IO_ERROR.READ_ONLY };
      }
      const websocket = socketRef.current;
      if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        reportClientIoError(CLIENT_IO_ERROR.DISCONNECTED);
        return { ok: false, error: CLIENT_IO_ERROR.DISCONNECTED };
      }
      // Serialize with resizes on the same session lane.
      const queued = enqueueSessionIo(session, (settle) => {
        if (!interactiveRef.current) {
          settle({
            ok: false,
            error_code: CLIENT_IO_ERROR.READ_ONLY,
          });
          return;
        }
        const command = {
          type: "terminal_input",
          session,
          data_base64: dataBase64,
        };
        admitControlledCommand(command, (result) => {
          reportCommandResult(result);
          settle(result);
        });
      }, Math.ceil(dataBase64.length * 0.75));
      return queued.ok ? { ok: true, queued: true } : queued;
    },
    [
      admitControlledCommand,
      enqueueSessionIo,
      reportClientIoError,
      reportCommandResult,
    ],
  );

  const sendTerminalResize = useCallback(
    (session, cols, rows, { onResult } = {}) => {
      if (
        !session ||
        !Number.isInteger(cols) ||
        !Number.isInteger(rows) ||
        cols < 1 ||
        rows < 1
      ) {
        reportClientIoError(CLIENT_IO_ERROR.INVALID_PAYLOAD);
        return { ok: false, error: CLIENT_IO_ERROR.INVALID_PAYLOAD };
      }
      if (!interactiveRef.current) {
        reportClientIoError(CLIENT_IO_ERROR.READ_ONLY);
        return { ok: false, error: CLIENT_IO_ERROR.READ_ONLY };
      }
      const websocket = socketRef.current;
      if (!websocket || websocket.readyState !== WebSocket.OPEN) {
        reportClientIoError(CLIENT_IO_ERROR.DISCONNECTED);
        return { ok: false, error: CLIENT_IO_ERROR.DISCONNECTED };
      }
      // Same serial lane as input: an older resize (incl. flow_control retries)
      // must finish before a newer resize is admitted, so the final PTY size is
      // the last enqueued target (B after A), never a late retry of A after B.
      const queued = enqueueSessionIo(session, (settle) => {
        if (!interactiveRef.current) {
          const fail = {
            ok: false,
            error_code: CLIENT_IO_ERROR.READ_ONLY,
            error: "read only",
            session,
            cols,
            rows,
          };
          onResult?.(fail);
          settle(fail);
          return;
        }
        const command = {
          type: "terminal_resize",
          session,
          cols,
          rows,
        };
        // Bind originating size so Terminal matches resize_result by id+cols+rows.
        const wrapResult = (result) => {
          const enriched = {
            ...result,
            session: result.session ?? session,
            cols,
            rows,
          };
          onResult?.(enriched);
          if (!result.ok && !onResult) {
            const code = result.error_code;
            // Client I/O codes are already (or should be) localized via
            // reportClientIoError — do not overwrite with backend detail strings.
            if (
              code === CLIENT_IO_ERROR.SEND_FAILED ||
              code === CLIENT_IO_ERROR.DISCONNECTED ||
              code === CLIENT_IO_ERROR.FLOW_CONTROL ||
              code === CLIENT_IO_ERROR.READ_ONLY
            ) {
              /* keep existing client notice */
            } else {
              reportCommandResult(result);
            }
          }
          settle(enriched);
        };
        admitControlledCommand(command, wrapResult);
      }, 1);
      // Queued on the session I/O lane; wire id is known only after admit.
      return queued.ok ? { ok: true, queued: true, id: null } : queued;
    },
    [
      admitControlledCommand,
      enqueueSessionIo,
      reportClientIoError,
      reportCommandResult,
    ],
  );

  useEffect(() => {
    if (!interactive) releaseControls();
  }, [interactive, releaseControls]);

  // Application heartbeat while interactive: renews control only for sessions
  // still in the visible subscription set (server ignores hidden holds).
  useEffect(() => {
    if (heartbeatTimerRef.current) {
      window.clearInterval(heartbeatTimerRef.current);
      heartbeatTimerRef.current = 0;
    }
    if (!interactive || connectionState !== "connected") {
      return undefined;
    }
    const tick = () => {
      sendClientCommandRef.current(
        { type: "client_heartbeat" },
        { silent: true },
      );
    };
    tick();
    heartbeatTimerRef.current = window.setInterval(tick, CLIENT_HEARTBEAT_MS);
    return () => {
      if (heartbeatTimerRef.current) {
        window.clearInterval(heartbeatTimerRef.current);
        heartbeatTimerRef.current = 0;
      }
    };
  }, [interactive, connectionState]);

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

      const nextSessionIds = message.sessions.map((session) => session.session);
      sessionIdsRef.current = nextSessionIds;
      // Subscriptions are driven only by setTerminalSubscriptions (visible
      // expanded/selected terminals). Never auto-subscribe every session.

      const signature = sessionsSignature(message.sessions);
      if (signature !== signatureRef.current) {
        signatureRef.current = signature;
        setSessions(message.sessions);
      }

      // Only parse terminal payloads the server included (subscribed set).
      pushTerminalEntries(message.terminals);
      reconcileTerminalSessions(new Set(nextSessionIds));
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

      if (
        parsed &&
        typeof parsed === "object" &&
        COMMAND_RESULT_TYPES.has(parsed.type)
      ) {
        settleCommandResult(parsed);
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

      pausePendingCommandsForReconnect();
      appliedSubscriptionsKeyRef.current = null;
      subscribeInFlightIdRef.current = null;
      if (mountedRef.current) {
        setConnectionState(everConnected ? "retrying" : "initial");
      }

      let websocket;
      try {
        websocket = new WebSocket(
          eventsWebSocketUrl(window.location, clientIdentityRef.current),
        );
      } catch (error) {
        reportStreamError(error);
        scheduleReconnect();
        return;
      }
      socket = websocket;
      socketRef.current = websocket;

      websocket.onopen = () => {
        if (cancelled || socket !== websocket) return;
        everConnected = true;
        attempt = 0;
        socketRef.current = websocket;
        appliedSubscriptionsKeyRef.current = null;
        subscribeInFlightIdRef.current = null;
        if (mountedRef.current) setConnectionState("connected");
        syncSubscriptionsRef.current();
        flushDeferredResyncsRef.current();
        clearStreamError();
      };

      websocket.onmessage = (event) => {
        if (cancelled || socket !== websocket) return;
        try {
          applyMessage(String(event.data ?? ""));
        } catch (error) {
          reportStreamError(error);
        }
      };

      websocket.onerror = () => {
        if (cancelled || socket !== websocket) return;
      };

      websocket.onclose = () => {
        if (cancelled || socket !== websocket) return;
        pausePendingCommandsForReconnect();
        appliedSubscriptionsKeyRef.current = null;
        subscribeInFlightIdRef.current = null;
        socket = null;
        if (socketRef.current === websocket) socketRef.current = null;
        if (mountedRef.current) setConnectionState("disconnected");
        // WS 403 has no status code in the browser. Probe REST with the
        // bootstrap cookie so Runtime restart surfaces a clear recovery action.
        void (async () => {
          if (cancelled || socket !== null) return;
          try {
            await getSessions({ timeoutMs: 2_500 });
          } catch (error) {
            if (cancelled) return;
            if (isCapabilityForbidden(error)) {
              reportStreamError(new Error(CAPABILITY_FORBIDDEN));
            }
          }
        })();
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
      failAllPendingCommands(CLIENT_IO_ERROR.DISCONNECTED);
      deferredReleaseRef.current.clear();
      releaseInFlightRef.current.clear();
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
    failAllPendingCommands,
    pausePendingCommandsForReconnect,
    reportStreamError,
    settleCommandResult,
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
    setTerminalSubscriptions,
    requestTerminalResync,
    /** Per-session counter bumped on (re)claim; Terminal resets lastSentSize. */
    controlEpochs,
  };
}
