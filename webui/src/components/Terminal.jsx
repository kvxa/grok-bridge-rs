import { useCallback, useEffect, useRef, useState } from "react";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal as XTerm } from "@xterm/xterm";
import { useTerminalIO } from "../context/TerminalIOContext.jsx";
import { useI18n } from "../i18n/index.js";
import {
  encodeUtf8ToBase64,
  decodeBase64ToUint8Array,
} from "../utils/base64.js";
import {
  TERMINAL_DELTA_QUEUE_MAX_BYTES,
  TERMINAL_DELTA_QUEUE_MAX_ENTRIES,
  TERMINAL_SNAPSHOT_STREAM_MAX_BYTES,
  isResetCont,
  isResetHead,
  isSnapshotPiece,
} from "../utils/terminalBounds.js";
import { subscribeTerminal } from "../utils/terminalFeeds.js";
import { clampTerminalGrid } from "../utils/terminalGrid.js";
import {
  TERMINAL_HEIGHT_DEFAULT,
  TERMINAL_HEIGHT_MIN,
  canFitElement,
  clampTerminalHeight,
  maxTerminalHeight,
  readTerminalHeight,
  subscribeTerminalHeight,
  writeTerminalHeight,
} from "../utils/terminalHeight.js";
import {
  TERMINAL_FONT_FAMILY,
  readTerminalTheme,
} from "../utils/terminalTheme.js";

const DEFAULT_ROWS = 24;
const DEFAULT_COLS = 80;
const RESIZE_STEP_PX = 24;
const RESIZE_DEBOUNCE_MS = 120;

/** Bound xterm async write queue entry count (ordinary deltas). */
export const TERMINAL_WRITE_QUEUE_MAX = TERMINAL_DELTA_QUEUE_MAX_ENTRIES;
/** Bound decoded payload bytes for ordinary PTY deltas (not snapshot stream). */
export const TERMINAL_WRITE_QUEUE_MAX_BYTES = TERMINAL_DELTA_QUEUE_MAX_BYTES;
/** Bound for queued multi-frame reset snapshot stream (head + cont). */
export const TERMINAL_SNAPSHOT_QUEUE_MAX_BYTES = TERMINAL_SNAPSHOT_STREAM_MAX_BYTES;

function safeRows(rows) {
  const value = Number(rows);
  if (!Number.isFinite(value) || value < 1) return DEFAULT_ROWS;
  return Math.min(Math.floor(value), 500);
}

function safeCols(cols) {
  const value = Number(cols);
  if (!Number.isFinite(value) || value < 1) return DEFAULT_COLS;
  return Math.min(Math.floor(value), 500);
}

/**
 * Per-terminal FIFO drain for xterm writes.
 * xterm `write(data, callback)` parses asynchronously; a later `reset()` must
 * not run until earlier queued writes' callbacks have fired, and a reset entry
 * must complete its snapshot write before later appends.
 */
/**
 * Merge two consecutive **delta** entries into one without losing bytes.
 * Snapshot pieces (reset / reset_cont) are never coalesced.
 */
export function coalesceTerminalEntries(left, right) {
  if (
    !left ||
    !right ||
    left.reset ||
    right.reset ||
    left.reset_cont ||
    right.reset_cont ||
    left.gap ||
    right.gap
  ) {
    return null;
  }
  const a = decodeBase64ToUint8Array(left.data_base64);
  const b = decodeBase64ToUint8Array(right.data_base64);
  const merged = new Uint8Array(a.length + b.length);
  merged.set(a, 0);
  merged.set(b, a.length);
  let binary = "";
  for (let i = 0; i < merged.length; i += 1) {
    binary += String.fromCharCode(merged[i]);
  }
  return {
    reset: false,
    reset_cont: false,
    data_base64: btoa(binary),
    session: left.session ?? right.session,
    _byteLen: merged.length,
  };
}

export function terminalEntryByteLength(entry) {
  if (!entry || entry.gap) return 0;
  if (typeof entry._byteLen === "number") return entry._byteLen;
  const raw = entry.data_base64;
  if (typeof raw !== "string" || raw.length === 0) return 0;
  // base64 length → approximate decoded bytes without allocating.
  const padding = raw.endsWith("==") ? 2 : raw.endsWith("=") ? 1 : 0;
  return Math.max(0, Math.floor((raw.length * 3) / 4) - padding);
}

function coalesceQueueInPlace(queue) {
  for (let i = 0; i < queue.length - 1; i += 1) {
    if (
      isSnapshotPiece(queue[i]) ||
      isSnapshotPiece(queue[i + 1]) ||
      queue[i].gap ||
      queue[i + 1].gap
    ) {
      continue;
    }
    const merged = coalesceTerminalEntries(queue[i], queue[i + 1]);
    if (!merged) continue;
    queue[i] = merged;
    queue.splice(i + 1, 1);
    return true;
  }
  return false;
}

/**
 * Per-terminal FIFO drain for xterm writes.
 *
 * **Reset snapshot stream** (`reset` head + `reset_cont` pieces): streamed with
 * a large bound so multi-frame ANSI snapshots (>1 MiB) never overflow-resync
 * into an infinite loop. Only the head calls `term.reset()`.
 *
 * **PTY deltas**: entry-count + decoded-byte bounds. Overflow → single gap +
 * `onResync` (exactly once until a new reset head arrives).
 *
 * Decode / `term.write` failures also enter the gap/resync path (never silent
 * drop, never stuck busy).
 */
export function createTerminalWriteQueue(
  term,
  {
    maxPending = TERMINAL_WRITE_QUEUE_MAX,
    maxBytes = TERMINAL_WRITE_QUEUE_MAX_BYTES,
    snapshotMaxBytes = TERMINAL_SNAPSHOT_QUEUE_MAX_BYTES,
    onResync = null,
  } = {},
) {
  const queue = [];
  let busy = false;
  let disposed = false;
  let queuedBytes = 0;
  let snapshotQueuedBytes = 0;
  let resyncRequested = false;
  /** Accepting reset_cont after a reset head until a non-snapshot entry. */
  let inResetStream = false;
  const byteBudget = Math.max(1, Number(maxBytes) || TERMINAL_WRITE_QUEUE_MAX_BYTES);
  const entryBudget = Math.max(1, Number(maxPending) || TERMINAL_WRITE_QUEUE_MAX);
  const snapshotBudget = Math.max(
    byteBudget,
    Number(snapshotMaxBytes) || TERMINAL_SNAPSHOT_QUEUE_MAX_BYTES,
  );

  const recomputeBytes = () => {
    queuedBytes = 0;
    snapshotQueuedBytes = 0;
    for (const entry of queue) {
      const n = terminalEntryByteLength(entry);
      queuedBytes += n;
      if (isSnapshotPiece(entry)) snapshotQueuedBytes += n;
    }
  };

  const requestResync = () => {
    if (resyncRequested) return;
    resyncRequested = true;
    inResetStream = false;
    queue.length = 0;
    queuedBytes = 0;
    snapshotQueuedBytes = 0;
    // Deterministic gap marker so consumers know a snapshot is required.
    queue.push({ reset: false, reset_cont: false, gap: true, data_base64: "" });
    try {
      onResync?.();
    } catch {
      /* ignore resync callback errors */
    }
  };

  const writeBytes = (bytes, done, { allowEmpty = false } = {}) => {
    if (disposed) {
      done({ ok: true });
      return;
    }
    if (!bytes || bytes.length === 0) {
      done({ ok: allowEmpty });
      return;
    }
    try {
      term.write(bytes, () => {
        done({ ok: true });
      });
    } catch {
      // Never leave pump busy forever; surface as gap/resync.
      done({ ok: false, error: "term_write_failed" });
    }
  };

  const processEntry = (entry, done) => {
    if (disposed) {
      done();
      return;
    }
    if (entry.gap) {
      done();
      return;
    }
    const raw = entry.data_base64;
    let bytes;
    try {
      bytes = decodeBase64ToUint8Array(raw);
    } catch {
      requestResync();
      done();
      return;
    }
    // Non-empty base64 that decodes empty is treated as corrupt payload.
    if (typeof raw === "string" && raw.length > 0 && bytes.length === 0) {
      requestResync();
      done();
      return;
    }
    if (isResetHead(entry)) {
      try {
        term.reset();
      } catch {
        /* ignore reset races after dispose */
      }
      writeBytes(bytes, (result) => {
        if (!result?.ok && bytes.length > 0) {
          requestResync();
        }
        done();
      }, { allowEmpty: true });
      return;
    }
    if (isResetCont(entry)) {
      writeBytes(bytes, (result) => {
        if (!result?.ok && bytes.length > 0) {
          requestResync();
        }
        done();
      }, { allowEmpty: true });
      return;
    }
    writeBytes(bytes, (result) => {
      if (!result?.ok && bytes.length > 0) {
        requestResync();
      }
      done();
    });
  };

  const pump = () => {
    if (disposed || busy) return;
    const entry = queue.shift();
    if (!entry) return;
    const n = terminalEntryByteLength(entry);
    queuedBytes = Math.max(0, queuedBytes - n);
    if (isSnapshotPiece(entry)) {
      snapshotQueuedBytes = Math.max(0, snapshotQueuedBytes - n);
    }
    busy = true;
    processEntry(entry, () => {
      busy = false;
      if (disposed) {
        queue.length = 0;
        queuedBytes = 0;
        snapshotQueuedBytes = 0;
        return;
      }
      pump();
    });
  };

  return {
    enqueue(entry) {
      if (disposed || !entry) return;

      // Authoritative snapshot head: supersede pending work and clear gap state.
      if (isResetHead(entry)) {
        queue.length = 0;
        queuedBytes = 0;
        snapshotQueuedBytes = 0;
        resyncRequested = false;
        inResetStream = true;
        const entryBytes = terminalEntryByteLength(entry);
        if (entryBytes > snapshotBudget) {
          // Pathological single head beyond absolute cap — still attempt stream
          // of this one piece (never loop on smaller budgets).
        }
        queue.push(entry);
        queuedBytes = entryBytes;
        snapshotQueuedBytes = entryBytes;
        pump();
        return;
      }

      // Multi-frame snapshot continuation: never treat as delta overflow.
      if (isResetCont(entry)) {
        if (resyncRequested) {
          // Wait for a new reset head after gap.
          return;
        }
        if (!inResetStream) {
          // Orphan cont without head — one resync, then wait for head.
          requestResync();
          return;
        }
        const entryBytes = terminalEntryByteLength(entry);
        if (snapshotQueuedBytes + entryBytes > snapshotBudget) {
          // Absolute safety valve only; legal multi-frame snapshots fit here.
          requestResync();
          return;
        }
        queue.push(entry);
        queuedBytes += entryBytes;
        snapshotQueuedBytes += entryBytes;
        pump();
        return;
      }

      if (resyncRequested) {
        // Wait for the authoritative reset snapshot; drop interim deltas.
        return;
      }

      // Ordinary delta ends the snapshot stream assembly window.
      inResetStream = false;
      const entryBytes = terminalEntryByteLength(entry);
      // Single delta larger than the budget cannot be buffered; resync instead.
      if (entryBytes > byteBudget) {
        requestResync();
        return;
      }
      // Mild pressure: coalesce consecutive deltas to reduce entry count.
      while (queue.length >= entryBudget) {
        if (!coalesceQueueInPlace(queue)) break;
        recomputeBytes();
      }
      // Hard memory bound for deltas (snapshot pieces already in queue count
      // toward total entries but use the separate snapshot byte budget).
      const deltaBytes = Math.max(0, queuedBytes - snapshotQueuedBytes);
      if (
        queue.length >= entryBudget ||
        deltaBytes + entryBytes > byteBudget
      ) {
        requestResync();
        return;
      }
      queue.push(entry);
      queuedBytes += entryBytes;
      pump();
    },
    dispose() {
      disposed = true;
      queue.length = 0;
      queuedBytes = 0;
      snapshotQueuedBytes = 0;
      inResetStream = false;
    },
    get pending() {
      return queue.length;
    },
    get pendingBytes() {
      return queuedBytes;
    },
    get isBusy() {
      return busy;
    },
    get needsResync() {
      return resyncRequested;
    },
    get inResetStream() {
      return inResetStream;
    },
  };
}

/**
 * Schedule FitAddon.fit only when the host has a real non-zero box.
 * Returns true when fit ran.
 */
export function fitTerminalHost(fitAddon, host) {
  if (!fitAddon || !canFitElement(host)) return false;
  try {
    fitAddon.fit();
    return true;
  } catch {
    return false;
  }
}

/**
 * xterm.js terminal driven by the WebSocket feed.
 * terminal_resize follows visible fit only while interactive is on so
 * read-only layout changes never touch the shared PTY.
 * terminal_input is gated strictly by the global interactive switch.
 */
export function Terminal({ id, heightKey, rows, cols, label }) {
  const { t } = useI18n();
  const {
    interactive,
    sendTerminalInput,
    sendTerminalResize,
    connectionState,
    requestTerminalResync,
    controlEpochs,
  } = useTerminalIO();

  const hostRef = useRef(null);
  const shellRef = useRef(null);
  const termRef = useRef(null);
  const fitRef = useRef(null);
  const fitRafRef = useRef(0);
  const dragRef = useRef(null);
  const interactiveRef = useRef(interactive);
  const sendInputRef = useRef(sendTerminalInput);
  const sendResizeRef = useRef(sendTerminalResize);
  const requestResyncRef = useRef(requestTerminalResync);
  const onDataDisposableRef = useRef(null);
  const lastSentSizeRef = useRef({ cols: 0, rows: 0 });
  /** Request id of the in-flight resize that may commit lastSentSize. */
  const pendingResizeRef = useRef(null);
  const resizeTimerRef = useRef(0);
  const resyncRetryTimerRef = useRef(0);
  /** Last applied control epoch for this session (skip initial 0). */
  const seenControlEpochRef = useRef(0);
  // Height is scoped to the Codex supervisor group, not the Grok session.
  const groupHeightKey = heightKey;
  const controlEpoch = controlEpochs?.[id] || 0;

  interactiveRef.current = interactive;
  sendInputRef.current = sendTerminalInput;
  sendResizeRef.current = sendTerminalResize;
  requestResyncRef.current = requestTerminalResync;

  const [height, setHeight] = useState(() =>
    readTerminalHeight(groupHeightKey),
  );

  const maybeSendResize = useCallback((term) => {
    if (!term) return;
    // Read-only layout fits locally; never claim/write the shared PTY.
    if (!interactiveRef.current) return;
    if (!canFitElement(hostRef.current)) return;
    const { cols: nextCols, rows: nextRows } = clampTerminalGrid(
      term.cols,
      term.rows,
    );
    const last = lastSentSizeRef.current;
    if (last.cols === nextCols && last.rows === nextRows) return;
    // Same size already in flight — wait for that request's result.
    const inflight = pendingResizeRef.current;
    if (
      inflight &&
      inflight.cols === nextCols &&
      inflight.rows === nextRows
    ) {
      return;
    }
    // Capture this attempt's size. A later different size supersedes it.
    const target = { cols: nextCols, rows: nextRows };
    const attempt = {
      requestId: null,
      cols: target.cols,
      rows: target.rows,
    };
    pendingResizeRef.current = attempt;
    // Commit only when the ack is for this attempt's request id (when known)
    // and the exact cols/rows. Session-only success must never commit.
    const sent = sendResizeRef.current(id, target.cols, target.rows, {
      onResult: (result) => {
        const pending = pendingResizeRef.current;
        if (!pending || pending !== attempt) return;
        // A newer resize replaced this target — ignore this ack for dedupe.
        if (pending.cols !== target.cols || pending.rows !== target.rows) {
          return;
        }
        if (
          pending.requestId != null &&
          result?.id != null &&
          result.id !== pending.requestId
        ) {
          return;
        }
        const clearIfCurrent = () => {
          if (
            pendingResizeRef.current &&
            pendingResizeRef.current.cols === target.cols &&
            pendingResizeRef.current.rows === target.rows
          ) {
            pendingResizeRef.current = null;
          }
        };
        if (!result?.ok) {
          clearIfCurrent();
          return;
        }
        // Exact size required on the result (originating command binds these).
        // Session-only or wrong-size acks never commit; clear so the size retries.
        if (
          Number(result.cols) !== target.cols ||
          Number(result.rows) !== target.rows
        ) {
          // Only clear when this result claims our request id (completed, wrong payload).
          if (
            result.id == null ||
            pending.requestId == null ||
            result.id === pending.requestId
          ) {
            clearIfCurrent();
          }
          return;
        }
        // Bind request id from the ack when the send was queued behind claim.
        if (pending.requestId == null && typeof result.id === "string") {
          pending.requestId = result.id;
        }
        if (
          pending.requestId != null &&
          result.id != null &&
          result.id !== pending.requestId
        ) {
          return;
        }
        lastSentSizeRef.current = { cols: target.cols, rows: target.rows };
        clearIfCurrent();
      },
    });
    if (!sent?.ok) {
      if (pendingResizeRef.current === attempt) {
        pendingResizeRef.current = null;
      }
      return;
    }
    // onResult may have fired synchronously on an admission failure. Never
    // resurrect an attempt that the callback already finalized.
    if (pendingResizeRef.current === attempt) {
      attempt.requestId = typeof sent.id === "string" ? sent.id : null;
    }
  }, [id]);

  const scheduleFit = useCallback(() => {
    if (fitRafRef.current) {
      cancelAnimationFrame(fitRafRef.current);
    }
    fitRafRef.current = requestAnimationFrame(() => {
      fitRafRef.current = 0;
      const fitted = fitTerminalHost(fitRef.current, hostRef.current);
      if (!fitted) return;
      const term = termRef.current;
      if (!term) return;
      if (resizeTimerRef.current) {
        window.clearTimeout(resizeTimerRef.current);
      }
      resizeTimerRef.current = window.setTimeout(() => {
        resizeTimerRef.current = 0;
        maybeSendResize(term);
      }, RESIZE_DEBOUNCE_MS);
    });
  }, [maybeSendResize]);

  const applyHeight = useCallback(
    (next) => {
      // Persists + notifies same-group terminals; always update local height.
      setHeight(writeTerminalHeight(groupHeightKey, next));
    },
    [groupHeightKey],
  );

  useEffect(() => {
    setHeight(readTerminalHeight(groupHeightKey));
    return subscribeTerminalHeight(groupHeightKey, setHeight);
  }, [groupHeightKey]);

  useEffect(() => {
    lastSentSizeRef.current = { cols: 0, rows: 0 };
    pendingResizeRef.current = null;
    seenControlEpochRef.current = 0;
  }, [id]);

  // After (re)claim, another tab may have resized the PTY. lastSentSize is no
  // longer authoritative — clear it and force the current local grid on the wire.
  useEffect(() => {
    if (!controlEpoch || controlEpoch === seenControlEpochRef.current) return;
    seenControlEpochRef.current = controlEpoch;
    lastSentSizeRef.current = { cols: 0, rows: 0 };
    pendingResizeRef.current = null;
    if (!interactiveRef.current) return;
    const term = termRef.current;
    if (!term) return;
    maybeSendResize(term);
  }, [controlEpoch, maybeSendResize]);

  // Mount xterm once per session id.
  useEffect(() => {
    const host = hostRef.current;
    if (!host) return undefined;

    const term = new XTerm({
      disableStdin: true,
      cursorBlink: false,
      convertEol: false,
      scrollback: 5000,
      fontFamily: TERMINAL_FONT_FAMILY,
      fontSize: 13,
      lineHeight: 1,
      theme: readTerminalTheme(),
      rows: safeRows(rows),
      cols: safeCols(cols),
      allowProposedApi: false,
    });

    const fitAddon = new FitAddon();
    term.loadAddon(fitAddon);
    term.open(host);
    termRef.current = term;
    fitRef.current = fitAddon;

    const writeQueue = createTerminalWriteQueue(term, {
      onResync: () => {
        // Overflow dropped buffered deltas; force a server ANSI reset snapshot.
        const request = () => {
          resyncRetryTimerRef.current = 0;
          let result;
          try {
            result = requestResyncRef.current?.(id);
          } catch {
            result = { ok: false, error: "send_failed" };
          }
          if (
            result?.ok === false &&
            ["flow_control", "disconnected", "send_failed"].includes(result.error) &&
            !resyncRetryTimerRef.current
          ) {
            resyncRetryTimerRef.current = window.setTimeout(request, 250);
          }
        };
        request();
      },
    });
    const unsubscribe = subscribeTerminal(id, (entry) => {
      writeQueue.enqueue(entry);
    });

    const runFit = () => {
      fitTerminalHost(fitAddon, host);
    };
    runFit();
    scheduleFit();

    let resizeObserver = null;
    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(() => {
        scheduleFit();
      });
      resizeObserver.observe(host);
    }

    const onWindowResize = () => {
      setHeight((current) => clampTerminalHeight(current));
      scheduleFit();
    };
    window.addEventListener("resize", onWindowResize);

    return () => {
      unsubscribe();
      writeQueue.dispose();
      window.removeEventListener("resize", onWindowResize);
      if (resizeObserver) resizeObserver.disconnect();
      if (fitRafRef.current) {
        cancelAnimationFrame(fitRafRef.current);
        fitRafRef.current = 0;
      }
      if (resizeTimerRef.current) {
        window.clearTimeout(resizeTimerRef.current);
        resizeTimerRef.current = 0;
      }
      if (resyncRetryTimerRef.current) {
        window.clearTimeout(resyncRetryTimerRef.current);
        resyncRetryTimerRef.current = 0;
      }
      if (onDataDisposableRef.current) {
        try {
          onDataDisposableRef.current.dispose();
        } catch {
          /* ignore */
        }
        onDataDisposableRef.current = null;
      }
      fitRef.current = null;
      termRef.current = null;
      try {
        fitAddon.dispose();
      } catch {
        /* ignore */
      }
      term.dispose();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- intentional mount-per-id
  }, [id, scheduleFit]);

  // Toggle stdin + onData without rebuilding Terminal.
  useEffect(() => {
    const term = termRef.current;
    if (!term) return undefined;

    if (onDataDisposableRef.current) {
      try {
        onDataDisposableRef.current.dispose();
      } catch {
        /* ignore */
      }
      onDataDisposableRef.current = null;
    }

    term.options.disableStdin = !interactive;
    term.options.cursorBlink = interactive;

    if (!interactive) {
      return undefined;
    }

    const disposable = term.onData((data) => {
      // Never buffer: if mode flipped off mid-event, drop immediately.
      if (!interactiveRef.current) return;
      // Admit the complete UTF-8 payload before the first socket send. The
      // backend accepts one raw write atomically up to its write limit; sending
      // chunks here would commit a prefix before a later flow-control failure.
      if (!interactiveRef.current) return;
      sendInputRef.current(id, encodeUtf8ToBase64(data));
    });
    onDataDisposableRef.current = disposable;

    return () => {
      if (onDataDisposableRef.current === disposable) {
        try {
          disposable.dispose();
        } catch {
          /* ignore */
        }
        onDataDisposableRef.current = null;
      }
    };
  }, [id, interactive]);

  // Read-only fits stay local; publish the current grid once control is armed.
  useEffect(() => {
    if (!interactive) return;
    scheduleFit();
  }, [interactive, scheduleFit]);

  useEffect(() => {
    scheduleFit();
  }, [height, scheduleFit]);

  useEffect(() => {
    const shell = shellRef.current;
    if (!shell || typeof ResizeObserver === "undefined") return undefined;
    const observer = new ResizeObserver(() => {
      scheduleFit();
    });
    observer.observe(shell);
    return () => observer.disconnect();
  }, [id, scheduleFit]);

  useEffect(() => {
    const applyTheme = () => {
      const term = termRef.current;
      if (!term) return;
      term.options.theme = readTerminalTheme();
    };

    applyTheme();
    const root = document.documentElement;
    const observer = new MutationObserver((mutations) => {
      for (const mutation of mutations) {
        if (
          mutation.type === "attributes" &&
          (mutation.attributeName === "data-resolved-theme" ||
            mutation.attributeName === "data-theme")
        ) {
          applyTheme();
          break;
        }
      }
    });
    observer.observe(root, {
      attributes: true,
      attributeFilter: ["data-resolved-theme", "data-theme"],
    });
    return () => observer.disconnect();
  }, [id]);

  useEffect(() => {
    return () => {
      const drag = dragRef.current;
      if (!drag) return;
      window.removeEventListener("pointermove", drag.onMove);
      window.removeEventListener("pointerup", drag.onUp);
      window.removeEventListener("pointercancel", drag.onUp);
      dragRef.current = null;
    };
  }, []);

  const onResizePointerDown = (event) => {
    if (event.button != null && event.button !== 0) return;
    event.preventDefault();
    const startY = event.clientY;
    const startHeight = height;
    const onMove = (moveEvent) => {
      const delta = moveEvent.clientY - startY;
      applyHeight(startHeight + delta);
    };
    const onUp = () => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      window.removeEventListener("pointercancel", onUp);
      dragRef.current = null;
      scheduleFit();
    };
    dragRef.current = { onMove, onUp };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
    window.addEventListener("pointercancel", onUp);
  };

  const onResizeKeyDown = (event) => {
    if (event.key === "ArrowUp") {
      event.preventDefault();
      applyHeight(height - RESIZE_STEP_PX);
    } else if (event.key === "ArrowDown") {
      event.preventDefault();
      applyHeight(height + RESIZE_STEP_PX);
    } else if (event.key === "Home") {
      event.preventDefault();
      applyHeight(TERMINAL_HEIGHT_MIN);
    } else if (event.key === "End") {
      event.preventDefault();
      applyHeight(maxTerminalHeight());
    } else if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      applyHeight(TERMINAL_HEIGHT_DEFAULT);
    }
  };

  const maxHeight = maxTerminalHeight();
  const ptyLabel = `${safeCols(cols)}×${safeRows(rows)}`;
  const headerLabel = interactive
    ? t("terminal.headerInteractive")
    : t("terminal.header");

  return (
    <div
      ref={shellRef}
      className="terminal-shell"
      data-terminal={id}
      data-readonly={interactive ? "false" : "true"}
      data-interactive={interactive ? "on" : "off"}
      data-terminal-height={height}
    >
      <div
        className="terminal-header"
        data-terminal-header="true"
      >
        <span className="subheader terminal-label">
          {headerLabel}
        </span>
        <span className="terminal-grid-label text-secondary">
          {ptyLabel}
          {connectionState !== "connected" && interactive
            ? ` · ${t("interactive.unavailableShort")}`
            : ""}
        </span>
      </div>
      <div
        ref={hostRef}
        className="terminal-xterm"
        style={{
          fontFamily: TERMINAL_FONT_FAMILY,
          height: `${height}px`,
          minHeight: `${TERMINAL_HEIGHT_MIN}px`,
          maxHeight: `${maxHeight}px`,
        }}
        role="log"
        aria-label={label || t("terminal.aria", { id })}
        aria-live="off"
        tabIndex={0}
        data-terminal-host="true"
      />
      <div
        className="terminal-resize-handle"
        role="separator"
        aria-orientation="horizontal"
        aria-label={t("terminal.resizeAria")}
        aria-valuemin={TERMINAL_HEIGHT_MIN}
        aria-valuemax={maxHeight}
        aria-valuenow={height}
        aria-valuetext={t("terminal.resizeValue", { height })}
        title={t("terminal.resizeTitle")}
        tabIndex={0}
        data-terminal-resize="true"
        onPointerDown={onResizePointerDown}
        onKeyDown={onResizeKeyDown}
      >
        <span
          className="terminal-resize-grip"
          aria-hidden="true"
        />
        <span className="visually-hidden">{t("terminal.resizeHint")}</span>
      </div>
    </div>
  );
}

export {
  TERMINAL_HEIGHT_DEFAULT,
  TERMINAL_HEIGHT_MIN,
  clampTerminalHeight,
  maxTerminalHeight,
} from "../utils/terminalHeight.js";
