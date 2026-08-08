import { useCallback, useEffect, useRef, useState } from "react";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal as XTerm } from "@xterm/xterm";
import { useTerminalIO } from "../context/TerminalIOContext.jsx";
import { useI18n } from "../i18n/index.js";
import {
  decodeBase64ToUint8Array,
  encodeUtf8ToBase64,
} from "../utils/base64.js";
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
export function createTerminalWriteQueue(term) {
  const queue = [];
  let busy = false;
  let disposed = false;

  const writeBytes = (bytes, done) => {
    if (disposed) {
      done();
      return;
    }
    if (!bytes || bytes.length === 0) {
      done();
      return;
    }
    try {
      term.write(bytes, () => {
        done();
      });
    } catch {
      done();
    }
  };

  const processEntry = (entry, done) => {
    if (disposed) {
      done();
      return;
    }
    const bytes = decodeBase64ToUint8Array(entry.data_base64);
    if (entry.reset) {
      try {
        term.reset();
      } catch {
        /* ignore reset races after dispose */
      }
      writeBytes(bytes, done);
      return;
    }
    writeBytes(bytes, done);
  };

  const pump = () => {
    if (disposed || busy) return;
    const entry = queue.shift();
    if (!entry) return;
    busy = true;
    processEntry(entry, () => {
      busy = false;
      if (disposed) {
        queue.length = 0;
        return;
      }
      pump();
    });
  };

  return {
    enqueue(entry) {
      if (disposed || !entry) return;
      queue.push(entry);
      pump();
    },
    dispose() {
      disposed = true;
      queue.length = 0;
    },
    get pending() {
      return queue.length;
    },
    get isBusy() {
      return busy;
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
 * terminal_resize follows the visible fit always (viewport sync): read-only
 * terminals still publish their grid so the server-side PTY/vt100 screen stays
 * in step with the local viewport. terminal_input is gated strictly by the
 * global interactive switch.
 */
export function Terminal({ id, heightKey, rows, cols, label }) {
  const { t } = useI18n();
  const {
    interactive,
    sendTerminalInput,
    sendTerminalResize,
    connectionState,
    unconfirmedInputs,
    subscribeResizeAck,
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
  const onDataDisposableRef = useRef(null);
  const lastSentSizeRef = useRef({ cols: 0, rows: 0 });
  /** In-flight resize awaiting its single result: dedupe commits only on ack. */
  const pendingResizeRef = useRef(null);
  const resizeTimerRef = useRef(0);
  // Height is scoped to the Codex supervisor group, not the Grok session.
  const groupHeightKey = heightKey;

  interactiveRef.current = interactive;
  sendInputRef.current = sendTerminalInput;
  sendResizeRef.current = sendTerminalResize;

  const [height, setHeight] = useState(() =>
    readTerminalHeight(groupHeightKey),
  );

  const maybeSendResize = useCallback((term) => {
    if (!term) return;
    if (!canFitElement(hostRef.current)) return;
    const { cols: nextCols, rows: nextRows } = clampTerminalGrid(
      term.cols,
      term.rows,
    );
    const last = lastSentSizeRef.current;
    if (last.cols === nextCols && last.rows === nextRows) return;
    const pending = pendingResizeRef.current;
    if (pending?.cols === nextCols && pending?.rows === nextRows) return;
    const result = sendResizeRef.current(id, nextCols, nextRows);
    // Dedupe commits only when the server acks this exact id
    // (subscribeResizeAck), never on send success, so a lost ack keeps the
    // size retryable after failure or reconnect.
    if (result?.ok && typeof result.id === "string") {
      pendingResizeRef.current = {
        id: result.id,
        cols: nextCols,
        rows: nextRows,
      };
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
  }, [id]);

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

    const writeQueue = createTerminalWriteQueue(term);
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
      // One terminal_input per event/paste (no chunking): an oversized input
      // is rejected whole by sendTerminalInput before any byte is sent.
      const dataBase64 = encodeUtf8ToBase64(data);
      if (!dataBase64) return;
      sendInputRef.current(id, dataBase64);
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

  // Commit resize dedupe only when the server acks this terminal's exact
  // pending resize id; a lost ack (disconnect) leaves it retryable.
  useEffect(() => {
    const unsubscribe = subscribeResizeAck((ackSession, ackId, ok) => {
      const pending = pendingResizeRef.current;
      if (!pending) return;
      if (ackSession !== id || ackId !== pending.id) return;
      if (ok) {
        lastSentSizeRef.current = { cols: pending.cols, rows: pending.rows };
      }
      pendingResizeRef.current = null;
      if (!ok) scheduleFit();
    });
    return unsubscribe;
  }, [id, scheduleFit, subscribeResizeAck]);

  // Any Keyboard mode transition invalidates the previous PTY-size claim.
  // Turning the mode back on schedules a fresh idempotent resize; turning it
  // off just clears the claim — the next fit still re-publishes the size.
  const previousInteractiveRef = useRef(interactive);
  useEffect(() => {
    const previous = previousInteractiveRef.current;
    previousInteractiveRef.current = interactive;
    if (previous === interactive) return;
    lastSentSizeRef.current = { cols: 0, rows: 0 };
    pendingResizeRef.current = null;
    if (interactive) scheduleFit();
  }, [interactive, scheduleFit]);

  // Resize is idempotent: after failure or disconnect the dedupe is cleared so
  // the visible size is re-published after reconnect. Input is never replayed.
  const previousConnectionRef = useRef(connectionState);
  useEffect(() => {
    const previous = previousConnectionRef.current;
    previousConnectionRef.current = connectionState;
    if (previous === "connected" && connectionState !== "connected") {
      lastSentSizeRef.current = { cols: 0, rows: 0 };
      pendingResizeRef.current = null;
    }
    if (connectionState === "connected" && previous !== "connected") {
      scheduleFit();
    }
  }, [connectionState, scheduleFit]);

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
        {unconfirmedInputs > 0 ? (
          <span
            className="terminal-indeterminate text-warning"
            data-terminal-indeterminate="true"
          >
            {t("interactive.indeterminate")}
          </span>
        ) : null}
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
