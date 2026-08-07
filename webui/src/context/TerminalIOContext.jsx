import { createContext, useContext } from "react";

/**
 * Shared WebSocket I/O + interactive flag for all terminal panels.
 * interactive is session-scoped to the page load (not persisted).
 */
export const TerminalIOContext = createContext({
  interactive: false,
  setInteractive: () => {},
  connectionState: "initial",
  sendTerminalInput: () => ({ ok: false, error: "send_failed" }),
  sendTerminalResize: () => ({ ok: false, error: "send_failed" }),
  setTerminalSubscriptions: () => ({ ok: false, error: "send_failed" }),
  requestTerminalResync: () => ({ ok: false, error: "send_failed" }),
  /** Per-session epoch bumped on (re)claim so Terminal re-sends current grid. */
  controlEpochs: /** @type {Record<string, number>} */ ({}),
});

export function useTerminalIO() {
  return useContext(TerminalIOContext);
}
