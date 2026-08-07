import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { I18nProvider } from "../i18n/index.js";
import { MockXTerm } from "../test/mockXterm.js";
import { activityLabel, activityOf } from "../sessions.js";
import { createTranslator } from "../i18n/translate.js";
import { TERMINAL_HEIGHT_DEFAULT } from "../utils/terminalHeight.js";
import { SupervisorGroup } from "./SupervisorGroup.jsx";

vi.mock("@xterm/xterm", () => ({
  Terminal: MockXTerm,
}));

vi.mock("@xterm/addon-fit", () => {
  class MockFitAddon {
    activate() {}
    fit() {}
    dispose() {}
  }
  return { FitAddon: MockFitAddon };
});

function baseSession(overrides = {}) {
  return {
    session: "gbt-1",
    owner: "Codex Tab Owner",
    client_session_id: "thread-tabs",
    client_state: "connected",
    phase: "running",
    title: "Very Long Grok Title That Must Never Appear On Tabs",
    cwd: "C:\\work\\tabs",
    process_id: 11,
    updated_at_ms: Date.now(),
    activity: "working",
    hook_event: null,
    tool_name: null,
    waiting_reason: null,
    rows: 24,
    cols: 80,
    ...overrides,
  };
}

const multiSessions = [
  baseSession({
    session: "gbt-1",
    title: "Alpha Task Title",
    activity: "working",
    phase: "running",
  }),
  baseSession({
    session: "gbt-2",
    title: "Beta Task Title",
    activity: "waiting",
    phase: "running",
    waiting_reason: "user",
    process_id: 12,
  }),
  baseSession({
    session: "gbt-3",
    title: "Gamma Task Title",
    activity: "done",
    phase: "idle",
    process_id: 13,
  }),
];

describe("SupervisorGroup session tabs", () => {
  let container;
  let root;
  const tEn = createTranslator("en");

  beforeEach(() => {
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
    MockXTerm.reset();
    vi.stubGlobal(
      "requestAnimationFrame",
      (cb) => {
        cb(0);
        return 1;
      },
    );
    vi.stubGlobal("cancelAnimationFrame", () => {});
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        unobserve() {}
        disconnect() {}
      },
    );
    Object.defineProperty(window, "innerHeight", {
      configurable: true,
      writable: true,
      value: 1200,
    });
    localStorage.clear();
  });

  afterEach(async () => {
    await act(async () => {
      root.unmount();
    });
    container.remove();
    MockXTerm.reset();
    vi.unstubAllGlobals();
  });

  async function renderGroup(sessions, props = {}) {
    const collapsedSessions = props.collapsedSessions ?? new Set();
    await act(async () => {
      root.render(
        <I18nProvider initialLocale="en">
          <SupervisorGroup
            groupKey="client:thread-tabs"
            owner="Codex Tab Owner"
            clientSessionId="thread-tabs"
            sessions={sessions}
            collapsed={false}
            collapsedSessions={collapsedSessions}
            onToggle={() => {}}
            onToggleSession={() => {}}
            onCloseGroup={() => {}}
            onCloseSession={() => {}}
            busy={false}
            {...props}
          />
        </I18nProvider>,
      );
    });
  }

  function tabs() {
    return [...container.querySelectorAll('[role="tab"]')];
  }

  function panels() {
    return [...container.querySelectorAll('[role="tabpanel"]')];
  }

  function visiblePanels() {
    return panels().filter((panel) => !panel.hidden);
  }

  it("renders a single-row tablist with one compact tab per session", async () => {
    await renderGroup(multiSessions);
    const tablist = container.querySelector('[role="tablist"]');
    expect(tablist).not.toBeNull();
    expect(tabs()).toHaveLength(3);
    expect(panels()).toHaveLength(3);
    // Custom overflow rules keep horizontal scrolling without a vertical bar.
    expect(tablist.className).toMatch(/session-tabs/);
    expect(tablist.className).not.toMatch(/overflow-x-auto/);
    expect(tablist.className).toMatch(/flex-nowrap/);
  });

  it("shows only ordinal + localized activity status in visible tab text, never titles", async () => {
    await renderGroup(multiSessions);
    for (const [index, session] of multiSessions.entries()) {
      const tab = tabs()[index];
      const status = activityLabel(activityOf(session), tEn);
      expect(tab.textContent).toContain(String(index + 1));
      expect(tab.textContent).toContain(status);
      expect(tab.textContent).not.toContain(session.title);
      expect(tab.textContent).not.toContain("Alpha");
      expect(tab.textContent).not.toContain("Beta");
      expect(tab.textContent).not.toContain("Gamma");
      // Accessible name may identify the session.
      expect(tab.getAttribute("aria-label")).toContain(session.title);
      expect(tab.getAttribute("aria-label")).toContain(status);
    }
  });

  it("shows exactly one visible tabpanel; hidden tabs do not mount terminals", async () => {
    await renderGroup(multiSessions);
    expect(visiblePanels()).toHaveLength(1);
    expect(visiblePanels()[0].getAttribute("data-session-panel")).toBe("gbt-1");
    expect(panels().filter((panel) => panel.hidden)).toHaveLength(2);
    // Only the selected session mounts a card/terminal (no hidden parse).
    expect(container.querySelectorAll("details.session")).toHaveLength(1);
    expect(container.querySelectorAll("[data-terminal]")).toHaveLength(1);
  });

  it("clicking a tab selects that session and links aria-selected/aria-controls", async () => {
    await renderGroup(multiSessions);
    const second = tabs()[1];
    await act(async () => second.click());
    expect(second.getAttribute("aria-selected")).toBe("true");
    expect(tabs()[0].getAttribute("aria-selected")).toBe("false");
    expect(tabs()[2].getAttribute("aria-selected")).toBe("false");
    const panelId = second.getAttribute("aria-controls");
    const panel = container.querySelector('[data-session-panel="gbt-2"]');
    expect(panel).not.toBeNull();
    expect(panel.id).toBe(panelId);
    expect(panel.getAttribute("aria-labelledby")).toBe(second.id);
    expect(panel.hidden).toBe(false);
    expect(visiblePanels()).toHaveLength(1);
    // Selected panel shows the full session title in the card body.
    expect(panel.textContent).toContain("Beta Task Title");
    // Other panels remain mounted but hidden.
    expect(
      container.querySelector('[data-session-panel="gbt-1"]').hidden,
    ).toBe(true);
    expect(
      container.querySelector('[data-session-panel="gbt-3"]').hidden,
    ).toBe(true);
  });

  it("supports ArrowLeft/ArrowRight/Home/End and roving tabIndex", async () => {
    await renderGroup(multiSessions);
    const [tab0, tab1, tab2] = tabs();
    expect(tab0.tabIndex).toBe(0);
    expect(tab1.tabIndex).toBe(-1);
    expect(tab2.tabIndex).toBe(-1);

    tab0.focus();
    await act(async () => {
      tab0.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true }),
      );
    });
    expect(tabs()[1].getAttribute("aria-selected")).toBe("true");
    expect(tabs()[1].tabIndex).toBe(0);
    expect(tabs()[0].tabIndex).toBe(-1);
    expect(document.activeElement).toBe(tabs()[1]);

    await act(async () => {
      tabs()[1].dispatchEvent(
        new KeyboardEvent("keydown", { key: "End", bubbles: true }),
      );
    });
    expect(tabs()[2].getAttribute("aria-selected")).toBe("true");
    expect(tabs()[2].tabIndex).toBe(0);
    expect(document.activeElement).toBe(tabs()[2]);

    await act(async () => {
      tabs()[2].dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowLeft", bubbles: true }),
      );
    });
    expect(tabs()[1].getAttribute("aria-selected")).toBe("true");

    await act(async () => {
      tabs()[1].dispatchEvent(
        new KeyboardEvent("keydown", { key: "Home", bubbles: true }),
      );
    });
    expect(tabs()[0].getAttribute("aria-selected")).toBe("true");
    expect(tabs()[0].tabIndex).toBe(0);
    expect(document.activeElement).toBe(tabs()[0]);
  });

  it("keeps selection stable across metadata updates", async () => {
    await renderGroup(multiSessions);
    await act(async () => tabs()[2].click());
    expect(visiblePanels()[0].getAttribute("data-session-panel")).toBe("gbt-3");

    const updated = multiSessions.map((session) =>
      session.session === "gbt-3"
        ? { ...session, activity: "working", phase: "running", title: "Gamma Renamed" }
        : { ...session, updated_at_ms: Date.now() },
    );
    await renderGroup(updated);
    expect(visiblePanels()).toHaveLength(1);
    expect(visiblePanels()[0].getAttribute("data-session-panel")).toBe("gbt-3");
    expect(tabs()[2].getAttribute("aria-selected")).toBe("true");
    // Tab still omits title; panel shows new title.
    expect(tabs()[2].textContent).not.toContain("Gamma Renamed");
    expect(visiblePanels()[0].textContent).toContain("Gamma Renamed");
  });

  it("falls back safely when the selected session is removed", async () => {
    await renderGroup(multiSessions);
    await act(async () => tabs()[1].click());
    expect(visiblePanels()[0].getAttribute("data-session-panel")).toBe("gbt-2");

    const remaining = [multiSessions[0], multiSessions[2]];
    await renderGroup(remaining);
    expect(tabs()).toHaveLength(2);
    expect(panels()).toHaveLength(2);
    expect(visiblePanels()).toHaveLength(1);
    // First remaining session becomes selected.
    expect(visiblePanels()[0].getAttribute("data-session-panel")).toBe("gbt-1");
    expect(tabs()[0].getAttribute("aria-selected")).toBe("true");
  });

  it("mounts only the selected terminal and remounts on tab switch", async () => {
    await renderGroup(multiSessions);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(MockXTerm.instances).toHaveLength(1);
    expect(container.querySelectorAll("[data-terminal]")).toHaveLength(1);
    expect(container.querySelector("[data-terminal]").dataset.terminal).toBe(
      "gbt-1",
    );

    await act(async () => tabs()[1].click());
    await act(async () => {
      await Promise.resolve();
    });
    expect(container.querySelectorAll("[data-terminal]")).toHaveLength(1);
    expect(container.querySelector("[data-terminal]").dataset.terminal).toBe(
      "gbt-2",
    );
    // Prior selected terminal is disposed so it cannot parse hidden output.
    expect(MockXTerm.instances[0].disposed).toBe(true);
    expect(MockXTerm.instances.length).toBeGreaterThanOrEqual(2);
  });

  it("preserves per-session collapse and close wiring for the visible card", async () => {
    const onCloseSession = vi.fn();
    const onToggleSession = vi.fn();
    const collapsedSessions = new Set(["gbt-2"]);
    await renderGroup(multiSessions, {
      onCloseSession,
      onToggleSession,
      collapsedSessions,
    });

    await act(async () => tabs()[1].click());
    const panel = container.querySelector('[data-session-panel="gbt-2"]');
    const session = panel.querySelector('details.session[data-session="gbt-2"]');
    expect(session.open).toBe(false);

    const closeBtn = [...panel.querySelectorAll("button")].find((button) =>
      button.textContent.includes("Close Grok") ||
      button.getAttribute("aria-label")?.includes("gbt-2"),
    );
    expect(closeBtn).not.toBeUndefined();
    await act(async () => closeBtn.click());
    expect(onCloseSession).toHaveBeenCalledWith("gbt-2");
  });

  it("places close-all in the summary header and does not toggle details on click", async () => {
    const onCloseGroup = vi.fn();
    const onToggle = vi.fn();
    await renderGroup(multiSessions, { onCloseGroup, onToggle, collapsed: false });

    const details = container.querySelector("details");
    expect(details.open).toBe(true);
    const summary = details.querySelector("summary");
    const closeAll = summary.querySelector('[data-close-all-group="true"]');
    expect(closeAll).not.toBeNull();
    // No standalone body row for close-all / closeHint.
    expect(container.textContent).not.toMatch(
      /terminates all Grok subagent processes/i,
    );
    expect(
      details.querySelector(
        '.border-t [data-close-all-group="true"], .border-b [data-close-all-group="true"]',
      ),
    ).toBeNull();

    await act(async () => closeAll.click());
    expect(onCloseGroup).toHaveBeenCalledWith(
      "Codex Tab Owner",
      "thread-tabs",
      3,
    );
    // Clicking the header action must not collapse the group.
    expect(details.open).toBe(true);
    expect(onToggle).not.toHaveBeenCalledWith(false);
  });

  it("syncs terminal height across sessions in the same supervisor group", async () => {
    await renderGroup(multiSessions);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    let shell = container.querySelector("[data-terminal]");
    expect(shell).not.toBeNull();
    expect(Number(shell.dataset.terminalHeight)).toBe(TERMINAL_HEIGHT_DEFAULT);
    const handle = shell.querySelector("[data-terminal-resize]");
    await act(async () => {
      handle.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true }),
      );
    });
    expect(Number(shell.dataset.terminalHeight)).toBe(
      TERMINAL_HEIGHT_DEFAULT + 24,
    );
    // Switching tabs remounts the terminal but reuses the group height key.
    await act(async () => tabs()[1].click());
    await act(async () => {
      await Promise.resolve();
    });
    shell = container.querySelector("[data-terminal]");
    expect(shell).not.toBeNull();
    expect(Number(shell.dataset.terminalHeight)).toBe(
      TERMINAL_HEIGHT_DEFAULT + 24,
    );
  });
});
