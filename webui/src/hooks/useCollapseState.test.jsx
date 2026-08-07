import { act, useState } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { useCollapseState } from "./useCollapseState.js";

function Probe({ groups, locale, focusedGroupKey, onState }) {
  const state = useCollapseState(groups, [], locale, focusedGroupKey);
  onState(state);
  return null;
}

function session(owner, phase, id = "gbt-1") {
  return {
    session: id,
    owner,
    phase,
    activity: phase === "running" ? "working" : "done",
    updated_at_ms: 1,
  };
}

describe("useCollapseState first-frame defaults", () => {
  let container;
  let root;
  let latest;

  beforeEach(() => {
    latest = null;
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });

  afterEach(async () => {
    await act(async () => root.unmount());
    container.remove();
  });

  it("first non-empty groups render applies attention defaults without full expand", async () => {
    const groups = [
      [
        "client:a",
        [session("a", "idle", "gbt-a1"), session("a", "idle", "gbt-a2")],
      ],
      [
        "client:b",
        [session("b", "running", "gbt-b1")],
      ],
    ];
    // b is attention (running/working); a is clean idle → only attention open.
    await act(async () => {
      root.render(
        <Probe
          groups={groups}
          locale="en"
          focusedGroupKey={null}
          onState={(state) => {
            latest = state;
          }}
        />,
      );
    });
    // No intermediate all-expanded: non-attention groups start collapsed.
    expect(latest.collapsedOwners.has("client:a")).toBe(true);
    expect(latest.collapsedOwners.has("client:b")).toBe(false);
    expect(latest.attentionState.order.length).toBe(2);
  });

  it("preserves user intent across later group syncs", async () => {
    const groups = [
      ["client:a", [session("a", "running", "gbt-a1")]],
    ];
    await act(async () => {
      root.render(
        <Probe
          groups={groups}
          locale="en"
          onState={(state) => {
            latest = state;
          }}
        />,
      );
    });
    expect(latest.collapsedOwners.has("client:a")).toBe(false);

    await act(async () => {
      latest.toggleOwner("client:a", false);
    });
    expect(latest.collapsedOwners.has("client:a")).toBe(true);

    // Same phase + session set: intent kept after re-render with same groups.
    await act(async () => {
      root.render(
        <Probe
          groups={groups}
          locale="en"
          onState={(state) => {
            latest = state;
          }}
        />,
      );
    });
    expect(latest.collapsedOwners.has("client:a")).toBe(true);
  });

  it("focus opens the focused group without expanding others", async () => {
    const groups = [
      ["client:a", [session("a", "idle", "gbt-a1")]],
      ["client:b", [session("b", "idle", "gbt-b1")]],
    ];
    await act(async () => {
      root.render(
        <Probe
          groups={groups}
          locale="en"
          focusedGroupKey="client:b"
          onState={(state) => {
            latest = state;
          }}
        />,
      );
    });
    expect(latest.collapsedOwners.has("client:b")).toBe(false);
    // Without attention, only latest clean-done or focus opens; a stays collapsed
    // when b is focused (default clean-done picks one, focus forces b).
    expect(latest.collapsedOwners.has("client:a")).toBe(true);
  });
});
