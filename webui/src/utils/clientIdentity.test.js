import { beforeEach, describe, expect, it } from "vitest";
import {
  allocateNewDocumentIdentityForTests,
  getWebUiClientIdentity,
  getWebUiRequestSeq,
  nextWebUiRequestSeq,
  resetWebUiClientIdentityForTests,
} from "./clientIdentity.js";

describe("getWebUiClientIdentity", () => {
  beforeEach(() => {
    resetWebUiClientIdentityForTests();
    sessionStorage.clear();
    localStorage.clear();
  });

  it("keeps one identity for the Document (reconnects reuse it)", () => {
    const first = getWebUiClientIdentity();
    expect(first.length).toBeGreaterThanOrEqual(8);
    expect(getWebUiClientIdentity()).toBe(first);
    // Not persisted to sessionStorage — copy-tab must not share it.
    expect(sessionStorage.getItem("grok-bridge-webui-tab-client-id")).toBeNull();
  });

  it("does not reuse a sessionStorage value from a duplicated tab", () => {
    sessionStorage.setItem(
      "grok-bridge-webui-tab-client-id",
      "copied-from-other-document",
    );
    const id = getWebUiClientIdentity();
    expect(id).not.toBe("copied-from-other-document");
  });

  it("allocates a distinct identity for a new Document (duplicate tab)", () => {
    const a = getWebUiClientIdentity();
    nextWebUiRequestSeq();
    nextWebUiRequestSeq();
    expect(getWebUiRequestSeq()).toBe(2);
    const b = allocateNewDocumentIdentityForTests();
    expect(b).not.toBe(a);
    expect(getWebUiRequestSeq()).toBe(0);
    expect(nextWebUiRequestSeq()).toBe(1);
  });

  it("namespaces request ids by Document identity with monotonic seq", () => {
    const identity = getWebUiClientIdentity();
    const prefix = identity.slice(0, 12);
    expect(`${prefix}-${nextWebUiRequestSeq()}`).toBe(`${prefix}-1`);
    expect(`${prefix}-${nextWebUiRequestSeq()}`).toBe(`${prefix}-2`);
  });
});
