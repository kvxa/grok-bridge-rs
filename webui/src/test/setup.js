import { beforeEach } from "vitest";
import { setWebUiCapabilityForTests } from "../utils/webUiCapability.js";

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

/** Fixed test capability (not a production secret). */
export const TEST_WEBUI_CAPABILITY =
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

// Node 26+ may expose experimental localStorage (especially with
// --localstorage-file). That store is process-global and can leak across
// Vitest files. Always install a shared in-memory Storage for both
// globalThis and window so tests stay isolated from disk and each other.
function createMemoryStorage() {
  const store = new Map();
  return {
    get length() {
      return store.size;
    },
    clear() {
      store.clear();
    },
    getItem(key) {
      const value = store.get(String(key));
      return value === undefined ? null : value;
    },
    key(index) {
      return [...store.keys()][index] ?? null;
    },
    removeItem(key) {
      store.delete(String(key));
    },
    setItem(key, value) {
      store.set(String(key), String(value));
    },
  };
}

function installIsolatedStorage(name) {
  const storage = createMemoryStorage();
  Object.defineProperty(globalThis, name, {
    configurable: true,
    enumerable: true,
    value: storage,
  });
  if (typeof window !== "undefined") {
    Object.defineProperty(window, name, {
      configurable: true,
      enumerable: true,
      value: storage,
    });
  }
}

installIsolatedStorage("localStorage");
installIsolatedStorage("sessionStorage");

beforeEach(() => {
  try {
    globalThis.localStorage.clear();
  } catch {
    // ignore
  }
  try {
    globalThis.sessionStorage.clear();
  } catch {
    // ignore
  }
  // Every page session needs a Runtime capability for API/WS calls.
  setWebUiCapabilityForTests(TEST_WEBUI_CAPABILITY);
});

if (!window.matchMedia) {
  window.matchMedia = () => ({
    matches: false,
    media: "",
    onchange: null,
    addEventListener() {},
    removeEventListener() {},
    addListener() {},
    removeListener() {},
    dispatchEvent() {
      return false;
    },
  });
}
