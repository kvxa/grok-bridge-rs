globalThis.IS_REACT_ACT_ENVIRONMENT = true;

/**
 * Node 26 exposes an experimental global `localStorage` getter (undefined
 * without --localstorage-file). Vitest copies it onto the jsdom window and
 * shadows jsdom's Storage, so install a small in-memory replacement only when
 * the provided storage is unusable.
 */
(function installLocalStorageShim() {
  const usable = (() => {
    try {
      return typeof window.localStorage?.getItem === "function";
    } catch {
      return false;
    }
  })();
  if (usable) return;

  const data = new Map();
  const storage = {
    get length() {
      return data.size;
    },
    key(index) {
      return [...data.keys()][index] ?? null;
    },
    getItem(key) {
      const normalized = String(key);
      return data.has(normalized) ? data.get(normalized) : null;
    },
    setItem(key, value) {
      data.set(String(key), String(value));
    },
    removeItem(key) {
      data.delete(String(key));
    },
    clear() {
      data.clear();
    },
  };
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    value: storage,
  });
})();

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
