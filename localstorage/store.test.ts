/**
 * LocalStorageStore Tests
 *
 * Uses a simple in-memory Storage mock since localStorage
 * is not available in Deno.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { LocalStorageStore } from "./store.ts";

/**
 * Minimal in-memory Storage mock for testing.
 */
function createMockStorage(): Storage {
  const data = new Map<string, string>();

  return {
    get length() {
      return data.size;
    },
    clear() {
      data.clear();
    },
    getItem(key: string) {
      return data.get(key) ?? null;
    },
    key(index: number) {
      return [...data.keys()][index] ?? null;
    },
    removeItem(key: string) {
      data.delete(key);
    },
    setItem(key: string, value: string) {
      data.set(key, value);
    },
  } as Storage;
}

runSharedStoreSuite("LocalStorageStore", {
  create: () =>
    new LocalStorageStore({
      keyPrefix: "test:",
      storage: createMockStorage(),
    }),
});
