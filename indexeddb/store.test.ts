/**
 * IndexedDBStore Tests
 *
 * Uses fake-indexeddb for testing since IndexedDB
 * is not available in Deno.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { IndexedDBStore } from "./store.ts";
import { indexedDB } from "fake-indexeddb";

let testCount = 0;

runSharedStoreSuite("IndexedDBStore", {
  create: () =>
    new IndexedDBStore({
      databaseName: `test-db-${++testCount}`,
      indexedDB,
    }),
});
