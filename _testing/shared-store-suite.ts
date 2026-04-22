/**
 * Shared Test Suite for Store Interface
 *
 * Tests that any implementation of Store behaves correctly
 * as **mechanical storage**.
 *
 * Store is batch-native: every operation takes arrays and returns
 * per-item results. This suite validates the contract:
 * - write(entries) → StoreWriteResult[]
 * - read(uris) → ReadResult[]  (trailing-slash = list)
 * - delete(uris) → DeleteResult[]
 * - status() → StatusResult
 * - capabilities() → StoreCapabilities (optional)
 *
 * Each store test file imports and runs this suite with a factory
 * function that creates a fresh Store instance for each test.
 */

/// <reference lib="deno.ns" />

import { assertEquals } from "jsr:@std/assert";
import type { Store } from "@bandeira-tech/b3nd-sdk/types";

/**
 * Factory and options for the shared Store test suite.
 */
export interface StoreTestConfig {
  /** Factory that returns a fresh Store for each test. */
  create: () => Store | Promise<Store>;

  /**
   * Whether this store supports reading back written data.
   * Set to false for write-only stores.
   * Defaults to true.
   */
  supportsRead?: boolean;

  /**
   * Whether this store supports trailing-slash list queries.
   * Defaults to true when supportsRead is true.
   */
  supportsList?: boolean;
}

/**
 * Run the complete shared Store test suite.
 */
export function runSharedStoreSuite(
  suiteName: string,
  config: StoreTestConfig,
) {
  const noSanitize = { sanitizeOps: false, sanitizeResources: false };
  const supportsRead = config.supportsRead !== false;
  const supportsList = config.supportsList ?? supportsRead;

  // ── Write ───────────────────────────────────────────────────────

  Deno.test({
    name: `${suiteName} - write single entry`,
    ...noSanitize,
    fn: async () => {
      const store = await Promise.resolve(config.create());

      const results = await store.write([
        { uri: "store://app/config", values: {}, data: { theme: "dark" } },
      ]);

      assertEquals(results.length, 1);
      assertEquals(results[0].success, true);
    },
  });

  Deno.test({
    name: `${suiteName} - write batch of entries`,
    ...noSanitize,
    fn: async () => {
      const store = await Promise.resolve(config.create());

      const results = await store.write([
        { uri: "store://app/a", values: {}, data: "A" },
        { uri: "store://app/b", values: { fire: 10 }, data: "B" },
        { uri: "store://app/c", values: {}, data: "C" },
      ]);

      assertEquals(results.length, 3);
      assertEquals(results.every((r) => r.success), true);
    },
  });

  // ── Write + Read ────────────────────────────────────────────────

  if (supportsRead) {
    Deno.test({
      name: `${suiteName} - write and read back`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          {
            uri: "store://app/config",
            values: {},
            data: { theme: "dark" },
          },
        ]);

        const results = await store.read(["store://app/config"]);
        assertEquals(results.length, 1);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, { theme: "dark" });
        assertEquals(results[0].record?.values, {});
      },
    });

    Deno.test({
      name: `${suiteName} - batch write and read all back`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://app/a", values: {}, data: "A" },
          { uri: "store://app/b", values: { fire: 10 }, data: "B" },
          { uri: "store://app/c", values: {}, data: "C" },
        ]);

        const results = await store.read([
          "store://app/a",
          "store://app/b",
          "store://app/c",
        ]);
        assertEquals(results.length, 3);
        assertEquals(results[0].record?.data, "A");
        assertEquals(results[1].record?.data, "B");
        assertEquals(results[1].record?.values, { fire: 10 });
        assertEquals(results[2].record?.data, "C");
      },
    });

    Deno.test({
      name: `${suiteName} - write overwrites existing value`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://app/x", values: {}, data: "old" },
        ]);
        await store.write([
          { uri: "store://app/x", values: {}, data: "new" },
        ]);

        const results = await store.read(["store://app/x"]);
        assertEquals(results[0].record?.data, "new");
      },
    });

    // ── Values preservation ─────────────────────────────────────────

    Deno.test({
      name: `${suiteName} - preserves values on write/read`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          {
            uri: "store://app/token",
            values: { fire: 100, water: 50 },
            data: null,
          },
        ]);

        const results = await store.read(["store://app/token"]);
        assertEquals(results[0].record?.values, { fire: 100, water: 50 });
      },
    });

    Deno.test({
      name: `${suiteName} - overwrite preserves new values`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://app/v", values: { fire: 100 }, data: null },
        ]);
        await store.write([
          {
            uri: "store://app/v",
            values: { fire: 75, usd: 25 },
            data: { memo: "updated" },
          },
        ]);

        const results = await store.read(["store://app/v"]);
        assertEquals(results[0].record?.values, { fire: 75, usd: 25 });
        assertEquals(results[0].record?.data, { memo: "updated" });
      },
    });

    // ── Scalar data types ───────────────────────────────────────────

    Deno.test({
      name: `${suiteName} - read/write string data`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://scalar/str", values: {}, data: "hello world" },
        ]);

        const results = await store.read(["store://scalar/str"]);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, "hello world");
      },
    });

    Deno.test({
      name: `${suiteName} - read/write number data`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://scalar/num", values: {}, data: 42 },
        ]);

        const results = await store.read(["store://scalar/num"]);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, 42);
      },
    });

    Deno.test({
      name: `${suiteName} - read/write boolean data`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://scalar/bool", values: {}, data: true },
        ]);

        const results = await store.read(["store://scalar/bool"]);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, true);
      },
    });

    Deno.test({
      name: `${suiteName} - read/write null data`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://scalar/null", values: {}, data: null },
        ]);

        const results = await store.read(["store://scalar/null"]);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, null);
      },
    });

    Deno.test({
      name: `${suiteName} - read/write empty string data`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://scalar/empty", values: {}, data: "" },
        ]);

        const results = await store.read(["store://scalar/empty"]);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, "");
      },
    });

    Deno.test({
      name: `${suiteName} - read/write zero data`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://scalar/zero", values: {}, data: 0 },
        ]);

        const results = await store.read(["store://scalar/zero"]);
        assertEquals(results[0].success, true);
        assertEquals(results[0].record?.data, 0);
      },
    });

    // ── Read: nonexistent, partial failures ─────────────────────────

    Deno.test({
      name: `${suiteName} - read nonexistent returns error`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        const results = await store.read(["store://app/missing"]);
        assertEquals(results.length, 1);
        assertEquals(results[0].success, false);
      },
    });

    Deno.test({
      name: `${suiteName} - read with partial failures`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://app/exists", values: {}, data: { ok: true } },
        ]);

        const results = await store.read([
          "store://app/exists",
          "store://app/missing",
        ]);
        assertEquals(results.length, 2);
        assertEquals(results[0].success, true);
        assertEquals(results[1].success, false);
      },
    });
  }

  // ── Read: not supported ─────────────────────────────────────────

  if (!supportsRead) {
    Deno.test({
      name: `${suiteName} - read returns error (write-only store)`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        const results = await store.read(["store://app/anything"]);
        assertEquals(results.length, 1);
        assertEquals(results[0].success, false);
      },
    });
  }

  // ── List (trailing slash) ───────────────────────────────────────

  if (supportsList) {
    Deno.test({
      name: `${suiteName} - trailing slash lists children`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          {
            uri: "store://users/alice",
            values: {},
            data: { name: "Alice" },
          },
          {
            uri: "store://users/bob",
            values: {},
            data: { name: "Bob" },
          },
        ]);

        const results = await store.read(["store://users/"]);
        assertEquals(results.length >= 2, true);
        assertEquals(results.every((r) => r.success), true);

        const uris = results.map((r) => r.uri).sort();
        assertEquals(uris.includes("store://users/alice"), true);
        assertEquals(uris.includes("store://users/bob"), true);
      },
    });
  }

  // ── Delete ──────────────────────────────────────────────────────

  Deno.test({
    name: `${suiteName} - delete returns success`,
    ...noSanitize,
    fn: async () => {
      const store = await Promise.resolve(config.create());

      await store.write([
        { uri: "store://app/x", values: {}, data: "hello" },
      ]);

      const deleteResults = await store.delete(["store://app/x"]);
      assertEquals(deleteResults.length, 1);
      assertEquals(deleteResults[0].success, true);
    },
  });

  if (supportsRead) {
    Deno.test({
      name: `${suiteName} - delete removes entry`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://app/x", values: {}, data: "hello" },
        ]);

        await store.delete(["store://app/x"]);

        const readResults = await store.read(["store://app/x"]);
        assertEquals(readResults[0].success, false);
      },
    });
  }

  Deno.test({
    name: `${suiteName} - delete nonexistent succeeds silently`,
    ...noSanitize,
    fn: async () => {
      const store = await Promise.resolve(config.create());

      const results = await store.delete(["store://app/missing"]);
      assertEquals(results.length, 1);
      assertEquals(results[0].success, true);
    },
  });

  if (supportsRead) {
    Deno.test({
      name: `${suiteName} - batch delete`,
      ...noSanitize,
      fn: async () => {
        const store = await Promise.resolve(config.create());

        await store.write([
          { uri: "store://app/a", values: {}, data: "A" },
          { uri: "store://app/b", values: {}, data: "B" },
          { uri: "store://app/c", values: {}, data: "C" },
        ]);

        await store.delete(["store://app/a", "store://app/c"]);

        const results = await store.read([
          "store://app/a",
          "store://app/b",
          "store://app/c",
        ]);
        assertEquals(results[0].success, false);
        assertEquals(results[1].success, true);
        assertEquals(results[1].record?.data, "B");
        assertEquals(results[2].success, false);
      },
    });
  }

  // ── Status ──────────────────────────────────────────────────────

  Deno.test({
    name: `${suiteName} - status returns healthy`,
    ...noSanitize,
    fn: async () => {
      const store = await Promise.resolve(config.create());

      const status = await store.status();
      assertEquals(status.status, "healthy");
    },
  });

  // ── Capabilities ────────────────────────────────────────────────

  Deno.test({
    name: `${suiteName} - capabilities returns valid shape`,
    ...noSanitize,
    fn: async () => {
      const store = await Promise.resolve(config.create());

      if (store.capabilities) {
        const caps = store.capabilities();
        assertEquals(typeof caps.atomicBatch, "boolean");
        assertEquals(typeof caps.binaryData, "boolean");
      }
    },
  });
}
