/**
 * S3Store Tests
 *
 * Uses a mock S3Executor backed by an in-memory Map.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { S3Store } from "./store.ts";
import type { S3Executor } from "./mod.ts";

/** In-memory S3 executor that simulates S3 bucket operations. */
function createMockS3Executor(): S3Executor {
  const objects = new Map<string, string>();

  return {
    putObject: async (key: string, body: string, _contentType: string) => {
      objects.set(key, body);
    },

    getObject: async (key: string) => {
      return objects.get(key) ?? null;
    },

    deleteObject: async (key: string) => {
      objects.delete(key);
    },

    listObjects: async (prefix: string) => {
      return [...objects.keys()].filter((k) => k.startsWith(prefix));
    },

    headBucket: async () => true,
  };
}

runSharedStoreSuite("S3Store", {
  create: () => {
    const executor = createMockS3Executor();
    return new S3Store("test-bucket", executor);
  },
});
