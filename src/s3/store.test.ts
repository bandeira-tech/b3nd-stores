/**
 * S3Store Tests
 *
 * Uses a mock S3Executor backed by an in-memory Map.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { S3Store } from "./store.ts";
import type { S3Executor } from "./mod.ts";
import { toBytes } from "../payload.ts";
import type { StorePayload } from "../types.ts";

/** In-memory S3 executor that simulates S3 bucket operations. */
function createMockS3Executor(): S3Executor {
  const objects = new Map<string, Uint8Array>();

  return {
    putObject: async (
      key: string,
      body: StorePayload,
      _contentType: string,
    ) => {
      objects.set(key, await toBytes(body));
    },

    getObject: (key: string) => {
      const bytes = objects.get(key);
      if (bytes === undefined) return Promise.resolve(null);
      return Promise.resolve(new Response(bytes as BodyInit).body!);
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
