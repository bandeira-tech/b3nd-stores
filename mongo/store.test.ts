/**
 * MongoStore Tests
 *
 * Uses a mock MongoExecutor backed by an in-memory Map.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { MongoStore } from "./store.ts";
import type { MongoExecutor } from "./mod.ts";

/** In-memory Mongo executor that simulates MongoDB collection operations. */
function createMockMongoExecutor(): MongoExecutor {
  const docs = new Map<string, Record<string, unknown>>();

  return {
    insertOne: async (doc: Record<string, unknown>) => {
      docs.set(doc.uri as string, { ...doc });
      return { acknowledged: true };
    },

    updateOne: async (
      filter: Record<string, unknown>,
      update: Record<string, unknown>,
      options?: Record<string, unknown>,
    ) => {
      const uri = filter.uri as string;
      const $set = (update as { $set: Record<string, unknown> }).$set;

      if (docs.has(uri)) {
        const existing = docs.get(uri)!;
        docs.set(uri, { ...existing, ...$set });
        return { matchedCount: 1, modifiedCount: 1 };
      }

      if (options?.upsert) {
        docs.set(uri, { uri, ...$set });
        return { matchedCount: 0, modifiedCount: 0, upsertedId: uri };
      }

      return { matchedCount: 0, modifiedCount: 0 };
    },

    findOne: async (filter: Record<string, unknown>) => {
      const uri = filter.uri as string;
      return docs.get(uri) ?? null;
    },

    findMany: async (filter: Record<string, unknown>) => {
      // Handle regex filter for prefix queries
      const uriFilter = filter.uri;
      if (
        uriFilter && typeof uriFilter === "object" && "$regex" in uriFilter
      ) {
        const regex = new RegExp(
          (uriFilter as { $regex: string }).$regex,
        );
        return [...docs.values()].filter((d) => regex.test(d.uri as string));
      }
      return [...docs.values()];
    },

    deleteOne: async (filter: Record<string, unknown>) => {
      const uri = filter.uri as string;
      const existed = docs.delete(uri);
      return { deletedCount: existed ? 1 : 0 };
    },

    ping: async () => true,
  };
}

runSharedStoreSuite("MongoStore", {
  create: () => {
    const executor = createMockMongoExecutor();
    return new MongoStore("test_collection", executor);
  },
});
