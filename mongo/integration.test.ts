/**
 * MongoStore Integration Tests
 *
 * Runs the shared store suite against a real MongoDB instance.
 * Requires a running MongoDB — see CI workflow or:
 *   cd /Users/m0/ws/b3nd && make up p=test
 *
 * Env: MONGODB_URL (default: mongodb://localhost:57017/b3nd_test)
 */

/// <reference lib="deno.ns" />

import { MongoClient as NativeMongoClient } from "npm:mongodb";
import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { MongoStore } from "./store.ts";
import type { MongoExecutor } from "./mod.ts";

const COLLECTION_NAME = "inttest";
const MONGODB_URL = Deno.env.get("MONGODB_URL") ??
  "mongodb://localhost:57017/b3nd_test";

let nativeClient: NativeMongoClient;

function createMongoExecutor(): MongoExecutor {
  nativeClient = new NativeMongoClient(MONGODB_URL);
  const db = nativeClient.db();
  const collection = db.collection(COLLECTION_NAME);

  return {
    async insertOne(doc) {
      const res = await collection.insertOne(doc);
      return { acknowledged: res.acknowledged };
    },
    async updateOne(filter, update, options) {
      const res = await collection.updateOne(filter, update, options);
      return {
        matchedCount: res.matchedCount,
        modifiedCount: res.modifiedCount,
        upsertedId: res.upsertedId,
      };
    },
    async findOne(filter) {
      const doc = await collection.findOne(filter);
      return (doc ?? null) as Record<string, unknown> | null;
    },
    async findMany(filter) {
      const docs = await collection.find(filter).toArray();
      return docs as Record<string, unknown>[];
    },
    async deleteOne(filter) {
      const res = await collection.deleteOne(filter);
      return { deletedCount: res.deletedCount };
    },
    async ping() {
      await db.command({ ping: 1 });
      return true;
    },
  };
}

runSharedStoreSuite("MongoStore (integration)", {
  create: async () => {
    const executor = createMongoExecutor();
    // Clean previous test data
    const db = nativeClient.db();
    const collections = await db.listCollections({ name: COLLECTION_NAME })
      .toArray();
    if (collections.length > 0) {
      await db.collection(COLLECTION_NAME).deleteMany({});
    }
    return new MongoStore(COLLECTION_NAME, executor);
  },
});

// Cleanup after all tests
Deno.test({
  name: "MongoStore (integration) - cleanup",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    try {
      const db = nativeClient.db();
      await db.collection(COLLECTION_NAME).drop().catch(() => {});
    } finally {
      await nativeClient.close();
    }
  },
});
