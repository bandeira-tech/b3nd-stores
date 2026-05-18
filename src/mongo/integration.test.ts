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

import { assert, assertEquals } from "@std/assert";
import { MongoClient as NativeMongoClient } from "mongodb";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { MongoStore } from "./store.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";
import type { MongoExecutor } from "./mod.ts";

const COLLECTION_NAME = "inttest";
const MONGODB_URL = Deno.env.get("MONGODB_URL") ??
  "mongodb://localhost:57017/b3nd_test";

let nativeClient: NativeMongoClient;

function createMongoExecutor(): MongoExecutor {
  nativeClient = new NativeMongoClient(MONGODB_URL);
  const db = nativeClient.db();

  return {
    async insertOne(collection, doc) {
      const res = await db.collection(collection).insertOne(doc);
      return { acknowledged: res.acknowledged };
    },
    async updateOne(collection, filter, update, options) {
      const res = await db.collection(collection).updateOne(
        filter,
        update,
        options,
      );
      return {
        matchedCount: res.matchedCount,
        modifiedCount: res.modifiedCount,
        upsertedId: res.upsertedId,
      };
    },
    async findOne(collection, filter) {
      const doc = await db.collection(collection).findOne(filter);
      return (doc ?? null) as Record<string, unknown> | null;
    },
    async findMany(collection, filter, options) {
      let cursor = db.collection(collection).find(filter);
      if (options?.projection) cursor = cursor.project(options.projection);
      if (options?.sort) cursor = cursor.sort(options.sort);
      if (options?.skip !== undefined) cursor = cursor.skip(options.skip);
      if (options?.limit !== undefined) cursor = cursor.limit(options.limit);
      const docs = await cursor.toArray();
      return docs as Record<string, unknown>[];
    },
    async countDocuments(collection, filter) {
      return await db.collection(collection).countDocuments(filter);
    },
    async deleteOne(collection, filter) {
      const res = await db.collection(collection).deleteOne(filter);
      return { deletedCount: res.deletedCount };
    },
    async ensureUriIndex(collection) {
      await db.collection(collection).createIndex(
        { uri: 1 },
        { unique: true, name: "uri_unique" },
      );
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
    await db.collection(COLLECTION_NAME).deleteMany({}).catch(() => {});
    return new MongoStore(COLLECTION_NAME, executor);
  },
});

// ── Native entity collections ─────────────────────────────────────

const userSchema: EntitySchema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
    { name: "active", type: [TYPE_TAGS.BOOLEAN] },
    { name: "extras", type: [TYPE_TAGS.JSON] },
    { name: "avatar", type: [TYPE_TAGS.BYTES] },
  ],
};

const postSchema: EntitySchema = {
  name: "posts",
  fields: [
    { name: "title", type: [TYPE_TAGS.STRING] },
    { name: "stars", type: [TYPE_TAGS.NUMBER] },
  ],
};

async function freshStore(): Promise<MongoStore> {
  const executor = createMongoExecutor();
  const db = nativeClient.db();
  await db.collection(`${COLLECTION_NAME}_users_data`).deleteMany({}).catch(
    () => {},
  );
  await db.collection(`${COLLECTION_NAME}_posts_data`).deleteMany({}).catch(
    () => {},
  );
  return new MongoStore(COLLECTION_NAME, executor);
}

Deno.test({
  name:
    "MongoStore (integration) - ensureEntity provisions a per-entity collection",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
    const support = await store.ensureEntity(userSchema);
    assertEquals(support.entity, "users");
    assertEquals(support.unsupported, []);
    assertEquals(
      support.supported.sort(),
      ["active", "age", "avatar", "extras", "name"],
    );
    const db = nativeClient.db();
    const indexes = await db.collection(`${COLLECTION_NAME}_users_data`)
      .indexes();
    const uriIndex = indexes.find((i) =>
      i.key && (i.key as Record<string, unknown>).uri === 1
    );
    assert(uriIndex, "uri index should exist");
    assertEquals(uriIndex?.unique, true);
  },
});

Deno.test({
  name: "MongoStore (integration) - write/read round-trip on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
    await store.ensureEntity(userSchema);
    const avatar = new Uint8Array([1, 2, 3, 4, 5]);
    const [w] = await store.write(userSchema, [{
      uri: "data://users/alice",
      record: {
        name: "Alice",
        age: 30,
        active: true,
        extras: { tags: ["admin"] },
        avatar,
      },
    }]);
    assertEquals(w.success, true);

    const [[, rec]] = await store.read(userSchema, ["data://users/alice"]);
    const r = rec as EntityRecord;
    assertEquals(r.name, "Alice");
    assertEquals(r.age, 30);
    assertEquals(r.active, true);
    assertEquals(r.extras, { tags: ["admin"] });
    assert(r.avatar instanceof Uint8Array);
    assertEquals(Array.from(r.avatar as Uint8Array), [1, 2, 3, 4, 5]);
  },
});

Deno.test({
  name: "MongoStore (integration) - strict validation rejects extra fields",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
    await store.ensureEntity(userSchema);
    const [r] = await store.write(userSchema, [{
      uri: "data://users/x",
      record: { name: "X", age: 0, mystery: "not declared" } as EntityRecord,
    }]);
    assertEquals(r.success, false);
    assert(r.error?.includes("not declared"));
    assertEquals(r.errorDetail?.uri, "data://users/x");
  },
});

Deno.test({
  name: "MongoStore (integration) - ls/count on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
    await store.ensureEntity(postSchema);
    await store.write(postSchema, [
      { uri: "data://posts/a", record: { title: "A", stars: 1 } },
      { uri: "data://posts/b", record: { title: "B", stars: 2 } },
      { uri: "data://posts/sub/deep", record: { title: "deep", stars: 9 } },
    ]);
    const [[, count]] = await store.read<number>(postSchema, [
      "data://posts/?fn=count",
    ]);
    assertEquals(count, 2);
    const [[, uris]] = await store.read<string[]>(postSchema, [
      "data://posts/?fn=ls&format=uris&sortBy=uri",
    ]);
    assertEquals(uris, ["data://posts/a", "data://posts/b"]);
    const [[, children]] = await store.read<Array<[string, EntityRecord]>>(
      postSchema,
      ["data://posts/?fn=ls&sortBy=uri"],
    );
    assertEquals(children.map(([u]) => u), [
      "data://posts/a",
      "data://posts/b",
    ]);
    assertEquals(children[0][1].title, "A");
  },
});

Deno.test({
  name: "MongoStore (integration) - delete removes from the entity collection",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
    await store.ensureEntity(userSchema);
    await store.write(userSchema, [{
      uri: "data://users/del",
      record: {
        name: "Del",
        age: 1,
        active: true,
        extras: {},
        avatar: new Uint8Array(0),
      },
    }]);
    const [d] = await store.delete(userSchema, ["data://users/del"]);
    assertEquals(d.success, true);
    const [[, rec]] = await store.read(userSchema, ["data://users/del"]);
    assertEquals(rec, undefined);
  },
});

Deno.test({
  name: "MongoStore (integration) - unsupported tags surface in EntitySupport",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
    const support = await store.ensureEntity({
      name: "weird",
      fields: [
        { name: "ok", type: [TYPE_TAGS.STRING] },
        { name: "money", type: ["some-protocol/money"] },
      ],
    });
    assertEquals(support.supported, ["ok"]);
    assertEquals(support.unsupported.map((u) => u.name), ["money"]);
    const db = nativeClient.db();
    await db.collection(`${COLLECTION_NAME}_weird_data`).drop().catch(() => {});
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
      await db.collection(`${COLLECTION_NAME}_users_data`).drop().catch(
        () => {},
      );
      await db.collection(`${COLLECTION_NAME}_posts_data`).drop().catch(
        () => {},
      );
    } finally {
      await nativeClient.close();
    }
  },
});
