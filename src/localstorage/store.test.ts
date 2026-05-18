/**
 * LocalStorageStore Tests
 *
 * Uses a simple in-memory Storage mock since localStorage
 * is not available in Deno.
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { LocalStorageStore } from "./store.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";

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

// ── Native entity layout ──────────────────────────────────────────

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

function freshStore(): LocalStorageStore {
  return new LocalStorageStore({
    keyPrefix: "test:",
    storage: createMockStorage(),
  });
}

Deno.test("LocalStorageStore - native entity round-trip", async () => {
  const store = freshStore();
  const support = await store.ensureEntity(userSchema);
  assertEquals(support.entity, "users");
  assertEquals(support.unsupported, []);

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
});

Deno.test("LocalStorageStore - strict validation rejects extras", async () => {
  const store = freshStore();
  await store.ensureEntity(userSchema);
  const [r] = await store.write(userSchema, [{
    uri: "data://users/x",
    record: { name: "X", age: 0, mystery: "not declared" } as EntityRecord,
  }]);
  assertEquals(r.success, false);
  assert(r.error?.includes("not declared"));
  assertEquals(r.errorDetail?.uri, "data://users/x");
});

Deno.test("LocalStorageStore - ls/count on a custom entity", async () => {
  const store = freshStore();
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
});

Deno.test("LocalStorageStore - delete removes from the entity keyspace", async () => {
  const store = freshStore();
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
});

Deno.test("LocalStorageStore - unsupported tags surface in EntitySupport", async () => {
  const store = freshStore();
  const support = await store.ensureEntity({
    name: "weird",
    fields: [
      { name: "ok", type: [TYPE_TAGS.STRING] },
      { name: "money", type: ["some-protocol/money"] },
    ],
  });
  assertEquals(support.supported, ["ok"]);
  assertEquals(support.unsupported.map((u) => u.name), ["money"]);
});
