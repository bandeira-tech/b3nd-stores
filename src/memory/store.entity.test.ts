/**
 * MemoryStore — EntityStore-facing tests.
 *
 * Covers the entity form of MemoryStore: `ensureEntity` reporting,
 * the BYTES_ENTITY redirect (byte form ⇄ entity form), and custom
 * entities. Strict-validation behavior (extra keys rejected) is
 * verified here too.
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { MemoryStore } from "./store.ts";
import { BYTES_ENTITY, type EntityRecord, TYPE_TAGS } from "../entity.ts";

const userSchema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
    { name: "blob", type: [TYPE_TAGS.BYTES] },
    { name: "extras", type: [TYPE_TAGS.JSON] },
  ],
};

// ── ensureEntity ──────────────────────────────────────────────────

Deno.test("MemoryStore.ensureEntity - reports canonical tags as supported", async () => {
  const store = new MemoryStore();
  const support = await store.ensureEntity(userSchema);
  assertEquals(support.entity, "users");
  assertEquals(support.unsupported, []);
  assertEquals(support.supported.sort(), ["age", "blob", "extras", "name"]);
});

Deno.test("MemoryStore.ensureEntity - flags unrecognised tags as unsupported", async () => {
  const store = new MemoryStore();
  const support = await store.ensureEntity({
    name: "mixed",
    fields: [
      { name: "ok", type: [TYPE_TAGS.STRING] },
      { name: "money", type: ["some-protocol/money"] },
      { name: "empty", type: [] },
      { name: "refined", type: [TYPE_TAGS.STRING, "some-protocol/email"] },
    ],
  });
  assertEquals(support.supported.sort(), ["ok", "refined"]);
  assertEquals(support.unsupported.map((u) => u.name).sort(), [
    "empty",
    "money",
  ]);
});

// ── Entity-form bytes redirect ────────────────────────────────────

Deno.test("MemoryStore.write(BYTES_ENTITY, …) lands in the same place as Store.write(…)", async () => {
  const store = new MemoryStore();
  await store.write(BYTES_ENTITY, [{
    uri: "mutable://app/config",
    record: { payload: new TextEncoder().encode("dark") },
  }]);
  const [[, bytes]] = await store.read(["mutable://app/config"]);
  assertEquals(new TextDecoder().decode(bytes as Uint8Array), "dark");
});

Deno.test("MemoryStore.read(BYTES_ENTITY, …) wraps the bytes as { payload }", async () => {
  const store = new MemoryStore();
  await store.write([{
    uri: "mutable://app/x",
    payload: new TextEncoder().encode("hi"),
  }]);
  const [[, rec]] = await store.read(BYTES_ENTITY, ["mutable://app/x"]);
  assert(rec);
  const payload = (rec as EntityRecord).payload;
  assert(payload instanceof Uint8Array);
  assertEquals(new TextDecoder().decode(payload), "hi");
});

Deno.test("MemoryStore.delete(BYTES_ENTITY, …) removes the bytes", async () => {
  const store = new MemoryStore();
  await store.write([{ uri: "mutable://app/x", payload: new Uint8Array([1]) }]);
  await store.delete(BYTES_ENTITY, ["mutable://app/x"]);
  const [[, rec]] = await store.read(BYTES_ENTITY, ["mutable://app/x"]);
  assertEquals(rec, undefined);
});

// ── Custom-entity round-trip ──────────────────────────────────────

Deno.test("MemoryStore - custom entity: write/read round-trip retains values", async () => {
  const store = new MemoryStore();
  await store.ensureEntity(userSchema);
  const blob = new Uint8Array([1, 2, 3, 4]);
  await store.write(userSchema, [{
    uri: "data://users/alice",
    record: { name: "Alice", age: 30, blob, extras: { tags: ["a"] } },
  }]);
  const [[uri, rec]] = await store.read(userSchema, ["data://users/alice"]);
  assertEquals(uri, "data://users/alice");
  assert(rec);
  assertEquals((rec as EntityRecord).name, "Alice");
  assertEquals((rec as EntityRecord).age, 30);
  assert((rec as EntityRecord).blob === blob);
  assertEquals((rec as EntityRecord).extras, { tags: ["a"] });
});

Deno.test("MemoryStore - custom entity: read miss returns undefined", async () => {
  const store = new MemoryStore();
  await store.ensureEntity(userSchema);
  const [[, rec]] = await store.read(userSchema, ["data://users/none"]);
  assertEquals(rec, undefined);
});

Deno.test("MemoryStore - custom entity: delete removes the record", async () => {
  const store = new MemoryStore();
  await store.ensureEntity(userSchema);
  await store.write(userSchema, [{
    uri: "data://users/alice",
    record: { name: "Alice" },
  }]);
  await store.delete(userSchema, ["data://users/alice"]);
  const [[, rec]] = await store.read(userSchema, ["data://users/alice"]);
  assertEquals(rec, undefined);
});

// ── Multi-entity isolation ────────────────────────────────────────

Deno.test("MemoryStore - hosts multiple entities side-by-side without interference", async () => {
  const store = new MemoryStore();
  const posts = {
    name: "posts",
    fields: [{ name: "title", type: [TYPE_TAGS.STRING] }],
  };
  await store.ensureEntity(userSchema);
  await store.ensureEntity(posts);

  await store.write(userSchema, [{
    uri: "data://x/alice",
    record: { name: "Alice" },
  }]);
  await store.write(posts, [{
    uri: "data://x/alice",
    record: { title: "Hello" },
  }]);

  const [[, u]] = await store.read(userSchema, ["data://x/alice"]);
  const [[, p]] = await store.read(posts, ["data://x/alice"]);
  assertEquals(u, { name: "Alice" });
  assertEquals(p, { title: "Hello" });
});

Deno.test("MemoryStore - byte face and custom-entity face do not interfere at the same URI", async () => {
  const store = new MemoryStore();
  await store.ensureEntity(userSchema);
  await store.write([{
    uri: "data://users/alice",
    payload: new TextEncoder().encode("bytes-side"),
  }]);
  await store.write(userSchema, [{
    uri: "data://users/alice",
    record: { name: "entity-side" },
  }]);
  const [[, bytes]] = await store.read(["data://users/alice"]);
  assertEquals(new TextDecoder().decode(bytes as Uint8Array), "bytes-side");
  const [[, rec]] = await store.read(userSchema, ["data://users/alice"]);
  assertEquals(rec, { name: "entity-side" });
});

// ── Strict validation: error reporting, no silent drops ───────────

Deno.test("MemoryStore - write with extra fields reports a per-entry error", async () => {
  const store = new MemoryStore();
  await store.ensureEntity(userSchema);
  const [r] = await store.write(userSchema, [{
    uri: "data://users/alice",
    record: { name: "Alice", extra: "not declared" },
  }]);
  assertEquals(r.success, false);
  assertEquals(r.errorDetail?.code, "STORAGE_ERROR");
  assertEquals(r.errorDetail?.uri, "data://users/alice");
  assert(r.error?.includes("not declared"));
});

Deno.test("MemoryStore - BYTES_ENTITY write with non-bytes payload errors out", async () => {
  const store = new MemoryStore();
  const [r] = await store.write(BYTES_ENTITY, [{
    uri: "data://x/y",
    record: { payload: "not bytes" } as unknown as EntityRecord,
  }]);
  assertEquals(r.success, false);
  assert(r.error?.includes("Uint8Array"));
});

Deno.test("MemoryStore.write(schema, …) auto-ensures on first use", async () => {
  // ensureEntity not called explicitly — the write path provisions.
  const store = new MemoryStore();
  const fresh = {
    name: "fresh",
    fields: [{ name: "k", type: [TYPE_TAGS.STRING] }],
  };
  const [r] = await store.write(fresh, [{
    uri: "data://fresh/1",
    record: { k: "v" },
  }]);
  assertEquals(r.success, true);
  const [[, rec]] = await store.read(fresh, ["data://fresh/1"]);
  assertEquals(rec, { k: "v" });
});

// ── Status reflects both faces ────────────────────────────────────

Deno.test("MemoryStore.status - lists byte programs and ensured entities", async () => {
  const store = new MemoryStore();
  await store.write([{
    uri: "mutable://app/x",
    payload: new Uint8Array([1]),
  }]);
  await store.ensureEntity(userSchema);
  const s = await store.status();
  assert((s.schema ?? []).includes("mutable://app"));
  assert((s.schema ?? []).includes("entity:users"));
});
