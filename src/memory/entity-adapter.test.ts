/// <reference lib="deno.ns" />

import { assert, assertEquals, assertRejects } from "@std/assert";
import { MemoryEntityAdapter } from "./entity-adapter.ts";
import { MemoryStore } from "./store.ts";
import { TYPE_TAGS } from "../entity.ts";

const userSchema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
    { name: "verified", type: [TYPE_TAGS.BOOLEAN] },
    { name: "since", type: [TYPE_TAGS.TIMESTAMP] },
    { name: "blob", type: [TYPE_TAGS.BYTES] },
    { name: "extras", type: [TYPE_TAGS.JSON] },
    { name: "big", type: [TYPE_TAGS.BIGINT] },
  ],
};

Deno.test("MemoryEntityAdapter - ensureEntity reports every canonical tag as supported", async () => {
  const adapter = new MemoryEntityAdapter();
  const support = await adapter.ensureEntity(userSchema);
  assertEquals(support.entity, "users");
  assertEquals(support.unsupported, []);
  assertEquals(
    support.supported.sort(),
    ["age", "big", "blob", "extras", "name", "since", "verified"],
  );
});

Deno.test("MemoryEntityAdapter - ensureEntity flags unrecognised tags as unsupported", async () => {
  const adapter = new MemoryEntityAdapter();
  const support = await adapter.ensureEntity({
    name: "mixed",
    fields: [
      { name: "ok", type: [TYPE_TAGS.STRING] },
      { name: "mystery", type: ["some-protocol/money"] },
      { name: "noTags", type: [] },
      { name: "hybrid", type: ["string", "some-protocol/email"] },
    ],
  });
  assertEquals(support.supported.sort(), ["hybrid", "ok"]);
  assertEquals(support.unsupported.length, 2);
  assertEquals(support.unsupported.map((u) => u.name).sort(), [
    "mystery",
    "noTags",
  ]);
});

Deno.test("MemoryEntityAdapter - write/read round-trip retains value identity per type", async () => {
  const adapter = new MemoryEntityAdapter();
  await adapter.ensureEntity(userSchema);
  const blob = new Uint8Array([1, 2, 3, 4]);
  await adapter.writeEntity("users", [{
    uri: "data://users/alice",
    record: {
      name: "Alice",
      age: 30,
      verified: true,
      since: 1700000000_000,
      blob,
      extras: { tags: ["a", "b"] },
      big: 9007199254740993n,
    },
  }]);
  const [[uri, rec]] = await adapter.readEntity("users", [
    "data://users/alice",
  ]);
  assertEquals(uri, "data://users/alice");
  assert(rec);
  assertEquals(rec.name, "Alice");
  assertEquals(rec.age, 30);
  assertEquals(rec.verified, true);
  assertEquals(rec.since, 1700000000_000);
  assert(rec.blob === blob, "bytes are stored by reference, not copied");
  assertEquals(rec.extras, { tags: ["a", "b"] });
  assertEquals(rec.big, 9007199254740993n);
});

Deno.test("MemoryEntityAdapter - write drops fields not in supported set", async () => {
  const adapter = new MemoryEntityAdapter();
  await adapter.ensureEntity({
    name: "narrow",
    fields: [{ name: "ok", type: [TYPE_TAGS.STRING] }],
  });
  await adapter.writeEntity("narrow", [{
    uri: "data://narrow/x",
    record: { ok: "kept", extra: "dropped" },
  }]);
  const [[, rec]] = await adapter.readEntity("narrow", ["data://narrow/x"]);
  assertEquals(rec, { ok: "kept" });
});

Deno.test("MemoryEntityAdapter - read miss returns undefined payload, uri echoes", async () => {
  const adapter = new MemoryEntityAdapter();
  await adapter.ensureEntity(userSchema);
  const [[uri, rec]] = await adapter.readEntity("users", [
    "data://users/missing",
  ]);
  assertEquals(uri, "data://users/missing");
  assertEquals(rec, undefined);
});

Deno.test("MemoryEntityAdapter - delete removes a record", async () => {
  const adapter = new MemoryEntityAdapter();
  await adapter.ensureEntity(userSchema);
  await adapter.writeEntity("users", [{
    uri: "data://users/alice",
    record: { name: "Alice" },
  }]);
  const [r] = await adapter.deleteEntity("users", ["data://users/alice"]);
  assertEquals(r.success, true);
  const [[, rec]] = await adapter.readEntity("users", ["data://users/alice"]);
  assertEquals(rec, undefined);
});

Deno.test("MemoryEntityAdapter - writeEntity before ensureEntity throws", async () => {
  const adapter = new MemoryEntityAdapter();
  await assertRejects(
    () =>
      adapter.writeEntity("users", [{
        uri: "data://users/a",
        record: { name: "A" },
      }]),
    Error,
    "ensureEntity('users') was never called",
  );
});

Deno.test("MemoryStore.entityAdapter() returns the same instance on repeat calls", () => {
  const store = new MemoryStore();
  const a = store.entityAdapter();
  const b = store.entityAdapter();
  assert(a === b);
});

Deno.test("MemoryEntityAdapter - byte face and entity face do not interfere", async () => {
  const store = new MemoryStore();
  const adapter = store.entityAdapter();
  await adapter.ensureEntity({
    name: "blobs",
    fields: [{ name: "name", type: [TYPE_TAGS.STRING] }],
  });

  await store.write([{
    uri: "data://blobs/x",
    payload: new TextEncoder().encode("bytes-side"),
  }]);
  await adapter.writeEntity("blobs", [{
    uri: "data://blobs/x",
    record: { name: "entity-side" },
  }]);

  const [[, bytes]] = await store.read(["data://blobs/x"]);
  assertEquals(new TextDecoder().decode(bytes as Uint8Array), "bytes-side");

  const [[, rec]] = await adapter.readEntity("blobs", ["data://blobs/x"]);
  assertEquals(rec, { name: "entity-side" });
});
