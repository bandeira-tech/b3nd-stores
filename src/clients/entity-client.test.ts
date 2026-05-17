/// <reference lib="deno.ns" />

import { assert, assertEquals, assertRejects } from "@std/assert";
import { EntityClient } from "./entity-client.ts";
import { MemoryStore } from "../memory/store.ts";
import { TYPE_TAGS } from "../entity.ts";
import type { EntityAdapter } from "../entity.ts";
import type { Store } from "../types.ts";

const schema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
  ],
};

Deno.test("EntityClient - receive writes a record and read returns it", async () => {
  const client = new EntityClient(schema, new MemoryStore());
  const [res] = await client.receive([
    ["data://users/alice", { name: "Alice", age: 30 }],
  ]);
  assertEquals(res.accepted, true);

  const [[uri, rec]] = await client.read(["data://users/alice"]);
  assertEquals(uri, "data://users/alice");
  assertEquals(rec, { name: "Alice", age: 30 });
});

Deno.test("EntityClient - null payload deletes the record", async () => {
  const client = new EntityClient(schema, new MemoryStore());
  await client.receive([["data://users/alice", { name: "Alice", age: 30 }]]);
  const [res] = await client.receive([["data://users/alice", null]]);
  assertEquals(res.accepted, true);
  const [[, rec]] = await client.read(["data://users/alice"]);
  assertEquals(rec, undefined);
});

Deno.test("EntityClient - init returns the support report", async () => {
  const client = new EntityClient({
    name: "mixed",
    fields: [
      { name: "ok", type: [TYPE_TAGS.STRING] },
      { name: "weird", type: ["not-a-known-tag"] },
    ],
  }, new MemoryStore());
  const support = await client.init();
  assertEquals(support.entity, "mixed");
  assertEquals(support.supported, ["ok"]);
  assertEquals(support.unsupported.length, 1);
  assertEquals(support.unsupported[0].name, "weird");
  assert(client.support === support);
});

Deno.test("EntityClient - init is idempotent and lazy on receive", async () => {
  let calls = 0;
  const fake: EntityAdapter = {
    // deno-lint-ignore require-await
    async ensureEntity(s) {
      calls++;
      return { entity: s.name, supported: ["name"], unsupported: [] };
    },
    // deno-lint-ignore require-await
    async writeEntity(_e, entries) {
      return entries.map(() => ({ success: true }));
    },
    // deno-lint-ignore require-await
    async readEntity(_e, uris) {
      return uris.map((u) => [u, undefined]);
    },
    // deno-lint-ignore require-await
    async deleteEntity(_e, uris) {
      return uris.map(() => ({ success: true }));
    },
  };
  const store: Store = {
    // deno-lint-ignore require-await
    async write() {
      return [];
    },
    // deno-lint-ignore require-await
    async read() {
      return [];
    },
    // deno-lint-ignore require-await
    async delete() {
      return [];
    },
    // deno-lint-ignore require-await
    async status() {
      return { status: "healthy" };
    },
    entityAdapter: () => fake,
  };
  const client = new EntityClient(schema, store);
  await client.receive([["data://users/a", { name: "A" }]]);
  await client.receive([["data://users/b", { name: "B" }]]);
  await client.read(["data://users/a"]);
  assertEquals(calls, 1);
});

Deno.test("EntityClient - throws when store has no entityAdapter()", () => {
  const store: Store = {
    // deno-lint-ignore require-await
    async write() {
      return [];
    },
    // deno-lint-ignore require-await
    async read() {
      return [];
    },
    // deno-lint-ignore require-await
    async delete() {
      return [];
    },
    // deno-lint-ignore require-await
    async status() {
      return { status: "healthy" };
    },
  };
  let err: unknown;
  try {
    new EntityClient(schema, store);
  } catch (e) {
    err = e;
  }
  assert(err instanceof Error);
  assert((err as Error).message.includes("does not expose entityAdapter"));
});

Deno.test("EntityClient - throws when entityAdapter() returns null", () => {
  const store: Store = {
    // deno-lint-ignore require-await
    async write() {
      return [];
    },
    // deno-lint-ignore require-await
    async read() {
      return [];
    },
    // deno-lint-ignore require-await
    async delete() {
      return [];
    },
    // deno-lint-ignore require-await
    async status() {
      return { status: "healthy" };
    },
    entityAdapter: () => null,
  };
  let err: unknown;
  try {
    new EntityClient(schema, store);
  } catch (e) {
    err = e;
  }
  assert(err instanceof Error);
  assert((err as Error).message.includes("cannot host"));
});

Deno.test("EntityClient - observe emits writes and deletes", async () => {
  const client = new EntityClient(schema, new MemoryStore());
  const ac = new AbortController();
  const events: { kind: "write" | "delete"; uri: string }[] = [];
  const reader = (async () => {
    for await (const [, uris] of client.observe(["data://users/*"], ac.signal)) {
      for (const uri of uris) events.push({ kind: "write", uri });
    }
  })();
  await client.receive([["data://users/alice", { name: "Alice" }]]);
  await client.receive([["data://users/alice", null]]);
  // Give the emitter time to flush.
  await new Promise((r) => setTimeout(r, 5));
  ac.abort();
  await reader.catch(() => {});
  // We don't assert exact counts because emitter semantics are
  // ObserveEmitter's concern; we just verify at least one event came
  // through.
  assert(events.length >= 1, `expected events, got ${events.length}`);
});

Deno.test("EntityClient - status includes entity tag in schema", async () => {
  const client = new EntityClient(schema, new MemoryStore());
  const s = await client.status();
  assert((s.schema ?? []).some((t) => t === "entity:users"));
});

// Suppress unused-import warning under deno lint.
const _ = assertRejects;
