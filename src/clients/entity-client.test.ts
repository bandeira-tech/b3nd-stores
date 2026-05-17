/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { EntityClient } from "./entity-client.ts";
import { MemoryStore } from "../memory/store.ts";
import { TYPE_TAGS } from "../entity.ts";

const userSchema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
  ],
};

const postSchema = {
  name: "posts",
  fields: [{ name: "title", type: [TYPE_TAGS.STRING] }],
};

Deno.test("EntityClient - receive writes a record and read returns it", async () => {
  const client = new EntityClient(userSchema, new MemoryStore());
  const [res] = await client.receive([
    ["data://users/alice", { name: "Alice", age: 30 }],
  ]);
  assertEquals(res.accepted, true);

  const [[uri, rec]] = await client.read(["data://users/alice"]);
  assertEquals(uri, "data://users/alice");
  assertEquals(rec, { name: "Alice", age: 30 });
});

Deno.test("EntityClient - null payload deletes the record", async () => {
  const client = new EntityClient(userSchema, new MemoryStore());
  await client.receive([["data://users/alice", { name: "Alice", age: 30 }]]);
  const [res] = await client.receive([["data://users/alice", null]]);
  assertEquals(res.accepted, true);
  const [[, rec]] = await client.read(["data://users/alice"]);
  assertEquals(rec, undefined);
});

Deno.test("EntityClient.init - reports support for the current target", async () => {
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
});

Deno.test("EntityClient.setTarget - routes subsequent ops to the new entity", async () => {
  const store = new MemoryStore();
  const client = new EntityClient(userSchema, store);
  await client.receive([["data://u/alice", { name: "Alice", age: 30 }]]);
  client.setTarget(postSchema);
  await client.receive([["data://p/hi", { title: "Hello" }]]);

  // Verify each entity got its own write.
  assertEquals(client.target.name, "posts");
  const [[, u]] = await store.read(userSchema, ["data://u/alice"]);
  const [[, p]] = await store.read(postSchema, ["data://p/hi"]);
  assertEquals(u, { name: "Alice", age: 30 });
  assertEquals(p, { title: "Hello" });
});

Deno.test("EntityClient - mismatched record surfaces the store error", async () => {
  const client = new EntityClient(userSchema, new MemoryStore());
  const [res] = await client.receive([
    ["data://users/alice", { name: "Alice", extra: "bad" }],
  ]);
  assertEquals(res.accepted, false);
  assert(res.error?.includes("not declared"));
});

Deno.test("EntityClient - observe emits on write and delete", async () => {
  const client = new EntityClient(userSchema, new MemoryStore());
  const ac = new AbortController();
  const events: string[] = [];
  const reader = (async () => {
    for await (const [, uris] of client.observe(["data://users/*"], ac.signal)) {
      for (const u of uris) events.push(u);
    }
  })();
  await client.receive([["data://users/alice", { name: "Alice" }]]);
  await client.receive([["data://users/alice", null]]);
  await new Promise((r) => setTimeout(r, 5));
  ac.abort();
  await reader.catch(() => {});
  assert(events.length >= 1);
});
