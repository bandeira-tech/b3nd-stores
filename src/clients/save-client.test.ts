/// <reference lib="deno.ns" />

import {
  assert,
  assertEquals,
  assertInstanceOf,
  assertThrows,
} from "@std/assert";
import { SaveClient } from "./save-client.ts";
import { MemoryStore } from "../memory/store.ts";
import { BYTES_ENTITY, TYPE_TAGS } from "../entity.ts";
import type { Store } from "../types.ts";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: unknown) =>
  b instanceof Uint8Array ? new TextDecoder().decode(b) : "";

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

// ── bytes mode (default target = BYTES_ENTITY) ──────────────────────

Deno.test("SaveClient - default target is BYTES_ENTITY", () => {
  const client = new SaveClient(new MemoryStore());
  assertEquals(client.target.name, BYTES_ENTITY.name);
});

Deno.test("SaveClient - bytes: receive writes bytes at the URI", async () => {
  const client = new SaveClient(new MemoryStore());
  const [res] = await client.receive([["mutable://app/config", enc("dark")]]);
  assertEquals(res.accepted, true);
  const [[, bytes]] = await client.read(["mutable://app/config"]);
  assertInstanceOf(bytes as Uint8Array, Uint8Array);
  assertEquals(dec(bytes), "dark");
});

Deno.test("SaveClient - bytes: batch receive preserves order", async () => {
  const client = new SaveClient(new MemoryStore());
  const results = await client.receive([
    ["mutable://app/a", enc("A")],
    ["mutable://app/b", enc("B")],
    ["mutable://app/c", enc("C")],
  ]);
  assertEquals(results.length, 3);
  assert(results.every((r) => r.accepted));
  const read = await client.read([
    "mutable://app/a",
    "mutable://app/b",
    "mutable://app/c",
  ]);
  assertEquals(dec(read[0]?.[1]), "A");
  assertEquals(dec(read[1]?.[1]), "B");
  assertEquals(dec(read[2]?.[1]), "C");
});

Deno.test("SaveClient - bytes: null payload deletes", async () => {
  const client = new SaveClient(new MemoryStore());
  await client.receive([["mutable://x", enc("a")]]);
  const [d] = await client.receive([["mutable://x", null]]);
  assertEquals(d.accepted, true);
  const [[, after]] = await client.read(["mutable://x"]);
  assertEquals(after, undefined);
});

Deno.test("SaveClient - bytes: observe emits on receive and delete", async () => {
  const client = new SaveClient(new MemoryStore());
  const ac = new AbortController();
  const events: string[] = [];
  const reader = (async () => {
    for await (const [, uris] of client.observe(["mutable://*"], ac.signal)) {
      for (const u of uris) events.push(u);
    }
  })();
  await client.receive([["mutable://app/x", enc("v")]]);
  await client.receive([["mutable://app/x", null]]);
  await new Promise((r) => setTimeout(r, 5));
  ac.abort();
  await reader.catch(() => {});
  assert(events.length >= 1);
});

Deno.test("SaveClient - observe works on a bare byte Store (no entity face)", async () => {
  const bareStore: Store = {
    write: (entries) =>
      Promise.resolve(entries.map(() => ({ success: true as const }))),
    read: () => Promise.resolve([]),
    delete: (uris) =>
      Promise.resolve(uris.map(() => ({ success: true as const }))),
    status: () => Promise.resolve({ status: "healthy" as const }),
  };
  const client = new SaveClient(bareStore);
  const ac = new AbortController();
  const observed: string[] = [];
  const done = (async () => {
    for await (const ev of client.observe(["mutable://x/:k"], ac.signal)) {
      observed.push(ev[1][0]);
      ac.abort();
    }
  })();
  await client.receive([["mutable://x/a", enc("42")]]);
  await done;
  assertEquals(observed, ["mutable://x/a"]);
});

Deno.test("SaveClient - status delegates to the store", async () => {
  const client = new SaveClient(new MemoryStore());
  const status = await client.status();
  assertEquals(status.status, "healthy");
});

// ── entity mode (custom target) ─────────────────────────────────────

Deno.test("SaveClient - entity: receive writes a record and read returns it", async () => {
  const client = new SaveClient(new MemoryStore(), userSchema);
  const [res] = await client.receive([
    ["data://users/alice", { name: "Alice", age: 30 }],
  ]);
  assertEquals(res.accepted, true);
  const [[uri, rec]] = await client.read(["data://users/alice"]);
  assertEquals(uri, "data://users/alice");
  assertEquals(rec, { name: "Alice", age: 30 });
});

Deno.test("SaveClient - entity: null payload deletes the record", async () => {
  const client = new SaveClient(new MemoryStore(), userSchema);
  await client.receive([["data://users/alice", { name: "Alice", age: 30 }]]);
  const [res] = await client.receive([["data://users/alice", null]]);
  assertEquals(res.accepted, true);
  const [[, rec]] = await client.read(["data://users/alice"]);
  assertEquals(rec, undefined);
});

Deno.test("SaveClient.init - reports support for the current target", async () => {
  const client = new SaveClient(new MemoryStore(), {
    name: "mixed",
    fields: [
      { name: "ok", type: [TYPE_TAGS.STRING] },
      { name: "weird", type: ["not-a-known-tag"] },
    ],
  });
  const support = await client.init();
  assertEquals(support?.entity, "mixed");
  assertEquals(support?.supported, ["ok"]);
  assertEquals(support?.unsupported.length, 1);
});

Deno.test("SaveClient - one store, multiple entities via separate clients", async () => {
  const store = new MemoryStore();
  const users = new SaveClient(store, userSchema);
  const posts = new SaveClient(store, postSchema);
  await users.receive([["data://u/alice", { name: "Alice", age: 30 }]]);
  await posts.receive([["data://p/hi", { title: "Hello" }]]);
  const [[, u]] = await store.read(userSchema, ["data://u/alice"]);
  const [[, p]] = await store.read(postSchema, ["data://p/hi"]);
  assertEquals(u, { name: "Alice", age: 30 });
  assertEquals(p, { title: "Hello" });
});

Deno.test("SaveClient - entity: mismatched record surfaces the store error", async () => {
  const client = new SaveClient(new MemoryStore(), userSchema);
  const [res] = await client.receive([
    ["data://users/alice", { name: "Alice", extra: "bad" }],
  ]);
  assertEquals(res.accepted, false);
  assert(res.error?.includes("not declared"));
});

Deno.test("SaveClient - entity: observe emits on write and delete", async () => {
  const client = new SaveClient(new MemoryStore(), userSchema);
  const ac = new AbortController();
  const events: string[] = [];
  const reader = (async () => {
    for await (
      const [, uris] of client.observe(["data://users/*"], ac.signal)
    ) {
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

// ── byte-only store enforcement ─────────────────────────────────────

Deno.test("SaveClient - byte-only store rejects non-BYTES_ENTITY target in ctor", () => {
  const bareStore: Store = {
    write: () => Promise.resolve([]),
    read: () => Promise.resolve([]),
    delete: () => Promise.resolve([]),
    status: () => Promise.resolve({ status: "healthy" as const }),
  };
  assertThrows(
    () => new SaveClient(bareStore, userSchema),
    Error,
    "EntityStore",
  );
});
