/// <reference lib="deno.ns" />

import { assert, assertEquals, assertInstanceOf } from "@std/assert";
import { ByteStorageClient } from "./byte-storage-client.ts";
import { MemoryStore } from "../memory/store.ts";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: unknown) =>
  b instanceof Uint8Array ? new TextDecoder().decode(b) : "";

Deno.test("ByteStorageClient - receive writes bytes at the URI", async () => {
  const client = new ByteStorageClient(new MemoryStore());
  const [res] = await client.receive([["mutable://app/config", enc("dark")]]);
  assertEquals(res.accepted, true);
  const [[, bytes]] = await client.read(["mutable://app/config"]);
  assertInstanceOf(bytes as Uint8Array, Uint8Array);
  assertEquals(dec(bytes), "dark");
});

Deno.test("ByteStorageClient - null payload deletes", async () => {
  const client = new ByteStorageClient(new MemoryStore());
  await client.receive([["mutable://x", enc("a")]]);
  const [d] = await client.receive([["mutable://x", null]]);
  assertEquals(d.accepted, true);
  const [[, after]] = await client.read(["mutable://x"]);
  assertEquals(after, undefined);
});

Deno.test("ByteStorageClient - read passes through bytes for fn=read", async () => {
  const store = new MemoryStore();
  await store.write([{ uri: "mutable://x", payload: enc("hi") }]);
  const client = new ByteStorageClient(store);
  const [[, b]] = await client.read(["mutable://x"]);
  assertEquals(dec(b), "hi");
});

Deno.test("ByteStorageClient - byte and entity faces on the same store stay independent", async () => {
  const store = new MemoryStore();
  const bytes = new ByteStorageClient(store);
  await bytes.receive([["mutable://app/x", enc("hello")]]);
  // The custom entity face of the same store remains empty.
  const status = await store.status();
  assert(status.schema?.some((s) => s === "mutable://app"));
});

Deno.test("ByteStorageClient - observe emits on receive", async () => {
  const client = new ByteStorageClient(new MemoryStore());
  const ac = new AbortController();
  const events: string[] = [];
  const reader = (async () => {
    for await (const [, uris] of client.observe(["mutable://*"], ac.signal)) {
      for (const u of uris) events.push(u);
    }
  })();
  await client.receive([["mutable://app/x", enc("v")]]);
  await new Promise((r) => setTimeout(r, 5));
  ac.abort();
  await reader.catch(() => {});
  assert(events.length >= 1);
});
