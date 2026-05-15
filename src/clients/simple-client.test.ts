/**
 * SimpleClient Tests
 *
 * Tests the bare ProtocolInterfaceNode wrapper over a Store. Payloads
 * are `Uint8Array` end-to-end — SimpleClient does no serialization.
 */

/// <reference lib="deno.ns" />

import { assertEquals, assertInstanceOf } from "@std/assert";
import { SimpleClient } from "./simple-client.ts";
import { MemoryStore } from "../memory/store.ts";
import type { Store } from "../types.ts";

const enc = (s: string): Uint8Array => new TextEncoder().encode(s);
const dec = (b: unknown): string =>
  b instanceof Uint8Array ? new TextDecoder().decode(b) : "";

Deno.test({
  name: "SimpleClient - receive writes message at its URI",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    const results = await client.receive([
      ["mutable://app/config", enc("dark")],
    ]);
    assertEquals(results.length, 1);
    assertEquals(results[0].accepted, true);

    const read = await client.read(["mutable://app/config"]);
    assertInstanceOf(read[0]?.[1], Uint8Array);
    assertEquals(dec(read[0]?.[1]), "dark");
  },
});

Deno.test({
  name: "SimpleClient - batch receive",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    const results = await client.receive([
      ["mutable://app/a", enc("A")],
      ["mutable://app/b", enc("B")],
      ["mutable://app/c", enc("C")],
    ]);
    assertEquals(results.length, 3);
    assertEquals(results.every((r) => r.accepted), true);

    const read = await client.read([
      "mutable://app/a",
      "mutable://app/b",
      "mutable://app/c",
    ]);
    assertEquals(read.length, 3);
    assertEquals(dec(read[0]?.[1]), "A");
    assertEquals(dec(read[1]?.[1]), "B");
    assertEquals(dec(read[2]?.[1]), "C");
  },
});

Deno.test({
  name: "SimpleClient - observe emits on successful write",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);
    const ac = new AbortController();

    const observed: string[] = [];
    const observePromise = (async () => {
      for await (
        const ev of client.observe(["mutable://app/*"], ac.signal)
      ) {
        observed.push(ev[1][0]);
        ac.abort();
      }
    })();

    await client.receive([["mutable://app/x", enc("hello")]]);
    await observePromise;

    // INV-style: observer learns "this uri changed"; consumer reads
    // the uri to get the value.
    assertEquals(observed, ["mutable://app/x"]);
    const [r] = await client.read(["mutable://app/x"]);
    assertEquals(dec(r?.[1]), "hello");
  },
});

Deno.test({
  name: "SimpleClient - observe works without store.observe (store-agnostic)",
  fn: async () => {
    // Store without observe — observe lives on the client.
    const bareStore: Store = {
      write: (entries) =>
        Promise.resolve(entries.map(() => ({ success: true as const }))),
      // Option-A: not-found = no Output emitted.
      read: () => Promise.resolve([]),
      delete: (uris) =>
        Promise.resolve(uris.map(() => ({ success: true as const }))),
      status: () => Promise.resolve({ status: "healthy" as const }),
    };
    const client = new SimpleClient(bareStore);
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
  },
});

Deno.test({
  name: "SimpleClient - status delegates to store",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    const status = await client.status();
    assertEquals(status.status, "healthy");
  },
});
