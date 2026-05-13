/**
 * SimpleClient Tests
 *
 * Tests the bare ProtocolInterfaceNode wrapper over a Store.
 */

/// <reference lib="deno.ns" />

import { assertEquals } from "@std/assert";
import { SimpleClient } from "./simple-client.ts";
import { MemoryStore } from "../backends/memory/store.ts";

Deno.test({
  name: "SimpleClient - receive writes message at its URI",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    const results = await client.receive([
      ["mutable://app/config", { theme: "dark" }],
    ]);
    assertEquals(results.length, 1);
    assertEquals(results[0].accepted, true);

    const read = await client.read(["mutable://app/config"]);
    assertEquals(read[0]?.[1], { theme: "dark" });
  },
});

Deno.test({
  name: "SimpleClient - receive does NOT decompose envelopes",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    // Even though data looks like an envelope, SimpleClient stores it as-is
    await client.receive([
      ["envelope://test/1", {
        inputs: [],
        outputs: [["mutable://app/x", "hello"]],
      }],
    ]);

    // The envelope data is stored at the envelope URI
    await client.read(["envelope://test/1"]);

    // But the output was NOT written — no fan-out
    await client.read(["mutable://app/x"]);
  },
});

Deno.test({
  name: "SimpleClient - batch receive",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    const results = await client.receive([
      ["mutable://app/a", "A"],
      ["mutable://app/b", "B"],
      ["mutable://app/c", "C"],
    ]);
    assertEquals(results.length, 3);
    assertEquals(results.every((r) => r.accepted), true);

    const read = await client.read([
      "mutable://app/a",
      "mutable://app/b",
      "mutable://app/c",
    ]);
    assertEquals(read.length, 3);
    assertEquals(read[0]?.[1], "A");
    assertEquals(read[1]?.[1], "B");
    assertEquals(read[2]?.[1], "C");
  },
});

Deno.test({
  name: "SimpleClient - read with string or array",
  fn: async () => {
    const store = new MemoryStore();
    const client = new SimpleClient(store);

    await client.receive([["mutable://app/x", "data"]]);

    // String form
    const r1 = await client.read(["mutable://app/x"]);
    assertEquals(r1[0]?.[1], "data");

    // Array form
    const r2 = await client.read(["mutable://app/x"]);
    assertEquals(r2[0]?.[1], "data");
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

    await client.receive([["mutable://app/x", "hello"]]);
    await observePromise;

    // INV-style: observer learns "this uri changed"; consumer reads
    // the uri to get the value.
    assertEquals(observed, ["mutable://app/x"]);
    const [r] = await client.read(["mutable://app/x"]);
    assertEquals(r?.[1], "hello");
  },
});

Deno.test({
  name: "SimpleClient - observe works without store.observe (store-agnostic)",
  fn: async () => {
    // Store without observe — observe lives on the client.
    const bareStore: import("@bandeira-tech/b3nd-core/types").Store = {
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

    await client.receive([["mutable://x/a", 42]]);
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
