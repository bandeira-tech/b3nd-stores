/**
 * MemoryStore Tests
 *
 * Note: MemoryStore deliberately does NOT use the package-wide shared
 * store suite in `_testing/shared-store-suite.ts`. That suite enforces
 * the *shallow direct-leaves* contract for `fn=ls`/`fn=count` (the
 * convention the rest of the b3nd-stores backends follow), whereas
 * MemoryStore intentionally walks recursively — see the locked
 * project decision in the package's memory. The standalone tests
 * below exercise MemoryStore's full contract directly.
 *
 * Observe is a client concern (see `ObserveEmitter` over in
 * `@bandeira-tech/b3nd-core`) — not tested here.
 */

/// <reference lib="deno.ns" />

import { assertEquals } from "@std/assert";
import { MemoryStore } from "./store.ts";

// ── Capabilities ──────────────────────────────────────────────────

Deno.test({
  name: "MemoryStore - capabilities shape",
  fn: () => {
    const store = new MemoryStore();
    const caps = store.capabilities();
    assertEquals(caps.atomicBatch, false);
    assertEquals(caps.binaryData, false);
  },
});

// ── fn dispatcher: read / ls / count / x-* ────────────────────────

async function seedUsers(): Promise<MemoryStore> {
  const s = new MemoryStore();
  await s.write([
    { uri: "mutable://app/users/alice", data: { age: 30 } },
    { uri: "mutable://app/users/bob", data: { age: 25 } },
    { uri: "mutable://app/users/carol", data: { age: 40 } },
  ]);
  return s;
}

Deno.test("MemoryStore.read - fn=read returns single record", async () => {
  const s = await seedUsers();
  const [r] = await s.read(["mutable://app/users/alice"]);
  assertEquals(r?.[1], { age: 30 });
});

Deno.test("MemoryStore.read - fn=ls returns full records by default", async () => {
  const s = await seedUsers();
  const [result] = await s.read(["mutable://app/users/?format=full"]);
  const entries = result?.[1] as Array<[string, unknown]>;
  assertEquals(entries.length, 3);
  for (const r of entries) {
    assertEquals(typeof r[0], "string");
    assertEquals(typeof r?.[1], "object");
  }
});

Deno.test("MemoryStore.read - fn=ls format=uris returns flat uri list", async () => {
  const s = await seedUsers();
  const [result] = await s.read(["mutable://app/users/?format=uris"]);
  const uris = result?.[1] as string[];
  assertEquals(uris.length, 3);
  for (const u of uris) assertEquals(typeof u, "string");
});

Deno.test("MemoryStore.read - fn=ls limit + page slices results", async () => {
  const s = await seedUsers();
  const [r1] = await s.read([
    "mutable://app/users/?limit=2&page=1&sortBy=uri",
  ]);
  const [r2] = await s.read([
    "mutable://app/users/?limit=2&page=2&sortBy=uri",
  ]);
  const page1 = r1?.[1] as Array<[string, unknown]>;
  const page2 = r2?.[1] as Array<[string, unknown]>;
  assertEquals(page1.length, 2);
  assertEquals(page2.length, 1);
  assertEquals(page1[0][0], "mutable://app/users/alice");
  assertEquals(page1[1][0], "mutable://app/users/bob");
  assertEquals(page2[0][0], "mutable://app/users/carol");
});

Deno.test("MemoryStore.read - fn=ls sortOrder=desc reverses", async () => {
  const s = await seedUsers();
  const [result] = await s.read([
    "mutable://app/users/?sortBy=uri&sortOrder=desc",
  ]);
  const entries = result?.[1] as Array<[string, unknown]>;
  assertEquals(entries.map((r) => r[0]), [
    "mutable://app/users/carol",
    "mutable://app/users/bob",
    "mutable://app/users/alice",
  ]);
});

Deno.test("MemoryStore.read - fn=count matches ls length", async () => {
  const s = await seedUsers();
  const [c] = await s.read(["mutable://app/users/?fn=count"]);
  assertEquals(c?.[1], 3);
});

Deno.test("MemoryStore.read - fn=count over empty prefix returns 0", async () => {
  const s = new MemoryStore();
  const [c] = await s.read(["mutable://nothing/here/?fn=count"]);
  assertEquals(c?.[1], 0);
});

Deno.test("MemoryStore.read - unsupported pattern throws", async () => {
  const s = await seedUsers();
  let threw = false;
  try {
    await s.read(["mutable://app/users/?pattern=a*"]);
  } catch (e) {
    threw = true;
    assertEquals(/pattern/.test(String(e)), true);
  }
  assertEquals(threw, true);
});

Deno.test("MemoryStore.read - x-* fn throws unsupported", async () => {
  const s = await seedUsers();
  let threw = false;
  try {
    await s.read(["mutable://app/users/?fn=x-pg.scan"]);
  } catch (e) {
    threw = true;
    assertEquals(/unsupported fn/.test(String(e)), true);
  }
  assertEquals(threw, true);
});

Deno.test("MemoryStore.status - advertises supported fns", async () => {
  const s = new MemoryStore();
  const status = await s.status();
  assertEquals(status.fns, ["read", "ls", "count"]);
});

Deno.test("MemoryStore.read - heterogeneous batch (read + count + ls)", async () => {
  const s = await seedUsers();
  const results = await s.read([
    "mutable://app/users/alice",
    "mutable://app/users/?fn=count",
    "mutable://app/users/?format=uris&sortBy=uri",
  ]);
  // 1:1 with input: 3 outer slots, payload shape varies by fn.
  assertEquals(results.length, 3);
  assertEquals(results[0]?.[1], { age: 30 });
  assertEquals(results[1]?.[1], 3);
  const uris = results[2]?.[1] as string[];
  assertEquals(uris, [
    "mutable://app/users/alice",
    "mutable://app/users/bob",
    "mutable://app/users/carol",
  ]);
});
