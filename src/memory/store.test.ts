/**
 * MemoryStore unit tests — runs the shared store suite.
 *
 * MemoryStore follows the same shallow `ls`/`count` contract as every
 * other backend in this package, so the shared suite covers the
 * contract entirely. Memory-specific shape checks live below.
 */

/// <reference lib="deno.ns" />

import { assertEquals } from "@std/assert";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { MemoryStore } from "./store.ts";
import { BYTES_ENTITY } from "../entity.ts";

runSharedStoreSuite("MemoryStore", {
  create: () => new MemoryStore(),
});

Deno.test("MemoryStore - capabilities shape", () => {
  const caps = new MemoryStore().capabilities();
  assertEquals(caps.atomicBatch, false);
});

Deno.test("MemoryStore - per-entry failure carries structured error with uri", async () => {
  // A malformed URI trips URL.parse and throws inside the bytes write
  // path; the other entries still succeed (atomicBatch: false).
  const store = new MemoryStore();
  await store.ensureEntity(BYTES_ENTITY);
  const results = await store.write(BYTES_ENTITY, [
    { uri: "store://app/good", record: { payload: new Uint8Array([1]) } },
    { uri: "not-a-url", record: { payload: new Uint8Array([2]) } },
  ]);
  assertEquals(results[0].success, true);
  assertEquals(results[1].success, false);
  assertEquals(results[1].errorDetail?.code, "STORAGE_ERROR");
  assertEquals(results[1].errorDetail?.uri, "not-a-url");
});
