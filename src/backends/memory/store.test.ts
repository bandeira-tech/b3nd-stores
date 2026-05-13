/**
 * MemoryStore unit tests — runs the shared store suite.
 *
 * MemoryStore follows the same shallow `ls`/`count` contract as every
 * other backend in this package, so the shared suite covers the
 * contract entirely. Memory-specific shape checks live below.
 */

/// <reference lib="deno.ns" />

import { assertEquals } from "@std/assert";
import { runSharedStoreSuite } from "../../../tests/runners/shared-store-suite.ts";
import { MemoryStore } from "./store.ts";

runSharedStoreSuite("MemoryStore", {
  create: () => new MemoryStore(),
});

Deno.test("MemoryStore - capabilities shape", () => {
  const caps = new MemoryStore().capabilities();
  assertEquals(caps.atomicBatch, false);
  assertEquals(caps.binaryData, false);
});
