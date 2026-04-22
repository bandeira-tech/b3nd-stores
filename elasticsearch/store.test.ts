/**
 * ElasticsearchStore Tests
 *
 * Uses a mock ElasticsearchExecutor backed by an in-memory Map.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { ElasticsearchStore } from "./store.ts";
import type { ElasticsearchExecutor } from "./mod.ts";

/** In-memory Elasticsearch executor that simulates ES operations. */
function createMockElasticsearchExecutor(): ElasticsearchExecutor {
  // index → (docId → document)
  const indices = new Map<string, Map<string, Record<string, unknown>>>();

  function getIndex(index: string): Map<string, Record<string, unknown>> {
    if (!indices.has(index)) {
      indices.set(index, new Map());
    }
    return indices.get(index)!;
  }

  return {
    index: async (
      index: string,
      id: string,
      body: Record<string, unknown>,
    ) => {
      getIndex(index).set(id, body);
    },

    get: async (index: string, id: string) => {
      return getIndex(index).get(id) ?? null;
    },

    search: async (index: string, _body: Record<string, unknown>) => {
      const idx = getIndex(index);
      const hits = [...idx.entries()].map(([id, source]) => ({
        _id: id,
        _source: source,
      }));
      return { hits };
    },

    delete: async (index: string, id: string) => {
      getIndex(index).delete(id);
    },

    ping: async () => true,
  };
}

runSharedStoreSuite("ElasticsearchStore", {
  create: () => {
    const executor = createMockElasticsearchExecutor();
    return new ElasticsearchStore("test", executor);
  },
});
