/**
 * ElasticsearchStore unit tests — runs the shared suite against an
 * in-memory mock that simulates the subset of the ES surface the store
 * uses (index/get/search/count/delete). The mock implements Lucene
 * regex auto-anchoring by anchoring the pattern itself on full match.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { ElasticsearchStore } from "./store.ts";
import type {
  ElasticsearchExecutor,
  ElasticsearchSearchResult,
} from "./mod.ts";

function matchQuery(
  query: Record<string, unknown> | undefined,
  docId: string,
): boolean {
  if (!query) return true;
  const regexp = (query as { regexp?: { _id: string } }).regexp;
  if (regexp) {
    // Lucene regex queries are implicitly anchored (full match).
    const re = new RegExp(`^${regexp._id}$`);
    return re.test(docId);
  }
  return true;
}

function applySort(
  entries: Array<[string, Record<string, unknown>]>,
  sort: Array<{ _id: "asc" | "desc" }> | undefined,
): Array<[string, Record<string, unknown>]> {
  if (!sort?.[0]?._id) return entries;
  const dir = sort[0]._id === "desc" ? -1 : 1;
  return [...entries].sort(([a], [b]) => a.localeCompare(b) * dir);
}

function createMockElasticsearchExecutor(): ElasticsearchExecutor {
  const indices = new Map<string, Map<string, Record<string, unknown>>>();

  const getIndex = (index: string) => {
    if (!indices.has(index)) indices.set(index, new Map());
    return indices.get(index)!;
  };

  return {
    index: (index, id, body) => {
      getIndex(index).set(id, body);
      return Promise.resolve();
    },

    get: (index, id) => Promise.resolve(getIndex(index).get(id) ?? null),

    search: (index, body) => {
      const idx = getIndex(index);
      const query = body.query as Record<string, unknown> | undefined;
      const sort = body.sort as Array<{ _id: "asc" | "desc" }> | undefined;
      const from = (body.from as number) ?? 0;
      const size = (body.size as number) ?? 10_000;
      const sourceOff = body._source === false;

      let entries = [...idx.entries()].filter(([id]) => matchQuery(query, id));
      entries = applySort(entries, sort);
      entries = entries.slice(from, from + size);

      const hits = entries.map(([id, source]) =>
        sourceOff ? { _id: id } : { _id: id, _source: source }
      );
      return Promise.resolve({ hits } as ElasticsearchSearchResult);
    },

    count: (index, body) => {
      const idx = getIndex(index);
      const query = body.query as Record<string, unknown> | undefined;
      const n = [...idx.keys()].filter((id) => matchQuery(query, id)).length;
      return Promise.resolve(n);
    },

    delete: (index, id) => {
      getIndex(index).delete(id);
      return Promise.resolve();
    },

    ping: () => Promise.resolve(true),
  };
}

runSharedStoreSuite("ElasticsearchStore", {
  create: () =>
    new ElasticsearchStore("test", createMockElasticsearchExecutor()),
});
