/**
 * ElasticsearchStore unit tests — runs the shared suite against an
 * in-memory mock that simulates the subset of the ES surface the store
 * uses (index/get/search/count/delete). The mock implements Lucene
 * regex auto-anchoring by anchoring the pattern itself on full match.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { ElasticsearchStore } from "./store.ts";
import type {
  ElasticsearchExecutor,
  ElasticsearchSearchResult,
} from "./mod.ts";

/**
 * Match a Lucene-style regexp against a doc's `path` source field
 * (which mirrors `_id` — see the matching write-side change in
 * store.ts). Anchored on both ends because Lucene regex queries
 * are implicitly full-match.
 */
function matchQuery(
  query: Record<string, unknown> | undefined,
  source: Record<string, unknown>,
): boolean {
  if (!query) return true;
  const regexp = (query as { regexp?: Record<string, string> }).regexp;
  if (regexp) {
    const field = Object.keys(regexp)[0];
    if (!field) return true;
    const fieldKey = field.replace(/\.keyword$/, "");
    const value = source[fieldKey] as string | undefined;
    if (value === undefined) return false;
    const re = new RegExp(`^${regexp[field]}$`);
    return re.test(value);
  }
  return true;
}

function applySort(
  entries: Array<[string, Record<string, unknown>]>,
  sort: Array<Record<string, "asc" | "desc">> | undefined,
): Array<[string, Record<string, unknown>]> {
  const spec = sort?.[0];
  if (!spec) return entries;
  const field = Object.keys(spec)[0];
  if (!field) return entries;
  const fieldKey = field.replace(/\.keyword$/, "");
  const dir = spec[field] === "desc" ? -1 : 1;
  return [...entries].sort(([, a], [, b]) =>
    String(a[fieldKey] ?? "").localeCompare(String(b[fieldKey] ?? "")) * dir
  );
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
      const sort = body.sort as
        | Array<Record<string, "asc" | "desc">>
        | undefined;
      const from = (body.from as number) ?? 0;
      const size = (body.size as number) ?? 10_000;
      const sourceOff = body._source === false;

      let entries = [...idx.entries()].filter(([, source]) =>
        matchQuery(query, source)
      );
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
      const n =
        [...idx.values()].filter((source) => matchQuery(query, source)).length;
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
