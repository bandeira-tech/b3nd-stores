/**
 * MongoStore unit tests — runs the shared suite against an in-memory
 * Mongo mock that supports the subset of the executor surface the
 * store actually uses (find/count with regex filter, sort, skip,
 * limit, projection).
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { MongoStore } from "./store.ts";
import type { MongoExecutor, MongoFindManyOptions } from "./mod.ts";

function filterByRegex(
  docs: Record<string, unknown>[],
  filter: Record<string, unknown>,
): Record<string, unknown>[] {
  const uriFilter = filter.uri;
  if (
    uriFilter && typeof uriFilter === "object" && "$regex" in uriFilter
  ) {
    const regex = new RegExp((uriFilter as { $regex: string }).$regex);
    return docs.filter((d) => regex.test(d.uri as string));
  }
  return docs;
}

function createMockMongoExecutor(): MongoExecutor {
  const docs = new Map<string, Record<string, unknown>>();

  return {
    insertOne: (doc) => {
      docs.set(doc.uri as string, { ...doc });
      return Promise.resolve({ acknowledged: true });
    },

    updateOne: (filter, update, options) => {
      const uri = filter.uri as string;
      const $set = (update as { $set: Record<string, unknown> }).$set;

      if (docs.has(uri)) {
        const existing = docs.get(uri)!;
        docs.set(uri, { ...existing, ...$set });
        return Promise.resolve({ matchedCount: 1, modifiedCount: 1 });
      }

      if (options?.upsert) {
        docs.set(uri, { uri, ...$set });
        return Promise.resolve({
          matchedCount: 0,
          modifiedCount: 0,
          upsertedId: uri,
        });
      }

      return Promise.resolve({ matchedCount: 0, modifiedCount: 0 });
    },

    findOne: (filter) => {
      const uri = filter.uri as string;
      return Promise.resolve(docs.get(uri) ?? null);
    },

    findMany: (filter, options?: MongoFindManyOptions) => {
      let out = filterByRegex([...docs.values()], filter);
      if (options?.sort?.uri) {
        const dir = options.sort.uri;
        out = [...out].sort((a, b) =>
          (a.uri as string).localeCompare(b.uri as string) * dir
        );
      }
      if (options?.skip) out = out.slice(options.skip);
      if (options?.limit !== undefined) out = out.slice(0, options.limit);
      if (options?.projection) {
        out = out.map((d) => {
          const proj: Record<string, unknown> = {};
          for (const [k, v] of Object.entries(options.projection!)) {
            if (v === 1 && k in d) proj[k] = d[k];
          }
          return proj;
        });
      }
      return Promise.resolve(out);
    },

    countDocuments: (filter) => {
      return Promise.resolve(filterByRegex([...docs.values()], filter).length);
    },

    deleteOne: (filter) => {
      const uri = filter.uri as string;
      const existed = docs.delete(uri);
      return Promise.resolve({ deletedCount: existed ? 1 : 0 });
    },

    ping: () => Promise.resolve(true),
  };
}

runSharedStoreSuite("MongoStore", {
  create: () => new MongoStore("test_collection", createMockMongoExecutor()),
});
