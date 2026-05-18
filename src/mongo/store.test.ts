/**
 * MongoStore unit tests — runs the shared suite against an in-memory
 * Mongo mock that supports the subset of the executor surface the
 * store actually uses (find/count with regex filter, sort, skip,
 * limit, projection) across multiple collections.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
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
  if (typeof uriFilter === "string") {
    return docs.filter((d) => d.uri === uriFilter);
  }
  return docs;
}

function createMockMongoExecutor(): MongoExecutor {
  const collections = new Map<string, Map<string, Record<string, unknown>>>();
  const bucket = (name: string) => {
    let b = collections.get(name);
    if (!b) {
      b = new Map();
      collections.set(name, b);
    }
    return b;
  };

  return {
    insertOne: (collection, doc) => {
      bucket(collection).set(doc.uri as string, { ...doc });
      return Promise.resolve({ acknowledged: true });
    },

    updateOne: (collection, filter, update, options) => {
      const docs = bucket(collection);
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

    findOne: (collection, filter) => {
      const docs = bucket(collection);
      const uri = filter.uri as string;
      return Promise.resolve(docs.get(uri) ?? null);
    },

    findMany: (
      collection,
      filter,
      options?: MongoFindManyOptions,
    ) => {
      let out = filterByRegex([...bucket(collection).values()], filter);
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

    countDocuments: (collection, filter) => {
      return Promise.resolve(
        filterByRegex([...bucket(collection).values()], filter).length,
      );
    },

    deleteOne: (collection, filter) => {
      const docs = bucket(collection);
      const uri = filter.uri as string;
      const existed = docs.delete(uri);
      return Promise.resolve({ deletedCount: existed ? 1 : 0 });
    },

    ensureUriIndex: (_collection) => Promise.resolve(),

    ping: () => Promise.resolve(true),
  };
}

runSharedStoreSuite("MongoStore", {
  create: () => new MongoStore("test_collection", createMockMongoExecutor()),
});
