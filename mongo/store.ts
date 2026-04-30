/**
 * MongoStore — MongoDB implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries, read entries, delete entries. Observe is not supported.
 *
 * Uses an injected MongoExecutor, keeping the SDK decoupled from any
 * specific MongoDB driver.
 *
 * @example
 * ```typescript
 * import { MongoStore } from "@bandeira-tech/b3nd-core";
 *
 * const store = new MongoStore("myCollection", executor);
 *
 * await store.write([
 *   { uri: "mutable://app/config", values: {}, data: { theme: "dark" } },
 * ]);
 *
 * const results = await store.read(["mutable://app/config"]);
 * console.log(results[0]?.record?.data); // { theme: "dark" }
 * ```
 */

import type {
  DeleteResult,
  ReadResult,
  StatusResult,
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "@bandeira-tech/b3nd-core/types";
import {
  decodeBinaryFromJson,
  encodeBinaryForJson,
} from "@bandeira-tech/b3nd-core";
import type { MongoExecutor } from "./mod.ts";

/**
 * Escape special regex characters in a string for safe use in a RegExp.
 */
function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export class MongoStore implements Store {
  private readonly collectionName: string;
  private readonly executor: MongoExecutor;

  constructor(collectionName: string, executor: MongoExecutor) {
    if (!collectionName) {
      throw new Error("collectionName is required");
    }
    if (!executor) {
      throw new Error("executor is required");
    }

    this.collectionName = collectionName;
    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encodedData = encodeBinaryForJson(entry.data);
        await this.executor.updateOne(
          { uri: entry.uri },
          {
            $set: {
              uri: entry.uri,
              values: entry.values,
              data: encodedData,
              updatedAt: new Date(),
            },
          },
          { upsert: true },
        );
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Write failed",
        });
      }
    }

    return results;
  }

  // ── Read ─────────────────────────────────────────────────────────

  async read<T = unknown>(uris: string[]): Promise<ReadResult<T>[]> {
    const results: ReadResult<T>[] = [];

    for (const uri of uris) {
      if (uri.endsWith("/")) {
        results.push(...await this._list<T>(uri));
      } else {
        results.push(await this._readOne<T>(uri));
      }
    }

    return results;
  }

  private async _readOne<T = unknown>(uri: string): Promise<ReadResult<T>> {
    try {
      const doc = await this.executor.findOne({ uri });

      if (!doc) {
        return { success: false, error: `Not found: ${uri}` };
      }

      const decodedData = decodeBinaryFromJson(doc.data) as T;
      const values = (doc.values ?? {}) as Record<string, number>;

      return { success: true, record: { values, data: decodedData } };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  private async _list<T = unknown>(uri: string): Promise<ReadResult<T>[]> {
    try {
      const prefix = uri.endsWith("/") ? uri : `${uri}/`;
      const escapedPrefix = escapeRegex(prefix);

      const docs = await this.executor.findMany({
        uri: { $regex: `^${escapedPrefix}` },
      });

      if (!docs.length) {
        return [];
      }

      const results: ReadResult<T>[] = [];
      for (const doc of docs) {
        const docUri = typeof doc.uri === "string" ? doc.uri : undefined;
        if (!docUri) continue;

        const decodedData = decodeBinaryFromJson(doc.data) as T;
        const values = (doc.values ?? {}) as Record<string, number>;
        results.push({
          success: true,
          uri: docUri,
          record: { values, data: decodedData },
        });
      }

      return results;
    } catch (_error) {
      return [];
    }
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        await this.executor.deleteOne?.({ uri });
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Delete failed",
        });
      }
    }

    return results;
  }

  // ── Status ───────────────────────────────────────────────────────

  async status(): Promise<StatusResult> {
    try {
      const ok = await this.executor.ping();
      if (!ok) {
        return {
          status: "unhealthy",
          message: "MongoDB ping failed",
        };
      }

      return {
        status: "healthy",
        message: "MongoDB store is operational",
        details: { collectionName: this.collectionName },
      };
    } catch (error) {
      return {
        status: "unhealthy",
        message: error instanceof Error ? error.message : String(error),
      };
    }
  }

  capabilities(): StoreCapabilities {
    return {
      atomicBatch: false,
      binaryData: false,
    };
  }
}
