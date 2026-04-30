/**
 * S3Store — Amazon S3 implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries, read entries, delete entries. Observe is not supported.
 *
 * Uses an injected S3Executor, keeping the SDK decoupled from any
 * specific S3 library.
 *
 * @example
 * ```typescript
 * import { S3Store } from "@bandeira-tech/b3nd-core";
 *
 * const store = new S3Store("my-bucket", executor, "data/");
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
import type { S3Executor } from "./mod.ts";

/**
 * Convert a URI to an S3 object key segment.
 * `protocol://host/path` becomes `protocol_host/path`.
 */
function uriToKey(uri: string): string {
  return uri.replace("://", "_").replace(/^\//, "");
}

/**
 * Convert an S3 object key back to a URI.
 * Strips optional prefix and `.json` extension, then restores `://`.
 */
function keyToUri(key: string, prefix?: string): string {
  let k = key;
  if (prefix) k = k.substring(prefix.length);
  if (k.endsWith(".json")) k = k.substring(0, k.length - 5);
  return k.replace("_", "://");
}

export class S3Store implements Store {
  private readonly bucket: string;
  private readonly executor: S3Executor;
  private readonly prefix: string;

  constructor(bucket: string, executor: S3Executor, prefix?: string) {
    if (!bucket) {
      throw new Error("bucket is required");
    }
    if (!executor) {
      throw new Error("executor is required");
    }

    this.bucket = bucket;
    this.executor = executor;
    this.prefix = prefix ?? "";
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encodedData = encodeBinaryForJson(entry.data);
        const body = JSON.stringify({
          values: entry.values,
          data: encodedData,
        });
        const key = this.resolveKey(entry.uri);
        await this.executor.putObject(key, body, "application/json");
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
      const key = this.resolveKey(uri);
      const content = await this.executor.getObject(key);

      if (content === null) {
        return { success: false, error: `Not found: ${uri}` };
      }

      const record = JSON.parse(content) as {
        values?: Record<string, number>;
        data: unknown;
      };
      const decodedData = decodeBinaryFromJson(record.data) as T;

      return {
        success: true,
        record: { values: record.values ?? {}, data: decodedData },
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  private async _list<T = unknown>(uri: string): Promise<ReadResult<T>[]> {
    try {
      const keyPrefix = `${this.prefix}${uriToKey(uri)}`;
      const keys = await this.executor.listObjects(keyPrefix);

      const results: ReadResult<T>[] = [];
      for (const k of keys.filter((k) => k.endsWith(".json"))) {
        const itemUri = keyToUri(k, this.prefix);

        const content = await this.executor.getObject(k);
        if (content !== null) {
          try {
            const record = JSON.parse(content) as {
              values?: Record<string, number>;
              data: unknown;
            };
            const decodedData = decodeBinaryFromJson(record.data) as T;
            results.push({
              success: true,
              uri: itemUri,
              record: { values: record.values ?? {}, data: decodedData },
            });
          } catch {
            // Skip malformed records
          }
        }
      }

      return results;
    } catch {
      return [];
    }
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        const key = this.resolveKey(uri);
        await this.executor.deleteObject(key);
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
      const ok = await this.executor.headBucket();
      if (!ok) {
        return {
          status: "unhealthy",
          message: `Bucket not accessible: ${this.bucket}`,
        };
      }

      return {
        status: "healthy",
        message: "S3 store is operational",
        details: {
          bucket: this.bucket,
          prefix: this.prefix || "(none)",
        },
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

  private resolveKey(uri: string): string {
    return `${this.prefix}${uriToKey(uri)}.json`;
  }
}
