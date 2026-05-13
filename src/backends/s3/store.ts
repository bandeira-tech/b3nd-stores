/**
 * S3Store — Amazon S3 implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness. One S3 object
 * per entry. Object body is the encoded payload at the top level —
 * the legacy `{ values, data }` envelope is gone with the
 * `StoreEntry.values` field in b3nd-core@0.15.
 *
 * `fn=ls` / `fn=count` are shallow direct-leaves only: list objects
 * under the URI's key prefix, then filter to keys whose remainder
 * contains no further `/`. The `format=uris` fast path returns the
 * URIs without issuing `getObject`; `format=full` only fetches the
 * selected page.
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "@bandeira-tech/b3nd-core/types";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import {
  decodeBinaryFromJson,
  dispatchRead,
  encodeBinaryForJson,
  validateReadParams,
} from "../../shared/mod.ts";
import type { S3Executor } from "./mod.ts";

const STORE_NAME = "S3Store";

/** `protocol://host/path` → `protocol_host/path`. */
function uriToKey(uri: string): string {
  return uri.replace("://", "_").replace(/^\//, "");
}

/** Inverse of `uriToKey` with `.json` suffix stripping. */
function keyTailToUri(tail: string): string {
  const noExt = tail.endsWith(".json") ? tail.slice(0, -5) : tail;
  return noExt.replace("_", "://");
}

export class S3Store implements Store {
  private readonly bucket: string;
  private readonly executor: S3Executor;
  private readonly prefix: string;

  constructor(bucket: string, executor: S3Executor, prefix?: string) {
    if (!bucket) throw new Error("bucket is required");
    if (!executor) throw new Error("executor is required");

    this.bucket = bucket;
    this.executor = executor;
    this.prefix = prefix ?? "";
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const body = JSON.stringify(encodeBinaryForJson(entry.data));
        await this.executor.putObject(
          this._resolveKey(entry.uri),
          body,
          "application/json",
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

  read<T = unknown>(urls: string[]): Promise<Output<T>[]> {
    return dispatchRead<T>(urls, STORE_NAME, {
      read: (p) => this._readOne(p.uri),
      ls: (p) => this._ls(p),
      count: (p) => this._count(p),
    });
  }

  private async _readOne(uri: string): Promise<unknown> {
    const content = await this.executor.getObject(this._resolveKey(uri));
    if (content === null) return undefined;
    try {
      return decodeBinaryFromJson(JSON.parse(content));
    } catch {
      return undefined;
    }
  }

  /** List direct-child URIs under a prefix (no recursion into sub-prefixes). */
  private async _listChildUris(prefixUri: string): Promise<string[]> {
    const keyPrefix = `${this.prefix}${uriToKey(prefixUri)}`;
    const keys = await this.executor.listObjects(keyPrefix);
    const uris: string[] = [];
    for (const key of keys) {
      if (!key.endsWith(".json")) continue;
      const tail = key.slice(keyPrefix.length);
      if (tail.includes("/")) continue; // nested, not a direct leaf
      uris.push(`${prefixUri}${keyTailToUri(tail)}`);
    }
    return uris;
  }

  private async _ls(parsed: ParsedUrl): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";

    let uris = await this._listChildUris(parsed.uri);

    if (params.sortBy === "uri") {
      const dir = params.sortOrder === "desc" ? -1 : 1;
      uris = [...uris].sort((a, b) => a.localeCompare(b) * dir);
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      const start = (page - 1) * params.limit;
      uris = uris.slice(start, start + params.limit);
    }

    if (format === "uris") return uris;

    const out: Output[] = [];
    for (const uri of uris) {
      out.push([uri, await this._readOne(uri)]);
    }
    return out;
  }

  private async _count(parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return (await this._listChildUris(parsed.uri)).length;
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        await this.executor.deleteObject(this._resolveKey(uri));
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
          fns: ["read", "ls", "count"],
        };
      }
      return {
        status: "healthy",
        message: "S3 store is operational",
        fns: ["read", "ls", "count"],
        details: {
          bucket: this.bucket,
          prefix: this.prefix || "(none)",
        },
      };
    } catch (error) {
      return {
        status: "unhealthy",
        message: error instanceof Error ? error.message : String(error),
        fns: ["read", "ls", "count"],
      };
    }
  }

  capabilities(): StoreCapabilities {
    return {
      atomicBatch: false,
      binaryData: false,
    };
  }

  private _resolveKey(uri: string): string {
    return `${this.prefix}${uriToKey(uri)}.json`;
  }
}
