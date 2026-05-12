/**
 * LocalStorageStore — browser localStorage implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness. Uses
 * `localStorage` as a flat key→string KV. Each entry's value is
 * persisted as JSON with `Uint8Array` round-tripped through
 * `_shared/binary.ts`.
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
  applyReadParams,
  decodeBinaryFromJson,
  dispatchRead,
  encodeBinaryForJson,
} from "../_shared/mod.ts";

const STORE_NAME = "LocalStorageStore";

export class LocalStorageStore implements Store {
  private readonly keyPrefix: string;
  private readonly storage: Storage;

  constructor(config: {
    keyPrefix?: string;
    storage?: Storage;
  } = {}) {
    this.keyPrefix = config.keyPrefix || "b3nd:";
    this.storage = config.storage ||
      (typeof localStorage !== "undefined" ? localStorage : null!);

    if (!this.storage) {
      throw new Error("localStorage is not available in this environment");
    }
  }

  private getKey(uri: string): string {
    return `${this.keyPrefix}${uri}`;
  }

  // ── Write ────────────────────────────────────────────────────────

  write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        this.storage.setItem(
          this.getKey(entry.uri),
          JSON.stringify(encodeBinaryForJson(entry.data)),
        );
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Write failed",
        });
      }
    }

    return Promise.resolve(results);
  }

  // ── Read ─────────────────────────────────────────────────────────

  read<T = unknown>(urls: string[]): Promise<Output<T>[]> {
    return dispatchRead<T>(urls, STORE_NAME, {
      read: (p) => this._readOne(p.uri),
      ls: (p) => this._ls(p),
      count: (p) => this._count(p),
    });
  }

  private _readOne(uri: string): unknown {
    const serialized = this.storage.getItem(this.getKey(uri));
    if (serialized === null) return undefined;
    return decodeBinaryFromJson(JSON.parse(serialized));
  }

  /**
   * Walk localStorage once, returning `[uri, payload]` for every entry
   * whose URI is `prefix + <segment>` with no further `/`. Subtree-only
   * paths are excluded by the slash check.
   */
  private _directLeaves(prefixUri: string): Output[] {
    const prefixKey = this.getKey(prefixUri);
    const out: Output[] = [];
    for (let i = 0; i < this.storage.length; i++) {
      const key = this.storage.key(i);
      if (!key || !key.startsWith(prefixKey)) continue;
      const rest = key.substring(prefixKey.length);
      if (rest === "" || rest.includes("/")) continue;
      const childUri = `${prefixUri}${rest}`;
      out.push([childUri, this._readOne(childUri)]);
    }
    return out;
  }

  private _ls(parsed: ParsedUrl): Output[] | string[] {
    return applyReadParams(
      this._directLeaves(parsed.uri),
      parsed.params,
      STORE_NAME,
    );
  }

  private _count(parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return this._directLeaves(parsed.uri).length;
  }

  // ── Delete ───────────────────────────────────────────────────────

  delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        this.storage.removeItem(this.getKey(uri));
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Delete failed",
        });
      }
    }

    return Promise.resolve(results);
  }

  // ── Status ───────────────────────────────────────────────────────

  status(): Promise<StatusResult> {
    try {
      const testKey = `${this.keyPrefix}__health_check__`;
      this.storage.setItem(testKey, "ok");
      this.storage.removeItem(testKey);
      return Promise.resolve({
        status: "healthy",
        schema: [],
        fns: ["read", "ls", "count"],
      });
    } catch {
      return Promise.resolve({
        status: "unhealthy",
        schema: [],
        fns: ["read", "ls", "count"],
      });
    }
  }

  capabilities(): StoreCapabilities {
    return { atomicBatch: false, binaryData: false };
  }
}
