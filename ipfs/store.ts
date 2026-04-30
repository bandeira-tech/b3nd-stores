/**
 * IpfsStore — IPFS implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries as pinned IPFS objects, read them back via CID lookup,
 * delete by unpinning. Observe is not supported.
 *
 * Maintains an in-memory index mapping URIs to Content IDs (CIDs).
 *
 * Uses an injected IpfsExecutor so the SDK does not depend on a specific
 * IPFS library.
 *
 * @example
 * ```typescript
 * import { IpfsStore } from "@bandeira-tech/b3nd-core";
 *
 * const store = new IpfsStore(executor);
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
import type { IpfsExecutor } from "./mod.ts";

interface IndexEntry {
  cid: string;
}

export class IpfsStore implements Store {
  private readonly executor: IpfsExecutor;
  private readonly index = new Map<string, IndexEntry>();

  constructor(executor: IpfsExecutor) {
    if (!executor) {
      throw new Error("executor is required");
    }

    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encodedData = encodeBinaryForJson(entry.data);
        const content = JSON.stringify({
          values: entry.values,
          data: encodedData,
        });

        const cid = await this.executor.add(content);
        await this.executor.pin(cid);

        // Unpin old CID if this URI was already indexed
        const existing = this.index.get(entry.uri);
        if (existing) {
          try {
            await this.executor.unpin(existing.cid);
          } catch {
            // Old CID may already be unpinned — ignore
          }
        }

        this.index.set(entry.uri, { cid });
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

  private async _readOne<T>(uri: string): Promise<ReadResult<T>> {
    try {
      const entry = this.index.get(uri);

      if (!entry) {
        return { success: false, error: `Not found: ${uri}` };
      }

      const content = await this.executor.cat(entry.cid);
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

  private async _list<T>(uri: string): Promise<ReadResult<T>[]> {
    const prefix = uri.endsWith("/") ? uri : uri + "/";
    const results: ReadResult<T>[] = [];

    for (const indexUri of this.index.keys()) {
      if (indexUri.startsWith(prefix)) {
        const result = await this._readOne<T>(indexUri);
        if (result.success) {
          results.push({ ...result, uri: indexUri });
        }
      }
    }

    return results;
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        const entry = this.index.get(uri);
        if (entry) {
          try {
            await this.executor.unpin(entry.cid);
          } catch {
            // CID may already be unpinned — ignore
          }
          this.index.delete(uri);
        }
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
      const online = await this.executor.isOnline();
      if (!online) {
        return {
          status: "unhealthy",
          message: "IPFS node is not reachable",
        };
      }

      // Derive schema from indexed URIs
      const programs = new Set<string>();
      for (const uri of this.index.keys()) {
        try {
          const url = new URL(uri);
          programs.add(`${url.protocol}//${url.hostname}`);
        } catch {
          // skip malformed URIs
        }
      }

      return {
        status: "healthy",
        schema: [...programs],
        details: {
          indexedUris: this.index.size,
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
}
