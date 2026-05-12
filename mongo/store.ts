/**
 * MongoStore — MongoDB implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness. Uses an
 * injected MongoExecutor so the package does not depend on a specific
 * MongoDB driver. `fn=ls`/`fn=count` push down to a regex prefix
 * query that enforces the shallow-direct-leaves contract:
 * `^<prefix>[^/]+$`.
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
} from "../_shared/mod.ts";
import type { MongoExecutor } from "./mod.ts";

const STORE_NAME = "MongoStore";

/** Escape special regex characters for safe use in a RegExp pattern. */
function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export class MongoStore implements Store {
  private readonly collectionName: string;
  private readonly executor: MongoExecutor;

  constructor(collectionName: string, executor: MongoExecutor) {
    if (!collectionName) throw new Error("collectionName is required");
    if (!executor) throw new Error("executor is required");

    this.collectionName = collectionName;
    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        await this.executor.updateOne(
          { uri: entry.uri },
          {
            $set: {
              uri: entry.uri,
              data: encodeBinaryForJson(entry.data),
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

  read<T = unknown>(urls: string[]): Promise<Output<T>[]> {
    return dispatchRead<T>(urls, STORE_NAME, {
      read: (p) => this._readOne(p.uri),
      ls: (p) => this._ls(p),
      count: (p) => this._count(p),
    });
  }

  private async _readOne(uri: string): Promise<unknown> {
    const doc = await this.executor.findOne({ uri });
    if (!doc) return undefined;
    return decodeBinaryFromJson(doc.data);
  }

  /** Build the shallow-direct-leaves regex filter for a prefix. */
  private _leafFilter(prefixUri: string): Record<string, unknown> {
    return { uri: { $regex: `^${escapeRegex(prefixUri)}[^/]+$` } };
  }

  private async _ls(parsed: ParsedUrl): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";

    const options: Parameters<MongoExecutor["findMany"]>[1] = {};
    if (params.sortBy === "uri") {
      options.sort = { uri: params.sortOrder === "desc" ? -1 : 1 };
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      options.limit = params.limit;
      options.skip = (page - 1) * params.limit;
    }
    if (format === "uris") {
      options.projection = { uri: 1, _id: 0 };
    }

    const docs = await this.executor.findMany(
      this._leafFilter(parsed.uri),
      options,
    );

    if (format === "uris") return docs.map((d) => d.uri as string);
    return docs.map((d): Output => [
      d.uri as string,
      decodeBinaryFromJson(d.data),
    ]);
  }

  private async _count(parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return await this.executor.countDocuments(this._leafFilter(parsed.uri));
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        await this.executor.deleteOne({ uri });
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
          fns: ["read", "ls", "count"],
        };
      }
      return {
        status: "healthy",
        message: "MongoDB store is operational",
        fns: ["read", "ls", "count"],
        details: { collectionName: this.collectionName },
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
}
