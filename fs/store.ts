/**
 * FsStore — Filesystem implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries as JSON files, read them back, delete them.
 * Observe is not supported.
 *
 * Uses an injected FsExecutor so the SDK does not depend on a specific
 * filesystem API (Node fs, Deno, etc.).
 *
 * @example
 * ```typescript
 * import { FsStore } from "@bandeira-tech/b3nd-core";
 *
 * const store = new FsStore("/data/store", executor);
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
import type { FsExecutor } from "./mod.ts";

/**
 * Convert a URI to a relative filesystem path.
 * `protocol://host/path` becomes `protocol_host/path.json`.
 */
function uriToRelPath(uri: string): string {
  return uri.replace("://", "_") + ".json";
}

/**
 * Convert a relative filesystem path back to a URI.
 * `protocol_host/path.json` becomes `protocol://host/path`.
 */
function relPathToUri(relPath: string): string {
  const withoutExt = relPath.replace(/\.json$/, "");
  return withoutExt.replace("_", "://");
}

export class FsStore implements Store {
  private readonly rootDir: string;
  private readonly executor: FsExecutor;

  constructor(rootDir: string, executor: FsExecutor) {
    if (!rootDir) {
      throw new Error("rootDir is required");
    }
    if (!executor) {
      throw new Error("executor is required");
    }

    this.rootDir = rootDir.replace(/\/+$/, "");
    this.executor = executor;
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
        const filePath = this.resolvePath(entry.uri);
        await this.executor.writeFile(filePath, body);
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
      const filePath = this.resolvePath(uri);
      const content = await this.executor.readFile(filePath);
      const record = JSON.parse(content) as {
        values?: Record<string, number>;
        data: unknown;
      };
      const decodedData = decodeBinaryFromJson(record.data) as T;

      return {
        success: true,
        record: { values: record.values ?? {}, data: decodedData },
      };
    } catch {
      return { success: false, error: `Not found: ${uri}` };
    }
  }

  private async _list<T>(uri: string): Promise<ReadResult<T>[]> {
    try {
      const relDir = uriToRelPath(uri).replace(/\.json$/, "").replace(
        /\/+$/,
        "",
      );
      const dirPath = `${this.rootDir}/${relDir}`;

      const files = await this.executor.listFiles(dirPath);

      const results: ReadResult<T>[] = [];
      for (const f of files.filter((f) => f.endsWith(".json"))) {
        const fullRel = `${relDir}/${f}`;
        const childUri = relPathToUri(fullRel);
        const result = await this._readOne<T>(childUri);
        if (result.success) {
          results.push({ ...result, uri: childUri });
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
        const filePath = this.resolvePath(uri);
        await this.executor.removeFile(filePath);
        results.push({ success: true });
      } catch {
        // File may not exist — succeed silently
        results.push({ success: true });
      }
    }

    return results;
  }

  // ── Status ───────────────────────────────────────────────────────

  async status(): Promise<StatusResult> {
    try {
      const rootExists = await this.executor.exists(this.rootDir);
      if (!rootExists) {
        return {
          status: "unhealthy",
          message: `Root directory not found: ${this.rootDir}`,
        };
      }

      return {
        status: "healthy",
        message: "Filesystem store is operational",
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

  private resolvePath(uri: string): string {
    return `${this.rootDir}/${uriToRelPath(uri)}`;
  }
}
