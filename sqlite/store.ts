/**
 * SqliteStore — SQLite implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries, read entries, delete entries. Observe is not supported.
 *
 * Uses an injected SqliteExecutor, keeping the SDK decoupled from any
 * specific SQLite driver. The executor is synchronous,
 * so results are wrapped in Promise.resolve() to satisfy the async Store
 * interface.
 *
 * @example
 * ```typescript
 * import { SqliteStore } from "@bandeira-tech/b3nd-core";
 *
 * const store = new SqliteStore("myapp", executor);
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
import type { SqliteExecutor } from "./mod.ts";

export class SqliteStore implements Store {
  private readonly tableName: string;
  private readonly executor: SqliteExecutor;

  constructor(tablePrefix: string, executor: SqliteExecutor) {
    if (!tablePrefix) {
      throw new Error("tablePrefix is required");
    }
    if (!executor) {
      throw new Error("executor is required");
    }

    this.tableName = `${tablePrefix}_data`;
    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encodedData = encodeBinaryForJson(entry.data);
        const jsonData = JSON.stringify(encodedData);
        const jsonValues = JSON.stringify(entry.values || {});

        this.executor.query(
          `INSERT INTO ${this.tableName} (uri, data, "values", updated_at)
           VALUES (?, ?, ?, datetime('now'))
           ON CONFLICT(uri) DO UPDATE SET
             data = excluded.data,
             "values" = excluded."values",
             updated_at = datetime('now')`,
          [entry.uri, jsonData, jsonValues],
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

  read<T = unknown>(uris: string[]): Promise<ReadResult<T>[]> {
    const results: ReadResult<T>[] = [];

    for (const uri of uris) {
      if (uri.endsWith("/")) {
        results.push(...this._list<T>(uri));
      } else {
        results.push(this._readOne<T>(uri));
      }
    }

    return Promise.resolve(results);
  }

  private _readOne<T = unknown>(uri: string): ReadResult<T> {
    try {
      const result = this.executor.query(
        `SELECT data, "values" FROM ${this.tableName} WHERE uri = ?`,
        [uri],
      );

      if (!result.rows.length) {
        return { success: false, error: `Not found: ${uri}` };
      }

      const row = result.rows[0];
      const rawData = typeof row.data === "string"
        ? JSON.parse(row.data)
        : row.data;
      const decodedData = decodeBinaryFromJson(rawData) as T;
      const values = typeof row.values === "string"
        ? JSON.parse(row.values)
        : (row.values || {});

      return { success: true, record: { values, data: decodedData } };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  private _list<T = unknown>(uri: string): ReadResult<T>[] {
    try {
      const prefixBase = uri.endsWith("/") ? uri : `${uri}/`;

      const result = this.executor.query(
        `SELECT uri, data, "values" FROM ${this.tableName} WHERE uri LIKE ?`,
        [`${prefixBase}%`],
      );

      if (!result.rows.length) {
        return [];
      }

      const results: ReadResult<T>[] = [];
      for (const row of result.rows) {
        const rawData = typeof row.data === "string"
          ? JSON.parse(row.data as string)
          : row.data;
        const decodedData = decodeBinaryFromJson(rawData) as T;
        const values = typeof row.values === "string"
          ? JSON.parse(row.values as string)
          : (row.values || {});
        results.push({
          success: true,
          uri: row.uri as string,
          record: {
            values: values as Record<string, number>,
            data: decodedData,
          },
        });
      }

      return results;
    } catch (_error) {
      return [];
    }
  }

  // ── Delete ───────────────────────────────────────────────────────

  delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        this.executor.query(
          `DELETE FROM ${this.tableName} WHERE uri = ?`,
          [uri],
        );
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
      this.executor.query("SELECT 1");
      return Promise.resolve({
        status: "healthy",
        message: "SQLite store is operational",
        details: { tableName: this.tableName },
      });
    } catch (error) {
      return Promise.resolve({
        status: "unhealthy",
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  capabilities(): StoreCapabilities {
    return {
      atomicBatch: true,
      binaryData: false,
    };
  }
}
