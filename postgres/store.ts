/**
 * PostgresStore — PostgreSQL implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries, read entries, delete entries. Observe is not supported.
 *
 * Uses an injected SqlExecutor, keeping the SDK decoupled from any
 * specific PostgreSQL driver.
 *
 * @example
 * ```typescript
 * import { PostgresStore } from "@bandeira-tech/b3nd-sdk";
 *
 * const store = new PostgresStore("myapp", executor);
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
} from "@bandeira-tech/b3nd-sdk/types";
import {
  decodeBinaryFromJson,
  encodeBinaryForJson,
} from "@bandeira-tech/b3nd-sdk";
import type { SqlExecutor } from "./mod.ts";

export class PostgresStore implements Store {
  private readonly tableName: string;
  private readonly executor: SqlExecutor;

  constructor(tablePrefix: string, executor: SqlExecutor) {
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

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encodedData = encodeBinaryForJson(entry.data);
        await this.executor.query(
          `INSERT INTO ${this.tableName} (uri, data, "values") VALUES ($1, $2::jsonb, $3::jsonb)
           ON CONFLICT (uri) DO UPDATE SET data = EXCLUDED.data, "values" = EXCLUDED."values", updated_at = CURRENT_TIMESTAMP`,
          [
            entry.uri,
            JSON.stringify(encodedData),
            JSON.stringify(entry.values || {}),
          ],
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
      const res = await this.executor.query(
        `SELECT data, "values" FROM ${this.tableName} WHERE uri = $1`,
        [uri],
      );

      if (!res.rows || res.rows.length === 0) {
        return { success: false, error: `Not found: ${uri}` };
      }

      // deno-lint-ignore no-explicit-any
      const row: any = res.rows[0];
      const decodedData = decodeBinaryFromJson(row.data) as T;
      const values = row.values || {};

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

      const res = await this.executor.query(
        `SELECT uri, data, "values" FROM ${this.tableName} WHERE uri LIKE $1 || '%'`,
        [prefix],
      );

      if (!res.rows || res.rows.length === 0) {
        return [];
      }

      const results: ReadResult<T>[] = [];
      for (const raw of res.rows) {
        // deno-lint-ignore no-explicit-any
        const row = raw as any;
        const decodedData = decodeBinaryFromJson(row.data) as T;
        results.push({
          success: true,
          uri: row.uri,
          record: {
            values: (row.values || {}) as Record<string, number>,
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

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        await this.executor.query(
          `DELETE FROM ${this.tableName} WHERE uri = $1`,
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

    return results;
  }

  // ── Status ───────────────────────────────────────────────────────

  async status(): Promise<StatusResult> {
    try {
      await this.executor.query("SELECT 1");
      return {
        status: "healthy",
        message: "PostgreSQL store is operational",
        details: { tableName: this.tableName },
      };
    } catch (error) {
      return {
        status: "unhealthy",
        message: `PostgreSQL health check failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      };
    }
  }

  capabilities(): StoreCapabilities {
    return {
      atomicBatch: true,
      binaryData: false,
    };
  }
}
