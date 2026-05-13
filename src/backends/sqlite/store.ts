/**
 * SqliteStore — SQLite implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness. Pushes
 * fn=ls/fn=count down to SQL using the shallow-direct-leaves
 * predicate: `uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'`.
 *
 * Uses an injected synchronous SqliteExecutor; results are wrapped in
 * Promise.resolve() to satisfy the async Store interface. JSON is
 * stored as TEXT — there is no JSONB in SQLite — and parsed on read.
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
import type { SqliteExecutor } from "./mod.ts";

const STORE_NAME = "SqliteStore";

export class SqliteStore implements Store {
  private readonly tableName: string;
  private readonly executor: SqliteExecutor;

  constructor(tablePrefix: string, executor: SqliteExecutor) {
    if (!tablePrefix) throw new Error("tablePrefix is required");
    if (!executor) throw new Error("executor is required");

    this.tableName = `${tablePrefix}_data`;
    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encoded = JSON.stringify(encodeBinaryForJson(entry.data));
        this.executor.query(
          `INSERT INTO ${this.tableName} (uri, data, updated_at)
           VALUES (?, ?, datetime('now'))
           ON CONFLICT(uri) DO UPDATE SET
             data = excluded.data,
             updated_at = datetime('now')`,
          [entry.uri, encoded],
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
    const result = this.executor.query(
      `SELECT data FROM ${this.tableName} WHERE uri = ?`,
      [uri],
    );
    if (!result.rows.length) return undefined;
    const row = result.rows[0];
    const raw = typeof row.data === "string" ? JSON.parse(row.data) : row.data;
    return decodeBinaryFromJson(raw);
  }

  private _ls(parsed: ParsedUrl): Output[] | string[] {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";

    const select = format === "uris" ? "uri" : "uri, data";
    const order = params.sortBy === "uri"
      ? ` ORDER BY uri ${params.sortOrder === "desc" ? "DESC" : "ASC"}`
      : "";

    let sql =
      `SELECT ${select} FROM ${this.tableName} WHERE uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'${order}`;
    const args: unknown[] = [parsed.uri, parsed.uri];

    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      sql += ` LIMIT ? OFFSET ?`;
      args.push(params.limit, (page - 1) * params.limit);
    }

    const result = this.executor.query(sql, args);
    const rows = result.rows as Array<{ uri: string; data?: unknown }>;

    if (format === "uris") return rows.map((r) => r.uri);
    return rows.map((r): Output => {
      const raw = typeof r.data === "string"
        ? JSON.parse(r.data as string)
        : r.data;
      return [r.uri, decodeBinaryFromJson(raw)];
    });
  }

  private _count(parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    const result = this.executor.query(
      `SELECT COUNT(*) AS n FROM ${this.tableName} WHERE uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'`,
      [parsed.uri, parsed.uri],
    );
    const row = result.rows[0] as { n: number } | undefined;
    return row?.n ?? 0;
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
        fns: ["read", "ls", "count"],
        details: { tableName: this.tableName },
      });
    } catch (error) {
      return Promise.resolve({
        status: "unhealthy",
        message: error instanceof Error ? error.message : String(error),
        fns: ["read", "ls", "count"],
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
