/**
 * PostgresStore — PostgreSQL implementation of Store.
 *
 * Pure mechanical byte storage with no protocol awareness. Uses an
 * injected SqlExecutor so the package does not depend on a specific
 * Postgres driver. The payload column is BYTEA; the store does not
 * inspect or transform its contents.
 *
 * `fn=ls` / `fn=count` push down to SQL: the shallow-direct-leaves
 * contract (`uri LIKE prefix% AND uri NOT LIKE prefix%/%`) is
 * enforced in the WHERE clause.
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import { dispatchRead, validateReadParams } from "../../shared/mod.ts";
import type {
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "../../types.ts";
import type { SqlExecutor } from "./mod.ts";

const STORE_NAME = "PostgresStore";

/**
 * Normalize a pg `bytea` row value into a `Uint8Array`. The `pg`
 * driver returns `Buffer`; some pools/wrappers return a hex-prefixed
 * string ("\\x..."). We accept both.
 */
function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string" && value.startsWith("\\x")) {
    const hex = value.slice(2);
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++) {
      out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    }
    return out;
  }
  throw new Error(
    `PostgresStore: unexpected payload type ${typeof value}`,
  );
}

export class PostgresStore implements Store {
  private readonly tableName: string;
  private readonly executor: SqlExecutor;

  constructor(tablePrefix: string, executor: SqlExecutor) {
    if (!tablePrefix) throw new Error("tablePrefix is required");
    if (!executor) throw new Error("executor is required");

    this.tableName = `${tablePrefix}_data`;
    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    if (entries.length === 0) return [];

    // `capabilities.atomicBatch` is true: the whole batch commits or
    // nothing does. The wire shape stays per-entry — on failure every
    // result is `{ success: false }` with the same root-cause error.
    try {
      await this.executor.transaction(async (tx) => {
        for (const entry of entries) {
          await tx.query(
            `INSERT INTO ${this.tableName} (uri, payload) VALUES ($1, $2)
             ON CONFLICT (uri) DO UPDATE SET payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP`,
            [entry.uri, entry.payload],
          );
        }
      });
      return entries.map(() => ({ success: true }));
    } catch (err) {
      const error = err instanceof Error ? err.message : "Write failed";
      return entries.map(() => ({ success: false, error }));
    }
  }

  // ── Read ─────────────────────────────────────────────────────────

  read<T = Uint8Array>(urls: string[]): Promise<Output<T>[]> {
    return dispatchRead<T>(urls, STORE_NAME, {
      read: (p) => this._readOne(p.uri),
      ls: (p) => this._ls(p),
      count: (p) => this._count(p),
    });
  }

  private async _readOne(uri: string): Promise<Uint8Array | undefined> {
    const res = await this.executor.query(
      `SELECT payload FROM ${this.tableName} WHERE uri = $1`,
      [uri],
    );
    if (!res.rows || res.rows.length === 0) return undefined;
    const row = res.rows[0] as { payload: unknown };
    return toBytes(row.payload);
  }

  private async _ls(parsed: ParsedUrl): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";

    // Shallow direct-leaves: starts with prefix AND has no further `/`
    // after it. Implemented as two LIKE predicates against the same
    // prefix bind so we don't duplicate the parameter.
    const select = format === "uris" ? "uri" : "uri, payload";
    const order = params.sortBy === "uri"
      ? ` ORDER BY uri ${params.sortOrder === "desc" ? "DESC" : "ASC"}`
      : "";

    let sql =
      `SELECT ${select} FROM ${this.tableName} WHERE uri LIKE $1 || '%' AND uri NOT LIKE $1 || '%/%'${order}`;
    const args: unknown[] = [parsed.uri];

    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      args.push(params.limit, (page - 1) * params.limit);
      sql += ` LIMIT $${args.length - 1} OFFSET $${args.length}`;
    }

    const res = await this.executor.query(sql, args);
    const rows = (res.rows ?? []) as Array<{ uri: string; payload?: unknown }>;

    if (format === "uris") return rows.map((r) => r.uri);
    return rows.map((r): Output => [r.uri, toBytes(r.payload)]);
  }

  private async _count(parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    const res = await this.executor.query(
      `SELECT COUNT(*)::int AS n FROM ${this.tableName} WHERE uri LIKE $1 || '%' AND uri NOT LIKE $1 || '%/%'`,
      [parsed.uri],
    );
    const row = res.rows?.[0] as { n: number } | undefined;
    return row?.n ?? 0;
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    if (uris.length === 0) return [];

    // Atomic batch: every uri either deletes together or not at all.
    try {
      await this.executor.transaction(async (tx) => {
        for (const uri of uris) {
          await tx.query(
            `DELETE FROM ${this.tableName} WHERE uri = $1`,
            [uri],
          );
        }
      });
      return uris.map(() => ({ success: true }));
    } catch (err) {
      const error = err instanceof Error ? err.message : "Delete failed";
      return uris.map(() => ({ success: false, error }));
    }
  }

  // ── Status ───────────────────────────────────────────────────────

  async status(): Promise<StatusResult> {
    try {
      await this.executor.query("SELECT 1");
      return {
        status: "healthy",
        message: "PostgreSQL store is operational",
        fns: ["read", "ls", "count"],
        details: { tableName: this.tableName },
      };
    } catch (error) {
      return {
        status: "unhealthy",
        message: `PostgreSQL health check failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
        fns: ["read", "ls", "count"],
      };
    }
  }

  capabilities(): StoreCapabilities {
    return { atomicBatch: true };
  }
}
