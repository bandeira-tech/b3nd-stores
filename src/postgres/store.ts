/**
 * PostgresStore — PostgreSQL implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → the legacy `{prefix}_data` table (`uri TEXT PK`,
 *   `payload BYTEA`). Existing deployments keep working without
 *   migration. Stream payloads are collected via `toBytes` before
 *   the transaction.
 * - any other schema → a per-entity table `{prefix}_{entity}_data`
 *   with one column per supported field, typed by the canonical
 *   `TYPE_TAGS` it carries (see `./columns.ts`).
 *
 * `ensureEntity` is idempotent and runs the `CREATE TABLE IF NOT
 * EXISTS` once per schema; the per-entity metadata is cached so
 * subsequent writes/reads don't pay the round-trip.
 *
 * `fn=ls` / `fn=count` push down to SQL on every layout — the
 * shallow-direct-leaves predicate (`uri LIKE prefix% AND uri NOT
 * LIKE prefix%/%`) is enforced in the WHERE clause against the
 * relevant table.
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import { dispatchRead } from "../dispatch.ts";
import { storageFailure } from "../errors.ts";
import { toBytes } from "../payload.ts";
import { validateReadParams } from "../read.ts";
import type { EntityStore } from "../entity-store.ts";
import {
  BYTES_ENTITY,
  type EntityRecord,
  type EntitySchema,
  type EntitySupport,
} from "../entity.ts";
import type { StoreCapabilities, StoreWriteResult } from "../types.ts";
import { type ColumnPlan, planColumns } from "./columns.ts";
import type { SqlExecutor } from "./mod.ts";

const STORE_NAME = "PostgresStore";

const TABLE_PREFIX = /^[a-zA-Z][a-zA-Z0-9_]*$/;

/** Cached metadata for a provisioned entity. */
interface EntityMeta {
  tableName: string;
  columns: ColumnPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

/** Coerce a pg `bytea` row value into a `Uint8Array`. */
function rowToBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string" && value.startsWith("\\x")) {
    const hex = value.slice(2);
    const out = new Uint8Array(hex.length / 2);
    for (let i = 0; i < out.length; i++) {
      out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    }
    return out;
  }
  throw new Error(`${STORE_NAME}: unexpected payload type ${typeof value}`);
}

function entityTableName(tablePrefix: string, entityName: string): string {
  if (!TABLE_PREFIX.test(entityName)) {
    throw new Error(
      `${STORE_NAME}: entity name '${entityName}' must match ${TABLE_PREFIX.source}`,
    );
  }
  return `${tablePrefix}_${entityName}_data`;
}

export class PostgresStore implements EntityStore {
  private readonly tablePrefix: string;
  private readonly bytesTable: string;
  private readonly executor: SqlExecutor;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(tablePrefix: string, executor: SqlExecutor) {
    if (!tablePrefix) throw new Error("tablePrefix is required");
    if (!TABLE_PREFIX.test(tablePrefix)) {
      throw new Error(
        `tablePrefix must match ${TABLE_PREFIX.source}; got '${tablePrefix}'`,
      );
    }
    if (!executor) throw new Error("executor is required");

    this.tablePrefix = tablePrefix;
    this.bytesTable = `${tablePrefix}_data`;
    this.executor = executor;
  }

  // ── EntityStore surface ──────────────────────────────────────────

  async ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return cached.support;

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        tableName: this.bytesTable,
        columns: [{ name: "payload", sqlType: "BYTEA", tag: "bytes" }],
        declared: new Set(["payload"]),
        support: {
          entity: schema.name,
          supported: ["payload"],
          unsupported: [],
        },
      };
      this.entities.set(schema.name, meta);
      return meta.support;
    }

    const { columns, unsupported } = planColumns(schema.fields);
    const tableName = entityTableName(this.tablePrefix, schema.name);
    const colDdl = columns
      .map((c) => `"${c.name}" ${c.sqlType}`)
      .join(",\n  ");
    const sql = `CREATE TABLE IF NOT EXISTS ${tableName} (
  uri TEXT PRIMARY KEY${columns.length > 0 ? ",\n  " + colDdl : ""},
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_${tableName}_uri ON ${tableName} (uri);`;
    await this.executor.query(sql);

    const meta: EntityMeta = {
      tableName,
      columns,
      declared: new Set(columns.map((c) => c.name)),
      support: {
        entity: schema.name,
        supported: columns.map((c) => c.name),
        unsupported,
      },
    };
    this.entities.set(schema.name, meta);
    return meta.support;
  }

  async write(
    schema: EntitySchema,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    if (entries.length === 0) return [];
    const meta = await this._meta(schema);

    if (isBytesSchema(schema)) return this._writeBytes(meta, entries);
    return this._writeEntity(meta, entries);
  }

  read<T = EntityRecord | undefined>(
    schema: EntitySchema,
    urls: string[],
  ): Promise<Output<T>[]> {
    return dispatchRead<T>(urls, STORE_NAME, {
      read: async (p) => {
        const meta = await this._meta(schema);
        return isBytesSchema(schema)
          ? this._readBytesOne(meta, p.uri)
          : this._readEntityOne(meta, p.uri);
      },
      ls: async (p) => {
        const meta = await this._meta(schema);
        return isBytesSchema(schema)
          ? this._lsBytes(meta, p)
          : this._lsEntity(meta, p);
      },
      count: async (p) => {
        const meta = await this._meta(schema);
        return this._count(meta, p);
      },
    });
  }

  async delete(
    schema: EntitySchema,
    uris: string[],
  ): Promise<DeleteResult[]> {
    if (uris.length === 0) return [];
    const meta = await this._meta(schema);
    try {
      await this.executor.transaction(async (tx) => {
        for (const uri of uris) {
          await tx.query(
            `DELETE FROM ${meta.tableName} WHERE uri = $1`,
            [uri],
          );
        }
      });
      return uris.map(() => ({ success: true }));
    } catch (err) {
      const failure = storageFailure(err, "Delete failed");
      return uris.map(() => ({ success: false, ...failure }));
    }
  }

  async status(): Promise<StatusResult> {
    try {
      await this.executor.query("SELECT 1");
      return {
        status: "healthy",
        message: "PostgreSQL store is operational",
        fns: ["read", "ls", "count"],
        details: { tablePrefix: this.tablePrefix },
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

  // ── Internals ────────────────────────────────────────────────────

  private async _meta(schema: EntitySchema): Promise<EntityMeta> {
    const cached = this.entities.get(schema.name);
    if (cached) return cached;
    await this.ensureEntity(schema);
    return this.entities.get(schema.name)!;
  }

  private async _writeBytes(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    // BYTES_ENTITY: collect any streams to bytes before the
    // transaction so a stream failure can't leave a half-applied
    // commit (capabilities.atomicBatch is true).
    const prepared: { uri: string; bytes: Uint8Array; index: number }[] = [];
    const failures: { index: number; result: StoreWriteResult }[] = [];
    for (let i = 0; i < entries.length; i++) {
      const { uri, record } = entries[i];
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        failures.push({
          index: i,
          result: {
            success: false,
            ...storageFailure(
              new Error(
                `${STORE_NAME}: record contains keys not declared in schema 'bytes': ${
                  extras.join(", ")
                }`,
              ),
              "Schema mismatch",
              uri,
            ),
          },
        });
        continue;
      }
      const payload = record.payload;
      if (
        !(payload instanceof Uint8Array) &&
        !(payload instanceof ReadableStream)
      ) {
        failures.push({
          index: i,
          result: {
            success: false,
            ...storageFailure(
              new Error(
                `${STORE_NAME}: BYTES_ENTITY record.payload must be Uint8Array or ReadableStream`,
              ),
              "Invalid record",
              uri,
            ),
          },
        });
        continue;
      }
      prepared.push({ uri, bytes: await toBytes(payload), index: i });
    }
    if (prepared.length === 0) return failures.map((f) => f.result);

    try {
      await this.executor.transaction(async (tx) => {
        for (const entry of prepared) {
          await tx.query(
            `INSERT INTO ${meta.tableName} (uri, payload) VALUES ($1, $2)
             ON CONFLICT (uri) DO UPDATE SET payload = EXCLUDED.payload, updated_at = CURRENT_TIMESTAMP`,
            [entry.uri, entry.bytes],
          );
        }
      });
      // Re-interleave: every prepared entry succeeds; failures sit in place.
      const out: StoreWriteResult[] = new Array(entries.length);
      let okIdx = 0;
      let failIdx = 0;
      for (let i = 0; i < entries.length; i++) {
        if (failures[failIdx]?.index === i) out[i] = failures[failIdx++].result;
        else if (prepared[okIdx]?.index === i) {
          out[i] = { success: true };
          okIdx++;
        }
      }
      return out;
    } catch (err) {
      const failure = storageFailure(err, "Write failed");
      return entries.map(() => ({ success: false, ...failure }));
    }
  }

  private async _writeEntity(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    // Validate per-entry first so an early failure can't leak partial
    // commits via the atomic batch.
    const out: StoreWriteResult[] = new Array(entries.length);
    const accepted: typeof entries = [];
    const acceptedIdx: number[] = [];

    for (let i = 0; i < entries.length; i++) {
      const { uri, record } = entries[i];
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        out[i] = {
          success: false,
          ...storageFailure(
            new Error(
              `${STORE_NAME}: record contains keys not declared in schema '${meta.tableName}': ${
                extras.join(", ")
              }`,
            ),
            "Schema mismatch",
            uri,
          ),
        };
        continue;
      }
      accepted.push(entries[i]);
      acceptedIdx.push(i);
    }
    if (accepted.length === 0) return out;

    const cols = meta.columns;
    const colList = cols.map((c) => `"${c.name}"`).join(", ");
    const placeholders = cols.map((_, i) => `$${i + 2}`).join(", ");
    const updates = cols
      .map((c) => `"${c.name}" = EXCLUDED."${c.name}"`)
      .concat(["updated_at = CURRENT_TIMESTAMP"])
      .join(", ");

    try {
      await this.executor.transaction(async (tx) => {
        for (const { uri, record } of accepted) {
          const args: unknown[] = [uri];
          for (const c of cols) args.push(adaptForWrite(c, record[c.name]));
          await tx.query(
            `INSERT INTO ${meta.tableName} (uri${
              colList ? ", " + colList : ""
            }) VALUES ($1${placeholders ? ", " + placeholders : ""})
             ON CONFLICT (uri) DO UPDATE SET ${updates}`,
            args,
          );
        }
      });
      for (const i of acceptedIdx) out[i] = { success: true };
      return out;
    } catch (err) {
      // Atomic batch failure — every accepted entry shares the same failure.
      const failure = storageFailure(err, "Write failed");
      for (const i of acceptedIdx) out[i] = { success: false, ...failure };
      return out;
    }
  }

  private async _readBytesOne(
    meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    const res = await this.executor.query(
      `SELECT payload FROM ${meta.tableName} WHERE uri = $1`,
      [uri],
    );
    if (!res.rows || res.rows.length === 0) return undefined;
    const row = res.rows[0] as { payload: unknown };
    return { payload: rowToBytes(row.payload) };
  }

  private async _readEntityOne(
    meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    if (meta.columns.length === 0) {
      // Empty schema: presence-only — return an empty record on hit.
      const res = await this.executor.query(
        `SELECT 1 FROM ${meta.tableName} WHERE uri = $1`,
        [uri],
      );
      return res.rows && res.rows.length > 0 ? {} : undefined;
    }
    const cols = meta.columns.map((c) => `"${c.name}"`).join(", ");
    const res = await this.executor.query(
      `SELECT ${cols} FROM ${meta.tableName} WHERE uri = $1`,
      [uri],
    );
    if (!res.rows || res.rows.length === 0) return undefined;
    return adaptRowForRead(meta, res.rows[0] as Record<string, unknown>);
  }

  private async _lsBytes(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<Output[] | string[]> {
    return this._lsImpl(
      meta,
      parsed,
      (row) => ({ payload: rowToBytes((row as { payload: unknown }).payload) }),
      `uri, payload`,
    );
  }

  private async _lsEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<Output[] | string[]> {
    const cols = meta.columns.length > 0
      ? "uri, " + meta.columns.map((c) => `"${c.name}"`).join(", ")
      : "uri";
    return this._lsImpl(
      meta,
      parsed,
      (row) => adaptRowForRead(meta, row as Record<string, unknown>),
      cols,
    );
  }

  private async _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    toRecord: (row: unknown) => EntityRecord,
    select: string,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    const selectClause = format === "uris" ? "uri" : select;
    const order = params.sortBy === "uri"
      ? ` ORDER BY uri ${params.sortOrder === "desc" ? "DESC" : "ASC"}`
      : "";

    let sql =
      `SELECT ${selectClause} FROM ${meta.tableName} WHERE uri LIKE $1 || '%' AND uri NOT LIKE $1 || '%/%'${order}`;
    const args: unknown[] = [parsed.uri];
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      args.push(params.limit, (page - 1) * params.limit);
      sql += ` LIMIT $${args.length - 1} OFFSET $${args.length}`;
    }
    const res = await this.executor.query(sql, args);
    const rows = (res.rows ?? []) as Array<Record<string, unknown>>;
    if (format === "uris") return rows.map((r) => r.uri as string);
    return rows.map((r): Output => [r.uri as string, toRecord(r)]);
  }

  private async _count(meta: EntityMeta, parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    const res = await this.executor.query(
      `SELECT COUNT(*)::int AS n FROM ${meta.tableName} WHERE uri LIKE $1 || '%' AND uri NOT LIKE $1 || '%/%'`,
      [parsed.uri],
    );
    const row = res.rows?.[0] as { n: number } | undefined;
    return row?.n ?? 0;
  }
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

/** Coerce a record field value into something the driver will accept. */
function adaptForWrite(col: ColumnPlan, value: unknown): unknown {
  if (value === undefined || value === null) return null;
  if (col.sqlType === "JSONB") {
    return typeof value === "string" ? value : JSON.stringify(value);
  }
  if (col.sqlType === "TIMESTAMPTZ") {
    if (value instanceof Date) return value.toISOString();
    if (typeof value === "number") return new Date(value).toISOString();
    return value;
  }
  return value;
}

/** Reconstruct an EntityRecord from a Postgres row. */
function adaptRowForRead(
  meta: EntityMeta,
  row: Record<string, unknown>,
): EntityRecord {
  const rec: EntityRecord = {};
  for (const col of meta.columns) {
    const v = row[col.name];
    if (v === null || v === undefined) {
      rec[col.name] = undefined;
      continue;
    }
    if (col.sqlType === "BYTEA") rec[col.name] = rowToBytes(v);
    else if (col.sqlType === "JSONB") {
      rec[col.name] = typeof v === "string" ? JSON.parse(v) : v;
    } else if (col.sqlType === "TIMESTAMPTZ") {
      rec[col.name] = v instanceof Date ? v : new Date(v as string);
    } else rec[col.name] = v;
  }
  return rec;
}
