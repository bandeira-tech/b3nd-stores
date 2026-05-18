/**
 * SqliteStore — SQLite implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → the legacy `{prefix}_data` table (`uri TEXT PK`,
 *   `payload BLOB`). Existing deployments keep working without
 *   migration.
 * - any other schema → a per-entity table `{prefix}_{entity}_data`
 *   with one column per supported field, typed by the canonical
 *   `TYPE_TAGS` it carries (see `./columns.ts`).
 *
 * `ensureEntity` is idempotent (`CREATE TABLE IF NOT EXISTS`) and
 * caches per-entity metadata so subsequent writes/reads pay no
 * round-trip.
 *
 * `fn=ls` / `fn=count` push the shallow-direct-leaves predicate
 * (`uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'`) into SQL on every
 * layout.
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
import type { SqliteExecutor } from "./mod.ts";

const STORE_NAME = "SqliteStore";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  tableName: string;
  columns: ColumnPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

function rowToBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (Array.isArray(value)) return new Uint8Array(value as number[]);
  throw new Error(`${STORE_NAME}: unexpected payload type ${typeof value}`);
}

function entityTableName(prefix: string, entityName: string): string {
  if (!NAME_PATTERN.test(entityName)) {
    throw new Error(
      `${STORE_NAME}: entity name '${entityName}' must match ${NAME_PATTERN.source}`,
    );
  }
  return `${prefix}_${entityName}_data`;
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

export class SqliteStore implements EntityStore {
  private readonly tablePrefix: string;
  private readonly bytesTable: string;
  private readonly executor: SqliteExecutor;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(tablePrefix: string, executor: SqliteExecutor) {
    if (!tablePrefix) throw new Error("tablePrefix is required");
    if (!NAME_PATTERN.test(tablePrefix)) {
      throw new Error(
        `tablePrefix must match ${NAME_PATTERN.source}; got '${tablePrefix}'`,
      );
    }
    if (!executor) throw new Error("executor is required");

    this.tablePrefix = tablePrefix;
    this.bytesTable = `${tablePrefix}_data`;
    this.executor = executor;
  }

  // ── EntityStore surface ──────────────────────────────────────────

  // deno-lint-ignore require-await
  async ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return cached.support;

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        tableName: this.bytesTable,
        columns: [{ name: "payload", sqlType: "BLOB", tag: "bytes" }],
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
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_${tableName}_uri ON ${tableName} (uri);`;
    // SQLite executors typically allow only one statement per query; split.
    for (const stmt of sql.split(";").map((s) => s.trim()).filter(Boolean)) {
      this.executor.query(stmt);
    }

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
    return isBytesSchema(schema)
      ? this._writeBytes(meta, entries)
      : this._writeEntity(meta, entries);
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
      this.executor.transaction((tx) => {
        for (const uri of uris) {
          tx.query(
            `DELETE FROM ${meta.tableName} WHERE uri = ?`,
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

  status(): Promise<StatusResult> {
    try {
      this.executor.query("SELECT 1");
      return Promise.resolve({
        status: "healthy",
        message: "SQLite store is operational",
        fns: ["read", "ls", "count"],
        details: { tablePrefix: this.tablePrefix },
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
      this.executor.transaction((tx) => {
        for (const entry of prepared) {
          tx.query(
            `INSERT INTO ${meta.tableName} (uri, payload, updated_at)
             VALUES (?, ?, datetime('now'))
             ON CONFLICT(uri) DO UPDATE SET
               payload = excluded.payload,
               updated_at = datetime('now')`,
            [entry.uri, entry.bytes],
          );
        }
      });
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

  private _writeEntity(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): StoreWriteResult[] {
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
    const placeholders = cols.map(() => "?").join(", ");
    const updates = cols
      .map((c) => `"${c.name}" = excluded."${c.name}"`)
      .concat(["updated_at = datetime('now')"])
      .join(", ");

    try {
      this.executor.transaction((tx) => {
        for (const { uri, record } of accepted) {
          const args: unknown[] = [uri];
          for (const c of cols) args.push(adaptForWrite(c, record[c.name]));
          tx.query(
            `INSERT INTO ${meta.tableName} (uri${
              colList ? ", " + colList : ""
            }) VALUES (?${placeholders ? ", " + placeholders : ""})
             ON CONFLICT(uri) DO UPDATE SET ${updates}`,
            args,
          );
        }
      });
      for (const i of acceptedIdx) out[i] = { success: true };
      return out;
    } catch (err) {
      const failure = storageFailure(err, "Write failed");
      for (const i of acceptedIdx) out[i] = { success: false, ...failure };
      return out;
    }
  }

  private _readBytesOne(
    meta: EntityMeta,
    uri: string,
  ): EntityRecord | undefined {
    const result = this.executor.query(
      `SELECT payload FROM ${meta.tableName} WHERE uri = ?`,
      [uri],
    );
    if (!result.rows.length) return undefined;
    return { payload: rowToBytes(result.rows[0].payload) };
  }

  private _readEntityOne(
    meta: EntityMeta,
    uri: string,
  ): EntityRecord | undefined {
    if (meta.columns.length === 0) {
      const result = this.executor.query(
        `SELECT 1 FROM ${meta.tableName} WHERE uri = ?`,
        [uri],
      );
      return result.rows.length > 0 ? {} : undefined;
    }
    const cols = meta.columns.map((c) => `"${c.name}"`).join(", ");
    const result = this.executor.query(
      `SELECT ${cols} FROM ${meta.tableName} WHERE uri = ?`,
      [uri],
    );
    if (!result.rows.length) return undefined;
    return adaptRowForRead(meta, result.rows[0]);
  }

  private _lsBytes(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Output[] | string[] {
    return this._lsImpl(
      meta,
      parsed,
      (row) => ({ payload: rowToBytes((row as { payload: unknown }).payload) }),
      "uri, payload",
    );
  }

  private _lsEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Output[] | string[] {
    const cols = meta.columns.length > 0
      ? "uri, " + meta.columns.map((c) => `"${c.name}"`).join(", ")
      : "uri";
    return this._lsImpl(
      meta,
      parsed,
      (row) => adaptRowForRead(meta, row),
      cols,
    );
  }

  private _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    toRecord: (row: Record<string, unknown>) => EntityRecord,
    select: string,
  ): Output[] | string[] {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    const selectClause = format === "uris" ? "uri" : select;
    const order = params.sortBy === "uri"
      ? ` ORDER BY uri ${params.sortOrder === "desc" ? "DESC" : "ASC"}`
      : "";

    let sql =
      `SELECT ${selectClause} FROM ${meta.tableName} WHERE uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'${order}`;
    const args: unknown[] = [parsed.uri, parsed.uri];
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      sql += ` LIMIT ? OFFSET ?`;
      args.push(params.limit, (page - 1) * params.limit);
    }
    const result = this.executor.query(sql, args);
    const rows = result.rows;
    if (format === "uris") return rows.map((r) => r.uri as string);
    return rows.map((r): Output => [r.uri as string, toRecord(r)]);
  }

  private _count(meta: EntityMeta, parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    const result = this.executor.query(
      `SELECT COUNT(*) AS n FROM ${meta.tableName} WHERE uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'`,
      [parsed.uri, parsed.uri],
    );
    const row = result.rows[0] as { n: number } | undefined;
    return row?.n ?? 0;
  }
}

/** Adapt a record field value into something the SQLite driver accepts. */
function adaptForWrite(col: ColumnPlan, value: unknown): unknown {
  if (value === undefined || value === null) return null;
  if (col.tag === "json") {
    return typeof value === "string" ? value : JSON.stringify(value);
  }
  if (col.tag === "timestamp") {
    if (value instanceof Date) return value.toISOString();
    if (typeof value === "number") return new Date(value).toISOString();
    return value;
  }
  if (col.tag === "boolean") return value ? 1 : 0;
  return value;
}

/** Reconstruct an EntityRecord from a SQLite row. */
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
    if (col.tag === "bytes") rec[col.name] = rowToBytes(v);
    else if (col.tag === "json") {
      rec[col.name] = typeof v === "string" ? JSON.parse(v) : v;
    } else if (col.tag === "timestamp") {
      rec[col.name] = v instanceof Date ? v : new Date(v as string);
    } else if (col.tag === "boolean") rec[col.name] = Boolean(v);
    else rec[col.name] = v;
  }
  return rec;
}
