/**
 * MongoStore — MongoDB implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → the legacy `{collectionName}` collection (one
 *   document per uri with `payload: Binary`). Existing deployments
 *   keep working without migration.
 * - any other schema → a per-entity collection
 *   `{collectionName}_{entity}_data`, with one document per uri and
 *   one field per supported field of the schema. Mongo is schema-
 *   flexible so we don't issue DDL beyond a unique index on `uri`.
 *
 * `ensureEntity` is idempotent and caches per-entity metadata.
 *
 * `fn=ls` / `fn=count` push the shallow-direct-leaves predicate
 * (`uri ^prefix[^/]+$`) into the regex filter on every layout.
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
import { type FieldPlan, planFields } from "./fields.ts";
import type { MongoExecutor, MongoFindManyOptions } from "./mod.ts";

const STORE_NAME = "MongoStore";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  collection: string;
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

/** Escape special regex characters for safe use in a RegExp pattern. */
function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Normalize a Mongo document field into a `Uint8Array`. The driver may
 * surface BSON `Binary` (exposes `.buffer`) or a raw `Uint8Array`
 * depending on options.
 */
function docToBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (
    value && typeof value === "object" &&
    "buffer" in (value as Record<string, unknown>) &&
    (value as { buffer: unknown }).buffer instanceof Uint8Array
  ) {
    return (value as { buffer: Uint8Array }).buffer;
  }
  if (
    value && typeof value === "object" &&
    "buffer" in (value as Record<string, unknown>) &&
    (value as { buffer: unknown }).buffer instanceof ArrayBuffer
  ) {
    return new Uint8Array((value as { buffer: ArrayBuffer }).buffer);
  }
  throw new Error(`${STORE_NAME}: unexpected payload type ${typeof value}`);
}

function entityCollection(prefix: string, entityName: string): string {
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

export class MongoStore implements EntityStore {
  private readonly bytesCollection: string;
  private readonly prefix: string;
  private readonly executor: MongoExecutor;
  private readonly entities = new Map<string, EntityMeta>();

  /**
   * @param collectionName legacy `BYTES_ENTITY` collection; also used
   *   as the prefix for per-entity collections
   *   (`{collectionName}_{entity}_data`).
   */
  constructor(collectionName: string, executor: MongoExecutor) {
    if (!collectionName) throw new Error("collectionName is required");
    if (!NAME_PATTERN.test(collectionName)) {
      throw new Error(
        `collectionName must match ${NAME_PATTERN.source}; got '${collectionName}'`,
      );
    }
    if (!executor) throw new Error("executor is required");

    this.bytesCollection = collectionName;
    this.prefix = collectionName;
    this.executor = executor;
  }

  // ── EntityStore surface ──────────────────────────────────────────

  async ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return cached.support;

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        collection: this.bytesCollection,
        fields: [{ name: "payload", tag: "bytes" }],
        declared: new Set(["payload"]),
        support: {
          entity: schema.name,
          supported: ["payload"],
          unsupported: [],
        },
      };
      this.entities.set(schema.name, meta);
      await this.executor.ensureUriIndex(meta.collection);
      return meta.support;
    }

    const { fields, unsupported } = planFields(schema.fields);
    const collection = entityCollection(this.prefix, schema.name);
    await this.executor.ensureUriIndex(collection);
    const meta: EntityMeta = {
      collection,
      fields,
      declared: new Set(fields.map((f) => f.name)),
      support: {
        entity: schema.name,
        supported: fields.map((f) => f.name),
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
    const results: DeleteResult[] = [];
    for (const uri of uris) {
      try {
        await this.executor.deleteOne(meta.collection, { uri });
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          ...storageFailure(err, "Delete failed", uri),
        });
      }
    }
    return results;
  }

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
        details: { collectionPrefix: this.prefix },
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
    return { atomicBatch: false };
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
    const out: StoreWriteResult[] = [];
    for (const { uri, record } of entries) {
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        out.push({
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
        });
        continue;
      }
      const payload = record.payload;
      if (
        !(payload instanceof Uint8Array) &&
        !(payload instanceof ReadableStream)
      ) {
        out.push({
          success: false,
          ...storageFailure(
            new Error(
              `${STORE_NAME}: BYTES_ENTITY record.payload must be Uint8Array or ReadableStream`,
            ),
            "Invalid record",
            uri,
          ),
        });
        continue;
      }
      try {
        const bytes = await toBytes(payload);
        await this.executor.updateOne(
          meta.collection,
          { uri },
          { $set: { uri, payload: bytes, updatedAt: new Date() } },
          { upsert: true },
        );
        out.push({ success: true });
      } catch (err) {
        out.push({
          success: false,
          ...storageFailure(err, "Write failed", uri),
        });
      }
    }
    return out;
  }

  private async _writeEntity(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    const out: StoreWriteResult[] = [];
    for (const { uri, record } of entries) {
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        out.push({
          success: false,
          ...storageFailure(
            new Error(
              `${STORE_NAME}: record contains keys not declared in schema '${meta.collection}': ${
                extras.join(", ")
              }`,
            ),
            "Schema mismatch",
            uri,
          ),
        });
        continue;
      }
      try {
        const doc: Record<string, unknown> = { uri };
        for (const f of meta.fields) {
          doc[f.name] = adaptForWrite(f, record[f.name]);
        }
        doc.updatedAt = new Date();
        await this.executor.updateOne(
          meta.collection,
          { uri },
          { $set: doc },
          { upsert: true },
        );
        out.push({ success: true });
      } catch (err) {
        out.push({
          success: false,
          ...storageFailure(err, "Write failed", uri),
        });
      }
    }
    return out;
  }

  private async _readBytesOne(
    meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    const doc = await this.executor.findOne(meta.collection, { uri });
    if (!doc) return undefined;
    return { payload: docToBytes(doc.payload) };
  }

  private async _readEntityOne(
    meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    const doc = await this.executor.findOne(meta.collection, { uri });
    if (!doc) return undefined;
    return adaptDocForRead(meta, doc);
  }

  /** Shallow-direct-leaves regex filter for a prefix. */
  private _leafFilter(prefixUri: string): Record<string, unknown> {
    return { uri: { $regex: `^${escapeRegex(prefixUri)}[^/]+$` } };
  }

  private async _lsBytes(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<Output[] | string[]> {
    return this._lsImpl(
      meta,
      parsed,
      (doc) => ({ payload: docToBytes(doc.payload) }),
      { uri: 1, payload: 1, _id: 0 },
    );
  }

  private async _lsEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<Output[] | string[]> {
    const projection: Record<string, 0 | 1> = { uri: 1, _id: 0 };
    for (const f of meta.fields) projection[f.name] = 1;
    return this._lsImpl(
      meta,
      parsed,
      (doc) => adaptDocForRead(meta, doc),
      projection,
    );
  }

  private async _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    toRecord: (doc: Record<string, unknown>) => EntityRecord,
    fullProjection: Record<string, 0 | 1>,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";

    const options: MongoFindManyOptions = {};
    if (params.sortBy === "uri") {
      options.sort = { uri: params.sortOrder === "desc" ? -1 : 1 };
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      options.limit = params.limit;
      options.skip = (page - 1) * params.limit;
    }
    options.projection = format === "uris"
      ? { uri: 1, _id: 0 }
      : fullProjection;

    const docs = await this.executor.findMany(
      meta.collection,
      this._leafFilter(parsed.uri),
      options,
    );
    if (format === "uris") return docs.map((d) => d.uri as string);
    return docs.map((d): Output => [d.uri as string, toRecord(d)]);
  }

  private _count(meta: EntityMeta, parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return this.executor.countDocuments(
      meta.collection,
      this._leafFilter(parsed.uri),
    );
  }
}

/** Coerce a record field value into something BSON will accept. */
function adaptForWrite(field: FieldPlan, value: unknown): unknown {
  if (value === undefined) return null;
  if (field.tag === "timestamp") {
    if (value instanceof Date) return value;
    if (typeof value === "number") return new Date(value);
    if (typeof value === "string") return new Date(value);
    return value;
  }
  // string / number / bigint / boolean / bytes / json — pass through.
  // bytes: Uint8Array → BSON Binary handled by the driver.
  // bigint: most drivers map JS bigint → Long automatically.
  // json: nested object stays an object (BSON subdocument).
  return value;
}

/** Reconstruct an EntityRecord from a Mongo document. */
function adaptDocForRead(
  meta: EntityMeta,
  doc: Record<string, unknown>,
): EntityRecord {
  const rec: EntityRecord = {};
  for (const f of meta.fields) {
    const v = doc[f.name];
    if (v === null || v === undefined) {
      rec[f.name] = undefined;
      continue;
    }
    if (f.tag === "bytes") rec[f.name] = docToBytes(v);
    else if (f.tag === "timestamp") {
      rec[f.name] = v instanceof Date ? v : new Date(v as string);
    } else rec[f.name] = v;
  }
  return rec;
}
