/**
 * S3Store — Amazon S3 implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → key `{prefix}{uriToKey(uri)}.bin`, body = raw
 *   payload bytes (current behaviour). One object per entry.
 * - any other schema → key
 *   `{prefix}entities/{entityName}/{uriToKey(uri)}.json`, body =
 *   JSON-encoded record. See `./fields.ts` for the canonical
 *   `TYPE_TAGS` ↔ JSON encoding (bytes → base64, bigint → string,
 *   timestamp → ISO-8601).
 *
 * `ensureEntity` is idempotent and caches the field plan. S3 has no
 * DDL, so provisioning is purely client-side bookkeeping.
 *
 * `fn=ls` / `fn=count` are shallow direct-leaves only: list objects
 * under the URI's key prefix, keep keys whose remainder has no further
 * `/`. `format=uris` returns the URIs without issuing `getObject`;
 * `format=full` only fetches the selected page.
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import { dispatchRead } from "../dispatch.ts";
import { storageFailure } from "../errors.ts";
import { validateReadParams } from "../read.ts";
import type { EntityStore } from "../entity-store.ts";
import {
  BYTES_ENTITY,
  type EntityRecord,
  type EntitySchema,
  type EntitySupport,
} from "../entity.ts";
import type { StoreCapabilities, StoreWriteResult } from "../types.ts";
import {
  decodeRecord,
  encodeRecord,
  type FieldPlan,
  planFields,
} from "./fields.ts";
import type { S3Executor } from "./mod.ts";

const STORE_NAME = "S3Store";
const BYTES_EXT = ".bin";
const ENTITY_EXT = ".json";
const ENTITY_ROOT = "entities/";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  /** `entities/{entityName}/` (relative to the store's prefix). */
  keyRoot: string;
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

/** `protocol://host/path` → `protocol_host/path`. */
function uriToKey(uri: string): string {
  return uri.replace("://", "_").replace(/^\//, "");
}

/** Inverse of `uriToKey` (with the given file extension stripped). */
function keyTailToUri(tail: string, ext: string): string {
  const noExt = tail.endsWith(ext) ? tail.slice(0, -ext.length) : tail;
  return noExt.replace("_", "://");
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

async function streamToString(
  stream: ReadableStream<Uint8Array>,
): Promise<string> {
  return await new Response(stream as BodyInit).text();
}

export class S3Store implements EntityStore {
  private readonly bucket: string;
  private readonly executor: S3Executor;
  private readonly prefix: string;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(bucket: string, executor: S3Executor, prefix?: string) {
    if (!bucket) throw new Error("bucket is required");
    if (!executor) throw new Error("executor is required");

    this.bucket = bucket;
    this.executor = executor;
    this.prefix = prefix ?? "";
  }

  // ── EntityStore surface ──────────────────────────────────────────

  ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return Promise.resolve(cached.support);

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        keyRoot: "",
        fields: [{ name: "payload", tag: "bytes" }],
        declared: new Set(["payload"]),
        support: {
          entity: schema.name,
          supported: ["payload"],
          unsupported: [],
        },
      };
      this.entities.set(schema.name, meta);
      return Promise.resolve(meta.support);
    }

    if (!NAME_PATTERN.test(schema.name)) {
      throw new Error(
        `${STORE_NAME}: entity name '${schema.name}' must match ${NAME_PATTERN.source}`,
      );
    }
    const { fields, unsupported } = planFields(schema.fields);
    const meta: EntityMeta = {
      keyRoot: `${ENTITY_ROOT}${schema.name}/`,
      fields,
      declared: new Set(fields.map((f) => f.name)),
      support: {
        entity: schema.name,
        supported: fields.map((f) => f.name),
        unsupported,
      },
    };
    this.entities.set(schema.name, meta);
    return Promise.resolve(meta.support);
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
          ? this._readBytesOne(p.uri)
          : this._readEntityOne(meta, p.uri);
      },
      ls: async (p) => {
        const meta = await this._meta(schema);
        return isBytesSchema(schema)
          ? this._lsBytes(p)
          : this._lsEntity(meta, p);
      },
      count: async (p) => {
        const meta = await this._meta(schema);
        return isBytesSchema(schema)
          ? this._countBytes(p)
          : this._countEntity(meta, p);
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
        if (isBytesSchema(schema)) {
          await this.executor.deleteObject(this._bytesKey(uri));
        } else {
          await this.executor.deleteObject(this._entityKey(meta, uri));
        }
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
      const ok = await this.executor.headBucket();
      if (!ok) {
        return {
          status: "unhealthy",
          message: `Bucket not accessible: ${this.bucket}`,
          fns: ["read", "ls", "count"],
        };
      }
      return {
        status: "healthy",
        message: "S3 store is operational",
        fns: ["read", "ls", "count"],
        details: {
          bucket: this.bucket,
          prefix: this.prefix || "(none)",
        },
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

  private _bytesKey(uri: string): string {
    return `${this.prefix}${uriToKey(uri)}${BYTES_EXT}`;
  }

  private _entityKey(meta: EntityMeta, uri: string): string {
    return `${this.prefix}${meta.keyRoot}${uriToKey(uri)}${ENTITY_EXT}`;
  }

  // ── BYTES_ENTITY layout ──────────────────────────────────────────

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
        await this.executor.putObject(
          this._bytesKey(uri),
          payload,
          "application/octet-stream",
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
    uri: string,
  ): Promise<EntityRecord | undefined> {
    const content = await this.executor.getObject(this._bytesKey(uri));
    if (!content) return undefined;
    return { payload: content };
  }

  private async _listBytesChildUris(prefixUri: string): Promise<string[]> {
    const keyPrefix = `${this.prefix}${uriToKey(prefixUri)}`;
    const keys = await this.executor.listObjects(keyPrefix);
    const uris: string[] = [];
    for (const key of keys) {
      if (!key.endsWith(BYTES_EXT)) continue;
      const tail = key.slice(keyPrefix.length);
      if (tail.includes("/")) continue;
      uris.push(`${prefixUri}${keyTailToUri(tail, BYTES_EXT)}`);
    }
    return uris;
  }

  private async _lsBytes(parsed: ParsedUrl): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    let uris = await this._listBytesChildUris(parsed.uri);

    if (params.sortBy === "uri") {
      const dir = params.sortOrder === "desc" ? -1 : 1;
      uris = [...uris].sort((a, b) => a.localeCompare(b) * dir);
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      const start = (page - 1) * params.limit;
      uris = uris.slice(start, start + params.limit);
    }
    if (format === "uris") return uris;
    const out: Output[] = [];
    for (const uri of uris) {
      const r = await this._readBytesOne(uri);
      out.push([uri, r]);
    }
    return out;
  }

  private async _countBytes(parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return (await this._listBytesChildUris(parsed.uri)).length;
  }

  // ── Custom-entity layout (JSON-encoded records) ──────────────────

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
              `${STORE_NAME}: record contains keys not declared in schema '${meta.keyRoot}': ${
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
        const encoded = JSON.stringify(encodeRecord(meta.fields, record));
        await this.executor.putObject(
          this._entityKey(meta, uri),
          new TextEncoder().encode(encoded),
          "application/json",
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

  private async _readEntityOne(
    meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    const stream = await this.executor.getObject(this._entityKey(meta, uri));
    if (!stream) return undefined;
    const text = await streamToString(stream);
    const json = JSON.parse(text) as Record<string, unknown>;
    return decodeRecord(meta.fields, json);
  }

  private async _listEntityChildUris(
    meta: EntityMeta,
    prefixUri: string,
  ): Promise<string[]> {
    const keyPrefix = `${this.prefix}${meta.keyRoot}${uriToKey(prefixUri)}`;
    const keys = await this.executor.listObjects(keyPrefix);
    const uris: string[] = [];
    for (const key of keys) {
      if (!key.endsWith(ENTITY_EXT)) continue;
      const tail = key.slice(keyPrefix.length);
      if (tail.includes("/")) continue;
      uris.push(`${prefixUri}${keyTailToUri(tail, ENTITY_EXT)}`);
    }
    return uris;
  }

  private async _lsEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    let uris = await this._listEntityChildUris(meta, parsed.uri);

    if (params.sortBy === "uri") {
      const dir = params.sortOrder === "desc" ? -1 : 1;
      uris = [...uris].sort((a, b) => a.localeCompare(b) * dir);
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      const start = (page - 1) * params.limit;
      uris = uris.slice(start, start + params.limit);
    }
    if (format === "uris") return uris;
    const out: Output[] = [];
    for (const uri of uris) {
      const r = await this._readEntityOne(meta, uri);
      out.push([uri, r]);
    }
    return out;
  }

  private async _countEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return (await this._listEntityChildUris(meta, parsed.uri)).length;
  }
}
