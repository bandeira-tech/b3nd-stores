/**
 * LocalStorageStore — browser localStorage implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → key `{keyPrefix}{uri}` with a base64-encoded
 *   payload string. Existing deployments keep working without
 *   migration.
 * - any other schema → key `{keyPrefix}entities/{entityName}/{uri}`
 *   with a JSON-encoded record string. Canonical `TYPE_TAGS` round-
 *   trip the JSON boundary (bytes → base64, bigint → string,
 *   timestamp → ISO-8601 — see `./fields.ts`).
 *
 * `ensureEntity` is idempotent and caches the per-entity field plan.
 * localStorage has no DDL; provisioning is client-side bookkeeping
 * only.
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import { decodeBase64, encodeBase64 } from "@bandeira-tech/b3nd-core";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import { dispatchRead } from "../dispatch.ts";
import { storageFailure } from "../errors.ts";
import { toBytes } from "../payload.ts";
import { applyReadParams } from "../read.ts";
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

const STORE_NAME = "LocalStorageStore";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  /** Full keyPrefix for entries of this entity, ending in `/`. */
  keyRoot: string;
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

export class LocalStorageStore implements EntityStore {
  private readonly keyPrefix: string;
  private readonly storage: Storage;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(config: {
    keyPrefix?: string;
    storage?: Storage;
  } = {}) {
    this.keyPrefix = config.keyPrefix || "b3nd:";
    this.storage = config.storage ||
      (typeof localStorage !== "undefined" ? localStorage : null!);

    if (!this.storage) {
      throw new Error("localStorage is not available in this environment");
    }
  }

  // ── EntityStore surface ──────────────────────────────────────────

  ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return Promise.resolve(cached.support);

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        keyRoot: this.keyPrefix,
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
      keyRoot: `${this.keyPrefix}entities/${schema.name}/`,
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
      : Promise.resolve(this._writeEntity(meta, entries));
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
        return this._lsImpl(meta, p, isBytesSchema(schema));
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
        this.storage.removeItem(`${meta.keyRoot}${uri}`);
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

  status(): Promise<StatusResult> {
    try {
      const testKey = `${this.keyPrefix}__health_check__`;
      this.storage.setItem(testKey, "ok");
      this.storage.removeItem(testKey);
      return Promise.resolve({
        status: "healthy",
        schema: [],
        fns: ["read", "ls", "count"],
      });
    } catch {
      return Promise.resolve({
        status: "unhealthy",
        schema: [],
        fns: ["read", "ls", "count"],
      });
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
        const bytes = await toBytes(payload);
        this.storage.setItem(`${meta.keyRoot}${uri}`, encodeBase64(bytes));
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

  private _readBytesOne(
    meta: EntityMeta,
    uri: string,
  ): EntityRecord | undefined {
    const serialized = this.storage.getItem(`${meta.keyRoot}${uri}`);
    if (serialized === null) return undefined;
    return { payload: decodeBase64(serialized) };
  }

  // ── Custom-entity layout ─────────────────────────────────────────

  private _writeEntity(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): StoreWriteResult[] {
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
        this.storage.setItem(`${meta.keyRoot}${uri}`, encoded);
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

  private _readEntityOne(
    meta: EntityMeta,
    uri: string,
  ): EntityRecord | undefined {
    const serialized = this.storage.getItem(`${meta.keyRoot}${uri}`);
    if (serialized === null) return undefined;
    return decodeRecord(meta.fields, JSON.parse(serialized));
  }

  // ── Shared ls/count ──────────────────────────────────────────────

  /** Direct-leaf URIs under `prefixUri` for the given entity layout. */
  private _directLeafUris(meta: EntityMeta, prefixUri: string): string[] {
    const prefixKey = `${meta.keyRoot}${prefixUri}`;
    const out: string[] = [];
    for (let i = 0; i < this.storage.length; i++) {
      const key = this.storage.key(i);
      if (!key || !key.startsWith(prefixKey)) continue;
      const rest = key.substring(prefixKey.length);
      if (rest === "" || rest.includes("/")) continue;
      out.push(`${prefixUri}${rest}`);
    }
    return out;
  }

  private _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    bytes: boolean,
  ): Output[] | string[] {
    const uris = this._directLeafUris(meta, parsed.uri);
    const rows: Output[] = uris.map((uri) => [
      uri,
      bytes ? this._readBytesOne(meta, uri) : this._readEntityOne(meta, uri),
    ]);
    return applyReadParams(rows, parsed.params, STORE_NAME);
  }

  private _count(meta: EntityMeta, parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return this._directLeafUris(meta, parsed.uri).length;
  }
}
