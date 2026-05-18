/**
 * IndexedDBStore — browser IndexedDB implementation of EntityStore.
 *
 * Two layouts share one object store, separated by storage-key
 * prefix. IDB's structured clone preserves typed values natively
 * (Uint8Array, Date, BigInt, plain JSON), so records are stored
 * verbatim — no JSON encoding step on the way in or out.
 *
 * - `BYTES_ENTITY` → record `{ uri, payload }` keyed by `uri`
 *   (current behaviour). Existing deployments keep working without
 *   migration.
 * - any other schema → record `{ uri, record }` keyed by
 *   `__entities__/{entityName}/{originalUri}`. The `__entities__/`
 *   prefix isolates entity slots from byte slots in the cursor's
 *   ordered key space.
 *
 * `ensureEntity` is idempotent and caches the per-entity field plan.
 * IDB needs an `onupgradeneeded` to create object stores, but since
 * we use a single store for everything, schema migrations are not
 * required to grow new entities.
 *
 * `fn=ls` / `fn=count` are shallow direct-leaves only: cursor over
 * the `uri_index` bounded by `[storageKey, storageKey + ￿)`,
 * keep records whose remainder under the prefix has no further `/`.
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

const STORE_NAME = "IndexedDBStore";
const ENTITY_PREFIX = "__entities__/";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  /** Storage-key root for this entity's slots (e.g. `__entities__/users/`). */
  storageRoot: string;
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

interface StoredBytes {
  uri: string;
  payload: Uint8Array;
}

interface StoredEntity {
  uri: string;
  record: EntityRecord;
}

// Minimal IndexedDB type definitions for cross-platform compatibility
interface IDBDatabase {
  name: string;
  version: number;
  objectStoreNames: { contains(name: string): boolean };
  close(): void;
  transaction(
    storeNames: string | string[],
    mode?: "readonly" | "readwrite",
  ): IDBTransaction;
  // deno-lint-ignore no-explicit-any
  createObjectStore(name: string, options?: any): IDBObjectStore;
}

interface IDBTransaction {
  objectStore(name: string): IDBObjectStore;
}

interface IDBObjectStore {
  // deno-lint-ignore no-explicit-any
  get(key: any): IDBRequest;
  // deno-lint-ignore no-explicit-any
  put(value: any): IDBRequest;
  // deno-lint-ignore no-explicit-any
  delete(key: any): IDBRequest;
  index(name: string): IDBIndex;
  // deno-lint-ignore no-explicit-any
  createIndex(name: string, keyPath: string): any;
}

interface IDBIndex {
  // deno-lint-ignore no-explicit-any
  openCursor(range?: any, direction?: "next" | "prev"): IDBRequest;
  // deno-lint-ignore no-explicit-any
  openKeyCursor(range?: any, direction?: "next" | "prev"): IDBRequest;
}

interface IDBRequest {
  // deno-lint-ignore no-explicit-any
  result: any;
  error: Error | null;
  // deno-lint-ignore no-explicit-any
  onsuccess: ((this: IDBRequest, ev: any) => void) | null;
  // deno-lint-ignore no-explicit-any
  onerror: ((this: IDBRequest, ev: any) => void) | null;
}

interface IDBOpenDBRequest extends IDBRequest {
  // deno-lint-ignore no-explicit-any
  onupgradeneeded: ((this: IDBOpenDBRequest, ev: any) => void) | null;
}

interface IDBKeyRange {
  // deno-lint-ignore no-explicit-any
  bound(lower: any, upper: any, lowerOpen?: boolean, upperOpen?: boolean): any;
}

interface IDBFactory {
  open(name: string, version?: number): IDBOpenDBRequest;
}

// deno-lint-ignore no-explicit-any
const idbKeyRange: IDBKeyRange | undefined = (globalThis as any).IDBKeyRange;

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

export class IndexedDBStore implements EntityStore {
  private readonly databaseName: string;
  private readonly storeName: string;
  private readonly version: number;
  private readonly indexedDB: IDBFactory;
  private readonly keyRange: IDBKeyRange | undefined;
  private db: IDBDatabase | null = null;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(config: {
    databaseName?: string;
    storeName?: string;
    version?: number;
    // deno-lint-ignore no-explicit-any
    indexedDB?: any;
    // deno-lint-ignore no-explicit-any
    IDBKeyRange?: any;
  } = {}) {
    this.databaseName = config.databaseName || "b3nd";
    this.storeName = config.storeName || "records";
    this.version = config.version || 1;
    this.indexedDB = config.indexedDB ||
      // deno-lint-ignore no-explicit-any
      ((globalThis as any).indexedDB ?? null);
    this.keyRange = config.IDBKeyRange ?? idbKeyRange;

    if (!this.indexedDB) {
      throw new Error("IndexedDB is not available in this environment");
    }
  }

  private initDB(): Promise<IDBDatabase> {
    if (this.db) return Promise.resolve(this.db);

    return new Promise<IDBDatabase>((resolve, reject) => {
      const request = this.indexedDB.open(this.databaseName, this.version);
      request.onerror = () =>
        reject(new Error(`Failed to open IndexedDB: ${request.error}`));
      request.onsuccess = () => {
        this.db = request.result;
        resolve(request.result);
      };
      request.onupgradeneeded = () => {
        const db = request.result as IDBDatabase;
        if (!db.objectStoreNames.contains(this.storeName)) {
          const store = db.createObjectStore(this.storeName, {
            keyPath: "uri",
          });
          store.createIndex("uri_index", "uri");
        }
      };
    });
  }

  private async getStore(
    mode: "readonly" | "readwrite" = "readonly",
  ): Promise<IDBObjectStore> {
    const db = await this.initDB();
    return db.transaction([this.storeName], mode).objectStore(this.storeName);
  }

  // ── EntityStore surface ──────────────────────────────────────────

  ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return Promise.resolve(cached.support);

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        storageRoot: "",
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
      storageRoot: `${ENTITY_PREFIX}${schema.name}/`,
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
    try {
      const store = await this.getStore("readwrite");
      for (const uri of uris) {
        try {
          const storageKey = `${meta.storageRoot}${uri}`;
          await new Promise<void>((resolve, reject) => {
            const request = store.delete(storageKey);
            request.onsuccess = () => resolve();
            request.onerror = () =>
              reject(new Error(`Failed to delete ${uri}: ${request.error}`));
          });
          results.push({ success: true });
        } catch (err) {
          results.push({
            success: false,
            ...storageFailure(err, "Delete failed", uri),
          });
        }
      }
    } catch (err) {
      const failure = storageFailure(err, "Delete failed");
      for (const _ of uris) results.push({ success: false, ...failure });
    }
    return results;
  }

  async status(): Promise<StatusResult> {
    try {
      await this.initDB();
      return {
        status: "healthy",
        schema: [],
        fns: ["read", "ls", "count"],
      };
    } catch {
      return {
        status: "unhealthy",
        schema: [],
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

  // ── BYTES_ENTITY layout ──────────────────────────────────────────

  private async _writeBytes(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    const out: StoreWriteResult[] = [];

    // Validate + collect streams BEFORE opening the IDB transaction.
    // An IDB transaction stays alive only across IDB-callback
    // microtasks; awaiting a non-IDB promise yields long enough for
    // the transaction to auto-commit.
    const prepared: { idx: number; record: StoredBytes }[] = [];
    for (let i = 0; i < entries.length; i++) {
      const { uri, record } = entries[i];
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        out[i] = {
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
        };
        continue;
      }
      const payload = record.payload;
      if (
        !(payload instanceof Uint8Array) &&
        !(payload instanceof ReadableStream)
      ) {
        out[i] = {
          success: false,
          ...storageFailure(
            new Error(
              `${STORE_NAME}: BYTES_ENTITY record.payload must be Uint8Array or ReadableStream`,
            ),
            "Invalid record",
            uri,
          ),
        };
        continue;
      }
      try {
        prepared.push({
          idx: i,
          record: { uri, payload: await toBytes(payload) },
        });
      } catch (err) {
        out[i] = {
          success: false,
          ...storageFailure(err, "Write failed", uri),
        };
      }
    }

    if (prepared.length === 0) return out;

    try {
      const store = await this.getStore("readwrite");
      for (const { idx, record } of prepared) {
        try {
          await new Promise<void>((resolve, reject) => {
            const request = store.put(record);
            request.onsuccess = () => resolve();
            request.onerror = () =>
              reject(
                new Error(`Failed to write ${record.uri}: ${request.error}`),
              );
          });
          out[idx] = { success: true };
        } catch (err) {
          out[idx] = {
            success: false,
            ...storageFailure(err, "Write failed", record.uri),
          };
        }
      }
    } catch (err) {
      const failure = storageFailure(err, "Write failed");
      for (const { idx } of prepared) {
        out[idx] = { success: false, ...failure };
      }
    }
    return out;
  }

  private async _readBytesOne(
    _meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    try {
      const store = await this.getStore();
      return await new Promise<EntityRecord | undefined>((resolve) => {
        const request = store.get(uri);
        request.onsuccess = () => {
          const record = request.result as StoredBytes | undefined;
          resolve(record ? { payload: record.payload } : undefined);
        };
        request.onerror = () => resolve(undefined);
      });
    } catch {
      return undefined;
    }
  }

  // ── Custom-entity layout ─────────────────────────────────────────

  private async _writeEntity(
    meta: EntityMeta,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    const out: StoreWriteResult[] = [];
    const accepted: { idx: number; storedKey: string; record: EntityRecord }[] =
      [];

    for (let i = 0; i < entries.length; i++) {
      const { uri, record } = entries[i];
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        out[i] = {
          success: false,
          ...storageFailure(
            new Error(
              `${STORE_NAME}: record contains keys not declared in schema '${meta.storageRoot}': ${
                extras.join(", ")
              }`,
            ),
            "Schema mismatch",
            uri,
          ),
        };
        continue;
      }
      accepted.push({
        idx: i,
        storedKey: `${meta.storageRoot}${uri}`,
        record,
      });
    }
    if (accepted.length === 0) return out;

    try {
      const store = await this.getStore("readwrite");
      for (const { idx, storedKey, record } of accepted) {
        try {
          // Structured clone preserves Uint8Array / Date / BigInt natively,
          // so we hand the record over verbatim. Strip the original URI
          // (we use the prefixed `storedKey` as the keyPath value).
          const doc: StoredEntity = { uri: storedKey, record };
          await new Promise<void>((resolve, reject) => {
            const request = store.put(doc);
            request.onsuccess = () => resolve();
            request.onerror = () =>
              reject(
                new Error(`Failed to write ${storedKey}: ${request.error}`),
              );
          });
          out[idx] = { success: true };
        } catch (err) {
          out[idx] = {
            success: false,
            ...storageFailure(err, "Write failed", entries[idx].uri),
          };
        }
      }
    } catch (err) {
      const failure = storageFailure(err, "Write failed");
      for (const { idx } of accepted) out[idx] = { success: false, ...failure };
    }
    return out;
  }

  private async _readEntityOne(
    meta: EntityMeta,
    uri: string,
  ): Promise<EntityRecord | undefined> {
    try {
      const store = await this.getStore();
      const storageKey = `${meta.storageRoot}${uri}`;
      return await new Promise<EntityRecord | undefined>((resolve) => {
        const request = store.get(storageKey);
        request.onsuccess = () => {
          const doc = request.result as StoredEntity | undefined;
          resolve(doc?.record);
        };
        request.onerror = () => resolve(undefined);
      });
    } catch {
      return undefined;
    }
  }

  // ── Shared ls/count via cursor over prefix range ─────────────────

  /**
   * Cursor over `uri_index` constrained to the entity's storage-key
   * range. Returns shallow direct-leaves (no further `/` in the
   * remainder after the URI prefix). Honours sortOrder, limit, page.
   */
  private async _walkLeaves(
    meta: EntityMeta,
    parsed: ParsedUrl,
    onlyKeys: boolean,
  ): Promise<Array<{ uri: string; doc?: StoredBytes | StoredEntity }>> {
    const { uri: prefix, params } = parsed;
    const desc = params.sortBy === "uri" && params.sortOrder === "desc";
    const direction = desc ? "prev" : "next";
    const limit = params.limit;
    const offset = limit !== undefined ? ((params.page ?? 1) - 1) * limit : 0;

    const lower = `${meta.storageRoot}${prefix}`;
    const upper = `${lower}￿`;
    const range = this.keyRange
      ? this.keyRange.bound(lower, upper, false, false)
      : undefined;

    const store = await this.getStore();
    const index = store.index("uri_index");

    return await new Promise<
      Array<{ uri: string; doc?: StoredBytes | StoredEntity }>
    >((resolve, reject) => {
      const out: Array<{ uri: string; doc?: StoredBytes | StoredEntity }> = [];
      let skipped = 0;
      const request = onlyKeys
        ? index.openKeyCursor(range, direction)
        : index.openCursor(range, direction);

      request.onsuccess = () => {
        const cursor = request.result;
        if (!cursor) {
          resolve(out);
          return;
        }
        const storedKey = (onlyKeys ? cursor.key : cursor.value.uri) as string;
        if (!storedKey.startsWith(lower)) {
          resolve(out);
          return;
        }
        const tail = storedKey.slice(lower.length);
        if (tail === "" || tail.includes("/")) {
          cursor.continue();
          return;
        }
        if (skipped < offset) {
          skipped++;
          cursor.continue();
          return;
        }
        const originalUri = `${prefix}${tail}`;
        out.push(
          onlyKeys
            ? { uri: originalUri }
            : { uri: originalUri, doc: cursor.value },
        );
        if (limit !== undefined && out.length >= limit) {
          resolve(out);
          return;
        }
        cursor.continue();
      };
      request.onerror = () =>
        reject(request.error ?? new Error("Cursor failed"));
    });
  }

  private async _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    bytes: boolean,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const format = parsed.params.format ?? "full";
    const onlyKeys = format === "uris";

    try {
      const entries = await this._walkLeaves(meta, parsed, onlyKeys);
      if (onlyKeys) return entries.map((e) => e.uri);
      return entries.map((e): Output => {
        if (!e.doc) return [e.uri, undefined];
        if (bytes) {
          return [e.uri, { payload: (e.doc as StoredBytes).payload }];
        }
        return [e.uri, (e.doc as StoredEntity).record];
      });
    } catch {
      return [];
    }
  }

  private async _count(meta: EntityMeta, parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    try {
      const entries = await this._walkLeaves(meta, parsed, true);
      return entries.length;
    } catch {
      return 0;
    }
  }
}
