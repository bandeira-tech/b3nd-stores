/**
 * MemoryStore — in-memory reference implementation of `Store` and
 * `EntityStore`.
 *
 * Two faces, one storage:
 *
 * - `Store` (byte form): `write(entries)` / `read(urls)` /
 *   `delete(uris)` — what every byte-only backend in this package
 *   exposes. Used by the shared store suite and by `SaveClient` when
 *   it backs onto a byte-only store.
 * - `EntityStore` (entity form): `ensureEntity(schema)` and
 *   `write(schema, entries)` / `read(schema, urls)` /
 *   `delete(schema, uris)`. Schema is per-call, so one MemoryStore
 *   instance hosts many entities. `SaveClient` routes through this
 *   face when given a target schema (or its `BYTES_ENTITY` default).
 *
 * The byte form **redirects to the entity form with `BYTES_ENTITY`**
 * — there is one internal path, not two. Writes/reads that arrive
 * through `Store.write([{ uri, payload }])` are wrapped into
 * `{ uri, record: { payload } }` and processed by the same code as
 * `EntityStore.write(BYTES_ENTITY, ...)`. Both forms continue to
 * work; existing byte tests pass unchanged.
 *
 * Storage layout:
 *
 * - `BYTES_ENTITY` records live in the per-program tree used for raw
 *   bytes (the recursive `fn=ls`/`fn=count` behavior is preserved).
 * - Every other entity lives in a flat `Map<uri, EntityRecord>` per
 *   entity name. `fn=ls`/`fn=count` over a custom entity returns
 *   the URIs in that map whose URI starts with the prefix (also
 *   recursive — same convention as bytes on this backend).
 *
 * Validation is strict — the store does not coerce. A record under
 * a schema may only contain keys declared in `schema.fields`; extra
 * keys produce a `StoreWriteResult` failure for that entry. Wiring
 * mismatches surface as errors so the rig can be fixed.
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import { parseUrl } from "@bandeira-tech/b3nd-core/url";
import { storageFailure } from "../errors.ts";
import { toBytes } from "../payload.ts";
import type {
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "../types.ts";
import type { EntityStore } from "../entity-store.ts";
import {
  BYTES_ENTITY,
  type EntityRecord,
  type EntitySchema,
  type EntitySupport,
  TYPE_TAGS,
} from "../entity.ts";

const KNOWN_TAGS: ReadonlySet<string> = new Set(Object.values(TYPE_TAGS));

type StorageNode = {
  value?: Uint8Array;
  children?: Map<string, StorageNode>;
};

type Storage = Map<string, StorageNode>;

function resolveTarget(
  uri: string,
  storage: Storage,
): {
  program: string;
  path: string;
  node: StorageNode | undefined;
  parts: string[];
} {
  const url = URL.parse(uri)!;
  const program = `${url.protocol}//${url.hostname}`;
  const node = storage.get(program);
  const parts = url.pathname.substring(1).split("/");
  return { program, path: url.pathname, node, parts };
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

export class MemoryStore implements Store, EntityStore {
  private storage: Storage;
  /** entityName → uri → record, for entities other than `bytes`. */
  private readonly records = new Map<string, Map<string, EntityRecord>>();
  /** entityName → set of declared field names (validation cache). */
  private readonly schemas = new Map<string, ReadonlySet<string>>();

  constructor(storage?: Storage) {
    this.storage = storage || new Map();
  }

  // ── Entity provisioning ──────────────────────────────────────────

  // deno-lint-ignore require-await
  async ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const supported: string[] = [];
    const unsupported: { name: string; reason: string }[] = [];

    for (const field of schema.fields) {
      const recognised = field.type.filter((t) => KNOWN_TAGS.has(t));
      if (recognised.length === 0) {
        unsupported.push({
          name: field.name,
          reason: field.type.length === 0
            ? "field declares no type tags"
            : `no recognised tag in [${field.type.join(", ")}]`,
        });
      } else {
        supported.push(field.name);
      }
    }

    this.schemas.set(schema.name, new Set(supported));
    if (!isBytesSchema(schema) && !this.records.has(schema.name)) {
      this.records.set(schema.name, new Map());
    }

    return { entity: schema.name, supported, unsupported };
  }

  // ── Write (overloaded: byte form ⇄ entity form) ──────────────────

  write(entries: StoreEntry[]): Promise<StoreWriteResult[]>;
  write(
    schema: EntitySchema,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]>;
  async write(
    arg1: StoreEntry[] | EntitySchema,
    arg2?: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    if (Array.isArray(arg1)) {
      // Byte form — redirect through entity form with BYTES_ENTITY.
      // Walk serially (same shape as the legacy byte path) so the
      // observe/bridge pipeline isn't held back by extra microtasks.
      const records: { uri: string; record: EntityRecord }[] = [];
      for (const e of arg1) {
        records.push({
          uri: e.uri,
          record: { payload: await toBytes(e.payload) } as EntityRecord,
        });
      }
      return this._writeEntity(BYTES_ENTITY, records);
    }
    return this._writeEntity(arg1, arg2!);
  }

  private async _writeEntity(
    schema: EntitySchema,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    if (!this.schemas.has(schema.name)) await this.ensureEntity(schema);
    const declared = this.schemas.get(schema.name)!;
    const results: StoreWriteResult[] = [];

    for (const { uri, record } of entries) {
      const extras = Object.keys(record).filter((k) => !declared.has(k));
      if (extras.length > 0) {
        results.push({
          success: false,
          ...storageFailure(
            new Error(
              `record contains keys not declared in schema '${schema.name}': ${
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
        if (isBytesSchema(schema)) {
          const payload = record.payload;
          if (!(payload instanceof Uint8Array)) {
            throw new Error(
              `BYTES_ENTITY record.payload must be Uint8Array, got ${typeof payload}`,
            );
          }
          this._writeBytes(uri, payload);
        } else {
          this.records.get(schema.name)!.set(uri, { ...record });
        }
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          ...storageFailure(err, "Write failed", uri),
        });
      }
    }
    return results;
  }

  private _writeBytes(uri: string, payload: Uint8Array): void {
    const { program, parts, node: existing } = resolveTarget(uri, this.storage);
    let current = existing;
    if (!current) {
      current = { children: new Map() };
      this.storage.set(program, current);
    }
    for (const segment of parts.filter(Boolean)) {
      if (!current.children) current.children = new Map();
      if (!current.children.get(segment)) {
        const child: StorageNode = {};
        current.children.set(segment, child);
        current = child;
      } else {
        current = current.children.get(segment)!;
      }
    }
    current.value = payload;
  }

  // ── Read (overloaded: byte form ⇄ entity form) ───────────────────

  read<T = Uint8Array>(urls: string[]): Promise<Output<T>[]>;
  read<T = EntityRecord | undefined>(
    schema: EntitySchema,
    urls: string[],
  ): Promise<Output<T>[]>;
  // deno-lint-ignore require-await
  async read<T = unknown>(
    arg1: string[] | EntitySchema,
    arg2?: string[],
  ): Promise<Output<T>[]> {
    if (Array.isArray(arg1)) {
      return this._readEntity<T>(BYTES_ENTITY, arg1, true);
    }
    return this._readEntity<T>(arg1, arg2!, false);
  }

  private _readEntity<T>(
    schema: EntitySchema,
    urls: string[],
    bytesUnwrap: boolean,
  ): Output<T>[] {
    return urls.map((url) => {
      const parsed = parseUrl(url);
      switch (parsed.fn) {
        case "read":
          return [url, this._readOne(schema, parsed.uri, bytesUnwrap) as T];
        case "ls":
          return [url, this._list(schema, parsed, bytesUnwrap) as T];
        case "count":
          return [url, this._count(schema, parsed) as T];
        default:
          throw new Error(`MemoryStore: unsupported fn '${parsed.fn}'`);
      }
    });
  }

  private _readOne(
    schema: EntitySchema,
    uri: string,
    bytesUnwrap: boolean,
  ): unknown {
    if (isBytesSchema(schema)) {
      const bytes = this._readBytesAt(uri);
      if (bytes === undefined) return undefined;
      return bytesUnwrap ? bytes : { payload: bytes };
    }
    return this.records.get(schema.name)?.get(uri);
  }

  private _readBytesAt(uri: string): Uint8Array | undefined {
    const { parts, node } = resolveTarget(uri, this.storage);
    if (!node) return undefined;
    let current: StorageNode | undefined = node;
    for (const part of parts.filter(Boolean)) {
      current = current?.children?.get(part);
      if (!current) return undefined;
    }
    return current.value;
  }

  /**
   * Collect the **direct leaves** under the prefix node — entries
   * whose URI is `prefix + <segment>` with no further `/`. Matches
   * the shallow `fn=ls`/`fn=count` contract enforced across every
   * backend in this package; clients that want recursion call
   * `ls` per level.
   */
  private _walkBytes(uri: string): Output<Uint8Array>[] {
    const { node, parts, program, path } = resolveTarget(uri, this.storage);
    if (!node) return [];
    let current: StorageNode | undefined = node;
    for (const part of parts.filter(Boolean)) {
      current = current?.children?.get(part);
      if (!current) return [];
    }
    if (!current.children) return [];
    const prefix = path.endsWith("/")
      ? `${program}${path}`
      : `${program}${path}/`;
    const out: Output<Uint8Array>[] = [];
    for (const [key, child] of current.children) {
      if (child.value !== undefined) {
        out.push([`${prefix}${key}`, child.value]);
      }
    }
    return out;
  }

  private _walkEntity(
    schema: EntitySchema,
    uri: string,
  ): Output<EntityRecord>[] {
    const bucket = this.records.get(schema.name);
    if (!bucket) return [];
    // Direct-children only — same shape as the bytes walk above.
    const prefix = uri.endsWith("/") ? uri : `${uri}/`;
    const out: Output<EntityRecord>[] = [];
    for (const [k, rec] of bucket) {
      if (!k.startsWith(prefix)) continue;
      const rest = k.slice(prefix.length);
      if (rest.length === 0 || rest.includes("/")) continue;
      out.push([k, rec]);
    }
    return out;
  }

  private _list(
    schema: EntitySchema,
    parsed: ParsedUrl,
    bytesUnwrap: boolean,
  ): unknown {
    const { params } = parsed;
    if (params.pattern !== undefined) {
      throw new Error("MemoryStore: pattern filter not supported");
    }
    if (params.sortBy !== undefined && params.sortBy !== "uri") {
      throw new Error(`MemoryStore: unsupported sortBy: ${params.sortBy}`);
    }
    const format = params.format ?? "full";
    if (format !== "full" && format !== "uris") {
      throw new Error(`MemoryStore: unsupported format: ${format}`);
    }

    let entries: Output<unknown>[];
    if (isBytesSchema(schema)) {
      const raw = this._walkBytes(parsed.uri);
      entries = bytesUnwrap ? raw : raw.map(([u, b]) => [u, { payload: b }]);
    } else {
      entries = this._walkEntity(schema, parsed.uri);
    }

    if (params.sortBy === "uri") {
      const dir = params.sortOrder === "desc" ? -1 : 1;
      entries = [...entries].sort(([a], [b]) => a.localeCompare(b) * dir);
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      const start = (page - 1) * params.limit;
      entries = entries.slice(start, start + params.limit);
    }
    if (format === "uris") return entries.map(([uri]) => uri);
    return entries;
  }

  private _count(schema: EntitySchema, parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error("MemoryStore: pattern filter not supported");
    }
    if (isBytesSchema(schema)) return this._walkBytes(parsed.uri).length;
    return this._walkEntity(schema, parsed.uri).length;
  }

  // ── Delete (overloaded) ──────────────────────────────────────────

  delete(uris: string[]): Promise<DeleteResult[]>;
  delete(schema: EntitySchema, uris: string[]): Promise<DeleteResult[]>;
  delete(
    arg1: string[] | EntitySchema,
    arg2?: string[],
  ): Promise<DeleteResult[]> {
    if (Array.isArray(arg1)) return this._deleteEntity(BYTES_ENTITY, arg1);
    return this._deleteEntity(arg1, arg2!);
  }

  private _deleteEntity(
    schema: EntitySchema,
    uris: string[],
  ): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];
    for (const uri of uris) {
      try {
        if (isBytesSchema(schema)) this._deleteBytesAt(uri);
        else this.records.get(schema.name)?.delete(uri);
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          ...storageFailure(err, "Delete failed", uri),
        });
      }
    }
    return Promise.resolve(results);
  }

  private _deleteBytesAt(uri: string): void {
    const { node, parts } = resolveTarget(uri, this.storage);
    if (!node) return;
    const filteredParts = parts.filter(Boolean);
    let current: StorageNode | undefined = node;
    const ancestors: { node: StorageNode; key: string }[] = [];
    for (const part of filteredParts) {
      if (!current?.children?.has(part)) return;
      ancestors.push({ node: current, key: part });
      current = current.children.get(part)!;
    }
    delete current.value;
    for (let i = ancestors.length - 1; i >= 0; i--) {
      const { node: parent, key } = ancestors[i];
      const child = parent.children!.get(key)!;
      if (!child.value && (!child.children || child.children.size === 0)) {
        parent.children!.delete(key);
      } else {
        break;
      }
    }
  }

  // ── Status ───────────────────────────────────────────────────────

  status(): Promise<StatusResult> {
    return Promise.resolve({
      status: "healthy",
      schema: [
        ...this.storage.keys(),
        ...[...this.records.keys()].map((n) => `entity:${n}`),
      ],
      fns: ["read", "ls", "count"],
    });
  }

  capabilities(): StoreCapabilities {
    return { atomicBatch: false };
  }
}
