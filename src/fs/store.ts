/**
 * FsStore — Filesystem implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → `{rootDir}/{uriToRelPath(uri)}.bin`, raw bytes
 *   body (current behaviour). One file per entry.
 * - any other schema → `{rootDir}/entities/{entityName}/{uriToRelPath(uri)}.json`,
 *   body = JSON-encoded record. Canonical `TYPE_TAGS` ↔ JSON encoding
 *   (bytes → base64, bigint → string, timestamp → ISO-8601) lives in
 *   `./fields.ts`.
 *
 * `ensureEntity` is idempotent and caches the field plan. The
 * filesystem has no DDL — provisioning is purely client-side
 * bookkeeping; the entity subdirectory is created lazily on first
 * write by the executor's `writeFile`.
 *
 * `fn=ls` / `fn=count` are shallow direct-leaves only: they list the
 * `.bin`/`.json` files directly inside the prefix's mapped directory,
 * never recursing into subdirectories.
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
import type { FsExecutor } from "./mod.ts";

const STORE_NAME = "FsStore";
const BYTES_EXT = ".bin";
const ENTITY_EXT = ".json";
const ENTITY_ROOT = "entities";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  /** Absolute path of the entity's root directory under `rootDir`. */
  dir: string;
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

function uriToRelPath(uri: string, ext: string): string {
  return uri.replace("://", "_") + ext;
}

function relPathToUri(relPath: string, ext: string): string {
  const withoutExt = relPath.endsWith(ext)
    ? relPath.slice(0, -ext.length)
    : relPath;
  return withoutExt.replace("_", "://");
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

async function streamToString(
  stream: ReadableStream<Uint8Array>,
): Promise<string> {
  return await new Response(stream as BodyInit).text();
}

export class FsStore implements EntityStore {
  private readonly rootDir: string;
  private readonly executor: FsExecutor;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(rootDir: string, executor: FsExecutor) {
    if (!rootDir) throw new Error("rootDir is required");
    if (!executor) throw new Error("executor is required");

    this.rootDir = rootDir.replace(/\/+$/, "");
    this.executor = executor;
  }

  // ── EntityStore surface ──────────────────────────────────────────

  ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return Promise.resolve(cached.support);

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        dir: this.rootDir,
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
      dir: `${this.rootDir}/${ENTITY_ROOT}/${schema.name}`,
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
        return this._count(meta, p, isBytesSchema(schema));
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
        await this.executor.removeFile(
          this._path(meta, uri, isBytesSchema(schema)),
        );
        results.push({ success: true });
      } catch {
        // File may not exist — succeed silently (miss-is-not-an-error).
        results.push({ success: true });
      }
    }
    return results;
  }

  async status(): Promise<StatusResult> {
    try {
      const rootExists = await this.executor.exists(this.rootDir);
      if (!rootExists) {
        return {
          status: "unhealthy",
          message: `Root directory not found: ${this.rootDir}`,
          fns: ["read", "ls", "count"],
        };
      }
      return {
        status: "healthy",
        message: "Filesystem store is operational",
        fns: ["read", "ls", "count"],
        details: { rootDir: this.rootDir },
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

  private _path(meta: EntityMeta, uri: string, bytes: boolean): string {
    const ext = bytes ? BYTES_EXT : ENTITY_EXT;
    return `${meta.dir}/${uriToRelPath(uri, ext)}`;
  }

  private _dirForPrefix(
    meta: EntityMeta,
    prefixUri: string,
    bytes: boolean,
  ): string {
    const ext = bytes ? BYTES_EXT : ENTITY_EXT;
    const rel = uriToRelPath(prefixUri, ext)
      .slice(0, -ext.length)
      .replace(/\/+$/, "");
    return rel ? `${meta.dir}/${rel}` : meta.dir;
  }

  private async _listChildUris(
    meta: EntityMeta,
    prefixUri: string,
    bytes: boolean,
  ): Promise<string[]> {
    let files: string[];
    try {
      files = await this.executor.listFiles(
        this._dirForPrefix(meta, prefixUri, bytes),
      );
    } catch {
      return [];
    }
    const ext = bytes ? BYTES_EXT : ENTITY_EXT;
    const relDir = uriToRelPath(prefixUri, ext)
      .slice(0, -ext.length)
      .replace(/\/+$/, "");
    return files
      .filter((f) => f.endsWith(ext) && !f.includes("/"))
      .map((f) => relPathToUri(`${relDir}/${f}`, ext));
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
        await this.executor.writeFile(this._path(meta, uri, true), payload);
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
    try {
      const content = await this.executor.readFile(this._path(meta, uri, true));
      return { payload: content };
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
    for (const { uri, record } of entries) {
      const extras = Object.keys(record).filter((k) => !meta.declared.has(k));
      if (extras.length > 0) {
        out.push({
          success: false,
          ...storageFailure(
            new Error(
              `${STORE_NAME}: record contains keys not declared in schema '${meta.dir}': ${
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
        await this.executor.writeFile(
          this._path(meta, uri, false),
          new TextEncoder().encode(encoded),
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
    try {
      const stream = await this.executor.readFile(this._path(meta, uri, false));
      const text = await streamToString(stream);
      const json = JSON.parse(text) as Record<string, unknown>;
      return decodeRecord(meta.fields, json);
    } catch {
      return undefined;
    }
  }

  // ── Shared ls/count ──────────────────────────────────────────────

  private async _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    bytes: boolean,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    let uris = await this._listChildUris(meta, parsed.uri, bytes);

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
      const r = bytes
        ? await this._readBytesOne(meta, uri)
        : await this._readEntityOne(meta, uri);
      out.push([uri, r]);
    }
    return out;
  }

  private async _count(
    meta: EntityMeta,
    parsed: ParsedUrl,
    bytes: boolean,
  ): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return (await this._listChildUris(meta, parsed.uri, bytes)).length;
  }
}
