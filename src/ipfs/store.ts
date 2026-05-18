/**
 * IpfsStore — IPFS implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → existing per-URI in-memory index; each entry's
 *   content is the raw bytes pinned to a CID (current behaviour).
 * - any other schema → a separate per-entity `uri → CID` index. The
 *   pinned content is a JSON-encoded record using the canonical
 *   `TYPE_TAGS` round-trip (bytes → base64, bigint → string,
 *   timestamp → ISO-8601 — see `./fields.ts`).
 *
 * `ensureEntity` is idempotent and caches the per-entity field plan
 * + index. IPFS has no DDL; provisioning is purely client-side
 * bookkeeping (a new index Map appears the first time a schema is
 * ensured).
 *
 * `fn=ls` / `fn=count` are shallow direct-leaves only: scan the
 * relevant entity's index, keep URIs whose remainder under the
 * prefix has no further `/`.
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
import type { IpfsExecutor } from "./mod.ts";

const STORE_NAME = "IpfsStore";

const NAME_PATTERN = /^[a-zA-Z][a-zA-Z0-9_]*$/;

interface EntityMeta {
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  /** uri → CID (in-memory; IPFS has no native key-value index). */
  index: Map<string, string>;
  support: EntitySupport;
}

function isBytesSchema(schema: EntitySchema): boolean {
  return schema.name === BYTES_ENTITY.name;
}

async function streamToString(
  stream: ReadableStream<Uint8Array>,
): Promise<string> {
  return await new Response(stream as BodyInit).text();
}

export class IpfsStore implements EntityStore {
  private readonly executor: IpfsExecutor;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(executor: IpfsExecutor) {
    if (!executor) throw new Error("executor is required");
    this.executor = executor;
  }

  // ── EntityStore surface ──────────────────────────────────────────

  ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return Promise.resolve(cached.support);

    if (isBytesSchema(schema)) {
      const meta: EntityMeta = {
        fields: [{ name: "payload", tag: "bytes" }],
        declared: new Set(["payload"]),
        index: new Map(),
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
      fields,
      declared: new Set(fields.map((f) => f.name)),
      index: new Map(),
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
    for (const uri of uris) {
      try {
        const cid = meta.index.get(uri);
        if (cid) {
          try {
            await this.executor.unpin(cid);
          } catch {
            // CID may already be unpinned — ignore.
          }
          meta.index.delete(uri);
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
      const online = await this.executor.isOnline();
      if (!online) {
        return {
          status: "unhealthy",
          message: "IPFS node is not reachable",
          fns: ["read", "ls", "count"],
        };
      }
      // Aggregate index sizes across every ensured entity.
      const indexedUris = [...this.entities.values()]
        .reduce((n, m) => n + m.index.size, 0);
      const programs = new Set<string>();
      for (const meta of this.entities.values()) {
        for (const uri of meta.index.keys()) {
          try {
            const url = new URL(uri);
            programs.add(`${url.protocol}//${url.hostname}`);
          } catch {
            // skip malformed URIs
          }
        }
      }
      return {
        status: "healthy",
        schema: [...programs],
        fns: ["read", "ls", "count"],
        details: { indexedUris },
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
        const cid = await this.executor.add(payload);
        await this.executor.pin(cid);
        const existing = meta.index.get(uri);
        if (existing) {
          try {
            await this.executor.unpin(existing);
          } catch {
            // ignore
          }
        }
        meta.index.set(uri, cid);
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
    const cid = meta.index.get(uri);
    if (!cid) return undefined;
    try {
      const content = await this.executor.cat(cid);
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
              `${STORE_NAME}: record contains keys not declared in schema: ${
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
        const cid = await this.executor.add(new TextEncoder().encode(encoded));
        await this.executor.pin(cid);
        const existing = meta.index.get(uri);
        if (existing) {
          try {
            await this.executor.unpin(existing);
          } catch {
            // ignore
          }
        }
        meta.index.set(uri, cid);
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
    const cid = meta.index.get(uri);
    if (!cid) return undefined;
    try {
      const stream = await this.executor.cat(cid);
      const text = await streamToString(stream);
      const json = JSON.parse(text) as Record<string, unknown>;
      return decodeRecord(meta.fields, json);
    } catch {
      return undefined;
    }
  }

  // ── Shared ls/count ──────────────────────────────────────────────

  /** Shallow direct-leaves from the in-memory URI index for an entity. */
  private _directLeafUris(meta: EntityMeta, prefixUri: string): string[] {
    const out: string[] = [];
    for (const uri of meta.index.keys()) {
      if (!uri.startsWith(prefixUri)) continue;
      const tail = uri.slice(prefixUri.length);
      if (tail === "" || tail.includes("/")) continue;
      out.push(uri);
    }
    return out;
  }

  private async _lsImpl(
    meta: EntityMeta,
    parsed: ParsedUrl,
    bytes: boolean,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    let uris = this._directLeafUris(meta, parsed.uri);

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

  private _count(meta: EntityMeta, parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return this._directLeafUris(meta, parsed.uri).length;
  }
}
