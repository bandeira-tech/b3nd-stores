/**
 * ElasticsearchStore — Elasticsearch implementation of EntityStore.
 *
 * Two layouts under one backend, picked by `EntitySchema`:
 *
 * - `BYTES_ENTITY` → URIs are partitioned into one index per
 *   `protocol_hostname` pair (`{prefix}_{protocol}_{hostname}`), with
 *   the path as `_id`. Payload is base64 in the `payload` field; the
 *   `path` mirror lets `ls`/`count` run `regexp` queries against
 *   `path.keyword`. Legacy behaviour — keeps existing deployments
 *   working without reindex.
 * - any other schema → a single index per entity
 *   (`{prefix}_{entity}_data`) with explicit mappings derived from
 *   `TYPE_TAGS` (see `./mappings.ts`). One doc per URI; the URI is
 *   both `_id` and a `uri` keyword field for prefix regex queries.
 *
 * `ensureEntity` is idempotent and caches per-entity metadata.
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
import { validateReadParams } from "../read.ts";
import type { EntityStore } from "../entity-store.ts";
import {
  BYTES_ENTITY,
  type EntityRecord,
  type EntitySchema,
  type EntitySupport,
} from "../entity.ts";
import type { StoreCapabilities, StoreWriteResult } from "../types.ts";
import { buildMappings, type FieldPlan, planFields } from "./mappings.ts";
import type { ElasticsearchExecutor } from "./mod.ts";

const STORE_NAME = "ElasticsearchStore";

const NAME_PATTERN = /^[a-z][a-z0-9_]*$/;

interface EntityMeta {
  index: string;
  fields: FieldPlan[];
  declared: ReadonlySet<string>;
  support: EntitySupport;
}

/** Escape characters that are special in Lucene regex syntax. */
function escapeLuceneRegex(input: string): string {
  return input.replace(/[.?+*|{}\[\]()"\\#@&<>~]/g, "\\$&");
}

/**
 * Parse a URI for the BYTES_ENTITY (per-program) layout.
 * `protocol://hostname/path` → index: `prefix_protocol_hostname`,
 * docId: `path` (without leading slash).
 */
function uriToBytesTarget(
  uri: string,
  indexPrefix: string,
): { index: string; docId: string } {
  const url = new URL(uri);
  const protocol = url.protocol.replace(":", "");
  const hostname = url.hostname;
  return {
    index: `${indexPrefix}_${protocol}_${hostname}`,
    docId: url.pathname.substring(1),
  };
}

function bytesIndexAndDocIdToUri(
  index: string,
  indexPrefix: string,
  docId: string,
): string {
  const withoutPrefix = index.substring(indexPrefix.length + 1);
  const firstUnderscore = withoutPrefix.indexOf("_");
  const protocol = withoutPrefix.substring(0, firstUnderscore);
  const hostname = withoutPrefix.substring(firstUnderscore + 1);
  return `${protocol}://${hostname}/${docId}`;
}

function entityIndex(prefix: string, entityName: string): string {
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

export class ElasticsearchStore implements EntityStore {
  private readonly indexPrefix: string;
  private readonly executor: ElasticsearchExecutor;
  private readonly entities = new Map<string, EntityMeta>();

  constructor(indexPrefix: string, executor: ElasticsearchExecutor) {
    if (!indexPrefix) throw new Error("indexPrefix is required");
    if (!NAME_PATTERN.test(indexPrefix)) {
      throw new Error(
        `indexPrefix must match ${NAME_PATTERN.source}; got '${indexPrefix}'`,
      );
    }
    if (!executor) throw new Error("executor is required");

    this.indexPrefix = indexPrefix;
    this.executor = executor;
  }

  // ── EntityStore surface ──────────────────────────────────────────

  async ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const cached = this.entities.get(schema.name);
    if (cached) return cached.support;

    if (isBytesSchema(schema)) {
      // Per-program indices are created lazily by ES on first index op
      // (existing behaviour). No DDL up front.
      const meta: EntityMeta = {
        index: "<per-program>",
        fields: [{ name: "payload", esType: "binary", tag: "bytes" }],
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

    const { fields, unsupported } = planFields(schema.fields);
    const index = entityIndex(this.indexPrefix, schema.name);
    await this.executor.ensureIndex(index, buildMappings(fields));
    const meta: EntityMeta = {
      index,
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
          const { index, docId } = uriToBytesTarget(uri, this.indexPrefix);
          await this.executor.delete(index, docId);
        } else {
          await this.executor.delete(meta.index, uri);
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
      const ok = await this.executor.ping();
      if (!ok) {
        return {
          status: "unhealthy",
          message: "Elasticsearch cluster is not reachable",
          fns: ["read", "ls", "count"],
        };
      }
      return {
        status: "healthy",
        message: "Elasticsearch store is operational",
        fns: ["read", "ls", "count"],
        details: { indexPrefix: this.indexPrefix },
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

  // ── BYTES_ENTITY layout (legacy per-program indices) ─────────────

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
        const { index, docId } = uriToBytesTarget(uri, this.indexPrefix);
        const bytes = await toBytes(payload);
        // Mirror docId into a `path` source field so ls/count can run
        // analyzed queries against it. ES dynamic mapping for a string
        // source field produces `path.keyword` automatically.
        await this.executor.index(index, docId, {
          payload: encodeBase64(bytes),
          path: docId,
        });
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

  private async _readBytesOne(uri: string): Promise<EntityRecord | undefined> {
    const { index, docId } = uriToBytesTarget(uri, this.indexPrefix);
    const doc = await this.executor.get(index, docId);
    if (!doc) return undefined;
    return { payload: decodeBase64(doc.payload as string) };
  }

  private _bytesLeafQuery(docPrefix: string): Record<string, unknown> {
    return {
      regexp: {
        "path.keyword": `${escapeLuceneRegex(docPrefix)}[^/]+`,
      },
    };
  }

  private async _lsBytes(parsed: ParsedUrl): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";
    const { index, docId } = uriToBytesTarget(parsed.uri, this.indexPrefix);

    const body: Record<string, unknown> = {
      query: this._bytesLeafQuery(docId),
    };
    if (params.sortBy === "uri") {
      body.sort = [{
        "path.keyword": params.sortOrder === "desc" ? "desc" : "asc",
      }];
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      body.size = params.limit;
      body.from = (page - 1) * params.limit;
    } else {
      body.size = 10_000;
    }
    if (format === "uris") body._source = false;

    const result = await this.executor.search(index, body);
    if (format === "uris") {
      return result.hits.map((hit) =>
        bytesIndexAndDocIdToUri(index, this.indexPrefix, hit._id)
      );
    }
    return result.hits.map((hit): Output => [
      bytesIndexAndDocIdToUri(index, this.indexPrefix, hit._id),
      hit._source
        ? { payload: decodeBase64(hit._source.payload as string) }
        : undefined,
    ]);
  }

  private async _countBytes(parsed: ParsedUrl): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    const { index, docId } = uriToBytesTarget(parsed.uri, this.indexPrefix);
    return await this.executor.count(index, {
      query: this._bytesLeafQuery(docId),
    });
  }

  // ── Custom-entity layout (one index per entity) ──────────────────

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
              `${STORE_NAME}: record contains keys not declared in schema '${meta.index}': ${
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
        const doc: Record<string, unknown> = { uri, updatedAt: new Date() };
        for (const f of meta.fields) {
          doc[f.name] = adaptForWrite(f, record[f.name]);
        }
        await this.executor.index(meta.index, uri, doc);
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
    const doc = await this.executor.get(meta.index, uri);
    if (!doc) return undefined;
    return adaptDocForRead(meta, doc);
  }

  private _entityLeafQuery(prefixUri: string): Record<string, unknown> {
    return {
      regexp: {
        "uri": `${escapeLuceneRegex(prefixUri)}[^/]+`,
      },
    };
  }

  private async _lsEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<Output[] | string[]> {
    validateReadParams(parsed.params, STORE_NAME);
    const { params } = parsed;
    const format = params.format ?? "full";

    const body: Record<string, unknown> = {
      query: this._entityLeafQuery(parsed.uri),
    };
    if (params.sortBy === "uri") {
      body.sort = [{ uri: params.sortOrder === "desc" ? "desc" : "asc" }];
    }
    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      body.size = params.limit;
      body.from = (page - 1) * params.limit;
    } else {
      body.size = 10_000;
    }
    if (format === "uris") body._source = ["uri"];

    const result = await this.executor.search(meta.index, body);
    if (format === "uris") {
      return result.hits.map((hit) => (hit._source?.uri as string) ?? hit._id);
    }
    return result.hits.map((hit): Output => [
      (hit._source?.uri as string) ?? hit._id,
      hit._source ? adaptDocForRead(meta, hit._source) : undefined,
    ]);
  }

  private async _countEntity(
    meta: EntityMeta,
    parsed: ParsedUrl,
  ): Promise<number> {
    if (parsed.params.pattern !== undefined) {
      throw new Error(`${STORE_NAME}: pattern filter not supported`);
    }
    return await this.executor.count(meta.index, {
      query: this._entityLeafQuery(parsed.uri),
    });
  }
}

/** Coerce a record field value into something ES will accept on index. */
function adaptForWrite(field: FieldPlan, value: unknown): unknown {
  if (value === undefined) return null;
  if (field.tag === "bytes") {
    if (value instanceof Uint8Array) return encodeBase64(value);
    return value;
  }
  if (field.tag === "timestamp") {
    if (value instanceof Date) return value.toISOString();
    if (typeof value === "number") return new Date(value).toISOString();
    return value;
  }
  // string / number / bigint / boolean / json — pass through.
  return value;
}

/** Reconstruct an EntityRecord from an ES `_source` document. */
function adaptDocForRead(
  meta: EntityMeta,
  src: Record<string, unknown>,
): EntityRecord {
  const rec: EntityRecord = {};
  for (const f of meta.fields) {
    const v = src[f.name];
    if (v === null || v === undefined) {
      rec[f.name] = undefined;
      continue;
    }
    if (f.tag === "bytes") {
      rec[f.name] = decodeBase64(v as string);
    } else if (f.tag === "timestamp") {
      rec[f.name] = v instanceof Date ? v : new Date(v as string);
    } else rec[f.name] = v;
  }
  return rec;
}
