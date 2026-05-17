/**
 * Entity types for structured, schema-aware storage.
 *
 * `b3nd-save` is byte-storage first — `Store` writes opaque bytes by
 * URI. Some backends can also store *structured* records: a Postgres
 * table with typed columns, a Mongo collection of documents, an
 * Elasticsearch index. The entity layer is how `b3nd-save` lets those
 * backends expose that ability without compromising the byte-only
 * contract that every backend already implements.
 *
 * The pieces:
 *
 * - `EntitySchema` describes an entity — its name and field names
 *   tagged with an open vocabulary of type strings.
 * - `EntityAdapter` is the optional capability a `Store` exposes when
 *   it can persist records by field. A `Store` returns one from
 *   `entityAdapter()` or `null` if it cannot.
 * - `EntityClient` (in `./clients/entity-client.ts`) wraps a
 *   `(schema, store)` pair and presents a `ProtocolInterfaceNode` so
 *   a Rig can route writes/reads through it.
 *
 * ## Open type vocabulary
 *
 * `EntityField.type` is `string[]`. This package publishes the canonical
 * tag strings it knows about under {@link TYPE_TAGS}, but the type itself
 * is *not* a literal union — entities are shared across protocols and
 * custom stores, and one protocol's "money" or "geo" tag should travel
 * unchanged through stores that don't recognise it. A field may carry
 * multiple tags (e.g. `["string", "email"]`) — they are refinements,
 * not a union of value types. An adapter consults the tags it knows
 * to decide how to materialise the field, and reports any field whose
 * tags it cannot make sense of as unsupported.
 */

import type { DeleteResult, Output } from "@bandeira-tech/b3nd-core/types";
import type { StoreWriteResult } from "./types.ts";

/**
 * Canonical type tags published by this package.
 *
 * Adapters in `b3nd-save` recognise these tags. Other protocols and
 * custom stores may freely add their own; the `type` field on
 * {@link EntityField} stays `string[]` so unknown tags pass through
 * intact.
 */
export const TYPE_TAGS = {
  STRING: "string",
  NUMBER: "number",
  BIGINT: "bigint",
  BOOLEAN: "boolean",
  BYTES: "bytes",
  TIMESTAMP: "timestamp",
  JSON: "json",
} as const;

/**
 * A field of an entity — a name plus an open list of type tags.
 *
 * Multiple tags are refinements describing the same value (e.g.
 * `["string", "email"]` — a string value semantically an email).
 * An adapter that understands `"string"` can store the value;
 * the `"email"` tag is a hint for adapters that want to enforce
 * or index it differently.
 */
export interface EntityField {
  name: string;
  type: string[];
}

/**
 * Declarative description of an entity.
 *
 * `name` is the entity's identifier in the medium — e.g. the Postgres
 * table name, the Mongo collection name. Authors should pick a value
 * that is safe for those mediums (lowercase, no special chars).
 */
export interface EntitySchema {
  name: string;
  fields: EntityField[];
}

/**
 * A record of an entity — values keyed by field name. The type is
 * `Record<string, unknown>` because the schema is open: per-field
 * value types are decided by the tags an adapter understands. Adapter
 * implementations cast on the way in.
 */
export type EntityRecord = Record<string, unknown>;

/**
 * Per-field support reported by an adapter after `ensureEntity`.
 *
 * A field is *supported* when the adapter recognised at least one of
 * its tags and was able to provision storage for it. Unsupported
 * fields carry a human-readable `reason` and are silently dropped on
 * write — the {@link EntityClient} surfaces them once at init time so
 * the caller can decide what to do.
 */
export interface EntitySupport {
  entity: string;
  supported: string[];
  unsupported: { name: string; reason: string }[];
}

/**
 * Adapter that lets a `Store` persist entity records.
 *
 * A `Store` returns one from `entityAdapter()` when its medium can
 * organise data by field (a SQL table, a document store, a typed
 * key/value of objects). Byte-only backends return `null`.
 *
 * `ensureEntity` is the medium-setup step — create the table, the
 * collection, the index. It MUST be idempotent: calling it twice
 * with the same schema is a no-op. It returns the support report so
 * the client knows which fields the medium will round-trip.
 *
 * `writeEntity`, `readEntity`, `deleteEntity` are batch-shaped to
 * match `Store`. The entity face is independent from the byte face
 * of the host store — writes and deletes here do NOT affect bytes
 * stored at the same URI by `Store.write`/`Store.delete`, and vice
 * versa. Authors should not mix the two faces at the same URI.
 */
export interface EntityAdapter {
  ensureEntity(schema: EntitySchema): Promise<EntitySupport>;
  writeEntity(
    entity: string,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]>;
  readEntity(
    entity: string,
    uris: string[],
  ): Promise<Output<EntityRecord | undefined>[]>;
  deleteEntity(entity: string, uris: string[]): Promise<DeleteResult[]>;
}
