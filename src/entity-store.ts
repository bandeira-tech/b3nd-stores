/**
 * EntityStore — schema-aware storage contract.
 *
 * Every operation takes the `EntitySchema` it targets. A single
 * store instance hosts many entities side-by-side; nothing is pinned
 * at construction. The store manages the medium-level layout
 * (separate tables, collections, prefixes, in-memory record maps —
 * whatever the backend prefers) keyed by `schema.name`.
 *
 * `ensureEntity` is the medium-setup step — provision the table,
 * collection, index, in-memory bucket. MUST be idempotent: repeat
 * calls with the same schema return the same support report and do
 * not re-provision. The returned {@link EntitySupport} declares which
 * fields the medium accepted and which it could not — callers see
 * unsupported fields once at init time and decide whether to keep
 * going.
 *
 * Writes/reads/deletes are strict. A record that does not match the
 * schema produces a `StoreWriteResult.error` (or `DeleteResult.error`
 * for delete-time failures). The store never silently drops or
 * coerces — coercion is the client's job (see `SaveClient`, which
 * projects raw bytes into a `BYTES_ENTITY` record when its target is
 * unset).
 *
 * Reads return `Output<EntityRecord | undefined>` for `fn=read`;
 * `fn=ls` and `fn=count` follow the same conventions as `Store`
 * (URI lists and numbers — see `./read.ts`).
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import type { EntityRecord, EntitySchema, EntitySupport } from "./entity.ts";
import type { StoreCapabilities, StoreWriteResult } from "./types.ts";

export interface EntityStore {
  /**
   * Provision the entity on the medium. Idempotent. Returns the per-
   * field support report so callers know which fields will round-trip.
   */
  ensureEntity(schema: EntitySchema): Promise<EntitySupport>;

  /**
   * Write records of `schema` at the given URIs. Per-entry result is
   * returned in input order. A record that does not match `schema`
   * produces a failure result for that entry — the store does not
   * coerce.
   */
  write(
    schema: EntitySchema,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]>;

  /**
   * Read records of `schema` at the given URLs. URLs follow the
   * standard `fn=read|ls|count|x-*` grammar — see `./read.ts`.
   * `fn=read` yields `Output<EntityRecord | undefined>` (undefined
   * payload means miss); `fn=ls` and `fn=count` follow the package
   * conventions.
   */
  read<T = EntityRecord | undefined>(
    schema: EntitySchema,
    urls: string[],
  ): Promise<Output<T>[]>;

  /**
   * Delete records of `schema` at the given URIs. Per-entry result
   * is returned in input order.
   */
  delete(schema: EntitySchema, uris: string[]): Promise<DeleteResult[]>;

  /** Health + capabilities, aggregated across all hosted entities. */
  status(): Promise<StatusResult>;

  /**
   * Optional capability reporting. Backends advertise what they can
   * do so clients can make informed decisions (e.g. wrap deletes +
   * writes in a transaction when `atomicBatch` is true).
   */
  capabilities?(): StoreCapabilities;
}
