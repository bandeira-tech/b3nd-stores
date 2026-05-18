/**
 * SaveClient — unified ProtocolInterfaceNode adapter over a save backend.
 *
 * One client. One shape. Backed by either an {@link EntityStore} or a
 * legacy byte {@link Store}, picked at construction by duck-typing on
 * `ensureEntity`.
 *
 * Construction shape:
 *
 *     new SaveClient(mapper, entity, store)
 *
 * Reads on the rig: "receive payloads X, map as Y into entity E,
 * store on S". All three are explicit; the client does not invent
 * defaults.
 *
 * - **mapper** — a {@link SaveMapper}: freeform projection from the
 *   wire payload `TIn` to an `EntityRecord` matching `entity`. Runs
 *   per write entry; throwing produces a per-entry `ReceiveResult`
 *   failure. {@link mapToBytes} and {@link passThroughRecord} cover
 *   the common cases.
 * - **entity** — the {@link EntitySchema} this client routes. Use
 *   {@link BYTES_ENTITY} for opaque-bytes wires.
 * - **store** — an {@link EntityStore}, or a legacy byte {@link Store}
 *   if `entity` is `BYTES_ENTITY`.
 *
 * Wire shape: a `receive` message is `Output<TIn | null>` —
 * `[uri, payload]` where `payload === null` is the delete-by-convention.
 * Reads come back as the stored records (or raw bytes when `entity`
 * is `BYTES_ENTITY`); the mapper is receive-only.
 *
 * A `SaveClient` is a one-shot, isolated thing: each one routes one
 * entity, and routing a different entity means constructing a
 * different client. A byte-only backing store accepts only
 * `BYTES_ENTITY` — passing any other entity throws.
 */

import type {
  Output,
  ProtocolInterfaceNode,
  ReceiveResult,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import { ObserveEmitter } from "@bandeira-tech/b3nd-core";
import type { EntityStore } from "../entity-store.ts";
import {
  BYTES_ENTITY,
  type EntityRecord,
  type EntitySchema,
  type EntitySupport,
} from "../entity.ts";
import type { Store, StorePayload } from "../types.ts";

type SaveStore = EntityStore | Store;

/**
 * Freeform projection from wire payload → `EntityRecord` for the
 * target schema. Runs per write entry inside `SaveClient.receive`.
 * Throwing produces a `ReceiveResult` failure for that entry; the
 * rest of the batch is unaffected.
 */
export type SaveMapper<TIn = unknown> = (
  uri: string,
  payload: TIn,
) => EntityRecord | Promise<EntityRecord>;

/**
 * Built-in mapper that wraps a byte payload as a `BYTES_ENTITY`
 * record (`{ payload: bytes }`). Use with `target: BYTES_ENTITY` when
 * the wire is already opaque bytes.
 */
export const mapToBytes: SaveMapper<StorePayload> = (_uri, payload) => ({
  payload,
});

/**
 * Built-in mapper that passes the wire payload through as-is,
 * assuming it already matches the target schema. Use when the
 * protocol upstream has already produced a valid `EntityRecord`.
 */
export const passThroughRecord: SaveMapper<EntityRecord> = (_uri, payload) =>
  payload;

function isEntityStore(store: SaveStore): store is EntityStore {
  return typeof (store as EntityStore).ensureEntity === "function";
}

/**
 * Unwrap `BYTES_ENTITY` read results back to bytes-on-the-wire.
 *
 * The store returns:
 * - `fn=read`: a single `EntityRecord` (or `undefined` for a miss).
 * - `fn=ls&format=full`: `Output<EntityRecord | undefined>[]`.
 * - `fn=ls&format=uris`: `string[]`.
 * - `fn=count`: `number`.
 *
 * Record-shaped payloads carry `{ payload: bytes }`; we surface the
 * inner bytes. Other shapes pass through unchanged.
 */
function unwrapBytes(value: unknown): unknown {
  if (value === undefined || value === null) return value;
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) {
    return value.map((row) => {
      if (Array.isArray(row) && row.length === 2) {
        const [childUri, childPayload] = row as [string, unknown];
        return [childUri, unwrapBytes(childPayload)];
      }
      return row;
    });
  }
  if (typeof value === "object") {
    const rec = value as Record<string, unknown>;
    if ("payload" in rec) return rec.payload;
  }
  return value;
}

export class SaveClient<TIn = unknown> extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly mapper: SaveMapper<TIn>;
  readonly target: EntitySchema;
  readonly store: SaveStore;
  private readonly _isEntity: boolean;
  private _ensurePromise: Promise<EntitySupport | undefined> | null = null;

  constructor(
    mapper: SaveMapper<TIn>,
    target: EntitySchema,
    store: SaveStore,
  ) {
    super();
    this.mapper = mapper;
    this.target = target;
    this.store = store;
    this._isEntity = isEntityStore(store);
    if (!this._isEntity && target.name !== BYTES_ENTITY.name) {
      throw new Error(
        `SaveClient: backing store does not implement EntityStore; ` +
          `target must be BYTES_ENTITY (got '${target.name}')`,
      );
    }
  }

  /**
   * Eagerly provision the target on an `EntityStore`. Returns the
   * {@link EntitySupport} report. Byte-only stores return `undefined`
   * — they don't carry per-entity metadata.
   */
  init(): Promise<EntitySupport | undefined> {
    if (this._ensurePromise) return this._ensurePromise;
    this._ensurePromise = this._isEntity
      ? (this.store as EntityStore).ensureEntity(this.target)
      : Promise.resolve(undefined);
    return this._ensurePromise;
  }

  async receive(
    msgs: Output<TIn | null>[],
  ): Promise<ReceiveResult[]> {
    await this.init();
    const target = this.target;

    const results: ReceiveResult[] = new Array(msgs.length);
    const writes: {
      uri: string;
      record: EntityRecord;
      wire: TIn;
      index: number;
    }[] = [];
    const deletes: { uri: string; index: number }[] = [];

    for (let i = 0; i < msgs.length; i++) {
      const [uri, payload] = msgs[i];
      if (!uri || typeof uri !== "string") {
        results[i] = { accepted: false, error: "URI is required" };
        continue;
      }
      if (payload === null) {
        deletes.push({ uri, index: i });
        continue;
      }
      try {
        const record = await this.mapper(uri, payload as TIn);
        writes.push({ uri, record, wire: payload as TIn, index: i });
      } catch (e) {
        results[i] = {
          accepted: false,
          error: e instanceof Error ? e.message : String(e),
        };
      }
    }

    if (writes.length > 0) {
      const writeResults = this._isEntity
        ? await (this.store as EntityStore).write(
          target,
          writes.map(({ uri, record }) => ({ uri, record })),
        )
        : await (this.store as Store).write(
          writes.map(({ uri, record }) => ({
            uri,
            payload: record.payload as StorePayload,
          })),
        );

      for (let j = 0; j < writeResults.length; j++) {
        const w = writes[j];
        const r = writeResults[j];
        results[w.index] = { accepted: r.success, error: r.error };
        if (r.success) this._emit(w.uri, w.wire);
      }
    }

    if (deletes.length > 0) {
      const delResults = this._isEntity
        ? await (this.store as EntityStore).delete(
          target,
          deletes.map((d) => d.uri),
        )
        : await (this.store as Store).delete(deletes.map((d) => d.uri));

      const deletedUris: string[] = [];
      for (let j = 0; j < delResults.length; j++) {
        const d = deletes[j];
        const r = delResults[j];
        results[d.index] = { accepted: r.success, error: r.error };
        if (r.success) deletedUris.push(d.uri);
      }
      if (deletedUris.length > 0) this._emitDeletes(deletedUris);
    }

    return results;
  }

  async read<T = EntityRecord | StorePayload | undefined>(
    urls: string[],
  ): Promise<Output<T>[]> {
    await this.init();
    const target = this.target;
    const isBytes = target.name === BYTES_ENTITY.name;

    if (!this._isEntity) {
      return (this.store as Store).read<T>(urls);
    }

    const rows = await (this.store as EntityStore).read<
      EntityRecord | undefined
    >(target, urls);

    if (!isBytes) return rows as unknown as Output<T>[];

    return rows.map(([uri, payload]) => [uri, unwrapBytes(payload) as T]);
  }

  status(): Promise<StatusResult> {
    return this.store.status();
  }
}
