/**
 * SaveClient — unified ProtocolInterfaceNode adapter over a save backend.
 *
 * One client. One shape. Backed by either an {@link EntityStore} or a
 * legacy byte {@link Store}, picked at construction by duck-typing on
 * `ensureEntity`.
 *
 * The target schema defaults to {@link BYTES_ENTITY}: bytes-on-the-wire
 * unless the caller asks for something else. Mirrors how
 * `MemoryStore` routes byte calls through `BYTES_ENTITY` while the rest
 * of the package's backends migrate to `EntityStore` — byte is the
 * lingua franca, structured entities are an opt-in.
 *
 * Wire shape (a `Message` is `[uri, payload | null]`):
 *
 * - `BYTES_ENTITY` target: `payload` is `Uint8Array | ReadableStream<Uint8Array>`.
 *   Writes wrap into `{ payload: bytes }`; reads unwrap back to bytes.
 * - Other target: `payload` is an `EntityRecord` (open
 *   `Record<string, unknown>`), pass-through on both directions.
 * - `null` payload deletes the URI under the current target.
 *
 * Target is swappable at runtime via {@link setTarget}. A byte-only
 * backing store accepts only `BYTES_ENTITY`; passing any other target
 * (in the constructor or via `setTarget`) throws — the store has
 * nowhere to put a non-byte record.
 */

import type {
  Message,
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

export class SaveClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: SaveStore;
  private _target: EntitySchema;
  private readonly _isEntity: boolean;
  private readonly _ensured = new Map<string, Promise<EntitySupport>>();

  constructor(store: SaveStore, target: EntitySchema = BYTES_ENTITY) {
    super();
    this.store = store;
    this._isEntity = isEntityStore(store);
    this._guard(target);
    this._target = target;
  }

  /** The schema currently being routed. */
  get target(): EntitySchema {
    return this._target;
  }

  /**
   * Swap the target schema. The next operation lazily provisions the
   * new entity on the store (entity backends only). A byte-only store
   * rejects anything other than `BYTES_ENTITY`.
   */
  setTarget(schema: EntitySchema): void {
    this._guard(schema);
    this._target = schema;
  }

  /**
   * Eagerly provision the current target on an `EntityStore`. Returns
   * the {@link EntitySupport} report. Byte-only stores return
   * `undefined` — they don't carry per-entity metadata.
   */
  init(): Promise<EntitySupport | undefined> {
    if (!this._isEntity) return Promise.resolve(undefined);
    const key = this._target.name;
    const cached = this._ensured.get(key);
    if (cached) return cached;
    const p = (this.store as EntityStore).ensureEntity(this._target);
    this._ensured.set(key, p);
    return p;
  }

  async receive(
    msgs: Message<EntityRecord | StorePayload | null>[],
  ): Promise<ReceiveResult[]> {
    await this.init();
    const target = this._target;
    const isBytes = target.name === BYTES_ENTITY.name;

    const results: ReceiveResult[] = new Array(msgs.length);
    const writes: { uri: string; payload: unknown; index: number }[] = [];
    const deletes: { uri: string; index: number }[] = [];

    for (let i = 0; i < msgs.length; i++) {
      const [uri, payload] = msgs[i];
      if (!uri || typeof uri !== "string") {
        results[i] = { accepted: false, error: "Message URI is required" };
        continue;
      }
      if (payload === null) deletes.push({ uri, index: i });
      else writes.push({ uri, payload, index: i });
    }

    if (writes.length > 0) {
      const writeResults = this._isEntity
        ? await (this.store as EntityStore).write(
          target,
          writes.map(({ uri, payload }) => ({
            uri,
            record: isBytes
              ? ({ payload } as EntityRecord)
              : (payload as EntityRecord),
          })),
        )
        : await (this.store as Store).write(
          writes.map(({ uri, payload }) => ({
            uri,
            payload: payload as StorePayload,
          })),
        );

      for (let j = 0; j < writeResults.length; j++) {
        const w = writes[j];
        const r = writeResults[j];
        results[w.index] = { accepted: r.success, error: r.error };
        if (r.success) this._emit(w.uri, w.payload);
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
    const target = this._target;
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

  private _guard(schema: EntitySchema): void {
    if (!this._isEntity && schema.name !== BYTES_ENTITY.name) {
      throw new Error(
        `SaveClient: backing store does not implement EntityStore; ` +
          `target must be BYTES_ENTITY (got '${schema.name}')`,
      );
    }
  }
}
