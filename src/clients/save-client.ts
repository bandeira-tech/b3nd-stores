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
 * Wire shape (an `Output` is `[uri, payload]`; we accept `null` as the
 * delete-by-convention payload):
 *
 * - `BYTES_ENTITY` target: `payload` is `Uint8Array | ReadableStream<Uint8Array>`.
 *   Writes wrap into `{ payload: bytes }`; reads unwrap back to bytes.
 * - Other target: `payload` is an `EntityRecord` (open
 *   `Record<string, unknown>`), pass-through on both directions.
 * - `null` payload deletes the URI under the target.
 *
 * The target is sealed at construction. A `SaveClient` is a one-shot,
 * isolated thing: each one routes one entity, and routing a different
 * entity means constructing a different client. A byte-only backing
 * store accepts only `BYTES_ENTITY` — passing any other target throws.
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
  readonly target: EntitySchema;
  private readonly _isEntity: boolean;
  private _ensurePromise: Promise<EntitySupport | undefined> | null = null;

  constructor(store: SaveStore, target: EntitySchema = BYTES_ENTITY) {
    super();
    this.store = store;
    this._isEntity = isEntityStore(store);
    if (!this._isEntity && target.name !== BYTES_ENTITY.name) {
      throw new Error(
        `SaveClient: backing store does not implement EntityStore; ` +
          `target must be BYTES_ENTITY (got '${target.name}')`,
      );
    }
    this.target = target;
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
    msgs: Output<EntityRecord | StorePayload | null>[],
  ): Promise<ReceiveResult[]> {
    await this.init();
    const target = this.target;
    const isBytes = target.name === BYTES_ENTITY.name;

    const results: ReceiveResult[] = new Array(msgs.length);
    const writes: { uri: string; payload: unknown; index: number }[] = [];
    const deletes: { uri: string; index: number }[] = [];

    for (let i = 0; i < msgs.length; i++) {
      const [uri, payload] = msgs[i];
      if (!uri || typeof uri !== "string") {
        results[i] = { accepted: false, error: "URI is required" };
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
