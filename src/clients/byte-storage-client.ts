/**
 * ByteStorageClient — bytes-on-the-wire ProtocolInterfaceNode over an
 * EntityStore.
 *
 * Pins the target schema to {@link BYTES_ENTITY} and adapts the
 * byte-shaped wire to the entity-shaped store API:
 *
 *   `receive([uri, bytes])` → store.write(BYTES_ENTITY, [{ uri, record: { payload: bytes } }])
 *   `receive([uri, null])`  → store.delete(BYTES_ENTITY, [uri])
 *   `read(urls)`            → store.read(BYTES_ENTITY, urls) → unwrap record.payload
 *
 * Coercion is the client's job (records are constructed here from
 * raw wire bytes); the store remains strict. A non-Uint8Array payload
 * coming through `receive` produces a per-entry `ReceiveResult`
 * error from the store and surfaces here unchanged.
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
import { BYTES_ENTITY, type EntityRecord } from "../entity.ts";
import type { StorePayload } from "../types.ts";

export class ByteStorageClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: EntityStore;
  private _ensured: Promise<unknown> | null = null;

  constructor(store: EntityStore) {
    super();
    this.store = store;
  }

  private init(): Promise<unknown> {
    if (this._ensured) return this._ensured;
    this._ensured = this.store.ensureEntity(BYTES_ENTITY);
    return this._ensured;
  }

  async receive(
    msgs: Message<StorePayload | null>[],
  ): Promise<ReceiveResult[]> {
    await this.init();

    const results: ReceiveResult[] = new Array(msgs.length);
    const writes: { uri: string; payload: StorePayload; index: number }[] = [];
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
      // Bytes might arrive as Uint8Array or ReadableStream<Uint8Array>.
      // MemoryStore's BYTES_ENTITY path expects Uint8Array — but the
      // byte form of `write` already collects streams via `toBytes`.
      // For the entity form here we hand the value through; if a
      // store wants streams in records it can accept them, otherwise
      // it errors out (strict-validation contract).
      const r = await this.store.write(
        BYTES_ENTITY,
        writes.map(({ uri, payload }) => ({
          uri,
          record: { payload } as EntityRecord,
        })),
      );
      for (let j = 0; j < r.length; j++) {
        const w = writes[j];
        results[w.index] = { accepted: r[j].success, error: r[j].error };
        if (r[j].success) this._emit(w.uri, w.payload);
      }
    }

    if (deletes.length > 0) {
      const r = await this.store.delete(
        BYTES_ENTITY,
        deletes.map((d) => d.uri),
      );
      const deleted: string[] = [];
      for (let j = 0; j < r.length; j++) {
        const d = deletes[j];
        results[d.index] = { accepted: r[j].success, error: r[j].error };
        if (r[j].success) deleted.push(d.uri);
      }
      if (deleted.length > 0) this._emitDeletes(deleted);
    }

    return results;
  }

  async read<T = StorePayload>(urls: string[]): Promise<Output<T>[]> {
    await this.init();
    // Read records and unwrap each `record.payload` so the wire is
    // bytes-only on the way out.
    const rows = await this.store.read<EntityRecord | undefined>(
      BYTES_ENTITY,
      urls,
    );
    return rows.map(([uri, rec]) => {
      if (rec === undefined) return [uri, undefined as unknown as T];
      // `fn=ls&format=full` returns Output<EntityRecord>[]; the wire
      // shape is bytes-only, so for that case the caller should use
      // `format=uris` or `fn=count`. Best-effort unwrap of point reads.
      if (
        typeof rec === "object" && rec !== null &&
        "payload" in (rec as Record<string, unknown>)
      ) {
        return [uri, (rec as EntityRecord).payload as T];
      }
      return [uri, rec as unknown as T];
    });
  }

  status(): Promise<StatusResult> {
    return this.store.status();
  }
}
