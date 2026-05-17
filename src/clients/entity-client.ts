/**
 * EntityClient — schema-aware ProtocolInterfaceNode over an EntityStore.
 *
 * The store is multi-entity; the client picks the **target** entity
 * it is currently routing to. Wire payloads on the rig are
 * `EntityRecord | null`, pass-through to the underlying store:
 *
 *   `receive([uri, record])` → store.write(target, [{ uri, record }])
 *   `receive([uri, null])`   → store.delete(target, [uri])
 *   `read(urls)`             → store.read(target, urls)
 *
 * The target can be swapped at runtime via {@link setTarget} — the
 * store does not need to be re-initialised, since `ensureEntity` for
 * the new schema is fired the next time it is needed. Useful when a
 * single client routes multiple entities by reconfiguration rather
 * than by URI pattern.
 *
 * The store is strict: any mismatch between the record and the
 * target schema comes back as a `ReceiveResult` failure for that
 * entry. EntityClient does not coerce or drop — that's a wiring
 * concern surfaced for a human to fix.
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
import type {
  EntityRecord,
  EntitySchema,
  EntitySupport,
} from "../entity.ts";

export class EntityClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: EntityStore;
  private _target: EntitySchema;
  private _ensured = new Map<string, Promise<EntitySupport>>();

  constructor(target: EntitySchema, store: EntityStore) {
    super();
    this.store = store;
    this._target = target;
  }

  /** The schema currently being routed. */
  get target(): EntitySchema {
    return this._target;
  }

  /**
   * Swap the target schema. The next operation will lazily provision
   * the new entity on the store; existing data under other entities
   * is untouched.
   */
  setTarget(schema: EntitySchema): void {
    this._target = schema;
  }

  /**
   * Eagerly provision the current target on the underlying store.
   * Returns the {@link EntitySupport} so the caller can see which
   * fields the medium accepted. Idempotent.
   */
  init(): Promise<EntitySupport> {
    const key = this._target.name;
    const cached = this._ensured.get(key);
    if (cached) return cached;
    const p = this.store.ensureEntity(this._target);
    this._ensured.set(key, p);
    return p;
  }

  async receive(
    msgs: Message<EntityRecord | null>[],
  ): Promise<ReceiveResult[]> {
    await this.init();
    const target = this._target;

    const results: ReceiveResult[] = new Array(msgs.length);
    const writes: { uri: string; record: EntityRecord; index: number }[] = [];
    const deletes: { uri: string; index: number }[] = [];

    for (let i = 0; i < msgs.length; i++) {
      const [uri, payload] = msgs[i];
      if (!uri || typeof uri !== "string") {
        results[i] = { accepted: false, error: "Message URI is required" };
        continue;
      }
      if (payload === null) deletes.push({ uri, index: i });
      else writes.push({ uri, record: payload, index: i });
    }

    if (writes.length > 0) {
      const r = await this.store.write(
        target,
        writes.map(({ uri, record }) => ({ uri, record })),
      );
      for (let j = 0; j < r.length; j++) {
        const w = writes[j];
        results[w.index] = { accepted: r[j].success, error: r[j].error };
        if (r[j].success) this._emit(w.uri, w.record);
      }
    }

    if (deletes.length > 0) {
      const r = await this.store.delete(target, deletes.map((d) => d.uri));
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

  async read<T = EntityRecord | undefined>(
    urls: string[],
  ): Promise<Output<T>[]> {
    await this.init();
    return this.store.read<T>(this._target, urls);
  }

  status(): Promise<StatusResult> {
    return this.store.status();
  }
}
