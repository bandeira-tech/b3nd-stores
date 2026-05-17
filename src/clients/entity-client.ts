/**
 * EntityClient — schema-aware ProtocolInterfaceNode over a Store.
 *
 * Wraps a `(schema, store)` pair: the store contributes an
 * `EntityAdapter` (Postgres tables, the in-memory parallel map, etc.),
 * the schema names the fields the client will route through it. The
 * client then exposes the standard `ProtocolInterfaceNode` so a Rig
 * can plug it into a `data://…` route.
 *
 * ```ts
 * const client = new EntityClient(userSchema, new MemoryStore());
 * rig({ receive: { connect: { "data://users": client } } });
 * ```
 *
 * Lifecycle: `ensureEntity` runs lazily on the first `receive` /
 * `read` (or eagerly via {@link init}). It is idempotent — repeat
 * calls hand back the cached support report.
 *
 * Wire conventions:
 *  - `receive([uri, record])`   → adapter.writeEntity
 *  - `receive([uri, null])`     → adapter.deleteEntity (delete-as-data,
 *    same convention as DataStoreClient)
 *  - `read([uri])`              → adapter.readEntity (point read)
 *
 * Unsupported fields reported by `ensureEntity` are dropped on write
 * (the adapter projects only the supported field set). The full
 * {@link EntitySupport} is available via {@link support} so callers
 * can surface a warning, abort, or react however they want.
 */

import type {
  Message,
  Output,
  ProtocolInterfaceNode,
  ReceiveResult,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import { ObserveEmitter } from "@bandeira-tech/b3nd-core";
import type { Store } from "../types.ts";
import type {
  EntityAdapter,
  EntityRecord,
  EntitySchema,
  EntitySupport,
} from "../entity.ts";

export class EntityClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: Store;
  readonly schema: EntitySchema;
  private readonly adapter: EntityAdapter;
  private _support: EntitySupport | null = null;
  private _initPromise: Promise<EntitySupport> | null = null;

  constructor(schema: EntitySchema, store: Store) {
    super();
    if (!store.entityAdapter) {
      throw new Error(
        `EntityClient: store does not expose entityAdapter() — ` +
          `byte-only stores cannot host entities`,
      );
    }
    const adapter = store.entityAdapter();
    if (!adapter) {
      throw new Error(
        `EntityClient: store.entityAdapter() returned null — ` +
          `this store cannot host the '${schema.name}' entity`,
      );
    }
    this.store = store;
    this.schema = schema;
    this.adapter = adapter;
  }

  /** Eagerly provision the entity on the underlying medium. */
  init(): Promise<EntitySupport> {
    if (this._support) return Promise.resolve(this._support);
    if (this._initPromise) return this._initPromise;
    this._initPromise = this.adapter.ensureEntity(this.schema).then((s) => {
      this._support = s;
      return s;
    });
    return this._initPromise;
  }

  /**
   * The support report returned by the adapter's `ensureEntity`.
   * `null` until {@link init} (or the first `receive`/`read`) resolves.
   */
  get support(): EntitySupport | null {
    return this._support;
  }

  async receive(
    msgs: Message<EntityRecord | null>[],
  ): Promise<ReceiveResult[]> {
    await this.init();

    const results: ReceiveResult[] = new Array(msgs.length);
    const writes: { uri: string; record: EntityRecord; index: number }[] = [];
    const deletes: { uri: string; index: number }[] = [];

    for (let i = 0; i < msgs.length; i++) {
      const [uri, payload] = msgs[i];
      if (!uri || typeof uri !== "string") {
        results[i] = { accepted: false, error: "Message URI is required" };
        continue;
      }
      if (payload === null) {
        deletes.push({ uri, index: i });
      } else {
        writes.push({ uri, record: payload, index: i });
      }
    }

    if (writes.length > 0) {
      const r = await this.adapter.writeEntity(
        this.schema.name,
        writes.map(({ uri, record }) => ({ uri, record })),
      );
      for (let j = 0; j < r.length; j++) {
        const w = writes[j];
        results[w.index] = { accepted: r[j].success, error: r[j].error };
        if (r[j].success) this._emit(w.uri, w.record);
      }
    }

    if (deletes.length > 0) {
      const r = await this.adapter.deleteEntity(
        this.schema.name,
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

  async read<T = EntityRecord | undefined>(
    urls: string[],
  ): Promise<Output<T>[]> {
    await this.init();
    const rows = await this.adapter.readEntity(this.schema.name, urls);
    return rows as unknown as Output<T>[];
  }

  async status(): Promise<StatusResult> {
    const base = await this.store.status();
    return {
      ...base,
      schema: [...(base.schema ?? []), `entity:${this.schema.name}`],
    };
  }
}
