/**
 * In-memory EntityAdapter for `MemoryStore`.
 *
 * Stores records as parsed objects in a parallel per-entity map
 * keyed by URI — the byte tree on the same `MemoryStore` is
 * untouched, so the byte face and the entity face of the store can
 * coexist (though authors should not point the same URI at both at
 * the same time).
 *
 * Every {@link TYPE_TAGS} value is supported — the adapter copies
 * values through without coercion, since the in-memory medium has no
 * column types to honor. Fields whose `type` array is empty or whose
 * tags are entirely unrecognised are reported as unsupported and
 * dropped on write.
 */

import type { DeleteResult, Output } from "@bandeira-tech/b3nd-core/types";
import { storageFailure } from "../errors.ts";
import type { StoreWriteResult } from "../types.ts";
import {
  type EntityAdapter,
  type EntityRecord,
  type EntitySchema,
  type EntitySupport,
  TYPE_TAGS,
} from "../entity.ts";

const KNOWN_TAGS: ReadonlySet<string> = new Set(Object.values(TYPE_TAGS));

export class MemoryEntityAdapter implements EntityAdapter {
  private readonly records = new Map<string, Map<string, EntityRecord>>();
  private readonly supportedFields = new Map<string, ReadonlySet<string>>();

  // deno-lint-ignore require-await
  async ensureEntity(schema: EntitySchema): Promise<EntitySupport> {
    const supported: string[] = [];
    const unsupported: { name: string; reason: string }[] = [];

    for (const field of schema.fields) {
      const recognised = field.type.filter((t) => KNOWN_TAGS.has(t));
      if (recognised.length === 0) {
        unsupported.push({
          name: field.name,
          reason: field.type.length === 0
            ? "field declares no type tags"
            : `no recognised tag in [${field.type.join(", ")}]`,
        });
      } else {
        supported.push(field.name);
      }
    }

    this.supportedFields.set(schema.name, new Set(supported));
    if (!this.records.has(schema.name)) {
      this.records.set(schema.name, new Map());
    }

    return { entity: schema.name, supported, unsupported };
  }

  async writeEntity(
    entity: string,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]> {
    const bucket = this._bucket(entity);
    const supported = this.supportedFields.get(entity)!;
    const results: StoreWriteResult[] = [];

    for (const { uri, record } of entries) {
      try {
        const projected: EntityRecord = {};
        for (const field of supported) {
          if (field in record) projected[field] = record[field];
        }
        bucket.set(uri, projected);
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          ...storageFailure(err, "Entity write failed", uri),
        });
      }
    }
    return Promise.resolve(results);
  }

  // deno-lint-ignore require-await
  async readEntity(
    entity: string,
    uris: string[],
  ): Promise<Output<EntityRecord | undefined>[]> {
    const bucket = this._bucket(entity);
    return uris.map((uri) => [uri, bucket.get(uri)]);
  }

  async deleteEntity(
    entity: string,
    uris: string[],
  ): Promise<DeleteResult[]> {
    const bucket = this._bucket(entity);
    const results: DeleteResult[] = [];
    for (const uri of uris) {
      try {
        bucket.delete(uri);
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          ...storageFailure(err, "Entity delete failed", uri),
        });
      }
    }
    return Promise.resolve(results);
  }

  private _bucket(entity: string): Map<string, EntityRecord> {
    const b = this.records.get(entity);
    if (!b) {
      throw new Error(
        `MemoryEntityAdapter: ensureEntity('${entity}') was never called`,
      );
    }
    return b;
  }
}
