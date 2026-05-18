/**
 * MongoDB backend for b3nd.
 *
 * Store implementation backed by MongoDB. Requires an injected
 * MongoExecutor so the package does not depend on a specific MongoDB
 * driver. Each method takes the target collection name, so a single
 * executor can serve many entity collections.
 */

export interface MongoFindManyOptions {
  sort?: Record<string, 1 | -1>;
  skip?: number;
  limit?: number;
  /** Mongo projection map (1 = include, 0 = exclude). */
  projection?: Record<string, 0 | 1>;
}

export interface MongoExecutor {
  insertOne(
    collection: string,
    doc: Record<string, unknown>,
  ): Promise<{ acknowledged?: boolean }>;
  updateOne(
    collection: string,
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ): Promise<
    { matchedCount?: number; modifiedCount?: number; upsertedId?: unknown }
  >;
  findOne(
    collection: string,
    filter: Record<string, unknown>,
  ): Promise<Record<string, unknown> | null>;
  findMany(
    collection: string,
    filter: Record<string, unknown>,
    options?: MongoFindManyOptions,
  ): Promise<Record<string, unknown>[]>;
  countDocuments(
    collection: string,
    filter: Record<string, unknown>,
  ): Promise<number>;
  deleteOne(
    collection: string,
    filter: Record<string, unknown>,
  ): Promise<{ deletedCount?: number }>;
  /**
   * Create the unique-on-`uri` index for an entity collection.
   * Called by `MongoStore.ensureEntity`; idempotent.
   */
  ensureUriIndex(collection: string): Promise<void>;
  ping(): Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { MongoStore } from "./store.ts";
