/**
 * MongoDB backend for b3nd.
 *
 * Store implementation backed by MongoDB. Requires an injected
 * MongoExecutor so the package does not depend on a specific MongoDB
 * driver.
 */

export interface MongoFindManyOptions {
  sort?: Record<string, 1 | -1>;
  skip?: number;
  limit?: number;
  /** Mongo projection map (1 = include, 0 = exclude). */
  projection?: Record<string, 0 | 1>;
}

export interface MongoExecutor {
  insertOne(doc: Record<string, unknown>): Promise<{ acknowledged?: boolean }>;
  updateOne(
    filter: Record<string, unknown>,
    update: Record<string, unknown>,
    options?: Record<string, unknown>,
  ): Promise<
    { matchedCount?: number; modifiedCount?: number; upsertedId?: unknown }
  >;
  findOne(
    filter: Record<string, unknown>,
  ): Promise<Record<string, unknown> | null>;
  findMany(
    filter: Record<string, unknown>,
    options?: MongoFindManyOptions,
  ): Promise<Record<string, unknown>[]>;
  countDocuments(filter: Record<string, unknown>): Promise<number>;
  deleteOne(
    filter: Record<string, unknown>,
  ): Promise<{ deletedCount?: number }>;
  ping(): Promise<boolean>;
  transaction?: <T>(fn: (executor: MongoExecutor) => Promise<T>) => Promise<T>;
  cleanup?: () => Promise<void>;
}

export { MongoStore } from "./store.ts";
