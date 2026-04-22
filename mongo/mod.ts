/**
 * MongoDB backend for b3nd.
 *
 * Store implementation backed by MongoDB. Requires an injected MongoExecutor
 * so the SDK does not depend on a specific MongoDB driver.
 */

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
    options?: {
      sort?: Record<string, 1 | -1>;
      skip?: number;
      limit?: number;
    },
  ): Promise<Record<string, unknown>[]>;
  countDocuments?: (filter: Record<string, unknown>) => Promise<number>;
  deleteOne?: (
    filter: Record<string, unknown>,
  ) => Promise<{ deletedCount?: number }>;
  ping(): Promise<boolean>;
  transaction?: <T>(fn: (executor: MongoExecutor) => Promise<T>) => Promise<T>;
  cleanup?: () => Promise<void>;
}

export { MongoStore } from "./store.ts";
