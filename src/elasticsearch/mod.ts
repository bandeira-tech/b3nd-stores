/**
 * Elasticsearch backend for b3nd.
 *
 * Store implementation backed by Elasticsearch. Requires an injected
 * ElasticsearchExecutor so the package does not depend on a specific
 * ES client.
 */

export interface ElasticsearchClientConfig {
  /** Index name prefix for b3nd data (e.g., "b3nd") */
  indexPrefix: string;
}

export interface ElasticsearchSearchResult {
  hits: Array<
    { _id: string; _source?: Record<string, unknown> }
  >;
}

export interface ElasticsearchExecutor {
  index: (
    index: string,
    id: string,
    body: Record<string, unknown>,
  ) => Promise<void>;
  get: (
    index: string,
    id: string,
  ) => Promise<Record<string, unknown> | null>;
  search: (
    index: string,
    body: Record<string, unknown>,
  ) => Promise<ElasticsearchSearchResult>;
  /**
   * Count documents matching a query. Maps to the ES `_count` endpoint.
   */
  count: (
    index: string,
    body: Record<string, unknown>,
  ) => Promise<number>;
  delete: (index: string, id: string) => Promise<void>;
  /**
   * Create an index with the given mappings if it doesn't already
   * exist. Called by `ElasticsearchStore.ensureEntity` for custom
   * entity schemas; idempotent.
   */
  ensureIndex: (
    index: string,
    mappings: Record<string, unknown>,
  ) => Promise<void>;
  ping: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { ElasticsearchStore } from "./store.ts";
