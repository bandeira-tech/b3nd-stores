/**
 * Elasticsearch backend for b3nd.
 *
 * Store implementation backed by Elasticsearch. Requires an injected
 * ElasticsearchExecutor so the SDK does not depend on a specific ES client.
 */

export interface ElasticsearchClientConfig {
  /** Index name prefix for b3nd data (e.g., "b3nd") */
  indexPrefix: string;
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
  ) => Promise<{
    hits: Array<{ _id: string; _source: Record<string, unknown> }>;
  }>;
  delete?: (
    index: string,
    id: string,
  ) => Promise<void>;
  ping: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { ElasticsearchStore } from "./store.ts";
