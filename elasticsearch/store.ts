/**
 * ElasticsearchStore — Elasticsearch implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries, read entries, delete entries. Observe is not supported.
 *
 * Uses an injected ElasticsearchExecutor, keeping the SDK decoupled
 * from any specific Elasticsearch library.
 *
 * @example
 * ```typescript
 * import { ElasticsearchStore } from "@bandeira-tech/b3nd-sdk";
 *
 * const store = new ElasticsearchStore("b3nd", executor);
 *
 * await store.write([
 *   { uri: "mutable://app/config", values: {}, data: { theme: "dark" } },
 * ]);
 *
 * const results = await store.read(["mutable://app/config"]);
 * console.log(results[0]?.record?.data); // { theme: "dark" }
 * ```
 */

import type {
  DeleteResult,
  ReadResult,
  StatusResult,
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "@bandeira-tech/b3nd-sdk/types";
import {
  decodeBinaryFromJson,
  encodeBinaryForJson,
} from "@bandeira-tech/b3nd-sdk";
import type { ElasticsearchExecutor } from "./mod.ts";

/**
 * Parse a URI into an Elasticsearch index name and document ID.
 * `protocol://hostname/path` -> index: `prefix_protocol_hostname`, docId: `path`
 */
function uriToIndexAndDocId(
  uri: string,
  indexPrefix: string,
): { index: string; docId: string } {
  const url = new URL(uri);
  const protocol = url.protocol.replace(":", "");
  const hostname = url.hostname;
  const index = `${indexPrefix}_${protocol}_${hostname}`;
  const docId = url.pathname.substring(1);
  return { index, docId };
}

/**
 * Reconstruct a URI from an Elasticsearch index name and document ID.
 */
function indexAndDocIdToUri(
  index: string,
  indexPrefix: string,
  docId: string,
): string {
  const withoutPrefix = index.substring(indexPrefix.length + 1);
  const firstUnderscore = withoutPrefix.indexOf("_");
  const protocol = withoutPrefix.substring(0, firstUnderscore);
  const hostname = withoutPrefix.substring(firstUnderscore + 1);
  return `${protocol}://${hostname}/${docId}`;
}

export class ElasticsearchStore implements Store {
  private readonly indexPrefix: string;
  private readonly executor: ElasticsearchExecutor;

  constructor(indexPrefix: string, executor: ElasticsearchExecutor) {
    if (!indexPrefix) {
      throw new Error("indexPrefix is required");
    }
    if (!executor) {
      throw new Error("executor is required");
    }

    this.indexPrefix = indexPrefix;
    this.executor = executor;
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const encodedData = encodeBinaryForJson(entry.data);
        const { index, docId } = uriToIndexAndDocId(
          entry.uri,
          this.indexPrefix,
        );
        await this.executor.index(index, docId, {
          values: entry.values,
          data: encodedData,
        });
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Write failed",
        });
      }
    }

    return results;
  }

  // ── Read ─────────────────────────────────────────────────────────

  async read<T = unknown>(uris: string[]): Promise<ReadResult<T>[]> {
    const results: ReadResult<T>[] = [];

    for (const uri of uris) {
      if (uri.endsWith("/")) {
        results.push(...await this._list<T>(uri));
      } else {
        results.push(await this._readOne<T>(uri));
      }
    }

    return results;
  }

  private async _readOne<T = unknown>(uri: string): Promise<ReadResult<T>> {
    try {
      const { index, docId } = uriToIndexAndDocId(uri, this.indexPrefix);
      const doc = await this.executor.get(index, docId);

      if (!doc) {
        return { success: false, error: `Not found: ${uri}` };
      }

      const values = (doc.values ?? {}) as Record<string, number>;
      const decodedData = decodeBinaryFromJson(doc.data) as T;

      return { success: true, record: { values, data: decodedData } };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  private async _list<T = unknown>(uri: string): Promise<ReadResult<T>[]> {
    try {
      const { index, docId } = uriToIndexAndDocId(uri, this.indexPrefix);
      const pathPrefix = docId;

      const searchResult = await this.executor.search(index, {
        query: { prefix: { _id: pathPrefix } },
        size: 10000,
      });

      if (!searchResult.hits.length) {
        return [];
      }

      const results: ReadResult<T>[] = [];
      for (const hit of searchResult.hits) {
        const hitUri = indexAndDocIdToUri(index, this.indexPrefix, hit._id);
        const values = (hit._source.values ?? {}) as Record<string, number>;
        const decodedData = decodeBinaryFromJson(hit._source.data) as T;
        results.push({
          success: true,
          uri: hitUri,
          record: { values, data: decodedData },
        });
      }

      return results;
    } catch (_error) {
      return [];
    }
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        const { index, docId } = uriToIndexAndDocId(uri, this.indexPrefix);
        await this.executor.delete?.(index, docId);
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Delete failed",
        });
      }
    }

    return results;
  }

  // ── Status ───────────────────────────────────────────────────────

  async status(): Promise<StatusResult> {
    try {
      const ok = await this.executor.ping();
      if (!ok) {
        return {
          status: "unhealthy",
          message: "Elasticsearch cluster is not reachable",
        };
      }

      return {
        status: "healthy",
        message: "Elasticsearch store is operational",
        details: { indexPrefix: this.indexPrefix },
      };
    } catch (error) {
      return {
        status: "unhealthy",
        message: error instanceof Error ? error.message : String(error),
      };
    }
  }

  capabilities(): StoreCapabilities {
    return {
      atomicBatch: false,
      binaryData: false,
    };
  }
}
