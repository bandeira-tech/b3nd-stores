/**
 * PostgresStore Tests
 *
 * Uses a mock SqlExecutor that simulates Postgres JSONB behavior.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { PostgresStore } from "./store.ts";
import type { SqlExecutor, SqlExecutorResult } from "./mod.ts";

/**
 * In-memory SQL executor that simulates Postgres behavior.
 * Stores values/data as JSON strings (matching the INSERT path),
 * returns parsed objects on SELECT (matching Postgres JSONB behavior).
 */
function createMockSqlExecutor(): SqlExecutor {
  const data = new Map<
    string,
    { uri: string; values: string; data: string }
  >();

  const executor: SqlExecutor = {
    query: async (
      sql: string,
      args?: unknown[],
    ): Promise<SqlExecutorResult> => {
      const upper = sql.trim().toUpperCase();

      // CREATE TABLE / DDL
      if (upper.startsWith("CREATE")) {
        return { rows: [], rowCount: 0 };
      }

      // SELECT 1 (health check)
      if (upper === "SELECT 1") {
        return { rows: [{ "?column?": 1 }] };
      }

      // INSERT ... ON CONFLICT (upsert)
      // Args: [uri, data_json, values_json]
      if (upper.startsWith("INSERT")) {
        const uri = args![0] as string;
        const dataJson = args![1] as string;
        const valuesJson = args![2] as string;
        data.set(uri, { uri, values: valuesJson, data: dataJson });
        return { rows: [], rowCount: 1 };
      }

      // SELECT with LIKE (list query)
      if (upper.includes("LIKE")) {
        const prefix = args![0] as string;
        const rows = [...data.values()]
          .filter((r) => r.uri.startsWith(prefix))
          .map((r) => ({
            uri: r.uri,
            // Postgres returns JSONB as parsed objects
            data: JSON.parse(r.data),
            values: JSON.parse(r.values),
          }));
        return { rows };
      }

      // SELECT (single read)
      if (upper.startsWith("SELECT")) {
        const uri = args![0] as string;
        const row = data.get(uri);
        if (!row) return { rows: [] };
        return {
          rows: [{
            // Postgres returns JSONB as parsed objects
            data: JSON.parse(row.data),
            values: JSON.parse(row.values),
          }],
        };
      }

      // DELETE
      if (upper.startsWith("DELETE")) {
        const uri = args![0] as string;
        data.delete(uri);
        return { rows: [], rowCount: 1 };
      }

      return { rows: [] };
    },

    transaction: async <T>(
      fn: (tx: SqlExecutor) => Promise<T>,
    ): Promise<T> => {
      return await fn(executor);
    },
  };

  return executor;
}

runSharedStoreSuite("PostgresStore", {
  create: () => {
    const executor = createMockSqlExecutor();
    return new PostgresStore("test", executor);
  },
});
