/**
 * SqliteStore Tests
 *
 * Uses a mock SqliteExecutor that simulates SQLite text storage behavior.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { SqliteStore } from "./store.ts";
import type { SqliteExecutor, SqliteExecutorResult } from "./mod.ts";

/**
 * In-memory SQLite executor that simulates synchronous SQLite behavior.
 * SQLite stores JSON as text strings (no JSONB), so the mock returns
 * raw strings that SqliteStore parses with JSON.parse().
 */
function createMockSqliteExecutor(): SqliteExecutor {
  const data = new Map<
    string,
    Record<string, unknown>
  >();

  const executor: SqliteExecutor = {
    query: (sql: string, args?: unknown[]): SqliteExecutorResult => {
      const upper = sql.trim().toUpperCase();

      // CREATE TABLE / DDL
      if (upper.startsWith("CREATE")) {
        return { rows: [], rowCount: 0 };
      }

      // SELECT 1 (health check)
      if (upper === "SELECT 1") {
        return { rows: [{ "1": 1 }] };
      }

      // INSERT ... ON CONFLICT (upsert)
      if (upper.startsWith("INSERT")) {
        const uri = args![0] as string;
        const dataJson = args![1] as string;
        const valuesJson = args![2] as string;
        // SQLite stores text columns as strings
        data.set(uri, { uri, data: dataJson, values: valuesJson });
        return { rows: [], rowCount: 1 };
      }

      // SELECT with LIKE (list query)
      if (upper.includes("LIKE")) {
        const pattern = args![0] as string;
        const prefix = pattern.replace(/%$/, "");
        const rows = [...data.values()]
          .filter((r) => (r.uri as string).startsWith(prefix));
        return { rows };
      }

      // SELECT (single read)
      if (upper.startsWith("SELECT")) {
        const uri = args![0] as string;
        const row = data.get(uri);
        return { rows: row ? [row] : [] };
      }

      // DELETE
      if (upper.startsWith("DELETE")) {
        const uri = args![0] as string;
        data.delete(uri);
        return { rows: [], rowCount: 1 };
      }

      return { rows: [] };
    },

    transaction: <T>(fn: (tx: SqliteExecutor) => T): T => {
      return fn(executor);
    },
  };

  return executor;
}

runSharedStoreSuite("SqliteStore", {
  create: () => {
    const executor = createMockSqliteExecutor();
    return new SqliteStore("test", executor);
  },
});
