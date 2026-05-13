/**
 * PostgresStore unit tests — runs the shared suite against an
 * in-memory mock executor that simulates Postgres JSONB behavior.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../../../tests/runners/shared-store-suite.ts";
import { PostgresStore } from "./store.ts";
import type { SqlExecutor, SqlExecutorResult } from "./mod.ts";

/**
 * In-memory SQL executor that simulates the subset of Postgres
 * behavior `PostgresStore` relies on. JSON columns round-trip as
 * parsed objects (mimicking JSONB).
 */
function createMockSqlExecutor(): SqlExecutor {
  const data = new Map<string, { uri: string; data: string }>();

  const executor: SqlExecutor = {
    query: (
      sql: string,
      args?: unknown[],
    ): Promise<SqlExecutorResult> => {
      const trimmed = sql.trim();
      const upper = trimmed.toUpperCase();

      // DDL
      if (upper.startsWith("CREATE")) return Promise.resolve({ rows: [] });

      // Health
      if (upper === "SELECT 1") {
        return Promise.resolve({ rows: [{ "?column?": 1 }] });
      }

      // Upsert: INSERT INTO X (uri, data) VALUES ($1, $2::jsonb) ON CONFLICT ...
      if (upper.startsWith("INSERT")) {
        const uri = args![0] as string;
        const dataJson = args![1] as string;
        data.set(uri, { uri, data: dataJson });
        return Promise.resolve({ rows: [], rowCount: 1 });
      }

      // Count: SELECT COUNT(*)::int AS n FROM X WHERE uri LIKE $1 || '%' AND uri NOT LIKE $1 || '%/%'
      if (upper.startsWith("SELECT COUNT(")) {
        const prefix = args![0] as string;
        const n = [...data.values()].filter((r) =>
          r.uri.startsWith(prefix) && !r.uri.slice(prefix.length).includes("/")
        ).length;
        return Promise.resolve({ rows: [{ n }] });
      }

      // ls: SELECT [uri | uri, data] FROM X WHERE uri LIKE $1 || '%' AND uri NOT LIKE $1 || '%/%' [ORDER BY uri [DESC]] [LIMIT $n OFFSET $m]
      if (upper.includes("LIKE") && upper.includes("NOT LIKE")) {
        const prefix = args![0] as string;
        let rows = [...data.values()].filter((r) =>
          r.uri.startsWith(prefix) && !r.uri.slice(prefix.length).includes("/")
        );

        if (upper.includes("ORDER BY URI")) {
          const desc = upper.includes(" DESC");
          rows = rows.sort((a, b) =>
            desc ? b.uri.localeCompare(a.uri) : a.uri.localeCompare(b.uri)
          );
        }

        // LIMIT $n OFFSET $m — args[1] and args[2] when present
        if (upper.includes("LIMIT")) {
          const limit = args![1] as number;
          const offset = (args![2] as number) ?? 0;
          rows = rows.slice(offset, offset + limit);
        }

        const selectsData = /SELECT\s+URI\s*,\s*DATA/.test(upper);
        return Promise.resolve({
          rows: rows.map((r) =>
            selectsData
              ? { uri: r.uri, data: JSON.parse(r.data) }
              : { uri: r.uri }
          ),
        });
      }

      // Point read: SELECT data FROM X WHERE uri = $1
      if (upper.startsWith("SELECT") && upper.includes("WHERE URI = $1")) {
        const uri = args![0] as string;
        const row = data.get(uri);
        if (!row) return Promise.resolve({ rows: [] });
        return Promise.resolve({
          rows: [{ data: JSON.parse(row.data) }],
        });
      }

      // Delete
      if (upper.startsWith("DELETE")) {
        const uri = args![0] as string;
        data.delete(uri);
        return Promise.resolve({ rows: [], rowCount: 1 });
      }

      return Promise.resolve({ rows: [] });
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
  create: () => new PostgresStore("test", createMockSqlExecutor()),
});
