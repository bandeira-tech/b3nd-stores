/**
 * SqliteStore unit tests — runs the shared suite against an in-memory
 * mock that simulates SQLite text-column behavior (no JSONB).
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { SqliteStore } from "./store.ts";
import type { SqliteExecutor, SqliteExecutorResult } from "./mod.ts";

function createMockSqliteExecutor(): SqliteExecutor {
  const data = new Map<string, { uri: string; data: string }>();

  const executor: SqliteExecutor = {
    query: (sql: string, args?: unknown[]): SqliteExecutorResult => {
      const trimmed = sql.trim();
      const upper = trimmed.toUpperCase();

      if (upper.startsWith("CREATE")) return { rows: [] };
      if (upper === "SELECT 1") return { rows: [{ "1": 1 }] };

      if (upper.startsWith("INSERT")) {
        const uri = args![0] as string;
        const dataJson = args![1] as string;
        data.set(uri, { uri, data: dataJson });
        return { rows: [], rowCount: 1 };
      }

      // Count: SELECT COUNT(*) AS n FROM X WHERE uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%'
      if (upper.startsWith("SELECT COUNT(")) {
        const prefix = args![0] as string;
        const n = [...data.values()].filter((r) =>
          r.uri.startsWith(prefix) && !r.uri.slice(prefix.length).includes("/")
        ).length;
        return { rows: [{ n }] };
      }

      // ls: SELECT [uri | uri, data] FROM X WHERE uri LIKE ? || '%' AND uri NOT LIKE ? || '%/%' [ORDER BY] [LIMIT/OFFSET]
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

        if (upper.includes("LIMIT")) {
          // args: [prefix, prefix, limit, offset]
          const limit = args![2] as number;
          const offset = (args![3] as number) ?? 0;
          rows = rows.slice(offset, offset + limit);
        }

        const selectsData = /SELECT\s+URI\s*,\s*DATA/.test(upper);
        return {
          rows: rows.map((r) =>
            selectsData ? { uri: r.uri, data: r.data } : { uri: r.uri }
          ),
        };
      }

      if (upper.startsWith("SELECT") && upper.includes("WHERE URI = ?")) {
        const uri = args![0] as string;
        const row = data.get(uri);
        if (!row) return { rows: [] };
        return { rows: [{ data: row.data }] };
      }

      if (upper.startsWith("DELETE")) {
        const uri = args![0] as string;
        data.delete(uri);
        return { rows: [], rowCount: 1 };
      }

      return { rows: [] };
    },

    transaction: <T>(fn: (tx: SqliteExecutor) => T): T => fn(executor),
  };

  return executor;
}

runSharedStoreSuite("SqliteStore", {
  create: () => new SqliteStore("test", createMockSqliteExecutor()),
});
