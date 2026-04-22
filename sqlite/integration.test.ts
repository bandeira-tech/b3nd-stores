/**
 * SqliteStore Integration Tests
 *
 * Runs the shared store suite against a real SQLite database (in-memory).
 * No external service needed — uses @db/sqlite with :memory:.
 */

/// <reference lib="deno.ns" />

import { type BindValue, Database } from "@db/sqlite";
import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { SqliteStore } from "./store.ts";
import { generateSqliteSchema } from "./schema.ts";
import type { SqliteExecutor, SqliteExecutorResult } from "./mod.ts";

const TABLE_PREFIX = "inttest";

function createSqliteExecutor(): { executor: SqliteExecutor; db: Database } {
  const db = new Database(":memory:");
  db.exec("PRAGMA journal_mode=WAL");

  const executor: SqliteExecutor = {
    query(sql: string, args?: unknown[]): SqliteExecutorResult {
      const stmt = db.prepare(sql);
      const isQuery = /^\s*(SELECT|PRAGMA|WITH|EXPLAIN)\b/i.test(sql);

      if (isQuery) {
        const rows = stmt.all(...((args ?? []) as BindValue[])) as Record<
          string,
          unknown
        >[];
        return { rows, rowCount: rows.length };
      } else {
        stmt.run(...((args ?? []) as BindValue[]));
        return { rows: [], rowCount: db.changes };
      }
    },

    transaction<T>(fn: (tx: SqliteExecutor) => T): T {
      let result: T;
      db.exec("BEGIN");
      try {
        const txExecutor: SqliteExecutor = {
          query(sql: string, args?: unknown[]): SqliteExecutorResult {
            const stmt = db.prepare(sql);
            const isQuery = /^\s*(SELECT|PRAGMA|WITH|EXPLAIN)\b/i.test(sql);
            if (isQuery) {
              const rows = stmt.all(
                ...((args ?? []) as BindValue[]),
              ) as Record<string, unknown>[];
              return { rows, rowCount: rows.length };
            } else {
              stmt.run(...((args ?? []) as BindValue[]));
              return { rows: [], rowCount: db.changes };
            }
          },
          transaction: () => {
            throw new Error("Nested transactions not supported");
          },
        };
        result = fn(txExecutor);
        db.exec("COMMIT");
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
      return result;
    },

    cleanup() {
      db.close();
    },
  };

  // Initialize schema
  const ddl = generateSqliteSchema(TABLE_PREFIX);
  db.exec(ddl);

  return { executor, db };
}

runSharedStoreSuite("SqliteStore (integration)", {
  create: () => {
    const { executor } = createSqliteExecutor();
    return new SqliteStore(TABLE_PREFIX, executor);
  },
});
