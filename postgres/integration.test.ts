/**
 * PostgresStore Integration Tests
 *
 * Runs the shared store suite against a real PostgreSQL database.
 * Requires a running Postgres instance — see CI workflow or:
 *   cd /Users/m0/ws/b3nd && make up p=test
 *
 * Env: POSTGRES_URL (default: postgresql://postgres:postgres@localhost:55432/b3nd_test)
 */

/// <reference lib="deno.ns" />

import { Client } from "pg";
import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { PostgresStore } from "./store.ts";
import { generatePostgresSchema } from "./schema.ts";
import type { SqlExecutor, SqlExecutorResult } from "./mod.ts";

const TABLE_PREFIX = "inttest";
const POSTGRES_URL = Deno.env.get("POSTGRES_URL") ??
  "postgresql://postgres:postgres@localhost:55432/b3nd_test";

let client: Client;

async function createPostgresExecutor(): Promise<SqlExecutor> {
  client = new Client(POSTGRES_URL);
  await client.connect();

  const executor: SqlExecutor = {
    async query(sql: string, args?: unknown[]): Promise<SqlExecutorResult> {
      // deno-lint-ignore no-explicit-any
      const res = await client.query(sql, args as any[]);
      return { rows: res.rows as unknown[], rowCount: res.rowCount ?? 0 };
    },
    async transaction<T>(fn: (tx: SqlExecutor) => Promise<T>): Promise<T> {
      await client.query("BEGIN");
      try {
        const txExecutor: SqlExecutor = {
          query: async (sql, args) => {
            // deno-lint-ignore no-explicit-any
            const res = await client.query(sql, args as any[]);
            return {
              rows: res.rows as unknown[],
              rowCount: res.rowCount ?? 0,
            };
          },
          transaction: () => {
            throw new Error("Nested transactions not supported");
          },
        };
        const result = await fn(txExecutor);
        await client.query("COMMIT");
        return result;
      } catch (error) {
        await client.query("ROLLBACK");
        throw error;
      }
    },
  };

  // Initialize schema (execute as one statement — DDL contains $$ blocks)
  const ddl = generatePostgresSchema(TABLE_PREFIX);
  await executor.query(ddl);

  return executor;
}

runSharedStoreSuite("PostgresStore (integration)", {
  create: async () => {
    const executor = await createPostgresExecutor();
    // Clean previous test data
    await executor.query(`DELETE FROM ${TABLE_PREFIX}_data`);
    return new PostgresStore(TABLE_PREFIX, executor);
  },
});

// Cleanup after all tests
Deno.test({
  name: "PostgresStore (integration) - cleanup",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    try {
      await client.query(
        `DROP TABLE IF EXISTS ${TABLE_PREFIX}_data CASCADE`,
      );
      await client.query(
        `DROP FUNCTION IF EXISTS update_${TABLE_PREFIX}_updated_at_column() CASCADE`,
      );
      await client.query(
        `DROP VIEW IF EXISTS ${TABLE_PREFIX}_data_by_program CASCADE`,
      );
    } finally {
      await client.end();
    }
  },
});
