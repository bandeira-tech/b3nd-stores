/**
 * SqliteStore Integration Tests
 *
 * Runs the shared store suite against a real SQLite database (in-memory).
 * No external service needed — uses @db/sqlite with :memory:.
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { type BindValue, Database } from "@db/sqlite";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { SqliteStore } from "./store.ts";
import { generateSqliteSchema } from "./schema.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";
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

// ── Native entity tables ──────────────────────────────────────────

const userSchema: EntitySchema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
    { name: "active", type: [TYPE_TAGS.BOOLEAN] },
    { name: "extras", type: [TYPE_TAGS.JSON] },
    { name: "avatar", type: [TYPE_TAGS.BYTES] },
  ],
};

const postSchema: EntitySchema = {
  name: "posts",
  fields: [
    { name: "title", type: [TYPE_TAGS.STRING] },
    { name: "stars", type: [TYPE_TAGS.BIGINT] },
  ],
};

function freshStore(): { store: SqliteStore; db: Database } {
  const { executor, db } = createSqliteExecutor();
  return { store: new SqliteStore(TABLE_PREFIX, executor), db };
}

Deno.test({
  name:
    "SqliteStore (integration) - ensureEntity provisions a per-entity table",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, db } = freshStore();
    const support = await store.ensureEntity(userSchema);
    assertEquals(support.entity, "users");
    assertEquals(support.unsupported, []);
    assertEquals(
      support.supported.sort(),
      ["active", "age", "avatar", "extras", "name"],
    );
    const cols = db
      .prepare(`PRAGMA table_info(${TABLE_PREFIX}_users_data)`)
      .all() as Array<{ name: string; type: string }>;
    const byName = Object.fromEntries(cols.map((c) => [c.name, c.type]));
    assertEquals(byName.uri, "TEXT");
    assertEquals(byName.name, "TEXT");
    assertEquals(byName.age, "REAL");
    assertEquals(byName.active, "INTEGER");
    assertEquals(byName.extras, "TEXT");
    assertEquals(byName.avatar, "BLOB");
    db.close();
  },
});

Deno.test({
  name: "SqliteStore (integration) - write/read round-trip on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, db } = freshStore();
    await store.ensureEntity(userSchema);
    const avatar = new Uint8Array([1, 2, 3, 4, 5]);
    const [w] = await store.write(userSchema, [{
      uri: "data://users/alice",
      record: {
        name: "Alice",
        age: 30,
        active: true,
        extras: { tags: ["admin"] },
        avatar,
      },
    }]);
    assertEquals(w.success, true);

    const [[, rec]] = await store.read(userSchema, ["data://users/alice"]);
    const r = rec as EntityRecord;
    assertEquals(r.name, "Alice");
    assertEquals(r.age, 30);
    assertEquals(r.active, true);
    assertEquals(r.extras, { tags: ["admin"] });
    assert(r.avatar instanceof Uint8Array);
    assertEquals(Array.from(r.avatar as Uint8Array), [1, 2, 3, 4, 5]);
    db.close();
  },
});

Deno.test({
  name: "SqliteStore (integration) - strict validation rejects extra fields",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, db } = freshStore();
    await store.ensureEntity(userSchema);
    const [r] = await store.write(userSchema, [{
      uri: "data://users/x",
      record: { name: "X", age: 0, mystery: "not declared" } as EntityRecord,
    }]);
    assertEquals(r.success, false);
    assert(r.error?.includes("not declared"));
    assertEquals(r.errorDetail?.uri, "data://users/x");
    db.close();
  },
});

Deno.test({
  name: "SqliteStore (integration) - ls/count on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, db } = freshStore();
    await store.ensureEntity(postSchema);
    await store.write(postSchema, [
      { uri: "data://posts/a", record: { title: "A", stars: 1 } },
      { uri: "data://posts/b", record: { title: "B", stars: 2 } },
      { uri: "data://posts/sub/deep", record: { title: "deep", stars: 9 } },
    ]);
    const [[, count]] = await store.read<number>(postSchema, [
      "data://posts/?fn=count",
    ]);
    assertEquals(count, 2);
    const [[, uris]] = await store.read<string[]>(postSchema, [
      "data://posts/?fn=ls&format=uris&sortBy=uri",
    ]);
    assertEquals(uris, ["data://posts/a", "data://posts/b"]);
    const [[, children]] = await store.read<Array<[string, EntityRecord]>>(
      postSchema,
      ["data://posts/?fn=ls&sortBy=uri"],
    );
    assertEquals(children.map(([u]) => u), [
      "data://posts/a",
      "data://posts/b",
    ]);
    assertEquals(children[0][1].title, "A");
    db.close();
  },
});

Deno.test({
  name: "SqliteStore (integration) - delete removes from the entity table",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, db } = freshStore();
    await store.ensureEntity(userSchema);
    await store.write(userSchema, [{
      uri: "data://users/del",
      record: {
        name: "Del",
        age: 1,
        active: true,
        extras: {},
        avatar: new Uint8Array(0),
      },
    }]);
    const [d] = await store.delete(userSchema, ["data://users/del"]);
    assertEquals(d.success, true);
    const [[, rec]] = await store.read(userSchema, ["data://users/del"]);
    assertEquals(rec, undefined);
    db.close();
  },
});

Deno.test({
  name: "SqliteStore (integration) - unsupported tags surface in EntitySupport",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, db } = freshStore();
    const support = await store.ensureEntity({
      name: "weird",
      fields: [
        { name: "ok", type: [TYPE_TAGS.STRING] },
        { name: "money", type: ["some-protocol/money"] },
      ],
    });
    assertEquals(support.supported, ["ok"]);
    assertEquals(support.unsupported.map((u) => u.name), ["money"]);
    db.close();
  },
});
