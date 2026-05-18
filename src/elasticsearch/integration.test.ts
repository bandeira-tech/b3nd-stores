/**
 * ElasticsearchStore Integration Tests
 *
 * Runs the shared store suite against a real Elasticsearch cluster.
 * In CI the cluster is spun up as a service container. Locally:
 *   docker run -d --name es \
 *     -p 59200:9200 \
 *     -e discovery.type=single-node \
 *     -e xpack.security.enabled=false \
 *     -e ES_JAVA_OPTS="-Xms512m -Xmx512m" \
 *     elasticsearch:8.15.0
 *
 * Env: ES_ENDPOINT (default: http://localhost:59200)
 *
 * Test isolation: each test gets a unique `indexPrefix` so its indexes
 * don't collide with prior tests'. A final cleanup test deletes every
 * `inttest-*` index it can find so the cluster doesn't accumulate.
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { ElasticsearchStore } from "./store.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";
import type {
  ElasticsearchExecutor,
  ElasticsearchSearchResult,
} from "./mod.ts";

const ES_ENDPOINT = Deno.env.get("ES_ENDPOINT") ??
  "http://localhost:59200";
const PREFIX_BASE = "inttest";

function createElasticsearchExecutor(): ElasticsearchExecutor {
  const base = ES_ENDPOINT.replace(/\/+$/, "");

  async function jsonRequest(
    method: string,
    path: string,
    body?: unknown,
  ): Promise<Response> {
    return await fetch(`${base}${path}`, {
      method,
      headers: body !== undefined ? { "Content-Type": "application/json" } : {},
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
  }

  return {
    async index(index, id, body) {
      // PUT /{index}/_doc/{id}?refresh=wait_for ensures the document
      // is visible to the next search — without refresh, the shared
      // suite's "write then ls" pattern races against the indexer.
      const res = await jsonRequest(
        "PUT",
        `/${encodeURIComponent(index)}/_doc/${
          encodeURIComponent(id)
        }?refresh=wait_for`,
        body,
      );
      if (!res.ok) {
        throw new Error(
          `ES index failed: ${res.status} ${await res.text()}`,
        );
      }
      await res.text();
    },

    async get(index, id) {
      const res = await jsonRequest(
        "GET",
        `/${encodeURIComponent(index)}/_doc/${encodeURIComponent(id)}`,
      );
      if (res.status === 404) {
        await res.text();
        return null;
      }
      if (!res.ok) {
        throw new Error(
          `ES get failed: ${res.status} ${await res.text()}`,
        );
      }
      const json = await res.json() as {
        found: boolean;
        _source?: Record<string, unknown>;
      };
      return json.found ? (json._source ?? null) : null;
    },

    async search(index, body) {
      const res = await jsonRequest(
        "POST",
        `/${encodeURIComponent(index)}/_search`,
        body,
      );
      if (res.status === 404) {
        // Index doesn't exist yet — no hits.
        await res.text();
        return { hits: [] } as ElasticsearchSearchResult;
      }
      if (!res.ok) {
        throw new Error(
          `ES search failed: ${res.status} ${await res.text()}`,
        );
      }
      const json = await res.json() as {
        hits: {
          hits: Array<
            { _id: string; _source?: Record<string, unknown> }
          >;
        };
      };
      return { hits: json.hits.hits };
    },

    async count(index, body) {
      const res = await jsonRequest(
        "POST",
        `/${encodeURIComponent(index)}/_count`,
        body,
      );
      if (res.status === 404) {
        await res.text();
        return 0;
      }
      if (!res.ok) {
        throw new Error(
          `ES count failed: ${res.status} ${await res.text()}`,
        );
      }
      const json = await res.json() as { count: number };
      return json.count;
    },

    async delete(index, id) {
      const res = await jsonRequest(
        "DELETE",
        `/${encodeURIComponent(index)}/_doc/${
          encodeURIComponent(id)
        }?refresh=wait_for`,
      );
      // 404 is fine — deleting a missing doc/index is a no-op.
      if (!res.ok && res.status !== 404) {
        throw new Error(
          `ES delete failed: ${res.status} ${await res.text()}`,
        );
      }
      await res.text();
    },

    async ensureIndex(index, mappings) {
      const exists = await jsonRequest(
        "HEAD",
        `/${encodeURIComponent(index)}`,
      );
      await exists.text();
      if (exists.status === 200) return;
      const res = await jsonRequest(
        "PUT",
        `/${encodeURIComponent(index)}`,
        { mappings },
      );
      // 400 with `resource_already_exists_exception` is a race we
      // can swallow — another caller created it first.
      if (!res.ok && res.status !== 400) {
        throw new Error(
          `ES create index failed: ${res.status} ${await res.text()}`,
        );
      }
      await res.text();
    },

    async ping() {
      try {
        const res = await fetch(`${base}/`, { method: "GET" });
        return res.ok;
      } catch {
        return false;
      }
    },
  };
}

let testCount = 0;
const runId = Date.now();

runSharedStoreSuite("ElasticsearchStore (integration)", {
  create: () =>
    new ElasticsearchStore(
      `${PREFIX_BASE}_${runId}_${++testCount}`,
      createElasticsearchExecutor(),
    ),
});

// ── Native entity indices ─────────────────────────────────────────

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
    { name: "stars", type: [TYPE_TAGS.NUMBER] },
  ],
};

function freshEntityStore(): { store: ElasticsearchStore; prefix: string } {
  const prefix = `${PREFIX_BASE}_${runId}_${++testCount}`;
  return {
    store: new ElasticsearchStore(prefix, createElasticsearchExecutor()),
    prefix,
  };
}

Deno.test({
  name:
    "ElasticsearchStore (integration) - ensureEntity creates index with mappings",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store, prefix } = freshEntityStore();
    const support = await store.ensureEntity(userSchema);
    assertEquals(support.entity, "users");
    assertEquals(support.unsupported, []);
    assertEquals(
      support.supported.sort(),
      ["active", "age", "avatar", "extras", "name"],
    );
    const base = ES_ENDPOINT.replace(/\/+$/, "");
    const res = await fetch(`${base}/${prefix}_users_data/_mapping`);
    const json = await res.json() as Record<string, {
      mappings: { properties: Record<string, { type: string }> };
    }>;
    const props = json[`${prefix}_users_data`].mappings.properties;
    assertEquals(props.uri.type, "keyword");
    assertEquals(props.name.type, "keyword");
    assertEquals(props.age.type, "double");
    assertEquals(props.active.type, "boolean");
    assertEquals(props.avatar.type, "binary");
    assertEquals(props.extras.type, "object");
  },
});

Deno.test({
  name:
    "ElasticsearchStore (integration) - write/read round-trip on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store } = freshEntityStore();
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
  },
});

Deno.test({
  name: "ElasticsearchStore (integration) - strict validation rejects extras",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store } = freshEntityStore();
    await store.ensureEntity(userSchema);
    const [r] = await store.write(userSchema, [{
      uri: "data://users/x",
      record: { name: "X", age: 0, mystery: "not declared" } as EntityRecord,
    }]);
    assertEquals(r.success, false);
    assert(r.error?.includes("not declared"));
    assertEquals(r.errorDetail?.uri, "data://users/x");
  },
});

Deno.test({
  name: "ElasticsearchStore (integration) - ls/count on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store } = freshEntityStore();
    await store.ensureEntity(postSchema);
    await store.write(postSchema, [
      { uri: "data://posts/a", record: { title: "A", stars: 1 } },
      { uri: "data://posts/b", record: { title: "B", stars: 2 } },
      { uri: "data://posts/sub/deep", record: { title: "deep", stars: 9 } },
    ]);
    // ES is near-real-time; refresh isn't on writes since ensureIndex
    // doesn't set it. Wait briefly to let the documents become
    // searchable for the ls/count below.
    await new Promise((r) => setTimeout(r, 1500));
    const [[, count]] = await store.read<number>(postSchema, [
      "data://posts/?fn=count",
    ]);
    assertEquals(count, 2);
    const [[, uris]] = await store.read<string[]>(postSchema, [
      "data://posts/?fn=ls&format=uris&sortBy=uri",
    ]);
    assertEquals(uris, ["data://posts/a", "data://posts/b"]);
  },
});

Deno.test({
  name:
    "ElasticsearchStore (integration) - delete removes from the entity index",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store } = freshEntityStore();
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
  },
});

Deno.test({
  name:
    "ElasticsearchStore (integration) - unsupported tags surface in EntitySupport",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const { store } = freshEntityStore();
    const support = await store.ensureEntity({
      name: "weird",
      fields: [
        { name: "ok", type: [TYPE_TAGS.STRING] },
        { name: "money", type: ["some-protocol/money"] },
      ],
    });
    assertEquals(support.supported, ["ok"]);
    assertEquals(support.unsupported.map((u) => u.name), ["money"]);
  },
});

// Cleanup: drop every index this test run created. Uses a wildcard
// DELETE against the run-scoped prefix so we don't touch other runs.
Deno.test({
  name: "ElasticsearchStore (integration) - cleanup",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const base = ES_ENDPOINT.replace(/\/+$/, "");
    await fetch(
      `${base}/${PREFIX_BASE}_${runId}_*?ignore_unavailable=true`,
      { method: "DELETE" },
    ).catch(() => {});
  },
});
