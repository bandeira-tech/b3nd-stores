/**
 * S3Store Integration Tests
 *
 * Runs the shared store suite against a real MinIO S3 instance.
 * Requires a running MinIO with a pre-created public bucket.
 *
 * In CI, the bucket is created by the workflow via `mc`.
 * Locally: cd /Users/m0/ws/b3nd && make up p=test
 *
 * Env: S3_ENDPOINT (default: http://localhost:59000)
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { S3Store } from "./store.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";
import type { S3Executor } from "./mod.ts";
import type { StorePayload } from "../types.ts";

const S3_ENDPOINT = Deno.env.get("S3_ENDPOINT") ??
  "http://localhost:59000";
const BUCKET = "b3nd-inttest";

function createS3Executor(): S3Executor {
  function url(key: string): string {
    return `${S3_ENDPOINT}/${BUCKET}/${key}`;
  }

  return {
    async putObject(
      key: string,
      body: StorePayload,
      contentType: string,
    ): Promise<void> {
      // Collect streams to bytes before the PUT. A streaming body to a
      // real S3-compatible endpoint needs sigv4 chunked signing (or
      // multipart upload); Deno's bare `fetch` also requires
      // `duplex: "half"` to send a stream body and S3 / MinIO won't
      // happily accept `Transfer-Encoding: chunked` without it. The
      // test executor stays bytes-only for the wire; the Store still
      // accepts the union and the buffering lives in the executor.
      const bytes = body instanceof Uint8Array
        ? body
        : new Uint8Array(await new Response(body).arrayBuffer());
      const res = await fetch(url(key), {
        method: "PUT",
        headers: { "Content-Type": contentType },
        body: bytes as BodyInit,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 PUT failed: ${res.status} ${text}`);
      }
      await res.text();
    },

    async getObject(
      key: string,
    ): Promise<ReadableStream<Uint8Array> | null> {
      const res = await fetch(url(key), { method: "GET" });
      if (res.status === 404) {
        await res.text();
        return null;
      }
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 GET failed: ${res.status} ${text}`);
      }
      // `res.body` is already a `ReadableStream<Uint8Array>` — return
      // it directly so large objects don't need to fit in memory.
      return res.body;
    },

    async deleteObject(key: string): Promise<void> {
      const res = await fetch(url(key), { method: "DELETE" });
      if (!res.ok && res.status !== 404) {
        const text = await res.text();
        throw new Error(`S3 DELETE failed: ${res.status} ${text}`);
      }
      await res.text();
    },

    async listObjects(prefix: string): Promise<string[]> {
      const params = new URLSearchParams({
        "list-type": "2",
        prefix,
      });
      const res = await fetch(
        `${S3_ENDPOINT}/${BUCKET}?${params.toString()}`,
        { method: "GET" },
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 LIST failed: ${res.status} ${text}`);
      }
      const xml = await res.text();
      const keys: string[] = [];
      const regex = /<Key>([^<]+)<\/Key>/g;
      let match;
      while ((match = regex.exec(xml)) !== null) {
        keys.push(match[1]);
      }
      return keys;
    },

    async headBucket(): Promise<boolean> {
      try {
        const res = await fetch(`${S3_ENDPOINT}/${BUCKET}`, {
          method: "HEAD",
        });
        return res.ok;
      } catch {
        return false;
      }
    },
  };
}

// Each test gets a unique key prefix so writes don't leak across the
// shared bucket. The bucket itself stays — fresh in CI per workflow
// run, and otherwise tolerant of leftover keys from prior runs.
let testCount = 0;

runSharedStoreSuite("S3Store (integration)", {
  create: () => {
    const executor = createS3Executor();
    const prefix = `inttest-${Date.now()}-${++testCount}/`;
    return new S3Store(BUCKET, executor, prefix);
  },
});

// ── Native entity layout ──────────────────────────────────────────

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

function freshStore(): S3Store {
  const executor = createS3Executor();
  const prefix = `inttest-${Date.now()}-${++testCount}/`;
  return new S3Store(BUCKET, executor, prefix);
}

Deno.test({
  name: "S3Store (integration) - write/read round-trip on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = freshStore();
    const support = await store.ensureEntity(userSchema);
    assertEquals(support.entity, "users");
    assertEquals(support.unsupported, []);

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
  name: "S3Store (integration) - strict validation rejects extras",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = freshStore();
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
  name: "S3Store (integration) - ls/count on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = freshStore();
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
  },
});

Deno.test({
  name: "S3Store (integration) - delete removes from the entity prefix",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = freshStore();
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
  name: "S3Store (integration) - unsupported tags surface in EntitySupport",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = freshStore();
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
