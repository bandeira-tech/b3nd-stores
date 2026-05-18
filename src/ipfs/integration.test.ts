/**
 * IpfsStore Integration Tests
 *
 * Runs the shared store suite against a real Kubo IPFS node.
 * Requires a running Kubo instance — see CI workflow or:
 *   cd /Users/m0/ws/b3nd && make up p=test
 *
 * Env: IPFS_API_URL (default: http://localhost:55001)
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { IpfsStore } from "./store.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";
import type { IpfsExecutor } from "./mod.ts";
import type { StorePayload } from "../types.ts";

const IPFS_API_URL = Deno.env.get("IPFS_API_URL") ??
  "http://localhost:55001";

function createIpfsExecutor(): IpfsExecutor {
  const base = IPFS_API_URL.replace(/\/+$/, "");

  return {
    async add(content: StorePayload): Promise<string> {
      // The IPFS `add` HTTP endpoint takes multipart form data. Both
      // bytes and streams collapse into a `Blob` body chunk; the
      // `Blob` constructor accepts either as a BlobPart, so this stays
      // streaming-friendly when the caller provides a stream.
      const form = new FormData();
      const blob = content instanceof ReadableStream
        ? await new Response(content).blob()
        : new Blob([content as BlobPart], {
          type: "application/octet-stream",
        });
      form.append("file", blob);

      const res = await fetch(`${base}/api/v0/add?pin=false&quiet=true`, {
        method: "POST",
        body: form,
      });

      if (!res.ok) {
        throw new Error(`IPFS add failed: ${res.status} ${await res.text()}`);
      }

      const json = await res.json();
      return json.Hash;
    },

    async cat(cid: string): Promise<ReadableStream<Uint8Array>> {
      const res = await fetch(
        `${base}/api/v0/cat?arg=${encodeURIComponent(cid)}`,
        { method: "POST" },
      );

      if (!res.ok) {
        throw new Error(`IPFS cat failed: ${res.status} ${await res.text()}`);
      }

      if (!res.body) {
        throw new Error("IPFS cat returned empty response body");
      }
      return res.body;
    },

    async pin(cid: string): Promise<void> {
      const res = await fetch(
        `${base}/api/v0/pin/add?arg=${encodeURIComponent(cid)}`,
        { method: "POST" },
      );

      if (!res.ok) {
        throw new Error(`IPFS pin failed: ${res.status} ${await res.text()}`);
      }
      await res.text();
    },

    async unpin(cid: string): Promise<void> {
      const res = await fetch(
        `${base}/api/v0/pin/rm?arg=${encodeURIComponent(cid)}`,
        { method: "POST" },
      );

      if (!res.ok) {
        const body = await res.text();
        if (!body.includes("not pinned")) {
          throw new Error(`IPFS unpin failed: ${res.status} ${body}`);
        }
      } else {
        await res.text();
      }
    },

    async listPins(): Promise<string[]> {
      const res = await fetch(`${base}/api/v0/pin/ls?type=recursive`, {
        method: "POST",
      });

      if (!res.ok) {
        throw new Error(
          `IPFS pin ls failed: ${res.status} ${await res.text()}`,
        );
      }

      const json = await res.json();
      return Object.keys(json.Keys || {});
    },

    async isOnline(): Promise<boolean> {
      try {
        const res = await fetch(`${base}/api/v0/id`, { method: "POST" });
        return res.ok;
      } catch {
        return false;
      }
    },
  };
}

runSharedStoreSuite("IpfsStore (integration)", {
  create: () => {
    const executor = createIpfsExecutor();
    return new IpfsStore(executor);
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

function freshStore(): IpfsStore {
  return new IpfsStore(createIpfsExecutor());
}

Deno.test({
  name: "IpfsStore (integration) - write/read round-trip on a custom entity",
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
  name: "IpfsStore (integration) - strict validation rejects extras",
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
  name: "IpfsStore (integration) - ls/count on a custom entity",
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
  name: "IpfsStore (integration) - delete removes from the entity index",
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
  name: "IpfsStore (integration) - unsupported tags surface in EntitySupport",
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
