/**
 * FsStore Integration Tests
 *
 * Runs the shared store suite against the real filesystem.
 * No external service needed — uses Deno.makeTempDir().
 */

/// <reference lib="deno.ns" />

import { assert, assertEquals } from "@std/assert";
import { ensureDir } from "@std/fs/ensure-dir";
import { dirname } from "@std/path";
import { runSharedStoreSuite } from "../../tests/runners/shared-store-suite.ts";
import { FsStore } from "./store.ts";
import { type EntityRecord, type EntitySchema, TYPE_TAGS } from "../entity.ts";
import type { FsExecutor } from "./mod.ts";
import type { StorePayload } from "../types.ts";

function createFsExecutor(_rootDir: string): FsExecutor {
  return {
    async readFile(path: string): Promise<ReadableStream<Uint8Array>> {
      const file = await Deno.open(path, { read: true });
      return file.readable;
    },

    async writeFile(path: string, content: StorePayload): Promise<void> {
      await ensureDir(dirname(path));
      if (content instanceof Uint8Array) {
        await Deno.writeFile(path, content);
        return;
      }
      // ReadableStream: open the file and pipe directly so large
      // payloads never need to fit in memory.
      const file = await Deno.open(path, {
        write: true,
        create: true,
        truncate: true,
      });
      await content.pipeTo(file.writable);
    },

    async removeFile(path: string): Promise<void> {
      await Deno.remove(path);
    },

    async exists(path: string): Promise<boolean> {
      try {
        await Deno.stat(path);
        return true;
      } catch {
        return false;
      }
    },

    async listFiles(dir: string): Promise<string[]> {
      const files: string[] = [];
      try {
        for await (const entry of Deno.readDir(dir)) {
          if (entry.isFile) files.push(entry.name);
        }
      } catch {
        return [];
      }
      return files;
    },
  };
}

// Each test gets a fresh temp directory
let lastTempDir: string | undefined;

runSharedStoreSuite("FsStore (integration)", {
  create: async () => {
    const tempDir = await Deno.makeTempDir({ prefix: "b3nd_fs_test_" });
    lastTempDir = tempDir;
    const executor = createFsExecutor(tempDir);
    return new FsStore(tempDir, executor);
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

async function freshStore(): Promise<FsStore> {
  const tempDir = await Deno.makeTempDir({ prefix: "b3nd_fs_entity_" });
  lastTempDir = tempDir;
  const executor = createFsExecutor(tempDir);
  return new FsStore(tempDir, executor);
}

Deno.test({
  name: "FsStore (integration) - write/read round-trip on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
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
  name: "FsStore (integration) - strict validation rejects extras",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
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
  name: "FsStore (integration) - ls/count on a custom entity",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
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
  name: "FsStore (integration) - delete removes from the entity directory",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
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
  name: "FsStore (integration) - unsupported tags surface in EntitySupport",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const store = await freshStore();
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

// Cleanup after all tests
Deno.test({
  name: "FsStore (integration) - cleanup",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    if (lastTempDir) {
      await Deno.remove(lastTempDir, { recursive: true }).catch(() => {});
    }
  },
});
