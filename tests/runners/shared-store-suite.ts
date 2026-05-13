/**
 * Shared Test Suite for Store Interface
 *
 * Validates every Store implementation against the b3nd-save
 * bytes-only contract:
 * - write(entries: { uri, payload: Uint8Array }[]) → StoreWriteResult[]
 * - read(urls)                                     → Output<T>[]
 * - delete(uris)                                   → DeleteResult[]
 * - status()                                       → StatusResult  (advertises `fns`)
 * - capabilities()                                 → StoreCapabilities (optional)
 *
 * Conventions enforced here:
 * - `payload` is `Uint8Array | ReadableStream<Uint8Array>` end-to-end —
 *   Stores never inspect content. The suite collects either shape into
 *   `Uint8Array` for assertions via `payloadBytes`.
 * - `fn=read` miss → payload is `undefined`.
 * - `fn=ls` is SHALLOW: direct leaves only (entries whose uri is
 *   `prefix + <segment>` with no further `/`). Subtree-only entries
 *   are absent from `ls` and `count`. `format=full` returns
 *   `Output[]`; `format=uris` returns `string[]`.
 * - `fn=count` returns the number of direct leaves under the prefix.
 * - Unsupported read params (`pattern`, `cursor`, unknown `sortBy`,
 *   unknown `format`) THROW — they are programmer errors, not misses.
 *
 * Each store test file imports and runs this suite with a factory.
 */

/// <reference lib="deno.ns" />

import { assertEquals, assertRejects } from "jsr:@std/assert";
import type { Output } from "@bandeira-tech/b3nd-core/types";
import type { Store } from "../../src/types.ts";

const enc = (s: string): Uint8Array => new TextEncoder().encode(s);

export interface StoreTestConfig {
  /** Factory that returns a fresh Store for each test. */
  create: () => Store | Promise<Store>;

  /** Defaults to true. Disable for write-only sinks (e.g. console). */
  supportsRead?: boolean;

  /** Defaults to `supportsRead`. */
  supportsLs?: boolean;

  /** Defaults to `supportsLs`. */
  supportsCount?: boolean;
}

function payloadOf(out: Output<unknown>): unknown {
  return out[1];
}

function uriOf(out: Output): string {
  return out[0];
}

/**
 * Collect a `Uint8Array | ReadableStream<Uint8Array>` payload to
 * bytes. The shared suite asserts byte-level equality regardless of
 * which shape the backend returned (buffered backends yield
 * `Uint8Array`; streamer backends yield `ReadableStream`).
 */
async function payloadBytes(value: unknown): Promise<Uint8Array> {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ReadableStream) {
    return new Uint8Array(
      await new Response(value as BodyInit).arrayBuffer(),
    );
  }
  throw new Error(
    `expected Uint8Array | ReadableStream, got ${
      value === null ? "null" : typeof value
    }`,
  );
}

async function assertBytesEqual(
  actual: unknown,
  expected: Uint8Array,
  msg?: string,
): Promise<void> {
  const bytes = await payloadBytes(actual);
  assertEquals(Array.from(bytes), Array.from(expected), msg);
}

export function runSharedStoreSuite(
  suiteName: string,
  config: StoreTestConfig,
) {
  const noSanitize = { sanitizeOps: false, sanitizeResources: false };
  const supportsRead = config.supportsRead !== false;
  const supportsLs = config.supportsLs ?? supportsRead;
  const supportsCount = config.supportsCount ?? supportsLs;

  const t = (name: string, fn: () => void | Promise<void>) =>
    Deno.test({ name: `${suiteName} — ${name}`, ...noSanitize, fn });

  // ── Write ─────────────────────────────────────────────────────────

  t("write single entry", async () => {
    const store = await Promise.resolve(config.create());
    const results = await store.write([
      { uri: "store://app/config", payload: enc("dark") },
    ]);
    assertEquals(results.length, 1);
    assertEquals(results[0].success, true);
  });

  t("write batch of entries", async () => {
    const store = await Promise.resolve(config.create());
    const results = await store.write([
      { uri: "store://app/a", payload: enc("A") },
      { uri: "store://app/b", payload: enc("B") },
      { uri: "store://app/c", payload: enc("C") },
    ]);
    assertEquals(results.length, 3);
    assertEquals(results.every((r) => r.success), true);
  });

  if (!supportsRead) {
    // Write-only stores stop here for round-trip coverage; the rest of
    // the suite assumes read-back.
    t("status returns healthy", async () => {
      const store = await Promise.resolve(config.create());
      const status = await store.status();
      assertEquals(status.status, "healthy");
    });
    return;
  }

  // ── Read: point reads ─────────────────────────────────────────────

  t("write and read back returns tuple [uri, bytes]", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://app/config", payload: enc("dark") },
    ]);
    const results = await store.read(["store://app/config"]);
    assertEquals(results.length, 1);
    assertEquals(uriOf(results[0]), "store://app/config");
    await assertBytesEqual(payloadOf(results[0]), enc("dark"));
  });

  t("batch read returns one Output per input url, in order", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://app/a", payload: enc("A") },
      { uri: "store://app/b", payload: enc("B") },
      { uri: "store://app/c", payload: enc("C") },
    ]);
    const urls = ["store://app/a", "store://app/b", "store://app/c"];
    const results = await store.read(urls);
    assertEquals(results.length, 3);
    assertEquals(results.map(uriOf), urls);
    await assertBytesEqual(payloadOf(results[0]), enc("A"));
    await assertBytesEqual(payloadOf(results[1]), enc("B"));
    await assertBytesEqual(payloadOf(results[2]), enc("C"));
  });

  t("write overwrites existing value", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", payload: enc("old") }]);
    await store.write([{ uri: "store://app/x", payload: enc("new") }]);
    const results = await store.read(["store://app/x"]);
    await assertBytesEqual(payloadOf(results[0]), enc("new"));
  });

  // ── Binary payload shapes ─────────────────────────────────────────

  t("read/write empty payload", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://bytes/empty", payload: new Uint8Array(0) },
    ]);
    const results = await store.read(["store://bytes/empty"]);
    await assertBytesEqual(payloadOf(results[0]), new Uint8Array(0));
  });

  t("read/write payload covering all byte values", async () => {
    const store = await Promise.resolve(config.create());
    const all = new Uint8Array(256);
    for (let i = 0; i < 256; i++) all[i] = i;
    await store.write([{ uri: "store://bytes/all", payload: all }]);
    const results = await store.read(["store://bytes/all"]);
    await assertBytesEqual(payloadOf(results[0]), all);
  });

  t("read/write large payload (32 KB random bytes)", async () => {
    const store = await Promise.resolve(config.create());
    const big = crypto.getRandomValues(new Uint8Array(32 * 1024));
    await store.write([{ uri: "store://bytes/big", payload: big }]);
    const results = await store.read(["store://bytes/big"]);
    await assertBytesEqual(payloadOf(results[0]), big);
  });

  t("write accepts a ReadableStream payload", async () => {
    const store = await Promise.resolve(config.create());
    const expected = enc("streamed content");
    const stream = new Response(expected as BodyInit).body!;
    await store.write([{ uri: "store://stream/x", payload: stream }]);
    const results = await store.read(["store://stream/x"]);
    await assertBytesEqual(payloadOf(results[0]), expected);
  });

  // ── Miss convention: payload === undefined ────────────────────────

  t("read miss returns payload undefined", async () => {
    const store = await Promise.resolve(config.create());
    const results = await store.read(["store://app/missing"]);
    assertEquals(results.length, 1);
    assertEquals(uriOf(results[0]), "store://app/missing");
    assertEquals(payloadOf(results[0]), undefined);
  });

  t("read with partial misses keeps positional order", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://app/exists", payload: enc("yes") },
    ]);
    const urls = ["store://app/exists", "store://app/missing"];
    const results = await store.read(urls);
    assertEquals(results.length, 2);
    assertEquals(uriOf(results[0]), "store://app/exists");
    await assertBytesEqual(payloadOf(results[0]), enc("yes"));
    assertEquals(uriOf(results[1]), "store://app/missing");
    assertEquals(payloadOf(results[1]), undefined);
  });

  // ── Read echoes input url including query string ─────────────────

  t("read echoes input url verbatim (with query string)", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", payload: enc("v") }]);
    const url = "store://app/x?fn=read";
    const results = await store.read([url]);
    assertEquals(uriOf(results[0]), url);
    await assertBytesEqual(payloadOf(results[0]), enc("v"));
  });

  // ── ls (trailing slash / fn=ls) ───────────────────────────────────

  if (supportsLs) {
    t("trailing-slash defaults to fn=ls (returns Output[])", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://users/alice", payload: enc("Alice") },
        { uri: "store://users/bob", payload: enc("Bob") },
      ]);
      const results = await store.read(["store://users/"]);
      assertEquals(results.length, 1);
      // The single Output payload is itself an Output[] of children.
      const children = payloadOf(results[0]) as Output[];
      const uris = children.map(uriOf).sort();
      assertEquals(uris, ["store://users/alice", "store://users/bob"]);
    });

    t("ls is shallow — nested entries are absent", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://users/alice", payload: enc("alice-leaf") },
        { uri: "store://users/bob/posts/1", payload: enc("deep") },
      ]);
      const results = await store.read(["store://users/"]);
      const children = payloadOf(results[0]) as Output[];
      const uris = children.map(uriOf);
      assertEquals(uris, ["store://users/alice"]);
    });

    t("ls with format=uris returns string[]", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://t/a", payload: enc("1") },
        { uri: "store://t/b", payload: enc("2") },
      ]);
      const results = await store.read([
        "store://t/?fn=ls&format=uris&sortBy=uri",
      ]);
      const uris = payloadOf(results[0]) as string[];
      assertEquals(uris, ["store://t/a", "store://t/b"]);
    });

    t("ls supports sortBy=uri (asc/desc)", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://s/c", payload: enc("3") },
        { uri: "store://s/a", payload: enc("1") },
        { uri: "store://s/b", payload: enc("2") },
      ]);
      const asc = await store.read([
        "store://s/?fn=ls&sortBy=uri&format=uris",
      ]);
      assertEquals(payloadOf(asc[0]) as string[], [
        "store://s/a",
        "store://s/b",
        "store://s/c",
      ]);
      const desc = await store.read([
        "store://s/?fn=ls&sortBy=uri&sortOrder=desc&format=uris",
      ]);
      assertEquals(payloadOf(desc[0]) as string[], [
        "store://s/c",
        "store://s/b",
        "store://s/a",
      ]);
    });

    t("ls supports limit + page", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://p/a", payload: enc("1") },
        { uri: "store://p/b", payload: enc("2") },
        { uri: "store://p/c", payload: enc("3") },
        { uri: "store://p/d", payload: enc("4") },
      ]);
      const p1 = await store.read([
        "store://p/?fn=ls&sortBy=uri&limit=2&page=1&format=uris",
      ]);
      assertEquals(payloadOf(p1[0]) as string[], [
        "store://p/a",
        "store://p/b",
      ]);
      const p2 = await store.read([
        "store://p/?fn=ls&sortBy=uri&limit=2&page=2&format=uris",
      ]);
      assertEquals(payloadOf(p2[0]) as string[], [
        "store://p/c",
        "store://p/d",
      ]);
    });

    t("ls of empty prefix returns empty list", async () => {
      const store = await Promise.resolve(config.create());
      const results = await store.read(["store://nothing/"]);
      const children = payloadOf(results[0]) as Output[];
      assertEquals(children, []);
    });

    t("ls throws on unsupported sortBy", async () => {
      const store = await Promise.resolve(config.create());
      await assertRejects(
        () => store.read(["store://t/?fn=ls&sortBy=payload"]),
        Error,
      );
    });

    t("ls throws on unsupported format", async () => {
      const store = await Promise.resolve(config.create());
      await assertRejects(
        () => store.read(["store://t/?fn=ls&format=weird"]),
        Error,
      );
    });

    t(
      "ls throws on pattern param (not supported in package baseline)",
      async () => {
        const store = await Promise.resolve(config.create());
        await assertRejects(
          () => store.read(["store://t/?fn=ls&pattern=*"]),
          Error,
        );
      },
    );
  }

  // ── count ────────────────────────────────────────────────────────

  if (supportsCount) {
    t("fn=count returns number of direct leaves", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://c/a", payload: enc("1") },
        { uri: "store://c/b", payload: enc("2") },
        { uri: "store://c/deep/x", payload: enc("9") },
      ]);
      const results = await store.read(["store://c/?fn=count"]);
      assertEquals(payloadOf(results[0]), 2);
    });

    t("fn=count returns 0 for empty prefix", async () => {
      const store = await Promise.resolve(config.create());
      const results = await store.read(["store://empty/?fn=count"]);
      assertEquals(payloadOf(results[0]), 0);
    });
  }

  // ── Unknown fn ───────────────────────────────────────────────────

  t("unknown fn throws", async () => {
    const store = await Promise.resolve(config.create());
    await assertRejects(
      () => store.read(["store://t/?fn=bogus"]),
      Error,
    );
  });

  // ── Delete ───────────────────────────────────────────────────────

  t("delete returns success", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", payload: enc("hello") }]);
    const results = await store.delete(["store://app/x"]);
    assertEquals(results.length, 1);
    assertEquals(results[0].success, true);
  });

  t("delete removes entry (read miss after delete)", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", payload: enc("hello") }]);
    await store.delete(["store://app/x"]);
    const results = await store.read(["store://app/x"]);
    assertEquals(payloadOf(results[0]), undefined);
  });

  t("delete nonexistent succeeds silently", async () => {
    const store = await Promise.resolve(config.create());
    const results = await store.delete(["store://app/missing"]);
    assertEquals(results.length, 1);
    assertEquals(results[0].success, true);
  });

  t("batch delete", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://app/a", payload: enc("A") },
      { uri: "store://app/b", payload: enc("B") },
      { uri: "store://app/c", payload: enc("C") },
    ]);
    await store.delete(["store://app/a", "store://app/c"]);
    const results = await store.read([
      "store://app/a",
      "store://app/b",
      "store://app/c",
    ]);
    assertEquals(payloadOf(results[0]), undefined);
    await assertBytesEqual(payloadOf(results[1]), enc("B"));
    assertEquals(payloadOf(results[2]), undefined);
  });

  // ── Status / Capabilities ────────────────────────────────────────

  t("status returns healthy and advertises fns", async () => {
    const store = await Promise.resolve(config.create());
    const status = await store.status();
    assertEquals(status.status, "healthy");
    if (status.fns) {
      // Every supported fn should be listed; we don't insist on exact
      // set so stores can advertise x-* extensions too.
      assertEquals(status.fns.includes("read"), true);
      if (supportsLs) assertEquals(status.fns.includes("ls"), true);
      if (supportsCount) assertEquals(status.fns.includes("count"), true);
    }
  });

  t("capabilities returns valid shape (if implemented)", async () => {
    const store = await Promise.resolve(config.create());
    if (store.capabilities) {
      const caps = store.capabilities();
      if (caps.atomicBatch !== undefined) {
        assertEquals(typeof caps.atomicBatch, "boolean");
      }
    }
  });
}
