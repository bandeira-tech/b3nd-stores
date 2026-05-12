/**
 * Shared Test Suite for Store Interface
 *
 * Validates every Store implementation against the b3nd-core@0.15
 * contract:
 * - write(entries: { uri, data }[]) → StoreWriteResult[]
 * - read(urls)                       → Output<T>[]   (tuple [uri, payload])
 * - delete(uris)                     → DeleteResult[]
 * - status()                         → StatusResult  (advertises `fns`)
 * - capabilities()                   → StoreCapabilities (optional)
 *
 * Conventions enforced here (see project memory `core_upgrade`):
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
import type { Output, Store } from "@bandeira-tech/b3nd-core/types";

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

function payloadOf<T>(out: Output<T>): T {
  return out[1];
}

function uriOf(out: Output): string {
  return out[0];
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
      { uri: "store://app/config", data: { theme: "dark" } },
    ]);
    assertEquals(results.length, 1);
    assertEquals(results[0].success, true);
  });

  t("write batch of entries", async () => {
    const store = await Promise.resolve(config.create());
    const results = await store.write([
      { uri: "store://app/a", data: "A" },
      { uri: "store://app/b", data: "B" },
      { uri: "store://app/c", data: "C" },
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

  t("write and read back returns tuple [uri, payload]", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://app/config", data: { theme: "dark" } },
    ]);
    const results = await store.read(["store://app/config"]);
    assertEquals(results.length, 1);
    assertEquals(uriOf(results[0]), "store://app/config");
    assertEquals(payloadOf(results[0]), { theme: "dark" });
  });

  t("batch read returns one Output per input url, in order", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([
      { uri: "store://app/a", data: "A" },
      { uri: "store://app/b", data: "B" },
      { uri: "store://app/c", data: "C" },
    ]);
    const urls = ["store://app/a", "store://app/b", "store://app/c"];
    const results = await store.read(urls);
    assertEquals(results.length, 3);
    assertEquals(results.map(uriOf), urls);
    assertEquals(results.map(payloadOf), ["A", "B", "C"]);
  });

  t("write overwrites existing value", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", data: "old" }]);
    await store.write([{ uri: "store://app/x", data: "new" }]);
    const results = await store.read(["store://app/x"]);
    assertEquals(payloadOf(results[0]), "new");
  });

  // ── Scalar data types ─────────────────────────────────────────────

  for (
    const [name, value] of [
      ["string", "hello world"],
      ["number", 42],
      ["boolean", true],
      ["null", null],
      ["empty string", ""],
      ["zero", 0],
    ] as const
  ) {
    t(`read/write ${name} data`, async () => {
      const store = await Promise.resolve(config.create());
      const uri = `store://scalar/${name.replace(/\s+/g, "_")}`;
      await store.write([{ uri, data: value }]);
      const results = await store.read([uri]);
      assertEquals(payloadOf(results[0]), value);
    });
  }

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
      { uri: "store://app/exists", data: { ok: true } },
    ]);
    const urls = ["store://app/exists", "store://app/missing"];
    const results = await store.read(urls);
    assertEquals(results.length, 2);
    assertEquals(uriOf(results[0]), "store://app/exists");
    assertEquals(payloadOf(results[0]), { ok: true });
    assertEquals(uriOf(results[1]), "store://app/missing");
    assertEquals(payloadOf(results[1]), undefined);
  });

  // ── Read echoes input url including query string ─────────────────

  t("read echoes input url verbatim (with query string)", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", data: 1 }]);
    const url = "store://app/x?fn=read";
    const results = await store.read([url]);
    assertEquals(uriOf(results[0]), url);
    assertEquals(payloadOf(results[0]), 1);
  });

  // ── ls (trailing slash / fn=ls) ───────────────────────────────────

  if (supportsLs) {
    t("trailing-slash defaults to fn=ls (returns Output[])", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://users/alice", data: { name: "Alice" } },
        { uri: "store://users/bob", data: { name: "Bob" } },
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
        { uri: "store://users/alice", data: "alice-leaf" },
        { uri: "store://users/bob/posts/1", data: "deep" },
      ]);
      const results = await store.read(["store://users/"]);
      const children = payloadOf(results[0]) as Output[];
      const uris = children.map(uriOf);
      assertEquals(uris, ["store://users/alice"]);
    });

    t("ls with format=uris returns string[]", async () => {
      const store = await Promise.resolve(config.create());
      await store.write([
        { uri: "store://t/a", data: 1 },
        { uri: "store://t/b", data: 2 },
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
        { uri: "store://s/c", data: 3 },
        { uri: "store://s/a", data: 1 },
        { uri: "store://s/b", data: 2 },
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
        { uri: "store://p/a", data: 1 },
        { uri: "store://p/b", data: 2 },
        { uri: "store://p/c", data: 3 },
        { uri: "store://p/d", data: 4 },
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
        () => store.read(["store://t/?fn=ls&sortBy=data"]),
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
        { uri: "store://c/a", data: 1 },
        { uri: "store://c/b", data: 2 },
        { uri: "store://c/deep/x", data: 9 },
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
    await store.write([{ uri: "store://app/x", data: "hello" }]);
    const results = await store.delete(["store://app/x"]);
    assertEquals(results.length, 1);
    assertEquals(results[0].success, true);
  });

  t("delete removes entry (read miss after delete)", async () => {
    const store = await Promise.resolve(config.create());
    await store.write([{ uri: "store://app/x", data: "hello" }]);
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
      { uri: "store://app/a", data: "A" },
      { uri: "store://app/b", data: "B" },
      { uri: "store://app/c", data: "C" },
    ]);
    await store.delete(["store://app/a", "store://app/c"]);
    const results = await store.read([
      "store://app/a",
      "store://app/b",
      "store://app/c",
    ]);
    assertEquals(payloadOf(results[0]), undefined);
    assertEquals(payloadOf(results[1]), "B");
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
      if (caps.binaryData !== undefined) {
        assertEquals(typeof caps.binaryData, "boolean");
      }
    }
  });
}
