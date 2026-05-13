/// <reference lib="deno.ns" />
import { assertEquals, assertRejects } from "jsr:@std/assert";
import { dispatchRead } from "./dispatch.ts";

Deno.test("dispatches read, ls, count by url shape", async () => {
  const out = await dispatchRead(
    [
      "s://a/k",
      "s://a/",
      "s://a/?fn=count",
    ],
    "test",
    {
      read: (p) => `read:${p.uri}`,
      ls: (p) => `ls:${p.uri}`,
      count: () => 42,
    },
  );

  assertEquals(out, [
    ["s://a/k", "read:s://a/k"],
    ["s://a/", "ls:s://a/"],
    ["s://a/?fn=count", 42],
  ]);
});

Deno.test("echoes full input url including query", async () => {
  const url = "s://a/?fn=ls&limit=5";
  const out = await dispatchRead([url], "test", {
    read: () => null,
    ls: () => "L",
    count: () => 0,
  });
  assertEquals(out[0][0], url);
  assertEquals(out[0][1], "L");
});

Deno.test("unknown fn throws", async () => {
  await assertRejects(
    () =>
      dispatchRead(["s://a/?fn=bogus"], "test", {
        read: () => null,
        ls: () => null,
        count: () => 0,
      }),
    Error,
    "unsupported fn 'bogus'",
  );
});

Deno.test("x-* extension routed to ext when provided", async () => {
  const out = await dispatchRead(["s://a/?fn=x-feed.tail"], "test", {
    read: () => null,
    ls: () => null,
    count: () => 0,
    ext: (p) => ({ ok: true, fn: p.fn }),
  });
  assertEquals(out[0][1], { ok: true, fn: "x-feed.tail" });
});

Deno.test("x-* without ext handler throws", async () => {
  await assertRejects(
    () =>
      dispatchRead(["s://a/?fn=x-feed.tail"], "test", {
        read: () => null,
        ls: () => null,
        count: () => 0,
      }),
    Error,
    "unsupported fn 'x-feed.tail'",
  );
});

Deno.test("async handlers are awaited", async () => {
  const out = await dispatchRead(["s://a/k"], "test", {
    read: () => Promise.resolve("async-read"),
    ls: () => null,
    count: () => 0,
  });
  assertEquals(out[0][1], "async-read");
});
