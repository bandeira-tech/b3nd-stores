/// <reference lib="deno.ns" />
import { assertEquals, assertThrows } from "jsr:@std/assert";
import type { Output } from "@bandeira-tech/b3nd-core/types";
import { applyReadParams } from "./read.ts";

const rows: Output<string>[] = [
  ["s://a/3", "c"],
  ["s://a/1", "a"],
  ["s://a/2", "b"],
];

Deno.test("default returns rows as full Output[]", () => {
  const out = applyReadParams(rows, {}, "test");
  assertEquals(out, rows);
});

Deno.test("sortBy=uri asc", () => {
  const out = applyReadParams(rows, { sortBy: "uri" }, "test") as Output[];
  assertEquals(out.map(([u]) => u), ["s://a/1", "s://a/2", "s://a/3"]);
});

Deno.test("sortBy=uri desc", () => {
  const out = applyReadParams(
    rows,
    { sortBy: "uri", sortOrder: "desc" },
    "test",
  ) as Output[];
  assertEquals(out.map(([u]) => u), ["s://a/3", "s://a/2", "s://a/1"]);
});

Deno.test("limit + page", () => {
  const out = applyReadParams(
    rows,
    { sortBy: "uri", limit: 1, page: 2 },
    "test",
  ) as Output[];
  assertEquals(out, [["s://a/2", "b"]]);
});

Deno.test("format=uris returns string[]", () => {
  const out = applyReadParams(
    rows,
    { sortBy: "uri", format: "uris" },
    "test",
  );
  assertEquals(out, ["s://a/1", "s://a/2", "s://a/3"]);
});

Deno.test("unsupported sortBy throws", () => {
  assertThrows(
    () => applyReadParams(rows, { sortBy: "data" }, "test"),
    Error,
    "unsupported sortBy",
  );
});

Deno.test("unsupported format throws", () => {
  assertThrows(
    () => applyReadParams(rows, { format: "weird" }, "test"),
    Error,
    "unsupported format",
  );
});

Deno.test("pattern throws", () => {
  assertThrows(
    () => applyReadParams(rows, { pattern: "*" }, "test"),
    Error,
    "pattern filter not supported",
  );
});

Deno.test("cursor throws", () => {
  assertThrows(
    () => applyReadParams(rows, { cursor: "abc" }, "test"),
    Error,
    "cursor not supported",
  );
});

Deno.test("does not mutate input", () => {
  const input: Output<string>[] = [["s://b", "x"], ["s://a", "y"]];
  applyReadParams(input, { sortBy: "uri" }, "test");
  assertEquals(input, [["s://b", "x"], ["s://a", "y"]]);
});
