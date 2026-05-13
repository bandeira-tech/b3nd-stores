/// <reference lib="deno.ns" />
import { assertEquals, assertInstanceOf } from "jsr:@std/assert";
import { decodeBinaryFromJson, encodeBinaryForJson } from "./binary.ts";

Deno.test("primitives pass through encode", () => {
  assertEquals(encodeBinaryForJson(42), 42);
  assertEquals(encodeBinaryForJson("hi"), "hi");
  assertEquals(encodeBinaryForJson(null), null);
  assertEquals(encodeBinaryForJson(undefined), undefined);
  assertEquals(encodeBinaryForJson(true), true);
});

Deno.test("Uint8Array round-trips", () => {
  const bytes = new Uint8Array([0, 1, 2, 250, 255]);
  const enc = encodeBinaryForJson(bytes);
  const json = JSON.parse(JSON.stringify(enc));
  const dec = decodeBinaryFromJson(json);
  assertInstanceOf(dec, Uint8Array);
  assertEquals(dec, bytes);
});

Deno.test("nested binary in object round-trips", () => {
  const data = {
    name: "doc",
    body: new Uint8Array([1, 2, 3]),
    meta: { thumb: new Uint8Array([9, 9]) },
    tags: ["a", "b"],
  };
  const json = JSON.parse(JSON.stringify(encodeBinaryForJson(data)));
  const dec = decodeBinaryFromJson(json) as typeof data;
  assertInstanceOf(dec.body, Uint8Array);
  assertEquals(dec.body, data.body);
  assertInstanceOf(dec.meta.thumb, Uint8Array);
  assertEquals(dec.meta.thumb, data.meta.thumb);
  assertEquals(dec.name, "doc");
  assertEquals(dec.tags, ["a", "b"]);
});

Deno.test("array of binaries round-trips", () => {
  const arr = [new Uint8Array([1]), new Uint8Array([2, 3])];
  const json = JSON.parse(JSON.stringify(encodeBinaryForJson(arr)));
  const dec = decodeBinaryFromJson(json) as Uint8Array[];
  assertInstanceOf(dec[0], Uint8Array);
  assertEquals(dec[0], arr[0]);
  assertEquals(dec[1], arr[1]);
});

Deno.test("large buffer does not blow stack", () => {
  const big = new Uint8Array(200_000);
  for (let i = 0; i < big.length; i++) big[i] = i & 0xff;
  const json = JSON.parse(JSON.stringify(encodeBinaryForJson(big)));
  const dec = decodeBinaryFromJson(json);
  assertInstanceOf(dec, Uint8Array);
  assertEquals((dec as Uint8Array).length, big.length);
  assertEquals((dec as Uint8Array)[12345], 12345 & 0xff);
});

Deno.test("non-envelope object passes through decode", () => {
  const data = { __b3nd_binary__: false, data: "not binary" };
  assertEquals(decodeBinaryFromJson(data), data);
});
