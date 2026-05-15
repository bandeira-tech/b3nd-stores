/**
 * Payload normalizers for backends.
 *
 * `StorePayload = Uint8Array | ReadableStream<Uint8Array>`. Backends
 * with no native streaming path call `toBytes` on write to collect
 * incoming streams. Backends that natively stream call `toStream` on
 * read to wrap a buffered value back into a `ReadableStream` when
 * that's the cheap shape for the caller.
 *
 * Both helpers go through `Response` so we get the platform's
 * battle-tested coercion (handles `Uint8Array`, `ReadableStream`,
 * locked stream errors, etc.) without re-implementing.
 */

import type { StorePayload } from "./types.ts";

/** Collect a `StorePayload` into a single `Uint8Array`. */
export async function toBytes(payload: StorePayload): Promise<Uint8Array> {
  if (payload instanceof Uint8Array) return payload;
  return new Uint8Array(await new Response(payload as BodyInit).arrayBuffer());
}

/**
 * Wrap a `StorePayload` as a `ReadableStream<Uint8Array>`. A
 * `Uint8Array` becomes a single-chunk stream; an existing stream
 * passes through. Use this on read paths from backends that prefer to
 * yield a stream to callers.
 */
export function toStream(payload: StorePayload): ReadableStream<Uint8Array> {
  if (payload instanceof ReadableStream) return payload;
  return new Response(payload as BodyInit).body!;
}
