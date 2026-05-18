/**
 * Local Store types for b3nd-save.
 *
 * The `Store` abstraction is internal to this package. `SaveClient`
 * wraps a `Store` (or `EntityStore`) and exposes a
 * `ProtocolInterfaceNode` to the rest of b3nd — outside the client,
 * nothing else in the framework sees a `Store`.
 *
 * Stores are opaque byte storage: payloads are bytes in, bytes out.
 * The payload type is the `Uint8Array | ReadableStream<Uint8Array>`
 * union so callers and backends can avoid materializing large
 * payloads when both sides know how to stream — same shape `fetch`
 * already uses for request/response bodies. Backends that have no
 * native streaming path collect the stream to bytes on write and
 * return `Uint8Array` on read. Backends with native streaming
 * (filesystem, S3, IPFS) keep streams end-to-end.
 *
 * Cross-cutting types (`Output`, `ProtocolInterfaceNode`,
 * `DeleteResult`, `StatusResult`) still come from b3nd-core.
 */

import type {
  B3ndError,
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";

/**
 * Payload type accepted by `write` and produced by `read`.
 *
 * A backend may accept either shape on write and is free to return
 * either shape on read — pick whichever avoids buffering when
 * possible. Callers that always want bytes can collect with
 * `new Response(payload).bytes()` or the equivalent helper.
 */
export type StorePayload = Uint8Array | ReadableStream<Uint8Array>;

/**
 * Entry for a batch write operation.
 *
 * @example
 * ```typescript
 * await store.write([
 *   { uri: "mutable://users/alice", payload: new TextEncoder().encode("alice") },
 * ]);
 * ```
 */
export interface StoreEntry {
  uri: string;
  payload: StorePayload;
}

/**
 * Per-entry result of a write operation.
 *
 * `errorDetail` mirrors `DeleteResult.errorDetail` — callers that want
 * to react to specific failure modes (e.g. `STORAGE_ERROR` from the
 * underlying driver vs. a programmer error from the store layer)
 * should switch on `errorDetail.code` rather than parsing `error`.
 * The `error` string remains for human-readable logs.
 */
export interface StoreWriteResult {
  success: boolean;
  error?: string;
  errorDetail?: B3ndError;
}

/**
 * Optional capability reporting for a Store.
 *
 * Backends declare what they can do so clients can make informed
 * decisions (e.g., wrap deletes + writes in a transaction when
 * `atomicBatch` is true).
 */
export interface StoreCapabilities {
  /** Whether write + delete within a single call can be made atomic. */
  atomicBatch?: boolean;
}

/**
 * Store — the batch-native byte-storage abstraction.
 *
 * Every operation takes arrays and returns per-item results. Each
 * backend optimizes for its technology (Postgres → multi-row INSERT,
 * S3 → parallel PutObject, etc.).
 *
 * The Store knows nothing about protocols, envelopes, message
 * semantics, or content shape. Read returns `Output<StorePayload>`
 * for `fn=read` and `fn=ls&format=full`; `fn=ls&format=uris` returns
 * `string[]`, `fn=count` returns `number`.
 *
 * @example
 * ```typescript
 * import { MemoryStore } from "@bandeira-tech/b3nd-save/memory";
 * const store = new MemoryStore();
 *
 * await store.write([
 *   { uri: "mutable://app/config", payload: new TextEncoder().encode("dark") },
 * ]);
 * const [[uri, payload]] = await store.read(["mutable://app/config"]);
 * const bytes = await new Response(payload as StorePayload).bytes();
 * ```
 */
export interface Store {
  write(entries: StoreEntry[]): Promise<StoreWriteResult[]>;
  read<T = StorePayload>(urls: string[]): Promise<Output<T>[]>;
  delete(uris: string[]): Promise<DeleteResult[]>;
  status(): Promise<StatusResult>;
  capabilities?(): StoreCapabilities;
}
