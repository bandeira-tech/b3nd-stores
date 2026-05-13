/**
 * Local Store types for b3nd-save.
 *
 * The `Store` abstraction is internal to this package. Clients
 * (`SimpleClient`, `DataStoreClient`) wrap a `Store` and expose a
 * `ProtocolInterfaceNode` to the rest of b3nd — outside the clients,
 * nothing else in the framework sees a `Store`.
 *
 * Stores are opaque byte storage: `payload: Uint8Array` in,
 * `Uint8Array` out. No JSON, no envelopes, no kind discriminators —
 * the store does not inspect content. Higher layers (protocols,
 * clients) own serialization.
 *
 * Cross-cutting types (`Output`, `Message`, `ProtocolInterfaceNode`,
 * `DeleteResult`, `StatusResult`) still come from b3nd-core.
 */

import type {
  B3ndError,
  DeleteResult,
  Output,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";

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
  payload: Uint8Array;
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
 * semantics, or content shape. Read returns `Output<Uint8Array>` for
 * `fn=read` and `fn=ls&format=full`; `fn=ls&format=uris` returns
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
 * const [[uri, bytes]] = await store.read<Uint8Array>(["mutable://app/config"]);
 * ```
 */
export interface Store {
  write(entries: StoreEntry[]): Promise<StoreWriteResult[]>;
  read<T = Uint8Array>(urls: string[]): Promise<Output<T>[]>;
  delete(uris: string[]): Promise<DeleteResult[]>;
  status(): Promise<StatusResult>;
  capabilities?(): StoreCapabilities;
}
