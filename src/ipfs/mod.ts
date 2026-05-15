/**
 * IPFS backend for b3nd.
 *
 * Store implementation backed by IPFS. Requires an injected
 * IpfsExecutor so the package does not depend on a specific IPFS
 * library. Blocks hold raw payload bytes — the store is opaque.
 *
 * `add` accepts the `StorePayload` union to support both buffered
 * and streaming callers; `cat` returns a `ReadableStream<Uint8Array>`
 * to avoid materializing the block in memory unless the caller wants
 * it.
 */

import type { StorePayload } from "../types.ts";

export interface IpfsExecutor {
  add: (content: StorePayload) => Promise<string>;
  cat: (cid: string) => Promise<ReadableStream<Uint8Array>>;
  pin: (cid: string) => Promise<void>;
  unpin: (cid: string) => Promise<void>;
  listPins: () => Promise<string[]>;
  isOnline: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { IpfsStore } from "./store.ts";
