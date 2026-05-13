/**
 * IPFS backend for b3nd.
 *
 * Store implementation backed by IPFS. Requires an injected
 * IpfsExecutor so the package does not depend on a specific IPFS
 * library. Blocks hold raw payload bytes — the store is opaque.
 */

export interface IpfsExecutor {
  add: (content: Uint8Array) => Promise<string>;
  cat: (cid: string) => Promise<Uint8Array>;
  pin: (cid: string) => Promise<void>;
  unpin: (cid: string) => Promise<void>;
  listPins: () => Promise<string[]>;
  isOnline: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { IpfsStore } from "./store.ts";
