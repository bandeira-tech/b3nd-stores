/**
 * IPFS backend for b3nd.
 *
 * Store implementation backed by IPFS. Requires an injected IpfsExecutor
 * so the SDK does not depend on a specific IPFS library.
 */

export interface IpfsExecutor {
  add: (content: string) => Promise<string>;
  cat: (cid: string) => Promise<string>;
  pin: (cid: string) => Promise<void>;
  unpin: (cid: string) => Promise<void>;
  listPins: () => Promise<string[]>;
  isOnline: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { IpfsStore } from "./store.ts";
