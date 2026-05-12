/**
 * Backend factory — resolve URL strings into Stores and protocol
 * clients.
 *
 * Maps URL protocols to the right Store implementation or transport
 * client. Storage protocols (`memory://`, `postgresql://`, etc.)
 * resolve to a `Store`; transport protocols (`https://`, `wss://`,
 * `console://`) resolve to a `ProtocolInterfaceNode` directly.
 *
 * Built-in storage: `memory://` only. Other backends (postgres,
 * mongo, sqlite, fs, ipfs, s3, elasticsearch, localstorage,
 * indexeddb) plug in via `BackendResolver[]`.
 */

export {
  createClientFromUrl,
  createClientResolver,
  createStoreFromUrl,
  createStoreResolver,
  getSupportedProtocols,
} from "./factory.ts";
export type {
  BackendFactoryOptions,
  BackendResolver,
  StoreClientConstructor,
} from "./factory.ts";
