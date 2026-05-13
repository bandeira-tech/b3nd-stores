/**
 * Backend factory — resolve URL strings into Stores and protocol
 * clients.
 *
 * Maps URL protocols to a `Store` (storage protocols like
 * `postgresql://`, `mongodb://`, etc.) and optionally wraps them in
 * a client (`ProtocolInterfaceNode`).
 *
 * No protocols are built-in. Every backend — memory included —
 * plugs in via `BackendResolver[]`. The factory only resolves what
 * you register.
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
