/**
 * @module
 * Backend factory — resolves URL strings into Stores and Clients.
 *
 * Two entry points:
 *   createStoreFromUrl(url, options)   → Store
 *   createClientFromUrl(url, options)  → ProtocolInterfaceNode
 *
 * createStoreFromUrl is the primitive — it maps a URL to a Store.
 * createClientFromUrl wraps that Store with a client class.
 *
 * Built-in protocols (always available, no registration needed):
 *   memory://             → MemoryStore
 *   https:// | http://    → HttpClient
 *   wss:// | ws://        → WebSocketClient
 *   console://            → ConsoleClient (write-only sink, no storage)
 *
 * External backends are registered via BackendResolver[]:
 *   postgresql://, mongodb://, sqlite://, file://, grpc://, etc.
 *
 * For gRPC support, see `@bandeira-tech/b3nd-servers/grpc/client` —
 * instantiate `GrpcClient` directly or wire it through a custom resolver.
 */

import type {
  ProtocolInterfaceNode,
  Store,
} from "@bandeira-tech/b3nd-core/types";
import { HttpClient } from "@bandeira-tech/b3nd-core/client-http";
import { WebSocketClient } from "@bandeira-tech/b3nd-core/client-ws";
import { ConsoleClient } from "@bandeira-tech/b3nd-core/client-console";
import { MemoryStore } from "../memory/store.ts";
import { SimpleClient } from "../_adapters/simple-client.ts";

/**
 * A user-provided backend resolver — maps URL protocols to Stores.
 *
 * Register one per backend type. The factory loops over resolvers
 * and uses the first whose `protocols` list matches the URL.
 *
 * @example
 * ```typescript
 * import { PostgresStore } from "@bandeira-tech/b3nd-stores/postgres";
 *
 * const postgres = (): BackendResolver => ({
 *   protocols: ["postgresql:", "postgres:"],
 *   resolve: async (url) => {
 *     const executor = await createPgExecutor(url);
 *     return new PostgresStore("b3nd", executor);
 *   },
 * });
 * ```
 */
export interface BackendResolver {
  /** URL protocols this resolver handles (e.g. `["postgresql:", "postgres:"]`). */
  protocols: string[];
  /** Create a Store from the given URL string. */
  resolve: (url: string) => Promise<Store> | Store;
}

/** Built-in transport protocols (no Store — return clients directly). */
const TRANSPORT_PROTOCOLS = new Set([
  "https:",
  "http:",
  "wss:",
  "ws:",
  "console:",
]);

/** Built-in storage protocols (always available). */
const BUILTIN_STORAGE_PROTOCOLS = ["memory://"];

/** Built-in transport protocol prefixes. */
const BUILTIN_TRANSPORT_PROTOCOLS = [
  "https://",
  "http://",
  "wss://",
  "ws://",
  "console://",
];

export interface BackendFactoryOptions {
  backends?: BackendResolver[];
}

/** Constructor type for clients that wrap a Store. */
export type StoreClientConstructor = new (
  store: Store,
) => ProtocolInterfaceNode;

/**
 * Returns the list of supported backend URL protocols, derived dynamically
 * from built-in protocols plus any registered backends.
 */
export function getSupportedProtocols(
  backends: BackendResolver[] = [],
): readonly string[] {
  const protocols = [
    ...BUILTIN_TRANSPORT_PROTOCOLS,
    ...BUILTIN_STORAGE_PROTOCOLS,
  ];
  for (const b of backends) {
    for (const p of b.protocols) {
      const prefix = p.endsWith(":") ? p + "//" : p;
      if (!protocols.includes(prefix)) {
        protocols.push(prefix);
      }
    }
  }
  return protocols;
}

// ── Storage protocols (URL → Store) ─────────────────────────────────

/**
 * Create a Store from a URL string.
 *
 * Works for storage protocols only (memory + registered backends).
 * Transport protocols (http, ws) throw — use createClientFromUrl instead.
 */
export async function createStoreFromUrl(
  url: string,
  options: BackendFactoryOptions = {},
): Promise<Store> {
  const parsed = new URL(url);
  const protocol = parsed.protocol;

  if (TRANSPORT_PROTOCOLS.has(protocol)) {
    throw new Error(
      `"${protocol}" is a transport protocol with no Store. ` +
        `Use createClientFromUrl() for HTTP/WebSocket backends.`,
    );
  }

  // Built-in: memory
  if (protocol === "memory:") {
    return new MemoryStore();
  }

  // Check registered backends
  const backends = options.backends ?? [];
  for (const backend of backends) {
    if (backend.protocols.includes(protocol)) {
      return await backend.resolve(url);
    }
  }

  const supported = getSupportedProtocols(backends);
  throw new Error(
    `Unsupported backend URL protocol: "${protocol}". ` +
      `Supported: ${supported.join(", ")}`,
  );
}

// ── Client from URL ─────────────────────────────────────────────────

/**
 * Create a ProtocolInterfaceNode client from a URL string.
 *
 * For storage protocols: creates a Store, wraps with the given client class
 * (defaults to SimpleClient).
 *
 * For transport protocols (http, ws): returns the transport client directly
 * (client arg is ignored — there's no Store to wrap).
 */
export async function createClientFromUrl(
  url: string,
  options?: BackendFactoryOptions & { client?: StoreClientConstructor },
): Promise<ProtocolInterfaceNode>;
/**
 * Create a ProtocolInterfaceNode client from a URL string with a specific
 * client class.
 */
export async function createClientFromUrl(
  url: string,
  Client: StoreClientConstructor,
  options?: BackendFactoryOptions,
): Promise<ProtocolInterfaceNode>;
export async function createClientFromUrl(
  url: string,
  clientOrOptions?:
    | StoreClientConstructor
    | (BackendFactoryOptions & { client?: StoreClientConstructor }),
  maybeOptions?: BackendFactoryOptions,
): Promise<ProtocolInterfaceNode> {
  let ClientClass: StoreClientConstructor;
  let options: BackendFactoryOptions;

  if (typeof clientOrOptions === "function") {
    // createClientFromUrl(url, Client, options?)
    ClientClass = clientOrOptions;
    options = maybeOptions ?? {};
  } else {
    // createClientFromUrl(url, options?)
    const opts = clientOrOptions ?? {};
    ClientClass = (opts as { client?: StoreClientConstructor }).client ??
      SimpleClient;
    options = opts;
  }

  const parsed = new URL(url);
  const protocol = parsed.protocol;

  // Transport protocols — return client directly (no Store)
  switch (protocol) {
    case "https:":
    case "http:":
      return new HttpClient({ url });
    case "wss:":
    case "ws:":
      return new WebSocketClient({ url });
    case "console:": {
      const label = parsed.hostname || "b3nd";
      return new ConsoleClient(label);
    }
  }

  // Storage protocols — create Store, wrap with client
  const store = await createStoreFromUrl(url, options);
  return new ClientClass(store);
}

// ── Resolvers (configure once, resolve many) ───────────────────────

/**
 * Create a store resolver — bind backends once, resolve URLs later.
 *
 * Only handles storage protocols (memory + registered backends).
 * Transport URLs (http, ws) will throw — those produce clients directly,
 * not Stores.
 *
 * @example
 * ```typescript
 * const resolveStore = createStoreResolver([postgresBackend()]);
 *
 * const urls = process.env.BACKEND_URLS!.split(",");
 * const stores = await Promise.all(urls.map(resolveStore));
 * ```
 */
export function createStoreResolver(
  backends: BackendResolver[] = [],
): (url: string) => Promise<Store> {
  const options: BackendFactoryOptions = { backends };
  return (url: string) => createStoreFromUrl(url, options);
}

/**
 * Create a client resolver — bind a client class and backends once,
 * resolve URLs later.
 *
 * For storage protocols: creates a Store and wraps it with the given client class.
 * For transport protocols (http, ws): returns the transport client directly
 * (client class is ignored — there's no Store to wrap).
 *
 * @example
 * ```typescript
 * import { DataStoreClient } from "@bandeira-tech/b3nd-sdk";
 *
 * const resolveClient = createClientResolver(DataStoreClient, [
 *   postgresBackend(),
 * ]);
 *
 * const urls = process.env.BACKEND_URLS!.split(",");
 * const clients = await Promise.all(urls.map(resolveClient));
 * ```
 */
export function createClientResolver(
  ClientClass: StoreClientConstructor = SimpleClient,
  backends: BackendResolver[] = [],
): (url: string) => Promise<ProtocolInterfaceNode> {
  const options: BackendFactoryOptions = { backends };
  return (url: string) => createClientFromUrl(url, ClientClass, options);
}
