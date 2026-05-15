/**
 * @module
 * Backend factory — resolves URL strings into Stores and Clients.
 *
 * Two entry points:
 *   createStoreFromUrl(url, options)   → Store
 *   createClientFromUrl(url, options)  → ProtocolInterfaceNode
 *
 * createStoreFromUrl is the primitive — it maps a URL to a Store.
 * createClientFromUrl wraps that Store with a client class
 * (defaults to `SimpleClient`).
 *
 * The factory has **no built-in protocols**. Every backend — including
 * `memory://` — registers via `BackendResolver[]`. Pass the resolvers
 * your app needs; the factory only resolves what you give it.
 *
 * **Transport protocols are out of scope.** The factory does not
 * handle `http://`, `ws://`, `console://`, or `grpc://` URLs. Those
 * produce *transport clients* (live in `@bandeira-tech/b3nd-move`
 * and `@bandeira-tech/b3nd-core/client-console`), not Stores; they
 * have no consistent factory pattern across schemes. Construct them
 * directly:
 *
 *   new HttpClient({ url })                      // b3nd-move/http/client
 *   new WebSocketClient({ url })                 // b3nd-move/ws/client
 *   new ConsoleClient(label)                     // b3nd-core/client-console
 *   new GrpcHttpClient({ url })                  // b3nd-move/grpc/http/client
 */

import type { ProtocolInterfaceNode } from "@bandeira-tech/b3nd-core/types";
import { SimpleClient } from "../clients/simple-client.ts";
import type { Store } from "../types.ts";

/**
 * A user-provided backend resolver — maps URL protocols to Stores.
 *
 * Register one per backend type. The factory loops over resolvers
 * and uses the first whose `protocols` list matches the URL.
 *
 * @example
 * ```typescript
 * import { PostgresStore } from "@bandeira-tech/b3nd-save/postgres";
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

export interface BackendFactoryOptions {
  backends?: BackendResolver[];
}

/** Constructor type for clients that wrap a Store. */
export type StoreClientConstructor = new (
  store: Store,
) => ProtocolInterfaceNode;

/**
 * Returns the list of supported storage URL protocols, derived from
 * the registered backends. Empty when no backends are registered.
 */
export function getSupportedProtocols(
  backends: BackendResolver[] = [],
): readonly string[] {
  const protocols: string[] = [];
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
 * Create a Store from a URL string. Storage protocols only.
 * Throws if the URL's protocol isn't a registered storage backend.
 */
export async function createStoreFromUrl(
  url: string,
  options: BackendFactoryOptions = {},
): Promise<Store> {
  const parsed = new URL(url);
  const protocol = parsed.protocol;

  const backends = options.backends ?? [];
  for (const backend of backends) {
    if (backend.protocols.includes(protocol)) {
      return await backend.resolve(url);
    }
  }

  const supported = getSupportedProtocols(backends);
  throw new Error(
    `Unsupported storage URL protocol: "${protocol}". ` +
      `Supported: ${supported.join(", ")}. ` +
      `Transport URLs (http://, ws://, console://, grpc://) are not ` +
      `handled by this factory — construct those clients directly ` +
      `from @bandeira-tech/b3nd-move/* or @bandeira-tech/b3nd-core/client-console.`,
  );
}

// ── Client from URL ─────────────────────────────────────────────────

/**
 * Create a ProtocolInterfaceNode client from a *storage* URL string —
 * the factory resolves the URL to a Store and wraps it with the given
 * client class (defaults to SimpleClient).
 *
 * Transport URLs (http://, ws://, etc.) are out of scope — construct
 * those clients directly from b3nd-move / b3nd-core.
 */
export async function createClientFromUrl(
  url: string,
  options?: BackendFactoryOptions & { client?: StoreClientConstructor },
): Promise<ProtocolInterfaceNode>;
/**
 * Create a ProtocolInterfaceNode client from a storage URL string
 * with a specific client class.
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
    ClientClass = clientOrOptions;
    options = maybeOptions ?? {};
  } else {
    const opts = clientOrOptions ?? {};
    ClientClass = (opts as { client?: StoreClientConstructor }).client ??
      SimpleClient;
    options = opts;
  }

  const store = await createStoreFromUrl(url, options);
  return new ClientClass(store);
}

// ── Resolvers (configure once, resolve many) ───────────────────────

/**
 * Create a store resolver — bind backends once, resolve URLs later.
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
 * @example
 * ```typescript
 * import { DataStoreClient } from "@bandeira-tech/b3nd-save/clients";
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
