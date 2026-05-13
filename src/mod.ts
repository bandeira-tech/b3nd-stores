/**
 * @module
 * b3nd-save — the data-saving layer for b3nd.
 *
 * This root barrel re-exports every public subpath as a namespace.
 * Use it for convenience or discoverability; for the smallest
 * footprint, import the subpaths directly (e.g.
 * `@bandeira-tech/b3nd-save/postgres`).
 *
 * Namespaces:
 *   factory       — URL → Store / Client resolution (no built-in protocols)
 *   clients       — Store → ProtocolInterfaceNode adapters
 *   shared        — Helpers for backend authors (binary, read, dispatch)
 *   memory        — In-memory reference backend
 *   postgres, mongo, sqlite, fs, ipfs, s3, elasticsearch,
 *   localstorage, indexeddb — Backend implementations
 */

export * as factory from "./factory/mod.ts";
export * as clients from "./clients/mod.ts";
export * as shared from "./shared/mod.ts";
export * as memory from "./backends/memory/mod.ts";
export * as postgres from "./backends/postgres/mod.ts";
export * as mongo from "./backends/mongo/mod.ts";
export * as sqlite from "./backends/sqlite/mod.ts";
export * as fs from "./backends/fs/mod.ts";
export * as ipfs from "./backends/ipfs/mod.ts";
export * as s3 from "./backends/s3/mod.ts";
export * as elasticsearch from "./backends/elasticsearch/mod.ts";
export * as localstorage from "./backends/localstorage/mod.ts";
export * as indexeddb from "./backends/indexeddb/mod.ts";
