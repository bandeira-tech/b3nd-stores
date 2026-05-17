/**
 * Store → `ProtocolInterfaceNode` adapter clients.
 *
 * These are the canonical ways to put any `Store` behind the
 * `ProtocolInterfaceNode` interface so a Rig (or any other consumer
 * of `ProtocolInterfaceNode`) can talk to it.
 *
 * - `SimpleClient` — bare wrapper. `receive` → `store.write`,
 *   `read` → `store.read`, `delete` → `store.delete`, `observe` via
 *   `ObserveEmitter`.
 * - `DataStoreClient` — `SimpleClient` plus envelope decomposition:
 *   accepts inline `{ inputs, outputs }` envelopes, performs the
 *   deletes-then-writes atomically (per backend), emits observe
 *   events grouped by program.
 * - `ByteStorageClient` — bytes-on-the-wire over an `EntityStore`.
 *   Pins `BYTES_ENTITY`; translates `[uri, bytes | null]` to/from
 *   `{ payload: bytes }` records. The entity-aware successor to
 *   `SimpleClient` / `DataStoreClient` for `EntityStore`-shaped
 *   backends.
 * - `EntityClient` — schema-aware client over an `EntityStore`.
 *   `receive([uri, record])` writes typed records; `receive([uri,
 *   null])` deletes; `read(urls)` returns records. Target schema is
 *   swappable at runtime via `setTarget`.
 */

export { SimpleClient } from "./simple-client.ts";
export { DataStoreClient } from "./data-store-client.ts";
export { ByteStorageClient } from "./byte-storage-client.ts";
export { EntityClient } from "./entity-client.ts";
