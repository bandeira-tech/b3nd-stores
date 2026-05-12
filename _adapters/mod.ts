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
 */

export { SimpleClient } from "./simple-client.ts";
export { DataStoreClient } from "./data-store-client.ts";
