/**
 * In-memory Store backend for b3nd.
 *
 * `MemoryStore` is the reference Store implementation — a recursive
 * tree-walking, in-memory key/value store. Useful as a deterministic
 * stand-in for tests, prototypes, and as the canonical backing for
 * the `memory://` URL scheme in the backend factory.
 *
 * Note: this Store's `fn=ls` and `fn=count` are RECURSIVE (deep walk),
 * which differs from the *shallow direct-leaves* contract enforced by
 * the rest of the b3nd-stores package. The two contracts are
 * intentionally different — see the project memory's locked decisions.
 * If you want shallow semantics in-memory, layer your own thin
 * shallow-only Store over a `Map`, or use one of the other backends.
 */

export { MemoryStore } from "./store.ts";
