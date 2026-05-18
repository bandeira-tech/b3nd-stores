/**
 * Store → `ProtocolInterfaceNode` adapter.
 *
 * `SaveClient` is the single client this package exposes. It backs
 * onto either an {@link EntityStore} or a legacy byte {@link Store}
 * and defaults its target to `BYTES_ENTITY` so byte-shaped wires work
 * out of the box. Pass a schema in the constructor (or via
 * `setTarget`) to route typed records on the wire instead.
 */

export { SaveClient } from "./save-client.ts";
