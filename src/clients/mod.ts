/**
 * Store → `ProtocolInterfaceNode` adapter.
 *
 * `SaveClient` is the single client this package exposes. It takes a
 * `SaveMapper` (wire payload → `EntityRecord`), the target
 * `EntitySchema`, and the backing store — read as "receive payloads
 * X, map as Y, store on S". All three are explicit; the client does
 * not invent defaults.
 *
 * Built-in mappers `mapToBytes` and `passThroughRecord` cover the
 * common cases (opaque bytes via `BYTES_ENTITY`, already-shaped
 * records via a custom schema).
 */

export {
  mapToBytes,
  passThroughRecord,
  SaveClient,
  type SaveMapper,
} from "./save-client.ts";
