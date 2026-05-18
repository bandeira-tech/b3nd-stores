# @bandeira-tech/b3nd-save

The **data-saving layer** for B3nd — everything between a node's
`ProtocolInterfaceNode` and the storage it persists to. One package covers:

- **Backends** — ten storage implementations on a single uniform contract.
- **Clients** — backend→`ProtocolInterfaceNode` adapters that turn raw storage
  into something a `Rig` can talk to.
- **Shared helpers** — for authors building their own backends without
  re-deriving the contract details.

## The contract

Every backend implements one interface: `EntityStore`. A backend instance hosts
many typed entities side by side; the schema is per-call, not pinned at
construction, so a single instance can serve any number of programs at once.

```ts
interface EntityStore {
  ensureEntity(schema: EntitySchema): Promise<EntitySupport>;
  write(
    schema: EntitySchema,
    entries: { uri: string; record: EntityRecord }[],
  ): Promise<StoreWriteResult[]>;
  read<T = EntityRecord | undefined>(
    schema: EntitySchema,
    urls: string[],
  ): Promise<Output<T>[]>;
  delete(schema: EntitySchema, uris: string[]): Promise<DeleteResult[]>;
  status(): Promise<StatusResult>;
}
```

`ensureEntity` provisions whatever the medium needs (a Postgres table, a Mongo
collection, an in-memory record map). It is idempotent — repeat calls are no-ops
— and returns an `EntitySupport` report listing which fields the medium
recognised and which it could not. The other three operations always carry the
`EntitySchema` they target, so a write/read/delete is fully self-describing.

Records are open `Record<string, unknown>`, validated against the schema by the
backend. The contract is **strict**: a record with extra or mistyped keys
produces a `StoreWriteResult` failure for that entry — backends never silently
coerce or drop fields. Coercion is the **client's** job (see `SaveClient` below
— its `BYTES_ENTITY` mode wraps raw bytes into a `{ payload }` record for the
store).

### Raw bytes — `BYTES_ENTITY`

Plain byte storage is the same contract under a canonical schema:

```ts
export const BYTES_ENTITY: EntitySchema = {
  name: "bytes",
  fields: [{ name: "payload", type: ["bytes"] }],
};
```

Every backend routes `BYTES_ENTITY` writes/reads through its native byte path
(Postgres `BYTEA`, S3 object body, the filesystem file itself), so byte-shaped
wires pay no schema overhead and benefit from native streaming on backends that
support it (`fs`, `s3`, `ipfs`). `SaveClient` defaults its target to
`BYTES_ENTITY`, so wires whose payload is opaque bytes need no configuration.

## Imports

Each subpath is independent — import only what you need and the rest stays out
of your bundle.

```ts
// Narrow imports — footprint-aware
import { PostgresStore } from "@bandeira-tech/b3nd-save/postgres";
import { SaveClient } from "@bandeira-tech/b3nd-save/clients";
import { BYTES_ENTITY, TYPE_TAGS } from "@bandeira-tech/b3nd-save/entity";
import type { EntityStore } from "@bandeira-tech/b3nd-save/entity-store";

// Root barrel — convenient, namespaced
import { clients, postgres } from "@bandeira-tech/b3nd-save";
```

## Backends

| Backend       | Import                                   | Executor                               | Push-down                                       | Streams? |
| ------------- | ---------------------------------------- | -------------------------------------- | ----------------------------------------------- | -------- |
| Memory        | `@bandeira-tech/b3nd-save/memory`        | none                                   | in-memory tree walk over direct children        | no       |
| PostgreSQL    | `@bandeira-tech/b3nd-save/postgres`      | inject any `pg`-style executor         | `ls` / `count` via `LIKE … AND NOT LIKE …%/%`   | no       |
| SQLite        | `@bandeira-tech/b3nd-save/sqlite`        | inject any `@db/sqlite`-style executor | same as Postgres                                | no       |
| MongoDB       | `@bandeira-tech/b3nd-save/mongo`         | inject a `MongoExecutor`               | regex filter `^<prefix>[^/]+$`                  | no       |
| Elasticsearch | `@bandeira-tech/b3nd-save/elasticsearch` | inject an `ElasticsearchExecutor`      | `regexp` query + `_count` endpoint              | no       |
| S3            | `@bandeira-tech/b3nd-save/s3`            | inject an `S3Executor`                 | `listObjects(prefix)` + client-side leaf filter | yes      |
| Filesystem    | `@bandeira-tech/b3nd-save/fs`            | inject an `FsExecutor`                 | direct-child file listing                       | yes      |
| IPFS          | `@bandeira-tech/b3nd-save/ipfs`          | inject an `IpfsExecutor`               | in-memory `uri → CID` index                     | yes      |
| LocalStorage  | `@bandeira-tech/b3nd-save/localstorage`  | injects browser `Storage`              | flat key scan                                   | no       |
| IndexedDB     | `@bandeira-tech/b3nd-save/indexeddb`     | injects `indexedDB` / `IDBKeyRange`    | bounded cursor with early termination           | no       |

"Streams?" = whether reads of `BYTES_ENTITY` return a
`ReadableStream<Uint8Array>` directly (no buffering). Buffered backends collect
streamed write input to bytes before storing and always return `Uint8Array` on
read.

## Client

`@bandeira-tech/b3nd-save/clients` exports a single class: **`SaveClient`** —
the adapter from a save backend (`EntityStore` or legacy byte `Store`) to
`ProtocolInterfaceNode`.

The target schema defaults to `BYTES_ENTITY`, so out-of-the-box the wire is
`[uri, bytes | null]` and works against every backend in the package. Pass a
schema to the constructor (or call `setTarget`) to switch to a typed record
wire, `[uri, record | null]`. `setTarget` swaps the routed entity at runtime
with no re-init. A byte-only backing store accepts only `BYTES_ENTITY` — asking
for any other target throws.

## Backend-author helpers

- **`@bandeira-tech/b3nd-save/entity`** — `EntitySchema`, `EntityField`,
  `EntityRecord`, `EntitySupport`, `TYPE_TAGS`, `BYTES_ENTITY`. The data
  vocabulary every backend speaks.
- **`@bandeira-tech/b3nd-save/entity-store`** — the `EntityStore` interface.
- **`@bandeira-tech/b3nd-save/dispatch`** — `dispatchRead` helper that handles
  the `fn=read|ls|count|x-*` switch so every backend stays consistent.
- **`@bandeira-tech/b3nd-save/read`** — `validateReadParams`, `applyReadParams`
  for the read-params contract.
- **`@bandeira-tech/b3nd-save/errors`** — `storageFailure`, the catch-block
  helper that builds a structured `B3ndError` for store result tuples.
- **`@bandeira-tech/b3nd-save/payload`** — `toBytes` / `toStream` payload
  normalizers for the `Uint8Array | ReadableStream<Uint8Array>` union used by
  `BYTES_ENTITY`.

Use these when implementing a new `EntityStore` so it matches the contract the
built-ins follow.

## Quick start

```ts
import { PostgresStore } from "jsr:@bandeira-tech/b3nd-save/postgres";
import { SaveClient } from "jsr:@bandeira-tech/b3nd-save/clients";
import { TYPE_TAGS } from "jsr:@bandeira-tech/b3nd-save/entity";

const userSchema = {
  name: "users",
  fields: [
    { name: "name", type: [TYPE_TAGS.STRING] },
    { name: "age", type: [TYPE_TAGS.NUMBER] },
  ],
};

// One backend, many entities.
const store = new PostgresStore("myapp", executor);

// Typed record wire.
const users = new SaveClient(store, userSchema);
await users.init();
//          ^ provisions the `users` table, returns EntitySupport

await users.receive([
  ["data://users/alice", { name: "Alice", age: 30 }],
  ["data://users/bob", { name: "Bob", age: 25 }],
]);

const [[, alice]] = await users.read(["data://users/alice"]);
// alice === { name: "Alice", age: 30 }

users.setTarget(otherSchema); // hot-swap routed entity, no re-init

// Byte wire on the same backend instance — default target is BYTES_ENTITY.
const bytes = new SaveClient(store);
await bytes.receive([["mutable://assets/logo.png", new Uint8Array([...])]]);
```

## Entities

### Open type vocabulary

`EntityField.type` is `string[]`, **not** a closed literal union. The canonical
tags this package recognises live in `TYPE_TAGS`:

```ts
TYPE_TAGS = {
  STRING: "string",
  NUMBER: "number",
  BIGINT: "bigint",
  BOOLEAN: "boolean",
  BYTES: "bytes",
  TIMESTAMP: "timestamp",
  JSON: "json",
};
```

Custom protocols may freely add their own tags (e.g. `"money"`, `"geo"`,
`"email"`). Multiple tags on a field are refinements describing the same value —
e.g. `["string", "email"]` is a string semantically known to be an email.
Backends consult the tags they understand to decide how to materialise the
column / field / index; unknown tags pass through the schema unchanged. After
`ensureEntity`, the returned `EntitySupport` declares which fields the medium
accepted and which it could not, so callers see incompatibilities once at init
time rather than mid-flight.

### Strict validation

Backends do not coerce. A record under `schema` may only contain keys declared
in `schema.fields`, with values compatible with the field's recognised tags.
Anything else produces a `StoreWriteResult` failure for that entry. This is
deliberate — rig misconfigurations stay loud rather than silently corrupting
data. Coercion lives in the client (this is what `SaveClient` does in its
default `BYTES_ENTITY` mode when it projects raw bytes into
`{ payload: bytes }`).

### Encoding bytes any shape you like

`BYTES_ENTITY.payload` is opaque bytes. Encode on write, decode on read —
backends never inspect content. JSON, protobuf, FlatBuffers, CBOR, MessagePack,
encrypted blobs — all work; only the encode/decode line in your code changes.

```ts
const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);

await bytes.receive([
  ["mutable://users/alice", enc(JSON.stringify({ name: "Alice" }))],
]);
const [[, payload]] = await bytes.read(["mutable://users/alice"]);
const user = JSON.parse(dec(payload as Uint8Array));
```

### Streaming large byte payloads

`BYTES_ENTITY.payload` accepts either `Uint8Array` or
`ReadableStream<Uint8Array>` (same union as `fetch` `BodyInit`). Backends with
native streaming (`fs`, `s3`, `ipfs`) keep streams end-to-end; the rest collect
to bytes.

```ts
const res = await fetch("https://example.com/big.bin");
await bytes.receive([["hash://big", res.body!]]);

const [[, payload]] = await fsBytes.read(["hash://big"]);
await (payload as ReadableStream<Uint8Array>).pipeTo(somewhere);
```

## Reading contract

`read()` takes **urls** (uri + query string). The url grammar is defined by
`@bandeira-tech/b3nd-core/url`:

```
<uri>[?fn=<fn>][&<param>=<value>...][&x-<ns>.<key>=<value>...]
```

Reserved `fn` values:

- `read` — point read of a single uri
- `ls` — list entries under a prefix
- `count` — count entries under a prefix
- `x-…` — provider-defined extension fns

Standard params honoured by every backend:

- `limit`, `page` — pagination
- `sortBy=uri`, `sortOrder=asc|desc` — sorting (only `uri` is supported
  package-wide)
- `format=full` returns `Output[]`; `format=uris` returns `string[]`

The package neither parses urls nor fills in absent fields — urls arrive
fully-resolved from upstream (`@bandeira-tech/b3nd-core/url`) and the backend
dispatches on whatever `fn` and params the parser produced.

Throws on `pattern`, `cursor`, unknown `sortBy`, and unknown `format`.

### Locked semantics

- **Strict schemas.** Records must match `schema.fields`. Extra or mistyped
  fields are per-entry errors, not silent coercions.
- **Miss is `payload === undefined`.** A point read for an absent uri returns
  `[inputUrl, undefined]`. Misses are _content_, not errors.
- **`ls` and `count` are shallow direct-leaves only.** An entry is _in_
  `ls(prefix)` iff its URI is `prefix + <segment>` with no further `/`.
  Subtree-only paths (`users/bob/posts/1` under `users/`) are absent from both.
  Callers that want recursion call `ls` per level.
- **`format=uris` skips payload reads.** Every backend implements this as a fast
  path (S3 / IPFS / FS / IndexedDB never fetch bodies; Postgres / SQLite issue
  `SELECT uri`; Mongo uses a projection; Elasticsearch passes `_source: false`).
- **Unsupported params throw.** Misses are payload, but bad params are
  programmer errors.
- **Atomic batches when advertised.** Backends that declare
  `capabilities.atomicBatch: true` (Postgres, SQLite) wrap the batch in a
  transaction — every entry commits together or none do. On failure every result
  carries the same root-cause error.
- **Structured errors.** Write and delete failures carry an
  `errorDetail?:
  B3ndError` with `code: "STORAGE_ERROR"` and (when
  entry-attributable) the failing `uri`. The `error: string` field is kept for
  human-readable logs.

## Testing

- `deno task test` — runs every backend's unit suite against an in-memory mock,
  plus the client tests and the cross-cutting integration suite under `tests/`.
- `deno task test:integration:{postgres,mongo,sqlite,fs,ipfs,s3,elasticsearch}`
  — runs the same suite against real backends. Wired up in CI; locally requires
  the matching service running on the conventional port.
- `deno task test:integration:{indexeddb,localstorage}` — runs the suites inside
  a real headless Chromium via Astral + esbuild. Astral downloads its own
  Chromium on first run.
- `deno task check`, `deno lint`, `deno fmt --check .` — type/lint/format gates.

## License

MIT — see `LICENSE`.
