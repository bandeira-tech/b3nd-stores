# @bandeira-tech/b3nd-save

The **data-saving layer** for B3nd — everything between a node's
`ProtocolInterfaceNode` and the storage it persists to. One package covers:

- **Backends** — ten `Store` implementations on a single uniform contract.
- **Clients** — Store→`ProtocolInterfaceNode` adapters that turn raw storage
  into something a `Rig` can talk to.
- **Factory** — URL-based composition so apps can wire backends at runtime
  without coupling to specific implementations.
- **Shared helpers** — for authors building their own backends without
  re-deriving the contract details.

```ts
type StorePayload = Uint8Array | ReadableStream<Uint8Array>;

interface StoreEntry {
  uri: string;
  payload: StorePayload;
}

interface Store {
  write(entries: StoreEntry[]): Promise<StoreWriteResult[]>;
  read<T = StorePayload>(urls: string[]): Promise<Output<T>[]>;
  delete(uris: string[]): Promise<DeleteResult[]>;
  status(): Promise<StatusResult>;
  capabilities?(): StoreCapabilities;
}
```

`Store` is **mechanical byte storage** with no protocol awareness — write, read,
delete bytes by uri. Higher layers (apps, protocol clients) own serialization.
Wrap a `Store` with a client from `@bandeira-tech/b3nd-save/clients`
(`SimpleClient`, `DataStoreClient`) to get a `ProtocolInterfaceNode`.

## Imports

Each subpath is independent — import only what you need and the rest stays out
of your bundle.

```ts
// Narrow imports — footprint-aware
import { PostgresStore } from "@bandeira-tech/b3nd-save/postgres";
import { SimpleClient } from "@bandeira-tech/b3nd-save/clients";
import { createStoreFromUrl } from "@bandeira-tech/b3nd-save/factory";

// Root barrel — convenient, namespaced
import { clients, factory, postgres } from "@bandeira-tech/b3nd-save";
const store = new postgres.PostgresStore("myapp", executor);
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

"Streams?" = whether `read` returns a `ReadableStream<Uint8Array>` directly (no
buffering) on this backend. Buffered backends collect any streamed write input
to bytes before storing and always return `Uint8Array` on read.

## Clients and factory

- **`@bandeira-tech/b3nd-save/clients`** — `SimpleClient` and `DataStoreClient`.
  These wrap any `Store` to produce a `ProtocolInterfaceNode` that a `Rig` can
  talk to.
- **`@bandeira-tech/b3nd-save/factory`** — `createStoreFromUrl`,
  `createClientFromUrl`, `createStoreResolver`, `createClientResolver`. Maps URL
  schemes to Stores or clients. **No protocols are built-in** — every backend
  (memory included) plugs in via `BackendResolver[]`. The factory resolves only
  what you register.
- **`@bandeira-tech/b3nd-save/shared`** — helpers for backend authors:
  `dispatchRead`, `validateReadParams`, `applyReadParams`, `storageFailure`
  (catch-block helper that builds a structured `B3ndError`), and `toBytes` /
  `toStream` payload normalizers. Use these when implementing a new `Store` so
  it matches the contract the built-ins follow.

## Quick start (Postgres)

```ts
import {
  generatePostgresSchema,
  PostgresStore,
} from "jsr:@bandeira-tech/b3nd-save/postgres";

const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);

// 1. Initialise the schema (one-time)
await myDb.query(generatePostgresSchema("myapp"));

// 2. Build a Store
const store = new PostgresStore("myapp", myExecutor);

// 3. Write — payloads are bytes. Encode whatever shape your app needs.
await store.write([
  {
    uri: "mutable://users/alice",
    payload: enc(JSON.stringify({ name: "Alice" })),
  },
  { uri: "mutable://users/bob", payload: enc(JSON.stringify({ name: "Bob" })) },
]);

// 4. Read — point read
const [[uri, alice]] = await store.read(["mutable://users/alice"]);
//                                       ^ tuple [uri, payload]
//                                         payload: Uint8Array on buffered backends,
//                                         ReadableStream<Uint8Array> on fs/s3/ipfs.
console.log(dec(alice as Uint8Array));

// 5. Read — list direct children
const [[, children]] = await store.read(["mutable://users/"]);
//                          children: Output[]  e.g. [["…/alice", …], ["…/bob", …]]

// 6. Read — count + uri-only listing
const [[, count]] = await store.read(["mutable://users/?fn=count"]);
const [[, uris]] = await store.read(["mutable://users/?fn=ls&format=uris"]);
```

The same shape works for every backend — only the constructor differs.

### Structured payloads — same shape, any encoding

The Store never inspects content. Encode on write, decode on read — the
round-trip is identical regardless of format. Pick whichever fits the data
(JSON for ad-hoc records, protobuf / FlatBuffers / CBOR / MessagePack for
schema-backed structured payloads, encrypted blobs for sealed content).

**JSON:**

```ts
const enc = (s: string) => new TextEncoder().encode(s);
const dec = (b: Uint8Array) => new TextDecoder().decode(b);

await store.write([
  {
    uri: "mutable://users/alice",
    payload: enc(JSON.stringify({ name: "Alice" })),
  },
]);

const [[, payload]] = await store.read(["mutable://users/alice"]);
const user = JSON.parse(dec(payload as Uint8Array));
// user.name === "Alice"
```

**Protobuf** (generated from a `.proto` with `protobuf-es` / `ts-proto` /
`protoc-gen-es`):

```ts
import { UserProfile } from "./gen/user_pb.ts";

await store.write([
  {
    uri: "mutable://users/alice",
    payload: new UserProfile({ name: "Alice" }).toBinary(),
  },
]);

const [[, payload]] = await store.read(["mutable://users/alice"]);
const user = UserProfile.fromBinary(payload as Uint8Array);
// user.name === "Alice"
```

Only the encode/decode line changes. The Store, the client, the wire shape, and
the read contract stay the same.

### Streaming large payloads

`StoreEntry.payload` accepts either bytes or a `ReadableStream<Uint8Array>`
(same union as `fetch` `BodyInit`). Backends that have native streaming (fs / s3
/ ipfs) keep streams end-to-end on both write and read; other backends collect
to bytes.

```ts
// Stream-in: pipe a fetch response into a Store without buffering
const res = await fetch("https://example.com/big.bin");
await store.write([{ uri: "hash://big", payload: res.body! }]);

// Stream-out on a streamer backend (fs, s3, ipfs)
const [[, payload]] = await fsStore.read(["hash://big"]);
// payload is ReadableStream<Uint8Array> on fs — pipe it somewhere
await (payload as ReadableStream<Uint8Array>).pipeTo(somewhere);

// Or collect on demand
import { toBytes } from "@bandeira-tech/b3nd-save/shared";
const bytes = await toBytes(payload as Uint8Array | ReadableStream<Uint8Array>);
```

## Reading contract

`read()` takes **urls** (uri + query string). The url grammar is defined by
`@bandeira-tech/b3nd-core/url`:

```
<uri>[?fn=<fn>][&<param>=<value>...][&x-<ns>.<key>=<value>...]
```

Reserved `fn`:

- `read` — point read (default for non-trailing-slash uris)
- `ls` — list under a prefix (default when uri ends with `/`)
- `count` — count of entries under a prefix
- `x-…` — provider-defined extension fns

Standard params honoured by every store in this package:

- `limit`, `page` — pagination
- `sortBy=uri`, `sortOrder=asc|desc` — sorting (only `uri` is supported
  package-wide)
- `format=full` (default) or `format=uris` — full `Output[]` vs. flat `string[]`

Throws on `pattern`, `cursor`, unknown `sortBy`, and unknown `format`.

### Locked semantics

- **Bytes-only payloads.** The Store does not parse, serialize, or otherwise
  inspect content. `payload` is `Uint8Array | ReadableStream<Uint8Array>` in;
  the same shape comes back out. Higher layers do JSON / encryption / signing /
  etc. on top.
- **Miss is `payload === undefined`.** A point read for an absent uri returns
  `[inputUrl, undefined]`. Misses are _content_, not errors.
- **`ls` and `count` are shallow direct-leaves only.** An entry is _in_
  `ls(prefix)` iff its URI is `prefix + <segment>` with no further `/`.
  Subtree-only paths (`users/bob/posts/1` under `users/`) are absent from both
  `ls` and `count`. Clients that want recursion call `ls` per level.
- **`format=uris` skips payload reads.** Every store implements this as a fast
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

- `deno task test` — runs every store's unit suite against an in-memory mock,
  plus the client and factory tests and the cross-cutting integration suite
  under `tests/`.
- `deno task test:integration:{postgres,mongo,sqlite,fs,ipfs,s3,elasticsearch}`
  — runs the same suite against real backends. Wired up in CI; locally requires
  the matching service running on the conventional port.
- `deno task test:integration:{indexeddb,localstorage}` — runs the suites inside
  a real headless Chromium via Astral + esbuild. Astral downloads its own
  Chromium on first run.
- `deno task check`, `deno lint`, `deno fmt --check .` — type/lint/format gates.

## License

MIT — see `LICENSE`.
