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
interface Store {
  write(entries: StoreEntry[]): Promise<StoreWriteResult[]>;
  read<T>(urls: string[]): Promise<Output<T>[]>;
  delete(uris: string[]): Promise<DeleteResult[]>;
  status(): Promise<StatusResult>;
  capabilities?(): StoreCapabilities;
}
```

`Store` is **mechanical storage** with no protocol awareness — write, read,
delete by uri. Wrap it with a client from `@bandeira-tech/b3nd-save/clients`
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

| Backend       | Import                                   | Executor                               | Push-down                                       |
| ------------- | ---------------------------------------- | -------------------------------------- | ----------------------------------------------- |
| Memory        | `@bandeira-tech/b3nd-save/memory`        | none                                   | in-memory tree walk over direct children        |
| PostgreSQL    | `@bandeira-tech/b3nd-save/postgres`      | inject any `pg`-style executor         | `ls` / `count` via `LIKE … AND NOT LIKE …%/%`   |
| SQLite        | `@bandeira-tech/b3nd-save/sqlite`        | inject any `@db/sqlite`-style executor | same as Postgres                                |
| MongoDB       | `@bandeira-tech/b3nd-save/mongo`         | inject a `MongoExecutor`               | regex filter `^<prefix>[^/]+$`                  |
| Elasticsearch | `@bandeira-tech/b3nd-save/elasticsearch` | inject an `ElasticsearchExecutor`      | `regexp` query + `_count` endpoint              |
| S3            | `@bandeira-tech/b3nd-save/s3`            | inject an `S3Executor`                 | `listObjects(prefix)` + client-side leaf filter |
| Filesystem    | `@bandeira-tech/b3nd-save/fs`            | inject an `FsExecutor`                 | direct-child file listing                       |
| IPFS          | `@bandeira-tech/b3nd-save/ipfs`          | inject an `IpfsExecutor`               | in-memory `uri → CID` index                     |
| LocalStorage  | `@bandeira-tech/b3nd-save/localstorage`  | injects browser `Storage`              | flat key scan                                   |
| IndexedDB     | `@bandeira-tech/b3nd-save/indexeddb`     | injects `indexedDB` / `IDBKeyRange`    | bounded cursor with early termination           |

## Clients and factory

- **`@bandeira-tech/b3nd-save/clients`** — `SimpleClient` and `DataStoreClient`.
  These wrap any `Store` to produce a `ProtocolInterfaceNode` that a `Rig` can
  talk to.
- **`@bandeira-tech/b3nd-save/factory`** — `createStoreFromUrl`,
  `createClientFromUrl`, `createStoreResolver`, `createClientResolver`. Maps URL
  schemes to Stores or clients. **No protocols are built-in** — every backend
  (memory included) plugs in via `BackendResolver[]`. The factory resolves only
  what you register.
- **`@bandeira-tech/b3nd-save/shared`** — helpers for backend authors: binary
  encode/decode, read-param validation, read-dispatch. Use these when
  implementing a new `Store` so it matches the contract the built-ins follow.

## Quick start (Postgres)

```ts
import {
  generatePostgresSchema,
  PostgresStore,
} from "jsr:@bandeira-tech/b3nd-save/postgres";

// 1. Initialise the schema (one-time)
await myDb.query(generatePostgresSchema("myapp"));

// 2. Build a Store
const store = new PostgresStore("myapp", myExecutor);

// 3. Write
await store.write([
  { uri: "mutable://users/alice", data: { name: "Alice" } },
  { uri: "mutable://users/bob", data: { name: "Bob" } },
]);

// 4. Read — point read
const [[uri, alice]] = await store.read(["mutable://users/alice"]);
//                                       ^ tuple [uri, payload]

// 5. Read — list direct children
const [[, children]] = await store.read(["mutable://users/"]);
//                          children: Output[]  e.g. [["…/alice", …], ["…/bob", …]]

// 6. Read — count + uri-only listing
const [[, count]] = await store.read(["mutable://users/?fn=count"]);
const [[, uris]] = await store.read(["mutable://users/?fn=ls&format=uris"]);
```

The same shape works for every backend — only the constructor differs.

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

## Testing

- `deno task test` — runs every store's unit suite (32 tests each) against an
  in-memory mock, plus the client and factory tests and the `_integration/`
  framework+memory integration suite.
- `deno task test:integration:{postgres,mongo,sqlite,fs,ipfs,s3,elasticsearch}`
  — runs the same 32 tests against real backends. Started in CI; locally
  requires the matching service running on the conventional port.
- `deno task test:integration:{indexeddb,localstorage}` — runs the suites inside
  a real headless Chromium via Astral + esbuild. Astral downloads its own
  Chromium on first run.
- `deno task check`, `deno lint`, `deno fmt --check .` — type/lint/format gates.

## License

MIT — see `LICENSE`.
