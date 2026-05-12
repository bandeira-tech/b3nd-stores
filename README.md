# @bandeira-tech/b3nd-stores

Persistent `Store` implementations for the B3nd framework. One package, nine
backends, one uniform contract:

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
delete by uri. Wrap a Store with a protocol client (`SimpleClient`,
`DataStoreClient`, etc.) from `@bandeira-tech/b3nd-core` to get a
`ProtocolInterfaceNode`.

## Backends

| Backend       | Import                                     | Executor                               | Push-down                                       |
| ------------- | ------------------------------------------ | -------------------------------------- | ----------------------------------------------- |
| PostgreSQL    | `@bandeira-tech/b3nd-stores/postgres`      | inject any `pg`-style executor         | `ls` / `count` via `LIKE … AND NOT LIKE …%/%`   |
| SQLite        | `@bandeira-tech/b3nd-stores/sqlite`        | inject any `@db/sqlite`-style executor | same as Postgres                                |
| MongoDB       | `@bandeira-tech/b3nd-stores/mongo`         | inject a `MongoExecutor`               | regex filter `^<prefix>[^/]+$`                  |
| Elasticsearch | `@bandeira-tech/b3nd-stores/elasticsearch` | inject an `ElasticsearchExecutor`      | `regexp` query + `_count` endpoint              |
| S3            | `@bandeira-tech/b3nd-stores/s3`            | inject an `S3Executor`                 | `listObjects(prefix)` + client-side leaf filter |
| Filesystem    | `@bandeira-tech/b3nd-stores/fs`            | inject an `FsExecutor`                 | direct-child file listing                       |
| IPFS          | `@bandeira-tech/b3nd-stores/ipfs`          | inject an `IpfsExecutor`               | in-memory `uri → CID` index                     |
| LocalStorage  | `@bandeira-tech/b3nd-stores/localstorage`  | injects browser `Storage`              | flat key scan                                   |
| IndexedDB     | `@bandeira-tech/b3nd-stores/indexeddb`     | injects `indexedDB` / `IDBKeyRange`    | bounded cursor with early termination           |

## Quick start (Postgres)

```ts
import {
  generatePostgresSchema,
  PostgresStore,
} from "jsr:@bandeira-tech/b3nd-stores/postgres";

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
  in-memory mock.
- `deno task test:integration:{postgres,mongo,sqlite,fs,ipfs,s3}` — runs the
  same 32 tests against real backends (started in CI; locally requires the
  matching service).
- `deno task check`, `deno lint`, `deno fmt --check .` — type/lint/format gates.

No integration tests for elasticsearch, localstorage, or indexeddb yet.

## License

MIT — see `LICENSE`.
