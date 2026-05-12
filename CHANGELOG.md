# Changelog

## 0.3.0 — Store contract migration to `b3nd-core@0.15`

**Breaking across every backend.** This release rewrites all nine stores against
the new `Store` contract from `@bandeira-tech/b3nd-core@^0.15.0`.

### Contract changes (apply to every store)

- **Tuple output.** `Store.read()` now returns `Output<T> = [uri, payload]`
  tuples, 1:1 with the input urls. The previous
  `{ success, record: { data,
  values } }` envelope is gone. Misses are encoded
  in the payload — package-wide convention is `payload === undefined`.
- **URLs, not URIs.** `read()` accepts urls (uri + query string). The function
  to run (`fn=read` / `fn=ls` / `fn=count` / `fn=x-…`) and the standard
  parameters (`limit`, `page`, `sortBy`, `sortOrder`, `format`, `pattern`,
  `cursor`) come from the query string. See `@bandeira-tech/b3nd-core/url`.
- **`fn=ls` and `fn=count` are shallow direct-leaves only.** Entries whose URI
  is `prefix + <segment>` with no further `/` are surfaced; subtree-only paths
  are absent. Diverges from the recursive `MemoryStore` reference — this is
  uniform across all nine backends so clients can reason about ls behaviour
  once. Clients that want recursion call `ls` repeatedly.
- **Strict params.** `pattern` and `cursor` throw "not supported" everywhere in
  this release. Unknown `sortBy` or `format` throws too. The previous
  silent-no-op behaviour is gone.
- **`fn=count` returns a number.** Number of direct leaves under the prefix.
- **`format=uris` fast path.** `fn=ls&format=uris` returns `string[]` instead of
  `Output[]`. Every store skips fetching payload bodies in this mode — matters
  most for s3, ipfs, fs, indexeddb, and elasticsearch (`_source: false`).
- **`StoreEntry.values` removed.** `StoreEntry` is now `{ uri, data }`.
- **`status().fns`** advertises the supported read functions, e.g.
  `["read","ls","count"]`. Rigs can validate caller requests against this.

### Per-backend breaking changes

| Store             | What broke                                                                                                                                                         | Migration                                                                                                                               |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| **postgres**      | Schema drops the `values` JSONB column from the data table and `by_program` view                                                                                   | `ALTER TABLE <prefix>_data DROP COLUMN "values"; DROP VIEW <prefix>_data_by_program; …` then regenerate with `generatePostgresSchema()` |
| **sqlite**        | Schema drops the `"values"` TEXT column from the data table                                                                                                        | `ALTER TABLE <prefix>_data DROP COLUMN "values"` (sqlite ≥ 3.35) or recreate the table                                                  |
| **mongo**         | `MongoExecutor.countDocuments` and `.deleteOne` are now required (were optional); `findMany` gains optional `projection: Record<string, 0 \| 1>`                   | Implement the two required methods on custom executors; existing real-driver wrappers already satisfy the surface                       |
| **elasticsearch** | `ElasticsearchExecutor.count` is new and required; `.delete` is now required (was optional); `search` returns `_source` as optional                                | Implement `count` (maps to `_count` endpoint) and `delete` on custom executors                                                          |
| **fs**            | File body simplified from `{ values, data }` envelope to top-level encoded payload; `FsExecutor.listFiles` is now documented to return **direct-child files only** | Greenfield only — existing files won't parse. Rewrite or migrate offline                                                                |
| **s3**            | Object body simplified from `{ values, data }` envelope to top-level encoded payload                                                                               | Greenfield only — existing objects won't parse                                                                                          |
| **ipfs**          | Pinned content body simplified from `{ values, data }` envelope to top-level encoded payload                                                                       | Greenfield only — existing pins won't parse                                                                                             |
| **indexeddb**     | `StoredRecord` schema drops the `values` field; new optional `IDBKeyRange` constructor parameter; `capabilities().binaryData` is now `true`                        | Recreate the IndexedDB store; pass `IDBKeyRange` if injecting a mock factory                                                            |
| **localstorage**  | Value body simplified to JSON-stringified encoded payload (no envelope)                                                                                            | Greenfield only                                                                                                                         |

### Internal & infra

- New `_shared/` helpers: `dispatchRead`, `validateReadParams`,
  `applyReadParams`, `encodeBinaryForJson` / `decodeBinaryFromJson` (replacing
  the helpers that were removed from `b3nd-core@0.15`).
- Shared test suite (`_testing/shared-store-suite.ts`) rewritten against the new
  contract. Every store passes 32 tests against an in-memory mock; real backend
  integrations run in CI for postgres, mongo, sqlite, fs, ipfs, s3.
- `indexeddb` unit tests now run in Deno via `npm:fake-indexeddb@5`.
- `s3` integration suite isolates tests with a unique per-test key prefix.
- No `elasticsearch` integration test yet; no real-browser test for `indexeddb`
  or `localstorage` yet — both planned as follow-ups.

## 0.2.0 and earlier

Predates this changelog. See git history.
