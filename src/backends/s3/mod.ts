/**
 * S3 backend for b3nd.
 *
 * Store implementation backed by Amazon S3. Requires an injected
 * S3Executor so the package does not depend on a specific S3 library.
 * Object bodies hold raw payload bytes — the store is opaque.
 *
 * `putObject` accepts the `StorePayload` union (fetch handles either
 * bytes or a `ReadableStream` as a request body); `getObject` returns
 * a `ReadableStream<Uint8Array>` so callers don't have to materialize
 * the whole object in memory unless they want to.
 */

import type { StorePayload } from "../../types.ts";

export interface S3Executor {
  putObject: (
    key: string,
    body: StorePayload,
    contentType: string,
  ) => Promise<void>;
  getObject: (key: string) => Promise<ReadableStream<Uint8Array> | null>;
  deleteObject: (key: string) => Promise<void>;
  listObjects: (prefix: string) => Promise<string[]>;
  headBucket: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { S3Store } from "./store.ts";
