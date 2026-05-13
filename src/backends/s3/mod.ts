/**
 * S3 backend for b3nd.
 *
 * Store implementation backed by Amazon S3. Requires an injected
 * S3Executor so the package does not depend on a specific S3 library.
 * Object bodies hold raw payload bytes — the store is opaque.
 */

export interface S3Executor {
  putObject: (
    key: string,
    body: Uint8Array,
    contentType: string,
  ) => Promise<void>;
  getObject: (key: string) => Promise<Uint8Array | null>;
  deleteObject: (key: string) => Promise<void>;
  listObjects: (prefix: string) => Promise<string[]>;
  headBucket: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { S3Store } from "./store.ts";
