/**
 * S3 backend for b3nd.
 *
 * Store implementation backed by Amazon S3. Requires an injected S3Executor
 * so the SDK does not depend on a specific S3 library.
 */

export interface S3Executor {
  putObject: (key: string, body: string, contentType: string) => Promise<void>;
  getObject: (key: string) => Promise<string | null>;
  deleteObject: (key: string) => Promise<void>;
  listObjects: (prefix: string) => Promise<string[]>;
  headBucket: () => Promise<boolean>;
  cleanup?: () => Promise<void>;
}

export { S3Store } from "./store.ts";
