/**
 * Filesystem backend for b3nd.
 *
 * Store implementation backed by the local filesystem. Requires an injected
 * FsExecutor so the SDK does not depend on a specific filesystem API.
 */

export interface FsExecutor {
  readFile: (path: string) => Promise<string>;
  writeFile: (path: string, content: string) => Promise<void>;
  removeFile: (path: string) => Promise<void>;
  exists: (path: string) => Promise<boolean>;
  listFiles: (dir: string) => Promise<string[]>;
  cleanup?: () => Promise<void>;
}

export { FsStore } from "./store.ts";
