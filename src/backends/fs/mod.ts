/**
 * Filesystem backend for b3nd.
 *
 * Store implementation backed by the local filesystem. Requires an
 * injected FsExecutor so the package does not depend on a specific
 * filesystem API. Files hold raw payload bytes — the store is opaque.
 */

export interface FsExecutor {
  readFile: (path: string) => Promise<Uint8Array>;
  writeFile: (path: string, content: Uint8Array) => Promise<void>;
  removeFile: (path: string) => Promise<void>;
  exists: (path: string) => Promise<boolean>;
  /**
   * List the names of *files* that are direct children of `dir`.
   * Subdirectories and entries deeper than one level MUST NOT be
   * returned — `ls` / `count` are shallow contracts.
   */
  listFiles: (dir: string) => Promise<string[]>;
  cleanup?: () => Promise<void>;
}

export { FsStore } from "./store.ts";
