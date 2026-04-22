/**
 * FsStore Tests
 *
 * Uses a mock FsExecutor backed by an in-memory Map.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { FsStore } from "./store.ts";
import type { FsExecutor } from "./mod.ts";

/** In-memory filesystem executor that simulates file operations. */
function createMockFsExecutor(): FsExecutor {
  const files = new Map<string, string>();

  return {
    readFile: async (path: string) => {
      const content = files.get(path);
      if (content === undefined) {
        throw new Error(`File not found: ${path}`);
      }
      return content;
    },

    writeFile: async (path: string, content: string) => {
      files.set(path, content);
    },

    removeFile: async (path: string) => {
      files.delete(path);
    },

    exists: async (path: string) => {
      // Root dir always exists, or check if any file starts with this path
      if (path === "/tmp/test-store") return true;
      return files.has(path) ||
        [...files.keys()].some((k) => k.startsWith(path));
    },

    listFiles: async (dir: string) => {
      // FsStore expects just filenames (not full paths) from the given directory
      const prefix = dir.endsWith("/") ? dir : `${dir}/`;
      const results: string[] = [];
      for (const key of files.keys()) {
        if (key.startsWith(prefix)) {
          // Return just the filename relative to the directory
          const relative = key.slice(prefix.length);
          // Only return direct children (no nested paths)
          if (!relative.includes("/")) {
            results.push(relative);
          }
        }
      }
      return results;
    },
  };
}

runSharedStoreSuite("FsStore", {
  create: () => {
    const executor = createMockFsExecutor();
    return new FsStore("/tmp/test-store", executor);
  },
});
