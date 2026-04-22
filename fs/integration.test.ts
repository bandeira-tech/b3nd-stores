/**
 * FsStore Integration Tests
 *
 * Runs the shared store suite against the real filesystem.
 * No external service needed — uses Deno.makeTempDir().
 */

/// <reference lib="deno.ns" />

import { ensureDir } from "@std/fs/ensure-dir";
import { walk } from "@std/fs/walk";
import { dirname, relative } from "@std/path";
import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { FsStore } from "./store.ts";
import type { FsExecutor } from "./mod.ts";

function createFsExecutor(_rootDir: string): FsExecutor {
  return {
    async readFile(path: string): Promise<string> {
      return await Deno.readTextFile(path);
    },

    async writeFile(path: string, content: string): Promise<void> {
      await ensureDir(dirname(path));
      await Deno.writeTextFile(path, content);
    },

    async removeFile(path: string): Promise<void> {
      await Deno.remove(path);
    },

    async exists(path: string): Promise<boolean> {
      try {
        await Deno.stat(path);
        return true;
      } catch {
        return false;
      }
    },

    async listFiles(dir: string): Promise<string[]> {
      try {
        await Deno.stat(dir);
      } catch {
        return [];
      }

      const files: string[] = [];
      for await (
        const entry of walk(dir, { includeFiles: true, includeDirs: false })
      ) {
        files.push(relative(dir, entry.path));
      }
      return files;
    },
  };
}

// Each test gets a fresh temp directory
let lastTempDir: string | undefined;

runSharedStoreSuite("FsStore (integration)", {
  create: async () => {
    const tempDir = await Deno.makeTempDir({ prefix: "b3nd_fs_test_" });
    lastTempDir = tempDir;
    const executor = createFsExecutor(tempDir);
    return new FsStore(tempDir, executor);
  },
});

// Cleanup after all tests
Deno.test({
  name: "FsStore (integration) - cleanup",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    if (lastTempDir) {
      await Deno.remove(lastTempDir, { recursive: true }).catch(() => {});
    }
  },
});
