/**
 * FsStore Integration Tests
 *
 * Runs the shared store suite against the real filesystem.
 * No external service needed — uses Deno.makeTempDir().
 */

/// <reference lib="deno.ns" />

import { ensureDir } from "@std/fs/ensure-dir";
import { dirname } from "@std/path";
import { runSharedStoreSuite } from "../../../tests/runners/shared-store-suite.ts";
import { FsStore } from "./store.ts";
import type { FsExecutor } from "./mod.ts";
import type { StorePayload } from "../../types.ts";

function createFsExecutor(_rootDir: string): FsExecutor {
  return {
    async readFile(path: string): Promise<ReadableStream<Uint8Array>> {
      const file = await Deno.open(path, { read: true });
      return file.readable;
    },

    async writeFile(path: string, content: StorePayload): Promise<void> {
      await ensureDir(dirname(path));
      if (content instanceof Uint8Array) {
        await Deno.writeFile(path, content);
        return;
      }
      // ReadableStream: open the file and pipe directly so large
      // payloads never need to fit in memory.
      const file = await Deno.open(path, {
        write: true,
        create: true,
        truncate: true,
      });
      await content.pipeTo(file.writable);
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
      const files: string[] = [];
      try {
        for await (const entry of Deno.readDir(dir)) {
          if (entry.isFile) files.push(entry.name);
        }
      } catch {
        return [];
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
