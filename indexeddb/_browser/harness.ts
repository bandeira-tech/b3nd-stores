/**
 * Browser entry point for the IndexedDBStore real-browser test
 * harness. This module is bundled by `indexeddb/integration.test.ts`
 * via esbuild and loaded by a tiny `<script type="module">` in
 * `harness.html`.
 *
 * On load it:
 *  1. Imports `deno-stub.ts` FIRST so `globalThis.Deno.test` exists
 *     before the shared suite registers any tests.
 *  2. Imports the real `IndexedDBStore` and the shared suite.
 *  3. Calls `runSharedStoreSuite` which (via the stub) collects every
 *     test definition into an in-memory list.
 *  4. Exposes `globalThis.runTests()` for the Deno-side driver to
 *     invoke — it runs each collected test against the real browser
 *     IndexedDB and returns a `{name, ok, error?}[]` result list.
 */

import { getCollectedTests } from "./deno-stub.ts";
import { IndexedDBStore } from "../store.ts";
import { runSharedStoreSuite } from "../../_testing/shared-store-suite.ts";

let testCount = 0;

runSharedStoreSuite("IndexedDBStore (browser)", {
  create: () =>
    new IndexedDBStore({
      databaseName: `b3nd-browser-test-${Date.now()}-${++testCount}`,
    }),
});

export interface BrowserTestResult {
  name: string;
  ok: boolean;
  error?: string;
}

async function runTests(): Promise<BrowserTestResult[]> {
  const results: BrowserTestResult[] = [];
  for (const def of getCollectedTests()) {
    try {
      await def.fn();
      results.push({ name: def.name, ok: true });
    } catch (e) {
      const err = e instanceof Error
        ? `${e.name}: ${e.message}${e.stack ? `\n${e.stack}` : ""}`
        : String(e);
      results.push({ name: def.name, ok: false, error: err });
    }
  }
  return results;
}

// deno-lint-ignore no-explicit-any
(globalThis as any).runTests = runTests;
// Signal harness is ready — the Deno driver waits on this flag
// rather than racing the module's top-level execution.
// deno-lint-ignore no-explicit-any
(globalThis as any).__b3ndHarnessReady = true;
