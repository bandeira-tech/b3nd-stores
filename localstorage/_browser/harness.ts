/**
 * Browser entry point for the LocalStorageStore real-browser test
 * harness. Bundled by `localstorage/integration.test.ts` via the
 * shared runner.
 *
 * First import wires up `globalThis.Deno.test` collection. The
 * shared store suite then registers tests against the real browser
 * `localStorage`. Each test creates a new LocalStorageStore with a
 * unique keyPrefix so the shared origin's storage doesn't leak
 * state between tests.
 */

import { setupHarness } from "../../_testing/browser/deno-stub.ts";
import { LocalStorageStore } from "../store.ts";
import { runSharedStoreSuite } from "../../_testing/shared-store-suite.ts";

let testCount = 0;

runSharedStoreSuite("LocalStorageStore (browser)", {
  create: () =>
    new LocalStorageStore({
      keyPrefix: `b3nd-browser-test-${Date.now()}-${++testCount}:`,
    }),
});

setupHarness();
