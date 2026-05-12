/**
 * LocalStorageStore Integration Tests — Real Browser.
 *
 * Drives the shared browser runner (`_testing/browser/runner.ts`)
 * with this store's harness. Each browser-side test surfaces as its
 * own `Deno.test`. No external services required — astral downloads
 * its own Chromium on first run and caches it.
 */

/// <reference lib="deno.ns" />

import { runBrowserSuite } from "../_testing/browser/runner.ts";

await runBrowserSuite({
  harnessEntry: new URL("./_browser/harness.ts", import.meta.url),
});
