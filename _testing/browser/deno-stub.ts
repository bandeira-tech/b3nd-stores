/**
 * Shared browser-side helpers for store integration test harnesses.
 *
 * Two responsibilities:
 *
 * 1. Install `globalThis.Deno = { test: (def) => collect }` so the
 *    shared store suite's `Deno.test` calls register into an
 *    in-memory list instead of running. This is a SIDE EFFECT of
 *    importing the module — must happen before any module that
 *    references `Deno.test`. ES modules execute in import order, so
 *    listing this side-effect import first in a store's harness is
 *    sufficient.
 *
 * 2. Expose `setupHarness()` to plumb `globalThis.runTests()` and
 *    `globalThis.__b3ndHarnessReady` for the Deno-side driver to
 *    pick up. Stores call this after their `runSharedStoreSuite(...)`
 *    invocation.
 */

export interface BrowserTestDef {
  name: string;
  fn: () => void | Promise<void>;
}

export interface BrowserTestResult {
  name: string;
  ok: boolean;
  error?: string;
}

const collected: BrowserTestDef[] = [];

// deno-lint-ignore no-explicit-any
(globalThis as any).Deno = {
  // deno-lint-ignore no-explicit-any
  test(arg: any, maybeFn?: () => void | Promise<void>) {
    // The shared suite uses the object form `Deno.test({ name, fn })`,
    // but accept the string-form variant too for forward compat.
    if (typeof arg === "object" && arg !== null) {
      collected.push({ name: arg.name, fn: arg.fn });
    } else if (typeof arg === "string" && typeof maybeFn === "function") {
      collected.push({ name: arg, fn: maybeFn });
    }
  },
  // Minimal shim — the shared suite doesn't read these but other
  // Deno-only paths sometimes do. Keep non-crashing.
  env: { get: (_: string) => undefined },
};

async function runTests(): Promise<BrowserTestResult[]> {
  const results: BrowserTestResult[] = [];
  for (const def of collected) {
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

/**
 * Plumb `globalThis.runTests()` and `__b3ndHarnessReady` so the
 * Deno-side driver can wait for the bundle to finish executing and
 * then trigger the run. Call once at the end of a store's harness.
 */
export function setupHarness(): void {
  // deno-lint-ignore no-explicit-any
  (globalThis as any).runTests = runTests;
  // deno-lint-ignore no-explicit-any
  (globalThis as any).__b3ndHarnessReady = true;
}
