/**
 * Browser stub for `Deno.test`.
 *
 * The shared store suite calls `Deno.test({ name, fn, ... })` to
 * register tests. In a real browser there is no `Deno`, so this
 * module installs a `globalThis.Deno = { test: (def) => push(def) }`
 * collector that just records the definitions. The harness later
 * iterates the collected list, runs each, and returns the results
 * to the Deno-side integration driver.
 *
 * IMPORTANT: this module must be imported BEFORE any module that
 * calls `Deno.test`. ES modules evaluate in import order, so listing
 * this side-effect import first in `harness.ts` is sufficient.
 */

export interface CollectedTest {
  name: string;
  fn: () => void | Promise<void>;
}

const collected: CollectedTest[] = [];

// deno-lint-ignore no-explicit-any
(globalThis as any).Deno = {
  // deno-lint-ignore no-explicit-any
  test(arg: any, maybeFn?: () => void | Promise<void>) {
    // Deno.test supports several call shapes; we only need the
    // object form the shared suite uses: `Deno.test({ name, fn })`.
    if (typeof arg === "object" && arg !== null) {
      collected.push({ name: arg.name, fn: arg.fn });
    } else if (typeof arg === "string" && typeof maybeFn === "function") {
      collected.push({ name: arg, fn: maybeFn });
    }
  },
  // The shared suite doesn't read these but other Deno-only paths
  // sometimes do. Keep them minimal but non-crashing.
  env: { get: (_: string) => undefined },
};

export function getCollectedTests(): CollectedTest[] {
  return collected;
}
