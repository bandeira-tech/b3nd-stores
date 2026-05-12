/**
 * IndexedDBStore Integration Tests — Real Browser
 *
 * Bundles `indexeddb/_browser/harness.ts` for the browser, serves
 * it via a tiny Deno HTTP server, launches headless Chromium via
 * `@astral/astral`, runs the shared store suite inside the page
 * against the real browser IndexedDB, and surfaces each browser-side
 * test result as its own `Deno.test`.
 *
 * The harness is a single bundled ES module loaded by
 * `harness.html`. esbuild + `@luca/esbuild-deno-loader` resolves JSR
 * (`@std/assert`, `@bandeira-tech/b3nd-core/*`) and relative
 * (`../store.ts`, `../../_testing/shared-store-suite.ts`) imports
 * during bundling, so the browser receives a single self-contained
 * `harness.js`.
 *
 * No external services required — astral downloads its own Chromium
 * on first run and caches it.
 */

/// <reference lib="deno.ns" />

import { launch } from "@astral/astral";
import * as esbuild from "esbuild";
import { denoPlugins } from "@luca/esbuild-deno-loader";
import { fromFileUrl } from "@std/path";

const HARNESS_DIR = fromFileUrl(new URL("./_browser/", import.meta.url));
const HARNESS_ENTRY = `${HARNESS_DIR}harness.ts`;
const HARNESS_HTML_PATH = `${HARNESS_DIR}harness.html`;
const CONFIG_PATH = fromFileUrl(new URL("../deno.json", import.meta.url));

interface BrowserTestResult {
  name: string;
  ok: boolean;
  error?: string;
}

async function bundleHarness(): Promise<string> {
  const result = await esbuild.build({
    plugins: [
      // deno-lint-ignore no-explicit-any
      ...(denoPlugins({ configPath: CONFIG_PATH }) as any),
    ],
    entryPoints: [HARNESS_ENTRY],
    bundle: true,
    format: "esm",
    target: "chrome120",
    write: false,
    sourcemap: "inline",
    logLevel: "warning",
  });
  const code = result.outputFiles?.[0]?.text;
  if (!code) throw new Error("esbuild produced no output");
  return code;
}

async function startServer(
  bundle: string,
  html: string,
): Promise<{ url: string; stop: () => Promise<void> }> {
  const ac = new AbortController();
  const server = Deno.serve(
    { port: 0, signal: ac.signal, onListen: () => {} },
    (req: Request) => {
      const path = new URL(req.url).pathname;
      if (path === "/" || path === "/harness.html") {
        return new Response(html, {
          headers: { "content-type": "text/html; charset=utf-8" },
        });
      }
      if (path === "/harness.js") {
        return new Response(bundle, {
          headers: {
            "content-type": "application/javascript; charset=utf-8",
          },
        });
      }
      return new Response("not found", { status: 404 });
    },
  );
  const url = `http://127.0.0.1:${server.addr.port}/`;
  return {
    url,
    stop: async () => {
      ac.abort();
      await server.finished;
    },
  };
}

async function runInBrowser(url: string): Promise<BrowserTestResult[]> {
  // GitHub-Actions runners can't host Chromium's SUID sandbox (no
  // user namespaces available in their kernel), so we disable it.
  // `--disable-dev-shm-usage` works around the tiny /dev/shm that
  // containerised runners ship — Chromium falls back to /tmp.
  // Both flags are safe here: the only thing we load is our own
  // bundle off localhost.
  const browser = await launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  try {
    const page = await browser.newPage(url);
    // Wait until the bundled harness has finished its top-level
    // setup and stamped __b3ndHarnessReady = true. Polled rather
    // than a single waitForFunction call to surface clearer errors
    // if the bundle fails to load at all.
    await page.waitForFunction(
      // deno-lint-ignore no-explicit-any
      () => (globalThis as any).__b3ndHarnessReady === true,
    );
    const results = await page.evaluate(
      // deno-lint-ignore no-explicit-any
      () => (globalThis as any).runTests() as Promise<BrowserTestResult[]>,
    );
    return results;
  } finally {
    await browser.close();
  }
}

// ── Top-level: bundle, serve, run, then register one Deno.test per
// browser-side test so each failure surfaces individually in the
// Deno test runner output.

const html = await Deno.readTextFile(HARNESS_HTML_PATH);
const bundle = await bundleHarness();
const server = await startServer(bundle, html);

let results: BrowserTestResult[];
try {
  results = await runInBrowser(server.url);
} finally {
  await server.stop();
  // esbuild keeps a worker alive across bundles; stop it so the
  // Deno test runner doesn't hang on unresolved resources.
  await esbuild.stop();
}

for (const r of results) {
  Deno.test({
    name: r.name,
    sanitizeOps: false,
    sanitizeResources: false,
    fn: () => {
      if (!r.ok) throw new Error(r.error ?? "test failed in browser");
    },
  });
}
