/**
 * Deno-side driver for browser-bound store integration tests.
 *
 * `runBrowserSuite({ harnessEntry })` bundles the harness with
 * esbuild + the deno loader, serves it via a one-shot Deno HTTP
 * server, launches headless Chromium via `@astral/astral`, runs the
 * collected shared-suite tests inside the page, and registers each
 * browser-side result as its own `Deno.test`.
 *
 * Each browser-bound store ships a thin `_browser/harness.ts` plus
 * an `integration.test.ts` that does:
 *
 *     await runBrowserSuite({
 *       harnessEntry: new URL("./_browser/harness.ts", import.meta.url),
 *     });
 *
 * The harness/html and esbuild bootstrap live here so the per-store
 * file stays a one-liner.
 */

/// <reference lib="deno.ns" />

import { launch } from "@astral/astral";
import * as esbuild from "esbuild";
import { denoPlugins } from "@luca/esbuild-deno-loader";
import { fromFileUrl } from "@std/path";
import type { BrowserTestResult } from "./deno-stub.ts";

const HARNESS_HTML_PATH = fromFileUrl(
  new URL("./harness.html", import.meta.url),
);

export interface RunBrowserSuiteOptions {
  /** URL or absolute path to the per-store browser harness entry. */
  harnessEntry: URL | string;
  /** Path to deno.json for the esbuild loader; defaults to repo root. */
  configPath?: string;
}

async function bundleHarness(
  entry: string,
  configPath: string,
): Promise<string> {
  const result = await esbuild.build({
    plugins: [
      // deno-lint-ignore no-explicit-any
      ...(denoPlugins({ configPath }) as any),
    ],
    entryPoints: [entry],
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
          headers: { "content-type": "application/javascript; charset=utf-8" },
        });
      }
      return new Response("not found", { status: 404 });
    },
  );
  return {
    url: `http://127.0.0.1:${server.addr.port}/`,
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
  // bundle off 127.0.0.1.
  const browser = await launch({
    headless: true,
    args: ["--no-sandbox", "--disable-dev-shm-usage"],
  });
  try {
    const page = await browser.newPage(url);
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

/**
 * Run the browser-side store suite and register each result as a
 * `Deno.test`. MUST be awaited at the top level of an integration
 * test file — Deno collects test registrations during top-level
 * module evaluation.
 */
export async function runBrowserSuite(
  opts: RunBrowserSuiteOptions,
): Promise<void> {
  const entry = typeof opts.harnessEntry === "string"
    ? opts.harnessEntry
    : fromFileUrl(opts.harnessEntry);
  const configPath = opts.configPath ?? fromFileUrl(
    new URL("../../deno.json", import.meta.url),
  );

  const html = await Deno.readTextFile(HARNESS_HTML_PATH);
  const bundle = await bundleHarness(entry, configPath);
  const server = await startServer(bundle, html);

  let results: BrowserTestResult[];
  try {
    results = await runInBrowser(server.url);
  } finally {
    await server.stop();
    // esbuild keeps a worker alive; stop it so the Deno test runner
    // doesn't hang on unresolved resources.
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
}
