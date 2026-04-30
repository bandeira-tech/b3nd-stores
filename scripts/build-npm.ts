#!/usr/bin/env -S deno run -A
// deno-lint-ignore-file no-import-prefix
/**
 * Build an NPM package from the Deno source via @deno/dnt.
 *
 * Output: ./npm/  — published as `@bandeira-tech/b3nd-stores` on
 * npmjs.com. Same import surface as JSR.
 *
 * Each subpath is environment-specific by design:
 *   - postgres / mongo / sqlite / fs / ipfs / s3 / elasticsearch  → Node
 *   - indexeddb / localstorage                                    → browser
 */

import { build, emptyDir } from "jsr:@deno/dnt@^0.42.1";

const denoJson = JSON.parse(await Deno.readTextFile("./deno.json"));
const version = denoJson.version as string;

await emptyDir("./npm");

await build({
  entryPoints: [
    { name: "./postgres", path: "./postgres/mod.ts" },
    { name: "./mongo", path: "./mongo/mod.ts" },
    { name: "./sqlite", path: "./sqlite/mod.ts" },
    { name: "./fs", path: "./fs/mod.ts" },
    { name: "./ipfs", path: "./ipfs/mod.ts" },
    { name: "./s3", path: "./s3/mod.ts" },
    { name: "./elasticsearch", path: "./elasticsearch/mod.ts" },
    { name: "./localstorage", path: "./localstorage/mod.ts" },
    { name: "./indexeddb", path: "./indexeddb/mod.ts" },
  ],
  outDir: "./npm",
  shims: { deno: false },
  test: false,
  scriptModule: false,
  compilerOptions: {
    target: "ES2022",
    lib: ["ES2022", "DOM", "DOM.Iterable"],
  },
  // TODO: once @bandeira-tech/b3nd-core is on npm, map the JSR core
  // dep so dnt declares it as a peer dep instead of vendoring its
  // source. Until then, the npm build will inline a copy of core's
  // types — small footprint, but suboptimal.
  package: {
    name: "@bandeira-tech/b3nd-stores",
    version,
    description: denoJson.description,
    license: "MIT",
    repository: {
      type: "git",
      url: "git+https://github.com/bandeira-tech/b3nd-stores.git",
    },
    bugs: {
      url: "https://github.com/bandeira-tech/b3nd-stores/issues",
    },
    homepage: "https://github.com/bandeira-tech/b3nd-stores#readme",
    engines: {
      node: ">=20",
    },
    sideEffects: false,
  },
  postBuild() {
    Deno.copyFileSync("LICENSE", "npm/LICENSE");

    const pkgPath = "npm/package.json";
    const pkg = JSON.parse(Deno.readTextFileSync(pkgPath));
    for (const [name, entry] of Object.entries(pkg.exports)) {
      const e = entry as { import?: string; types?: string };
      if (e.import && !e.types) {
        e.types = e.import.replace(/\.js$/, ".d.ts");
        pkg.exports[name] = { types: e.types, import: e.import };
      }
    }
    Deno.writeTextFileSync(pkgPath, JSON.stringify(pkg, null, 2) + "\n");
  },
});

console.log(`\n✔ Built @bandeira-tech/b3nd-stores@${version} → ./npm/`);
