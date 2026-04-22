/**
 * S3Store Integration Tests
 *
 * Runs the shared store suite against a real MinIO S3 instance.
 * Requires a running MinIO with a pre-created public bucket.
 *
 * In CI, the bucket is created by the workflow via `mc`.
 * Locally: cd /Users/m0/ws/b3nd && make up p=test
 *
 * Env: S3_ENDPOINT (default: http://localhost:59000)
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { S3Store } from "./store.ts";
import type { S3Executor } from "./mod.ts";

const S3_ENDPOINT = Deno.env.get("S3_ENDPOINT") ??
  "http://localhost:59000";
const BUCKET = "b3nd-inttest";

function createS3Executor(): S3Executor {
  function url(key: string): string {
    return `${S3_ENDPOINT}/${BUCKET}/${key}`;
  }

  return {
    async putObject(
      key: string,
      body: string,
      contentType: string,
    ): Promise<void> {
      const res = await fetch(url(key), {
        method: "PUT",
        headers: { "Content-Type": contentType },
        body,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 PUT failed: ${res.status} ${text}`);
      }
      await res.text();
    },

    async getObject(key: string): Promise<string | null> {
      const res = await fetch(url(key), { method: "GET" });
      if (res.status === 404) {
        await res.text();
        return null;
      }
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 GET failed: ${res.status} ${text}`);
      }
      return await res.text();
    },

    async deleteObject(key: string): Promise<void> {
      const res = await fetch(url(key), { method: "DELETE" });
      if (!res.ok && res.status !== 404) {
        const text = await res.text();
        throw new Error(`S3 DELETE failed: ${res.status} ${text}`);
      }
      await res.text();
    },

    async listObjects(prefix: string): Promise<string[]> {
      const params = new URLSearchParams({
        "list-type": "2",
        prefix,
      });
      const res = await fetch(
        `${S3_ENDPOINT}/${BUCKET}?${params.toString()}`,
        { method: "GET" },
      );
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 LIST failed: ${res.status} ${text}`);
      }
      const xml = await res.text();
      const keys: string[] = [];
      const regex = /<Key>([^<]+)<\/Key>/g;
      let match;
      while ((match = regex.exec(xml)) !== null) {
        keys.push(match[1]);
      }
      return keys;
    },

    async headBucket(): Promise<boolean> {
      try {
        const res = await fetch(`${S3_ENDPOINT}/${BUCKET}`, {
          method: "HEAD",
        });
        return res.ok;
      } catch {
        return false;
      }
    },
  };
}

runSharedStoreSuite("S3Store (integration)", {
  create: () => {
    const executor = createS3Executor();
    return new S3Store(BUCKET, executor);
  },
});
