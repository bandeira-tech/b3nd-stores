/**
 * S3Store Integration Tests
 *
 * Runs the shared store suite against a real MinIO S3 instance.
 * Requires a running MinIO — see CI workflow or:
 *   cd /Users/m0/ws/b3nd && make up p=test
 *
 * Env: S3_ENDPOINT (default: http://localhost:59000)
 *      S3_ACCESS_KEY (default: minioadmin)
 *      S3_SECRET_KEY (default: minioadmin)
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { S3Store } from "./store.ts";
import type { S3Executor } from "./mod.ts";

const S3_ENDPOINT = Deno.env.get("S3_ENDPOINT") ??
  "http://localhost:59000";
const S3_ACCESS_KEY = Deno.env.get("S3_ACCESS_KEY") ?? "minioadmin";
const S3_SECRET_KEY = Deno.env.get("S3_SECRET_KEY") ?? "minioadmin";
const BUCKET = "b3nd-inttest";

function authHeaders(contentType?: string): Record<string, string> {
  const headers: Record<string, string> = {};
  if (contentType) headers["Content-Type"] = contentType;
  const credentials = btoa(`${S3_ACCESS_KEY}:${S3_SECRET_KEY}`);
  headers["Authorization"] = `Basic ${credentials}`;
  return headers;
}

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
        headers: authHeaders(contentType),
        body,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`S3 PUT failed: ${res.status} ${text}`);
      }
      await res.text();
    },

    async getObject(key: string): Promise<string | null> {
      const res = await fetch(url(key), {
        method: "GET",
        headers: authHeaders(),
      });
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
      const res = await fetch(url(key), {
        method: "DELETE",
        headers: authHeaders(),
      });
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
        {
          method: "GET",
          headers: authHeaders(),
        },
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
          headers: authHeaders(),
        });
        return res.ok;
      } catch {
        return false;
      }
    },
  };
}

// Setup: create bucket before tests
Deno.test({
  name: "S3Store (integration) - setup bucket",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const res = await fetch(`${S3_ENDPOINT}/${BUCKET}`, {
      method: "PUT",
      headers: authHeaders(),
    });
    // 200 = created, 409 = already exists — both OK
    if (!res.ok && res.status !== 409) {
      const text = await res.text();
      throw new Error(`Failed to create bucket: ${res.status} ${text}`);
    }
    await res.text();
  },
});

runSharedStoreSuite("S3Store (integration)", {
  create: () => {
    const executor = createS3Executor();
    return new S3Store(BUCKET, executor);
  },
});

// Cleanup: delete all objects and bucket
Deno.test({
  name: "S3Store (integration) - cleanup",
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    // List all objects
    const params = new URLSearchParams({
      "list-type": "2",
      prefix: "",
    });
    const listRes = await fetch(
      `${S3_ENDPOINT}/${BUCKET}?${params.toString()}`,
      {
        method: "GET",
        headers: authHeaders(),
      },
    );

    if (listRes.ok) {
      const xml = await listRes.text();
      const keys: string[] = [];
      const regex = /<Key>([^<]+)<\/Key>/g;
      let match;
      while ((match = regex.exec(xml)) !== null) {
        keys.push(match[1]);
      }

      // Delete each object
      for (const key of keys) {
        await fetch(`${S3_ENDPOINT}/${BUCKET}/${key}`, {
          method: "DELETE",
          headers: authHeaders(),
        }).then((r) => r.text());
      }
    } else {
      await listRes.text();
    }

    // Delete bucket
    await fetch(`${S3_ENDPOINT}/${BUCKET}`, {
      method: "DELETE",
      headers: authHeaders(),
    }).then((r) => r.text());
  },
});
