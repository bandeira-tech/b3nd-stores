/**
 * IpfsStore Tests
 *
 * Uses a mock IpfsExecutor backed by an in-memory Map.
 */

/// <reference lib="deno.ns" />

import { runSharedStoreSuite } from "../_testing/shared-store-suite.ts";
import { IpfsStore } from "./store.ts";
import type { IpfsExecutor } from "./mod.ts";

/** In-memory IPFS executor that simulates IPFS node operations. */
function createMockIpfsExecutor(): IpfsExecutor {
  const objects = new Map<string, string>();
  const pins = new Set<string>();
  let cidCounter = 0;

  return {
    add: async (content: string) => {
      const cid = `QmTest${++cidCounter}`;
      objects.set(cid, content);
      return cid;
    },

    cat: async (cid: string) => {
      const content = objects.get(cid);
      if (content === undefined) {
        throw new Error(`CID not found: ${cid}`);
      }
      return content;
    },

    pin: async (cid: string) => {
      pins.add(cid);
    },

    unpin: async (cid: string) => {
      pins.delete(cid);
    },

    listPins: async () => {
      return [...pins];
    },

    isOnline: async () => true,
  };
}

runSharedStoreSuite("IpfsStore", {
  create: () => {
    const executor = createMockIpfsExecutor();
    return new IpfsStore(executor);
  },
  // IPFS store uses content-addressed CIDs + an internal index.
  // Listing via trailing slash depends on the internal URI→CID index.
  supportsList: true,
});
