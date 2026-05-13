/**
 * DataStoreClient — null-aware ProtocolInterfaceNode over a Store.
 *
 * Wraps any `Store` and translates the wire convention into Store
 * calls. Knows nothing about envelopes, signatures, or protocol
 * payload shapes — its only job is the deletion-as-data convention:
 *
 *   `[uri, null]`    → store.delete([uri])
 *   `[uri, bytes]`   → store.write([{ uri, payload: bytes }])
 *
 * The Store is byte-only — message payloads must be `Uint8Array` or
 * `null` (to signal delete). Higher layers own serialization.
 *
 * Observe is implemented at the client layer via `ObserveEmitter`:
 * each successful write emits `(uri, bytes)`; each successful delete
 * emits `(uri, null)`.
 *
 * @example
 * ```typescript
 * import { DataStoreClient } from "@bandeira-tech/b3nd-save/clients";
 * import { MemoryStore } from "@bandeira-tech/b3nd-save/memory";
 *
 * const store = new MemoryStore();
 * const client = new DataStoreClient(store);
 *
 * // Write
 * await client.receive([
 *   ["mutable://app/config", new TextEncoder().encode("dark")],
 * ]);
 *
 * // Delete (null payload is the convention)
 * await client.receive([["mutable://app/config", null]]);
 * ```
 */

import type {
  Message,
  Output,
  ProtocolInterfaceNode,
  ReceiveResult,
  StatusResult,
} from "@bandeira-tech/b3nd-core/types";
import { ObserveEmitter } from "@bandeira-tech/b3nd-core";
import type { Store } from "../types.ts";

export class DataStoreClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: Store;

  constructor(store: Store) {
    super();
    this.store = store;
  }

  async receive(
    msgs: Message<Uint8Array | null>[],
  ): Promise<ReceiveResult[]> {
    const results: ReceiveResult[] = [];
    // Partition into writes and deletes. Most batches are uniform —
    // partitioning preserves Store batching when it is.
    const writeEntries: {
      uri: string;
      payload: Uint8Array;
      index: number;
    }[] = [];
    const deleteUris: { uri: string; index: number }[] = [];
    for (let i = 0; i < msgs.length; i++) {
      const [uri, payload] = msgs[i];
      if (!uri || typeof uri !== "string") {
        results[i] = { accepted: false, error: "Message URI is required" };
        continue;
      }
      if (payload === null) {
        deleteUris.push({ uri, index: i });
      } else {
        writeEntries.push({ uri, payload, index: i });
      }
    }

    // Initialize results with placeholders so the order stays correct.
    for (let i = 0; i < msgs.length; i++) {
      if (results[i] === undefined) {
        results[i] = { accepted: false, error: "not processed" };
      }
    }

    if (writeEntries.length > 0) {
      const writeResults = await this.store.write(
        writeEntries.map((e) => ({ uri: e.uri, payload: e.payload })),
      );
      for (let j = 0; j < writeResults.length; j++) {
        const e = writeEntries[j];
        const r = writeResults[j];
        results[e.index] = { accepted: r.success, error: r.error };
        if (r.success) this._emit(e.uri, e.payload);
      }
    }

    if (deleteUris.length > 0) {
      const deleteResults = await this.store.delete(
        deleteUris.map((d) => d.uri),
      );
      const deleted: string[] = [];
      for (let j = 0; j < deleteResults.length; j++) {
        const d = deleteUris[j];
        const r = deleteResults[j];
        results[d.index] = { accepted: r.success, error: r.error };
        if (r.success) deleted.push(d.uri);
      }
      if (deleted.length > 0) this._emitDeletes(deleted);
    }

    return results;
  }

  read<T = Uint8Array>(urls: string[]): Promise<Output<T>[]> {
    return this.store.read<T>(urls);
  }

  status(): Promise<StatusResult> {
    return this.store.status();
  }
}
