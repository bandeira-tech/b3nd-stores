/**
 * SimpleClient — bare ProtocolInterfaceNode over a Store.
 *
 * No protocol awareness. receive() writes the message at its URI.
 * No envelope decomposition, no input deletion, no output fan-out.
 *
 * Use this when you want raw b3nd storage without any protocol
 * semantics — the data shape is entirely up to the caller.
 *
 * Observe is implemented at the client layer via `ObserveEmitter`:
 * each successful write emits a change event. Since SimpleClient
 * never deletes, observe only surfaces writes.
 *
 * @example
 * ```typescript
 * import { SimpleClient } from "@bandeira-tech/b3nd-sdk";
 * import { MemoryStore } from "@bandeira-tech/b3nd-sdk";
 *
 * const store = new MemoryStore();
 * const client = new SimpleClient(store);
 *
 * // Writes the message at "mutable://app/config"
 * await client.receive([
 *   ["mutable://app/config", { theme: "dark" }],
 * ]);
 *
 * // Reads it back
 * const results = await client.read(["mutable://app/config"]);
 *
 * // Observe writes for a pattern url
 * const ac = new AbortController();
 * for await (const change of client.observe(["mutable://app/*"], ac.signal)) {
 *   console.log(change.uri, change.record?.data);
 * }
 * ```
 */

import type {
  Message,
  Output,
  ProtocolInterfaceNode,
  ReceiveResult,
  StatusResult,
  Store,
} from "@bandeira-tech/b3nd-core/types";
import { ObserveEmitter } from "@bandeira-tech/b3nd-core";

export class SimpleClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: Store;

  constructor(store: Store) {
    super();
    this.store = store;
  }

  async receive(msgs: Message[]): Promise<ReceiveResult[]> {
    const entries = msgs.map(([uri, payload]) => ({
      uri,
      data: payload,
    }));

    const writeResults = await this.store.write(entries);

    for (let i = 0; i < writeResults.length; i++) {
      if (writeResults[i].success) {
        this._emit(entries[i].uri, entries[i].data);
      }
    }

    return writeResults.map((r) => ({
      accepted: r.success,
      error: r.error,
    }));
  }

  read<T = unknown>(urls: string[]): Promise<Output<T>[]> {
    return this.store.read<T>(urls);
  }

  status(): Promise<StatusResult> {
    return this.store.status();
  }
}
