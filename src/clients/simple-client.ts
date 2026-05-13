/**
 * SimpleClient — bare ProtocolInterfaceNode over a Store.
 *
 * No protocol awareness. `receive()` writes each message's payload at
 * its URI. The Store is byte-only — message payloads must be
 * `Uint8Array`. Higher layers (apps, protocol clients) own
 * serialization to bytes.
 *
 * Observe is implemented at the client layer via `ObserveEmitter`:
 * each successful write emits a change event. Since SimpleClient
 * never deletes, observe only surfaces writes.
 *
 * @example
 * ```typescript
 * import { SimpleClient } from "@bandeira-tech/b3nd-save/clients";
 * import { MemoryStore } from "@bandeira-tech/b3nd-save/memory";
 *
 * const store = new MemoryStore();
 * const client = new SimpleClient(store);
 *
 * await client.receive([
 *   ["mutable://app/config", new TextEncoder().encode("dark")],
 * ]);
 *
 * const [[, bytes]] = await client.read(["mutable://app/config"]);
 * new TextDecoder().decode(bytes as Uint8Array); // "dark"
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
import type { Store, StorePayload } from "../types.ts";

export class SimpleClient extends ObserveEmitter
  implements ProtocolInterfaceNode {
  readonly store: Store;

  constructor(store: Store) {
    super();
    this.store = store;
  }

  async receive(msgs: Message<StorePayload>[]): Promise<ReceiveResult[]> {
    const entries = msgs.map(([uri, payload]) => ({ uri, payload }));

    const writeResults = await this.store.write(entries);

    for (let i = 0; i < writeResults.length; i++) {
      if (writeResults[i].success) {
        this._emit(entries[i].uri, entries[i].payload);
      }
    }

    return writeResults.map((r) => ({
      accepted: r.success,
      error: r.error,
    }));
  }

  read<T = StorePayload>(urls: string[]): Promise<Output<T>[]> {
    return this.store.read<T>(urls);
  }

  status(): Promise<StatusResult> {
    return this.store.status();
  }
}
