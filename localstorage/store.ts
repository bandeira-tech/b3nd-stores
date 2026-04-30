/**
 * LocalStorageStore — browser localStorage implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Uses localStorage for simple persistent browser storage.
 */

import type {
  DeleteResult,
  ReadResult,
  StatusResult,
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "@bandeira-tech/b3nd-core/types";

/** Wrap Uint8Array for JSON round-tripping through localStorage */
function serializeData(data: unknown): unknown {
  if (data instanceof Uint8Array) {
    return {
      __b3nd_binary__: true,
      encoding: "base64",
      data: btoa(String.fromCharCode(...data)),
    };
  }
  return data;
}

/** Unwrap binary marker back to Uint8Array */
function deserializeData(data: unknown): unknown {
  if (
    data && typeof data === "object" &&
    (data as Record<string, unknown>).__b3nd_binary__ === true &&
    (data as Record<string, unknown>).encoding === "base64" &&
    typeof (data as Record<string, unknown>).data === "string"
  ) {
    const binary = atob((data as Record<string, unknown>).data as string);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }
  return data;
}

export class LocalStorageStore implements Store {
  private readonly keyPrefix: string;
  private readonly storage: Storage;

  constructor(config: {
    keyPrefix?: string;
    storage?: Storage;
  } = {}) {
    this.keyPrefix = config.keyPrefix || "b3nd:";
    this.storage = config.storage ||
      (typeof localStorage !== "undefined" ? localStorage : null!);

    if (!this.storage) {
      throw new Error("localStorage is not available in this environment");
    }
  }

  private getKey(uri: string): string {
    return `${this.keyPrefix}${uri}`;
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        const record = {
          values: entry.values,
          data: serializeData(entry.data),
        };
        this.storage.setItem(this.getKey(entry.uri), JSON.stringify(record));
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Write failed",
        });
      }
    }

    return results;
  }

  // ── Read ─────────────────────────────────────────────────────────

  async read<T = unknown>(uris: string[]): Promise<ReadResult<T>[]> {
    const results: ReadResult<T>[] = [];

    for (const uri of uris) {
      if (uri.endsWith("/")) {
        results.push(...this._list<T>(uri));
      } else {
        results.push(this._readOne<T>(uri));
      }
    }

    return results;
  }

  private _readOne<T>(uri: string): ReadResult<T> {
    try {
      const serialized = this.storage.getItem(this.getKey(uri));
      if (serialized === null) {
        return { success: false, error: "Not found" };
      }
      const raw = JSON.parse(serialized) as {
        values?: Record<string, number>;
        data: unknown;
      };
      return {
        success: true,
        record: {
          values: raw.values || {},
          data: deserializeData(raw.data) as T,
        },
      };
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : "Read failed",
      };
    }
  }

  private _list<T>(uri: string): ReadResult<T>[] {
    const results: ReadResult<T>[] = [];
    const prefix = this.getKey(uri);

    for (let i = 0; i < this.storage.length; i++) {
      const key = this.storage.key(i);
      if (key && key.startsWith(prefix)) {
        const childUri = key.substring(this.keyPrefix.length);
        const result = this._readOne<T>(childUri);
        if (result.success) {
          results.push({ ...result, uri: childUri });
        }
      }
    }

    return results;
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        this.storage.removeItem(this.getKey(uri));
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Delete failed",
        });
      }
    }

    return results;
  }

  // ── Status ───────────────────────────────────────────────────────

  status(): Promise<StatusResult> {
    try {
      const testKey = `${this.keyPrefix}__health_check__`;
      this.storage.setItem(testKey, "ok");
      this.storage.removeItem(testKey);
      return Promise.resolve({ status: "healthy", schema: [] });
    } catch {
      return Promise.resolve({ status: "unhealthy", schema: [] });
    }
  }

  capabilities(): StoreCapabilities {
    return { atomicBatch: false, binaryData: false };
  }
}
