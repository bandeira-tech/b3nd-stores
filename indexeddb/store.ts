/**
 * IndexedDBStore — browser IndexedDB implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Uses IndexedDB for large-scale persistent browser storage.
 */

/// <reference lib="dom" />

import type {
  DeleteResult,
  ReadResult,
  StatusResult,
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "@bandeira-tech/b3nd-sdk/types";

interface StoredRecord {
  uri: string;
  values: Record<string, number>;
  data: unknown;
}

// Minimal IndexedDB type definitions for cross-platform compatibility
interface IDBDatabase {
  name: string;
  version: number;
  objectStoreNames: { contains(name: string): boolean };
  close(): void;
  transaction(
    storeNames: string | string[],
    mode?: "readonly" | "readwrite",
  ): IDBTransaction;
  // deno-lint-ignore no-explicit-any
  createObjectStore(name: string, options?: any): IDBObjectStore;
}

interface IDBTransaction {
  objectStore(name: string): IDBObjectStore;
}

interface IDBObjectStore {
  // deno-lint-ignore no-explicit-any
  get(key: any): IDBRequest;
  // deno-lint-ignore no-explicit-any
  put(value: any): IDBRequest;
  // deno-lint-ignore no-explicit-any
  delete(key: any): IDBRequest;
  index(name: string): IDBIndex;
  // deno-lint-ignore no-explicit-any
  createIndex(name: string, keyPath: string): any;
}

interface IDBIndex {
  openCursor(): IDBRequest;
}

interface IDBRequest {
  // deno-lint-ignore no-explicit-any
  result: any;
  error: Error | null;
  // deno-lint-ignore no-explicit-any
  onsuccess: ((this: IDBRequest, ev: any) => void) | null;
  // deno-lint-ignore no-explicit-any
  onerror: ((this: IDBRequest, ev: any) => void) | null;
}

interface IDBOpenDBRequest extends IDBRequest {
  // deno-lint-ignore no-explicit-any
  onupgradeneeded: ((this: IDBOpenDBRequest, ev: any) => void) | null;
}

interface IDBFactory {
  open(name: string, version?: number): IDBOpenDBRequest;
}

export class IndexedDBStore implements Store {
  private readonly databaseName: string;
  private readonly storeName: string;
  private readonly version: number;
  private readonly indexedDB: IDBFactory;
  private db: IDBDatabase | null = null;

  constructor(config: {
    databaseName?: string;
    storeName?: string;
    version?: number;
    // deno-lint-ignore no-explicit-any
    indexedDB?: any;
  } = {}) {
    this.databaseName = config.databaseName || "b3nd";
    this.storeName = config.storeName || "records";
    this.version = config.version || 1;
    this.indexedDB = config.indexedDB ||
      // deno-lint-ignore no-explicit-any
      ((globalThis as any).indexedDB ?? null);

    if (!this.indexedDB) {
      throw new Error("IndexedDB is not available in this environment");
    }
  }

  private async initDB(): Promise<IDBDatabase> {
    if (this.db) return this.db;

    return new Promise<IDBDatabase>((resolve, reject) => {
      const request = this.indexedDB.open(this.databaseName, this.version);

      request.onerror = () =>
        reject(new Error(`Failed to open IndexedDB: ${request.error}`));

      request.onsuccess = () => {
        this.db = request.result;
        resolve(request.result);
      };

      request.onupgradeneeded = () => {
        const db = request.result as IDBDatabase;
        if (!db.objectStoreNames.contains(this.storeName)) {
          const store = db.createObjectStore(this.storeName, {
            keyPath: "uri",
          });
          store.createIndex("uri_index", "uri");
        }
      };
    });
  }

  private async getStore(
    mode: "readonly" | "readwrite" = "readonly",
  ): Promise<IDBObjectStore> {
    const db = await this.initDB();
    return db.transaction([this.storeName], mode).objectStore(this.storeName);
  }

  // ── Write ────────────────────────────────────────────────────────

  async write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    try {
      const store = await this.getStore("readwrite");

      for (const entry of entries) {
        try {
          const record: StoredRecord = {
            uri: entry.uri,
            values: entry.values,
            data: entry.data,
          };
          await new Promise<void>((resolve, reject) => {
            const request = store.put(record);
            request.onsuccess = () => resolve();
            request.onerror = () =>
              reject(
                new Error(`Failed to write ${entry.uri}: ${request.error}`),
              );
          });
          results.push({ success: true });
        } catch (err) {
          results.push({
            success: false,
            error: err instanceof Error ? err.message : "Write failed",
          });
        }
      }
    } catch (err) {
      // DB init failure — all fail
      for (const _ of entries) {
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
        results.push(...await this._list<T>(uri));
      } else {
        results.push(await this._readOne<T>(uri));
      }
    }

    return results;
  }

  private async _readOne<T>(uri: string): Promise<ReadResult<T>> {
    try {
      const store = await this.getStore();
      return new Promise<ReadResult<T>>((resolve) => {
        const request = store.get(uri);
        request.onsuccess = () => {
          const record = request.result as StoredRecord | undefined;
          if (!record) {
            resolve({ success: false, error: "Not found" });
          } else {
            resolve({
              success: true,
              record: { values: record.values, data: record.data as T },
            });
          }
        };
        request.onerror = () =>
          resolve({ success: false, error: `Read failed: ${request.error}` });
      });
    } catch (err) {
      return {
        success: false,
        error: err instanceof Error ? err.message : "Read failed",
      };
    }
  }

  private async _list<T>(uri: string): Promise<ReadResult<T>[]> {
    try {
      const store = await this.getStore();
      const index = store.index("uri_index");

      return new Promise<ReadResult<T>[]>((resolve) => {
        const results: ReadResult<T>[] = [];
        const request = index.openCursor();

        request.onsuccess = () => {
          const cursor = request.result;
          if (cursor) {
            const record = cursor.value as StoredRecord;
            if (record.uri.startsWith(uri)) {
              results.push({
                success: true,
                uri: record.uri,
                record: { values: record.values, data: record.data as T },
              });
            }
            cursor.continue();
          } else {
            resolve(results);
          }
        };
        request.onerror = () => resolve([]);
      });
    } catch {
      return [];
    }
  }

  // ── Delete ───────────────────────────────────────────────────────

  async delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    try {
      const store = await this.getStore("readwrite");

      for (const uri of uris) {
        try {
          await new Promise<void>((resolve, reject) => {
            const request = store.delete(uri);
            request.onsuccess = () => resolve();
            request.onerror = () =>
              reject(new Error(`Failed to delete ${uri}: ${request.error}`));
          });
          results.push({ success: true });
        } catch (err) {
          results.push({
            success: false,
            error: err instanceof Error ? err.message : "Delete failed",
          });
        }
      }
    } catch (err) {
      for (const _ of uris) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Delete failed",
        });
      }
    }

    return results;
  }

  // ── Status ───────────────────────────────────────────────────────

  async status(): Promise<StatusResult> {
    try {
      await this.initDB();
      return { status: "healthy", schema: [] };
    } catch {
      return { status: "unhealthy", schema: [] };
    }
  }

  capabilities(): StoreCapabilities {
    return { atomicBatch: false, binaryData: false };
  }
}
