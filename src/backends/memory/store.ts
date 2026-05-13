/**
 * MemoryStore — in-memory reference implementation of Store.
 *
 * Pure mechanical storage with no protocol awareness.
 * Write entries, read entries, delete entries.
 * Observation is a client concern — see `ObserveEmitter`.
 *
 * @example
 * ```typescript
 * import { MemoryStore } from "@bandeira-tech/b3nd-sdk";
 *
 * const store = new MemoryStore();
 *
 * await store.write([
 *   { uri: "mutable://app/config", data: { theme: "dark" } },
 * ]);
 *
 * const results = await store.read(["mutable://app/config"]);
 * console.log(results[0]?.record?.data); // { theme: "dark" }
 * ```
 */

import type {
  DeleteResult,
  Output,
  StatusResult,
  Store,
  StoreCapabilities,
  StoreEntry,
  StoreWriteResult,
} from "@bandeira-tech/b3nd-core/types";
import type { ParsedUrl } from "@bandeira-tech/b3nd-core/url";
import { parseUrl } from "@bandeira-tech/b3nd-core/url";

type StorageNode<T = unknown> = {
  value?: { data: T };
  children?: Map<string, StorageNode>;
};

type Storage = Map<string, StorageNode>;

function resolveTarget(
  uri: string,
  storage: Storage,
): {
  program: string;
  path: string;
  node: StorageNode | undefined;
  parts: string[];
} {
  const url = URL.parse(uri)!;
  const program = `${url.protocol}//${url.hostname}`;
  const node = storage.get(program);
  const parts = url.pathname.substring(1).split("/");
  return { program, path: url.pathname, node, parts };
}

export class MemoryStore implements Store {
  private storage: Storage;

  constructor(storage?: Storage) {
    this.storage = storage || new Map();
  }

  // ── Write ────────────────────────────────────────────────────────

  write(entries: StoreEntry[]): Promise<StoreWriteResult[]> {
    const results: StoreWriteResult[] = [];

    for (const entry of entries) {
      try {
        this._writeOne(entry.uri, {
          data: entry.data,
        });
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Write failed",
        });
      }
    }

    return Promise.resolve(results);
  }

  private _writeOne(
    uri: string,
    record: { data: unknown },
  ): void {
    const { program, parts, node: existing } = resolveTarget(uri, this.storage);

    let current = existing;
    if (!current) {
      current = { children: new Map() };
      this.storage.set(program, current);
    }

    for (const segment of parts.filter(Boolean)) {
      if (!current.children) current.children = new Map();
      if (!current.children.get(segment)) {
        const child: StorageNode = {};
        current.children.set(segment, child);
        current = child;
      } else {
        current = current.children.get(segment)!;
      }
    }

    current.value = record;
  }

  // ── Read ─────────────────────────────────────────────────────────

  // deno-lint-ignore require-await
  async read<T = unknown>(urls: string[]): Promise<Output<T>[]> {
    // 1:1 with input urls. Each output is [inputUrl, payload]; payload
    // shape varies by fn (see ProtocolInterfaceNode.read docs).
    // `async` here turns synchronous parameter-validation throws into
    // a rejected promise — matches the contract every other backend
    // already satisfies via real async I/O.
    return urls.map((url) => {
      const parsed = parseUrl(url);
      switch (parsed.fn) {
        case "read":
          return [url, this._readOne<T>(parsed.uri) as T];
        case "ls":
          return [url, this._list<T>(parsed) as unknown as T];
        case "count":
          return [url, this._count(parsed) as unknown as T];
        default:
          throw new Error(`MemoryStore: unsupported fn '${parsed.fn}'`);
      }
    });
  }

  private _readOne<T>(uri: string): T | undefined {
    const { parts, node } = resolveTarget(uri, this.storage);
    if (!node) return undefined;

    let current: StorageNode | undefined = node;
    for (const part of parts.filter(Boolean)) {
      current = current?.children?.get(part);
      if (!current) return undefined;
    }

    if (!current.value) return undefined;
    return (current.value as { data: T }).data;
  }

  /**
   * Collect the **direct leaves** under the prefix node — entries
   * whose URI is `prefix + <segment>` with no further `/`. Matches
   * the shallow `fn=ls`/`fn=count` contract enforced across every
   * backend in this package; clients that want recursion call
   * `ls` per level.
   */
  private _walk(uri: string): Output[] {
    const { node, parts, program, path } = resolveTarget(uri, this.storage);
    if (!node) return [];
    let current: StorageNode | undefined = node;

    for (const part of parts.filter(Boolean)) {
      current = current?.children?.get(part);
      if (!current) return [];
    }

    if (!current.children) return [];

    const prefix = path.endsWith("/")
      ? `${program}${path}`
      : `${program}${path}/`;

    const out: Output[] = [];
    for (const [key, child] of current.children) {
      if (child.value !== undefined) {
        out.push([
          `${prefix}${key}`,
          (child.value as { data: unknown }).data,
        ]);
      }
    }
    return out;
  }

  private _list<T>(parsed: ParsedUrl): Output<T>[] | string[] {
    const { params } = parsed;

    // Programmer errors: unsupported params throw — option-A reserves
    // "not found" for the absence channel.
    if (params.pattern !== undefined) {
      throw new Error("MemoryStore: pattern filter not supported");
    }
    if (params.sortBy !== undefined && params.sortBy !== "uri") {
      throw new Error(`MemoryStore: unsupported sortBy: ${params.sortBy}`);
    }
    const format = params.format ?? "full";
    if (format !== "full" && format !== "uris") {
      throw new Error(`MemoryStore: unsupported format: ${format}`);
    }

    let entries = this._walk(parsed.uri);

    if (params.sortBy === "uri") {
      const dir = params.sortOrder === "desc" ? -1 : 1;
      entries.sort(([a], [b]) => a.localeCompare(b) * dir);
    }

    if (params.limit !== undefined) {
      const page = params.page ?? 1;
      const start = (page - 1) * params.limit;
      entries = entries.slice(start, start + params.limit);
    }

    // `format=uris` flattens to a uri list — no inner tuples, no
    // undefined payloads. `format=full` keeps `Output<T>[]`.
    if (format === "uris") {
      return entries.map(([uri]) => uri);
    }
    return entries as Output<T>[];
  }

  private _count(parsed: ParsedUrl): number {
    if (parsed.params.pattern !== undefined) {
      throw new Error("MemoryStore: pattern filter not supported");
    }
    return this._walk(parsed.uri).length;
  }

  // ── Delete ───────────────────────────────────────────────────────

  delete(uris: string[]): Promise<DeleteResult[]> {
    const results: DeleteResult[] = [];

    for (const uri of uris) {
      try {
        this._deleteOne(uri);
        results.push({ success: true });
      } catch (err) {
        results.push({
          success: false,
          error: err instanceof Error ? err.message : "Delete failed",
        });
      }
    }

    return Promise.resolve(results);
  }

  private _deleteOne(uri: string): void {
    const { node, parts } = resolveTarget(uri, this.storage);
    if (!node) return;
    const filteredParts = parts.filter(Boolean);

    let current: StorageNode | undefined = node;
    const ancestors: { node: StorageNode; key: string }[] = [];

    for (const part of filteredParts) {
      if (!current?.children?.has(part)) return;
      ancestors.push({ node: current, key: part });
      current = current.children.get(part)!;
    }

    delete current.value;

    // Clean up empty ancestors (leaf-to-root)
    for (let i = ancestors.length - 1; i >= 0; i--) {
      const { node: parent, key } = ancestors[i];
      const child = parent.children!.get(key)!;
      if (!child.value && (!child.children || child.children.size === 0)) {
        parent.children!.delete(key);
      } else {
        break;
      }
    }
  }

  // ── Status ───────────────────────────────────────────────────────

  status(): Promise<StatusResult> {
    return Promise.resolve({
      status: "healthy",
      schema: [...this.storage.keys()],
      fns: ["read", "ls", "count"],
    });
  }

  capabilities(): StoreCapabilities {
    return {
      atomicBatch: false,
      binaryData: false,
    };
  }
}
