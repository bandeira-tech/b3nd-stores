/**
 * SQLite schema utilities for b3nd.
 *
 * Generates SQLite-compatible DDL. The schema is intentionally
 * minimal: `uri` (PK) + `data` (TEXT, JSON-encoded) + timestamps.
 * The `values` column carried by older revisions is gone with the
 * `StoreEntry.values` field in b3nd-core@0.15.
 */

export function generateSqliteSchema(tablePrefix: string): string {
  if (!tablePrefix) {
    throw new Error("tablePrefix is required and cannot be empty");
  }

  if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(tablePrefix)) {
    throw new Error(
      "tablePrefix must start with a letter and contain only letters, numbers, and underscores",
    );
  }

  return `-- SQLite schema for b3nd storage
-- Table prefix: ${tablePrefix}

CREATE TABLE IF NOT EXISTS ${tablePrefix}_data (
    uri TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
`;
}
