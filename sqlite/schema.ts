/**
 * SQLite schema utilities for b3nd
 *
 * Generates SQLite-compatible DDL for the b3nd storage system.
 * Stores uri, values (JSON), and data (JSON) per record.
 */

/**
 * Generate SQLite schema SQL with custom table prefix
 *
 * @param tablePrefix - Prefix for table names (required, no default)
 * @returns SQL string for creating the schema
 */
export function generateSqliteSchema(tablePrefix: string): string {
  if (!tablePrefix) {
    throw new Error("tablePrefix is required and cannot be empty");
  }

  // Validate table prefix to prevent SQL injection
  if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(tablePrefix)) {
    throw new Error(
      "tablePrefix must start with a letter and contain only letters, numbers, and underscores",
    );
  }

  return `-- SQLite schema for b3nd storage
-- Table prefix: ${tablePrefix}

CREATE TABLE IF NOT EXISTS ${tablePrefix}_data (
    uri TEXT PRIMARY KEY,
    "values" TEXT NOT NULL DEFAULT '{}',
    data TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
`;
}
