/**
 * PostgreSQL schema utilities for b3nd.
 *
 * Generates DDL for the b3nd storage table. The schema is
 * intentionally minimal: `uri` (PK) + `payload` (BYTEA) + timestamps.
 * The store is opaque — `payload` is raw bytes, not JSON; higher
 * layers own serialization.
 */

/**
 * Generate PostgreSQL schema SQL with custom table prefix.
 *
 * @param tablePrefix - Prefix for table names (required, no default)
 * @returns SQL string for creating the schema
 */
export function generatePostgresSchema(tablePrefix: string): string {
  if (!tablePrefix) {
    throw new Error("tablePrefix is required and cannot be empty");
  }

  // Validate table prefix to prevent SQL injection
  if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(tablePrefix)) {
    throw new Error(
      "tablePrefix must start with a letter and contain only letters, numbers, and underscores",
    );
  }

  return `-- PostgreSQL schema for b3nd storage
-- Table prefix: ${tablePrefix}

-- Create ${tablePrefix}_data table for storing URI-keyed payloads
CREATE TABLE IF NOT EXISTS ${tablePrefix}_data (
    uri VARCHAR(2048) PRIMARY KEY,
    payload BYTEA NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_${tablePrefix}_data_uri_prefix ON ${tablePrefix}_data (uri);
CREATE INDEX IF NOT EXISTS idx_${tablePrefix}_data_created_at ON ${tablePrefix}_data (created_at);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_${tablePrefix}_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for updated_at
DROP TRIGGER IF EXISTS update_${tablePrefix}_data_updated_at ON ${tablePrefix}_data;
CREATE TRIGGER update_${tablePrefix}_data_updated_at
    BEFORE UPDATE ON ${tablePrefix}_data
    FOR EACH ROW
    EXECUTE FUNCTION update_${tablePrefix}_updated_at_column();

-- Create a view for easier querying by program/protocol
CREATE OR REPLACE VIEW ${tablePrefix}_data_by_program AS
SELECT
    uri,
    split_part(uri, '://', 1) as program,
    split_part(uri, '://', 2) as path,
    payload,
    created_at,
    updated_at
FROM ${tablePrefix}_data;

-- Create index on program for faster queries
CREATE INDEX IF NOT EXISTS idx_${tablePrefix}_data_program ON ${tablePrefix}_data (split_part(uri, '://', 1));`;
}

/**
 * Schema initialization options
 */
export interface SchemaInitOptions {
  tablePrefix: string;
  grantPermissions?: boolean;
  databaseUser?: string;
}

/**
 * Generate complete schema initialization SQL including optional permissions
 *
 * @param options - Schema initialization options
 * @returns Complete SQL for schema setup
 */
export function generateCompleteSchemaSQL(options: SchemaInitOptions): string {
  const { tablePrefix, grantPermissions, databaseUser } = options;

  let sql = generatePostgresSchema(tablePrefix);

  if (grantPermissions && databaseUser) {
    if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(databaseUser)) {
      throw new Error(
        "databaseUser must start with a letter and contain only letters, numbers, and underscores",
      );
    }

    sql += `\n\n-- Grant permissions to ${databaseUser}\n`;
    sql +=
      `GRANT ALL PRIVILEGES ON TABLE ${tablePrefix}_data TO ${databaseUser};\n`;
  }

  return sql;
}

/**
 * Extract schema version from SQL for migration tracking.
 * Placeholder — bump when the table shape changes.
 */
export function extractSchemaVersion(_sql: string): string {
  return "v3.0.0";
}
