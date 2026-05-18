/**
 * TYPE_TAGS → Postgres column type mapping for native entity tables.
 *
 * Each canonical tag in `TYPE_TAGS` resolves to one SQL type. A field
 * carrying multiple tags (e.g. `["string", "email"]`) picks the first
 * recognized canonical tag in declaration order — extra refinement
 * tags pass through without affecting storage.
 */

import { type EntityField, TYPE_TAGS } from "../entity.ts";

const SQL_TYPE: Record<string, string> = {
  [TYPE_TAGS.STRING]: "TEXT",
  [TYPE_TAGS.NUMBER]: "DOUBLE PRECISION",
  [TYPE_TAGS.BIGINT]: "BIGINT",
  [TYPE_TAGS.BOOLEAN]: "BOOLEAN",
  [TYPE_TAGS.BYTES]: "BYTEA",
  [TYPE_TAGS.TIMESTAMP]: "TIMESTAMPTZ",
  [TYPE_TAGS.JSON]: "JSONB",
};

const FIELD_NAME = /^[a-zA-Z][a-zA-Z0-9_]*$/;

export interface ColumnPlan {
  /** Field/column name. */
  name: string;
  /** Postgres SQL type. */
  sqlType: string;
  /** The recognized tag that decided `sqlType` (for diagnostics). */
  tag: string;
}

export interface ColumnPlanResult {
  columns: ColumnPlan[];
  unsupported: { name: string; reason: string }[];
}

/**
 * Resolve a schema's fields into Postgres column plans.
 *
 * Skips fields whose tags are all outside `TYPE_TAGS`; reports them
 * via `unsupported`. Rejects invalid field names up front so we never
 * emit unsafe SQL identifiers.
 */
export function planColumns(fields: EntityField[]): ColumnPlanResult {
  const columns: ColumnPlan[] = [];
  const unsupported: { name: string; reason: string }[] = [];

  for (const field of fields) {
    if (!FIELD_NAME.test(field.name)) {
      unsupported.push({
        name: field.name,
        reason:
          `field name must match ${FIELD_NAME.source}; got '${field.name}'`,
      });
      continue;
    }
    const tag = field.type.find((t) => SQL_TYPE[t] !== undefined);
    if (!tag) {
      unsupported.push({
        name: field.name,
        reason: field.type.length === 0
          ? "field declares no type tags"
          : `no recognised tag in [${field.type.join(", ")}]`,
      });
      continue;
    }
    columns.push({ name: field.name, sqlType: SQL_TYPE[tag], tag });
  }
  return { columns, unsupported };
}
