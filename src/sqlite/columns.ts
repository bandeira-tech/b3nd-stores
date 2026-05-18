/**
 * TYPE_TAGS → SQLite column type mapping for native entity tables.
 *
 * SQLite uses storage classes (NULL/INTEGER/REAL/TEXT/BLOB) with
 * affinity rules — we still declare a column type because driver
 * adapters and human readers expect it. JSON and TIMESTAMP land on
 * TEXT (JSON serialised; ISO-8601 strings) and get adapted at the
 * Store boundary.
 */

import { type EntityField, TYPE_TAGS } from "../entity.ts";

const SQL_TYPE: Record<string, string> = {
  [TYPE_TAGS.STRING]: "TEXT",
  [TYPE_TAGS.NUMBER]: "REAL",
  [TYPE_TAGS.BIGINT]: "INTEGER",
  [TYPE_TAGS.BOOLEAN]: "INTEGER",
  [TYPE_TAGS.BYTES]: "BLOB",
  [TYPE_TAGS.TIMESTAMP]: "TEXT",
  [TYPE_TAGS.JSON]: "TEXT",
};

const FIELD_NAME = /^[a-zA-Z][a-zA-Z0-9_]*$/;

export interface ColumnPlan {
  name: string;
  sqlType: string;
  tag: string;
}

export interface ColumnPlanResult {
  columns: ColumnPlan[];
  unsupported: { name: string; reason: string }[];
}

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
