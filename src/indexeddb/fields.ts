/**
 * TYPE_TAGS recognition for IndexedDB entity records.
 *
 * IndexedDB's structured clone preserves Uint8Array, Date, BigInt,
 * and plain JSON natively — we don't need to JSON-encode records.
 * This module exists to: (a) report which fields the backend
 * recognises in `EntitySupport`, (b) drive the strict-validation
 * check (extras rejected) on write.
 */

import { type EntityField, TYPE_TAGS } from "../entity.ts";

const KNOWN = new Set<string>(Object.values(TYPE_TAGS));
const FIELD_NAME = /^[a-zA-Z][a-zA-Z0-9_]*$/;

export interface FieldPlan {
  name: string;
  tag: string;
}

export interface FieldPlanResult {
  fields: FieldPlan[];
  unsupported: { name: string; reason: string }[];
}

export function planFields(fields: EntityField[]): FieldPlanResult {
  const out: FieldPlan[] = [];
  const unsupported: { name: string; reason: string }[] = [];

  for (const f of fields) {
    if (!FIELD_NAME.test(f.name)) {
      unsupported.push({
        name: f.name,
        reason: `field name must match ${FIELD_NAME.source}; got '${f.name}'`,
      });
      continue;
    }
    const tag = f.type.find((t) => KNOWN.has(t));
    if (!tag) {
      unsupported.push({
        name: f.name,
        reason: f.type.length === 0
          ? "field declares no type tags"
          : `no recognised tag in [${f.type.join(", ")}]`,
      });
      continue;
    }
    out.push({ name: f.name, tag });
  }
  return { fields: out, unsupported };
}
