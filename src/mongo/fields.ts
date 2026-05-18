/**
 * TYPE_TAGS → BSON-friendly field plan for MongoStore.
 *
 * Mongo collections are schema-flexible — we don't enforce a JSON
 * schema validator here. The plan exists so we can: (a) report
 * which fields the backend recognises in `EntitySupport`, (b) reject
 * records with extra keys in the strict-validation path, and (c)
 * coerce values to BSON-friendly shapes on write / restore canonical
 * JS shapes on read.
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
