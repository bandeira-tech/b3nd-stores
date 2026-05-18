/**
 * TYPE_TAGS → Elasticsearch field mapping for native entity indices.
 *
 * Each canonical tag in `TYPE_TAGS` resolves to one ES field type:
 *
 *   STRING    → keyword     (exact-match queries; switch to text in
 *                             a follow-up if analyzed search is wanted)
 *   NUMBER    → double
 *   BIGINT    → long
 *   BOOLEAN   → boolean
 *   BYTES     → binary       (base64-encoded on write/read)
 *   TIMESTAMP → date
 *   JSON      → object       (dynamic, ES infers nested mappings)
 *
 * Multi-tag fields pick the first recognized canonical tag.
 */

import { type EntityField, TYPE_TAGS } from "../entity.ts";

const ES_TYPE: Record<string, string> = {
  [TYPE_TAGS.STRING]: "keyword",
  [TYPE_TAGS.NUMBER]: "double",
  [TYPE_TAGS.BIGINT]: "long",
  [TYPE_TAGS.BOOLEAN]: "boolean",
  [TYPE_TAGS.BYTES]: "binary",
  [TYPE_TAGS.TIMESTAMP]: "date",
  [TYPE_TAGS.JSON]: "object",
};

const FIELD_NAME = /^[a-zA-Z][a-zA-Z0-9_]*$/;

export interface FieldPlan {
  name: string;
  esType: string;
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
    const tag = f.type.find((t) => ES_TYPE[t] !== undefined);
    if (!tag) {
      unsupported.push({
        name: f.name,
        reason: f.type.length === 0
          ? "field declares no type tags"
          : `no recognised tag in [${f.type.join(", ")}]`,
      });
      continue;
    }
    out.push({ name: f.name, esType: ES_TYPE[tag], tag });
  }
  return { fields: out, unsupported };
}

/** Build the `mappings.properties` map for an entity index. */
export function buildMappings(fields: FieldPlan[]): Record<string, unknown> {
  const properties: Record<string, unknown> = {
    uri: { type: "keyword" },
    updatedAt: { type: "date" },
  };
  for (const f of fields) {
    properties[f.name] = f.esType === "object"
      ? { type: "object", dynamic: true }
      : { type: f.esType };
  }
  return { properties };
}
