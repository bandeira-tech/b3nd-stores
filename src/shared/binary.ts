/**
 * Binary data marshaling for stores that round-trip values through
 * JSON (postgres jsonb, mongo bson, fs, ipfs, s3 bodies, elasticsearch
 * docs, localstorage strings).
 *
 * `Uint8Array` is not JSON-safe. We tag-and-base64 on the way in and
 * detect-and-decode on the way out. Stores that natively persist
 * binary (indexeddb structured clone) should NOT call these helpers.
 *
 * The tag shape — `{ __b3nd_binary__: true, encoding: "base64", data }` —
 * is internal to this package. Callers reading from a store always
 * receive a `Uint8Array` back; the wire format is invisible.
 */

import { decodeBase64, encodeBase64 } from "@bandeira-tech/b3nd-core";

const TAG = "__b3nd_binary__";

interface BinaryEnvelope {
  __b3nd_binary__: true;
  encoding: "base64";
  data: string;
}

function isBinaryEnvelope(v: unknown): v is BinaryEnvelope {
  if (!v || typeof v !== "object") return false;
  const r = v as Record<string, unknown>;
  return r[TAG] === true && r.encoding === "base64" &&
    typeof r.data === "string";
}

/**
 * Recursively walk `data` and replace any `Uint8Array` with a JSON-safe
 * envelope. Non-binary values pass through unchanged. Plain objects and
 * arrays are walked structurally; class instances are returned as-is.
 */
export function encodeBinaryForJson(data: unknown): unknown {
  if (data instanceof Uint8Array) {
    return {
      [TAG]: true,
      encoding: "base64",
      data: encodeBase64(data),
    };
  }
  if (Array.isArray(data)) return data.map(encodeBinaryForJson);
  if (data && typeof data === "object" && data.constructor === Object) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(data as Record<string, unknown>)) {
      out[k] = encodeBinaryForJson(v);
    }
    return out;
  }
  return data;
}

/**
 * Inverse of `encodeBinaryForJson`. Recursively walks `data`, decoding
 * any binary envelope back to a `Uint8Array`.
 */
export function decodeBinaryFromJson(data: unknown): unknown {
  if (isBinaryEnvelope(data)) return decodeBase64(data.data);
  if (Array.isArray(data)) return data.map(decodeBinaryFromJson);
  if (data && typeof data === "object" && data.constructor === Object) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(data as Record<string, unknown>)) {
      out[k] = decodeBinaryFromJson(v);
    }
    return out;
  }
  return data;
}
