/**
 * Shared helpers for backend authors.
 *
 * Every built-in backend uses these to stay consistent with the
 * `Store` contract — binary encode/decode, read-param validation
 * and application, and the read-dispatch helper. Backend authors
 * building their own `Store` should use these too rather than
 * reimplementing the contract details.
 */

export { decodeBinaryFromJson, encodeBinaryForJson } from "./binary.ts";
export { applyReadParams, validateReadParams } from "./read.ts";
export { dispatchRead, type ReadHandlers } from "./dispatch.ts";
