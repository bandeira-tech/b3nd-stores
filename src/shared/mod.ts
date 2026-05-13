/**
 * Shared helpers for backend authors.
 *
 * Every built-in backend uses these to stay consistent with the
 * `Store` contract — read-param validation/application and the
 * read-dispatch helper. Backend authors building their own `Store`
 * should use these too rather than reimplementing the contract
 * details.
 */

export { applyReadParams, validateReadParams } from "./read.ts";
export { dispatchRead, type ReadHandlers } from "./dispatch.ts";
export { storageFailure } from "./errors.ts";
export { toBytes, toStream } from "./payload.ts";
