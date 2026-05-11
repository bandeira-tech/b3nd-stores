/**
 * Internal shared helpers used by every store implementation.
 *
 * Not part of the public package surface — these are convenience
 * modules to keep store implementations uniform. Import with the
 * full path (`_shared/binary.ts` etc.) from within the package.
 */

export { decodeBinaryFromJson, encodeBinaryForJson } from "./binary.ts";
export { applyReadParams } from "./read.ts";
export { dispatchRead, type ReadHandlers } from "./dispatch.ts";
