/**
 * Backend-author helpers for translating thrown executor errors into
 * the structured failure shape used by `StoreWriteResult` and
 * `DeleteResult`.
 *
 * Every Save backend ends up wanting the same `try/catch → { error,
 * errorDetail }` translation; this keeps the per-store catch blocks
 * one line and the error code consistent (`STORAGE_ERROR` for
 * anything that came from the underlying driver).
 */

import { Errors } from "@bandeira-tech/b3nd-core";
import type { B3ndError } from "@bandeira-tech/b3nd-core/types";

/**
 * Build the structured-failure half of a Store result from a thrown
 * value. `fallback` is used when `err` isn't an `Error` (rare). `uri`
 * is included on `errorDetail` when the failure attributes to a
 * specific entry — omit for atomic-batch failures that affect the
 * whole batch.
 */
export function storageFailure(
  err: unknown,
  fallback: string,
  uri?: string,
): { error: string; errorDetail: B3ndError } {
  const message = err instanceof Error ? err.message : fallback;
  return {
    error: message,
    errorDetail: Errors.storageError(message, uri),
  };
}
