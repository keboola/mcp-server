import type { MiddlewareFn } from '@keboola/api-client';

/**
 * Retry middleware for the `@keboola/api-client` fetch clients.
 *
 * Retries transient failures — network errors and the same retryable HTTP status set the raw
 * client uses (408/409/425/429/5xx) — with exponential backoff, so a flaky or briefly
 * unavailable Keboola service doesn't fail a tool call.
 *
 * We implement it here (rather than importing the api-client's own `createRetryMiddleware`)
 * because that symbol lives on the package's root barrel, which transitively pulls
 * `dayjs/plugin/utc` and fails to resolve under the test runner. `MiddlewareFn` is a
 * type-only import, fully erased at build/runtime, so this file never loads the barrel.
 */
const RETRYABLE_STATUS = new Set([408, 409, 425, 429, 500, 502, 503, 504]);
const DEFAULT_MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 1000;
const MAX_BACKOFF_MS = 10_000;

const wait = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const backoff = (attempt: number): number =>
  Math.min(BACKOFF_BASE_MS * 2 ** attempt, MAX_BACKOFF_MS);

/** Reads an HTTP status off a thrown ApiError (`{ response: Response }`) without importing it. */
const statusOf = (value: unknown): number | undefined => {
  if (value && typeof value === 'object' && 'response' in value) {
    const response = (value as { response?: { status?: number } }).response;
    if (response && typeof response.status === 'number') return response.status;
  }
  return undefined;
};

export const createRetryMiddleware = (maxRetries = DEFAULT_MAX_RETRIES): MiddlewareFn => {
  return (next) => async (request) => {
    for (let attempt = 0; ; attempt++) {
      try {
        const result = await next(request);
        // Some clients return a response (with status) instead of throwing on non-2xx.
        const status = (result as { response?: { status?: number } })?.response?.status;
        if (status !== undefined && RETRYABLE_STATUS.has(status) && attempt < maxRetries) {
          await wait(backoff(attempt));
          continue;
        }
        return result;
      } catch (error) {
        // Retry only on an explicit retryable HTTP status (5xx/429/...). A statusless error
        // (a genuine network failure, but also e.g. an unmocked request in a unit test) is
        // NOT retried here — network-level retries are the raw client's job, and retrying
        // every statusless throw would turn fast failures into slow ones.
        const status = statusOf(error);
        const retryable = status !== undefined && RETRYABLE_STATUS.has(status);
        if (!retryable || attempt >= maxRetries) throw error;
        await wait(backoff(attempt));
      }
    }
  };
};
