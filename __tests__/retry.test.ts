import { afterEach, describe, expect, it, vi } from 'vitest';

import { createRetryMiddleware } from '@/clients/retry';

// A thrown error shaped like the api-client ApiError ({ response: Response }).
const apiError = (status: number): Error =>
  Object.assign(new Error(`HTTP ${status}`), { response: { status } });

const okResponse = { response: { status: 200 }, data: {} };

afterEach(() => {
  vi.useRealTimers();
});

describe('createRetryMiddleware', () => {
  it('retries a 500 then returns the eventual success', async () => {
    vi.useFakeTimers();
    let calls = 0;
    const next = vi.fn(async () => {
      calls += 1;
      if (calls <= 2) throw apiError(500);
      return okResponse;
    });

    const run = createRetryMiddleware(3)(next as never)({} as never);
    await vi.runAllTimersAsync();
    const result = await run;

    expect(calls).toBe(3); // two 500s + one success
    expect(result).toBe(okResponse);
  });

  it('does not retry a non-retryable status (400)', async () => {
    const next = vi.fn(async () => {
      throw apiError(400);
    });
    await expect(createRetryMiddleware(3)(next as never)({} as never)).rejects.toThrow('HTTP 400');
    expect(next).toHaveBeenCalledTimes(1);
  });

  it('gives up after maxRetries and rethrows the last error', async () => {
    vi.useFakeTimers();
    const next = vi.fn(async () => {
      throw apiError(503);
    });
    const run = createRetryMiddleware(2)(next as never)({} as never);
    const assertion = expect(run).rejects.toThrow('HTTP 503');
    await vi.runAllTimersAsync();
    await assertion;
    expect(next).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it('retries when a retryable status is returned (not thrown)', async () => {
    vi.useFakeTimers();
    let calls = 0;
    const next = vi.fn(async () => {
      calls += 1;
      return calls === 1 ? { response: { status: 502 }, data: {} } : okResponse;
    });
    const run = createRetryMiddleware(3)(next as never)({} as never);
    await vi.runAllTimersAsync();
    expect(await run).toBe(okResponse);
    expect(calls).toBe(2);
  });
});
