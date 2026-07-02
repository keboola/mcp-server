import { describe, expect, it } from 'vitest';

import { describeToolError } from '@/mcp/tool';

describe('describeToolError', () => {
  it('enriches an api-client ApiError with the response body detail + exception id', () => {
    // Shape of @keboola/api-client ApiError: message is the HTTP status text; the real
    // reason + support id are on `.data` (this is the run_sync_action "Bad Request" case).
    const apiError = Object.assign(new Error('Bad Request'), {
      data: { error: 'Invalid access token\n', code: 0, exceptionId: 'exception-abc123' },
    });
    expect(describeToolError(apiError)).toBe(
      'Bad Request: Invalid access token (exception ID: exception-abc123)',
    );
  });

  it('uses data.message when data.error is absent', () => {
    const err = Object.assign(new Error('Unprocessable Entity'), {
      data: { message: 'query must not be empty' },
    });
    expect(describeToolError(err)).toBe('Unprocessable Entity: query must not be empty');
  });

  it('passes a plain Error (e.g. our raw client, which already composes detail) through', () => {
    expect(describeToolError(new Error('404 Not Found\nAPI error: nope'))).toBe(
      '404 Not Found\nAPI error: nope',
    );
  });

  it('does not duplicate when the detail equals the base message', () => {
    const err = Object.assign(new Error('Invalid access token'), {
      data: { error: 'Invalid access token' },
    });
    expect(describeToolError(err)).toBe('Invalid access token');
  });

  it('handles non-Error values', () => {
    expect(describeToolError('boom')).toBe('boom');
  });
});
