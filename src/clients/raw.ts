/**
 * Raw Keboola HTTP client — a faithful port of the Python `RawKeboolaClient`.
 *
 * Used for endpoints where `@keboola/api-client`'s typed methods diverge from the
 * exact Storage/Queue API calls the Python server made (e.g. table+column metadata,
 * job creation). For endpoints api-client covers cleanly, prefer the typed client.
 */
const RETRYABLE_STATUS = new Set([408, 409, 425, 429, 500, 502, 503, 504]);
const MAX_RETRIES = 3;
const BACKOFF_BASE_MS = 1000;
const MAX_BACKOFF_MS = 10_000;

export type RawRequestOptions = {
  params?: Record<string, string | number | boolean | string[] | undefined>;
  body?: unknown;
  headers?: Record<string, string>;
};

export type RawClientOptions = {
  baseUrl: string;
  /** SAPI token, or an `Authorization` value prefixed with `Bearer `. */
  token?: string;
  readonly?: boolean;
  fetchFn?: typeof fetch;
};

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const buildUrl = (
  baseUrl: string,
  endpoint: string,
  params?: RawRequestOptions['params'],
): string => {
  const url = new URL(`${baseUrl}/${endpoint}`);
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value === undefined) continue;
      if (Array.isArray(value)) {
        for (const item of value) url.searchParams.append(key, item);
      } else {
        url.searchParams.append(key, String(value));
      }
    }
  }
  return url.toString();
};

/** Error carrying the HTTP status, so callers can branch on it (e.g. 404 fallbacks). */
export class RawHttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = 'RawHttpError';
  }
}

/** Builds a detailed error from a failed response (port of `_raise_for_status`). */
const errorFromResponse = async (response: Response): Promise<Error> => {
  const parts = [`${response.status} ${response.statusText}`.trim()];
  const text = await response.text();
  try {
    const data = JSON.parse(text) as Record<string, unknown>;
    const apiError = (data.exception as string) ?? (data.error as string);
    if (apiError) parts.push(`API error: ${apiError}`);
    if (data.exceptionId) {
      parts.push(`Exception ID: ${String(data.exceptionId)}`);
      parts.push('When contacting Keboola support please provide the exception ID.');
    }
  } catch {
    if (text) parts.push(`API error: ${text}`);
  }
  return new RawHttpError(parts.join('\n'), response.status);
};

export type RawClient = {
  get: <T = unknown>(endpoint: string, options?: RawRequestOptions) => Promise<T>;
  getText: (endpoint: string, options?: RawRequestOptions) => Promise<string>;
  post: <T = unknown>(endpoint: string, options?: RawRequestOptions) => Promise<T>;
  put: <T = unknown>(endpoint: string, options?: RawRequestOptions) => Promise<T>;
  patch: <T = unknown>(endpoint: string, options?: RawRequestOptions) => Promise<T>;
  delete: <T = unknown>(endpoint: string, options?: RawRequestOptions) => Promise<T>;
};

export const createRawClient = (options: RawClientOptions): RawClient => {
  const doFetch = options.fetchFn ?? fetch;
  const baseHeaders: Record<string, string> = {
    'Content-Type': 'application/json',
    'Accept-Encoding': 'gzip',
  };
  if (options.token) {
    if (options.token.startsWith('Bearer ')) {
      baseHeaders['Authorization'] = options.token;
    } else {
      baseHeaders['X-StorageAPI-Token'] = options.token;
    }
  }

  const request = async (
    method: string,
    endpoint: string,
    opts: RawRequestOptions = {},
  ): Promise<Response> => {
    if (options.readonly && method !== 'GET') {
      throw new Error(`Forbidden ${method} operation on a readonly client: ${options.baseUrl}`);
    }
    const url = buildUrl(options.baseUrl, endpoint, opts.params);
    const headers = { ...baseHeaders, ...opts.headers };
    const init: RequestInit = { method, headers };
    if (method !== 'GET' && method !== 'HEAD') {
      init.body = JSON.stringify(opts.body ?? {});
    }

    let lastError: unknown;
    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
      try {
        const response = await doFetch(url, init);
        if (response.ok || !RETRYABLE_STATUS.has(response.status) || attempt === MAX_RETRIES) {
          return response;
        }
      } catch (error) {
        lastError = error;
        if (attempt === MAX_RETRIES) throw error;
      }
      await wait(Math.min(BACKOFF_BASE_MS * 2 ** attempt, MAX_BACKOFF_MS));
    }
    throw lastError instanceof Error ? lastError : new Error('Request failed');
  };

  const json = async <T>(
    method: string,
    endpoint: string,
    opts?: RawRequestOptions,
  ): Promise<T> => {
    const response = await request(method, endpoint, opts);
    if (!response.ok) throw await errorFromResponse(response);
    return response.json() as Promise<T>;
  };

  return {
    get: (endpoint, opts) => json('GET', endpoint, opts),
    getText: async (endpoint, opts) => {
      const response = await request('GET', endpoint, opts);
      if (!response.ok) throw await errorFromResponse(response);
      return response.text();
    },
    post: (endpoint, opts) => json('POST', endpoint, opts),
    put: (endpoint, opts) => json('PUT', endpoint, opts),
    patch: (endpoint, opts) => json('PATCH', endpoint, opts),
    // DELETE commonly returns 204 No Content / empty body; tolerate that instead of
    // throwing a JSON parse error (port of Python's `if response.content` guard).
    delete: async <T>(endpoint: string, opts?: RawRequestOptions): Promise<T> => {
      const response = await request('DELETE', endpoint, opts);
      if (!response.ok) throw await errorFromResponse(response);
      const text = await response.text();
      return (text ? JSON.parse(text) : null) as T;
    },
  };
};
