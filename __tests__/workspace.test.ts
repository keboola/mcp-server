import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { createRawClient } from '@/clients/raw';
import { Config } from '@/config';
import type { JobSubmittedInfo } from '@/workspace';
import { WorkspaceManager } from '@/workspace';

/**
 * Port of `tests/test_workspace.py`.
 *
 * The TypeScript workspace layer is built on the `@keboola/api-client` Query Service
 * client + a raw Storage client (no per-workspace `QueryServiceClient` mock surface like
 * the Python version had), so these tests exercise the *exported* `WorkspaceManager`
 * end-to-end over msw — mirroring `tools.sql.test.ts` — rather than reaching into private
 * internals. They cover the same behaviors as the Python suite: branch-aware resolution,
 * storage-branches fallback, schema/metadata/auto-create discovery, the on-job-submitted
 * callback, cancellation short-circuiting, dialect quoting, and BigQuery error normalization.
 */

const STORAGE_URL = 'https://connection.test';
const QUERY_URL = 'https://query.test';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const SNOWFLAKE_WS = {
  id: 123,
  connection: { backend: 'snowflake', schema: 'WORKSPACE_123', user: 'u' },
  readOnlyStorageAccess: true,
};

const BIGQUERY_WS = {
  id: 123,
  connection: {
    backend: 'bigquery',
    schema: 'dataset_123',
    user: JSON.stringify({ project_id: 'my-proj' }),
  },
  readOnlyStorageAccess: true,
};

/** Builds a WorkspaceManager wired to the msw-backed Storage + Query Service. */
const makeManager = async (cfg: Config): Promise<WorkspaceManager> => {
  const token = cfg.bearerToken ? `Bearer ${cfg.bearerToken}` : (cfg.storageToken ?? '');
  const makeStorage = () => createRawClient({ baseUrl: `${STORAGE_URL}/v2/storage`, token });
  return WorkspaceManager.create(cfg, {
    rawStorage: makeStorage(),
    makeProdRawStorage: makeStorage,
    queryServiceUrl: QUERY_URL,
    queryServiceToken: token,
  });
};

/** Records every Storage API path requested, so tests can assert branch routing. */
const recordingStorageHandler = (
  paths: string[],
  opts: {
    features?: string[];
    wsDetail?: Record<string, unknown>;
    metadata?: { key: string; value: unknown }[];
    wsList?: Record<string, unknown>[];
  } = {},
) =>
  http.all(`${STORAGE_URL}/*`, ({ request }) => {
    const p = new URL(request.url).pathname;
    paths.push(p);
    if (p.endsWith('/tokens/verify')) {
      return HttpResponse.json({
        owner: {
          id: '42',
          features: opts.features ?? [],
          defaultBackend: 'snowflake',
        },
      });
    }
    if (/\/branch\/[^/]+\/metadata$/.test(p)) {
      return HttpResponse.json(
        opts.metadata ?? [{ key: WorkspaceManager.MCP_META_KEY, value: 123 }],
      );
    }
    if (/\/branch\/[^/]+\/workspaces\/123$/.test(p)) {
      return HttpResponse.json(opts.wsDetail ?? SNOWFLAKE_WS);
    }
    if (/\/branch\/[^/]+\/workspaces$/.test(p)) {
      return HttpResponse.json(opts.wsList ?? [SNOWFLAKE_WS]);
    }
    if (p.endsWith('/dev-branches')) {
      return HttpResponse.json([{ id: '999', isDefault: true }]);
    }
    return undefined;
  });

const querySuccessHandlers = (opts: {
  columns?: { name: string }[];
  data?: unknown[][];
  numberOfRows?: number;
  message?: string;
  jobStatus?: string;
  resultsStatus?: string;
}) => [
  http.post(`${QUERY_URL}/api/v1/branches/:bid/workspaces/:wid/queries`, () =>
    HttpResponse.json({ queryJobId: 'job-1' }),
  ),
  http.get(`${QUERY_URL}/api/v1/queries/job-1`, () =>
    HttpResponse.json({
      status: opts.jobStatus ?? 'completed',
      statements: [{ id: 'stmt-1' }],
    }),
  ),
  http.get(`${QUERY_URL}/api/v1/queries/job-1/stmt-1/results`, () =>
    HttpResponse.json({
      status: opts.resultsStatus ?? 'completed',
      columns: opts.columns ?? [{ name: 'col' }],
      data: opts.data ?? [['v']],
      numberOfRows: opts.numberOfRows ?? (opts.data ?? [['v']]).length,
      message: opts.message ?? 'ok',
    }),
  ),
];

describe('WorkspaceManager.create — branch awareness', () => {
  it.each([
    // [label, branchId, hasSbFeature, expectedBranchInPath]
    ['default branch always production (feature on)', undefined, true, 'default'],
    ['default branch always production (feature off)', undefined, false, 'default'],
    ['dev branch + storage-branches keeps dev branch', '456', true, '456'],
    ['dev branch without storage-branches falls back to production', '456', false, 'default'],
  ])('%s', async (_label, branchId, hasSb, expectedBranch) => {
    const paths: string[] = [];
    server.use(
      recordingStorageHandler(paths, {
        features: hasSb ? ['storage-branches'] : [],
      }),
      ...querySuccessHandlers({}),
    );

    const cfg = new Config({
      storageApiUrl: STORAGE_URL,
      storageToken: 'tok',
      branchId,
    });
    const manager = await makeManager(cfg);
    // Force workspace resolution (which issues the branch-scoped metadata lookup).
    await manager.getWorkspaceId();

    const metaPath = paths.find((p) => /\/branch\/[^/]+\/metadata$/.test(p));
    expect(metaPath).toBeDefined();
    expect(metaPath).toContain(`/branch/${expectedBranch}/metadata`);
  });

  it('skips the feature lookup on the default branch', async () => {
    const paths: string[] = [];
    server.use(recordingStorageHandler(paths, {}), ...querySuccessHandlers({}));

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    await manager.getWorkspaceId();

    // On the default branch the feature check (tokens/verify) is short-circuited.
    expect(paths.some((p) => p.endsWith('/tokens/verify'))).toBe(false);
  });

  it('performs the feature lookup on a dev branch', async () => {
    const paths: string[] = [];
    server.use(
      recordingStorageHandler(paths, { features: ['storage-branches'] }),
      ...querySuccessHandlers({}),
    );

    const cfg = new Config({
      storageApiUrl: STORAGE_URL,
      storageToken: 'tok',
      branchId: '456',
    });
    const manager = await makeManager(cfg);
    await manager.getWorkspaceId();

    expect(paths.some((p) => p.endsWith('/tokens/verify'))).toBe(true);
  });
});

describe('WorkspaceManager — workspace discovery', () => {
  it('resolves a workspace via branch metadata + read-only detail', async () => {
    const paths: string[] = [];
    server.use(recordingStorageHandler(paths, {}), ...querySuccessHandlers({}));

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    expect(await manager.getWorkspaceId()).toBe(123);
    expect(await manager.getSqlDialect()).toBe('Snowflake');
    expect(paths.some((p) => /\/branch\/[^/]+\/metadata$/.test(p))).toBe(true);
  });

  it('resolves a workspace by explicit schema (never touches branch metadata)', async () => {
    const paths: string[] = [];
    server.use(
      recordingStorageHandler(paths, { wsList: [SNOWFLAKE_WS] }),
      ...querySuccessHandlers({}),
    );

    const cfg = new Config({
      storageApiUrl: STORAGE_URL,
      storageToken: 'tok',
      workspaceSchema: 'WORKSPACE_123',
    });
    const manager = await makeManager(cfg);

    expect(await manager.getWorkspaceId()).toBe(123);
    // The schema path lists workspaces; it must not read branch metadata.
    expect(paths.some((p) => /\/branch\/[^/]+\/workspaces$/.test(p))).toBe(true);
    expect(paths.some((p) => /\/branch\/[^/]+\/metadata$/.test(p))).toBe(false);
  });

  it('throws when an explicit schema matches no workspace', async () => {
    const paths: string[] = [];
    server.use(recordingStorageHandler(paths, { wsList: [] }));

    const cfg = new Config({
      storageApiUrl: STORAGE_URL,
      storageToken: 'tok',
      workspaceSchema: 'MISSING',
    });
    const manager = await makeManager(cfg);

    await expect(manager.getWorkspaceId()).rejects.toThrow(/No Keboola workspace found/);
  });

  it('auto-creates a workspace + writes metadata when none exists', async () => {
    const posts: { path: string; body: unknown }[] = [];
    server.use(
      http.all(`${STORAGE_URL}/*`, async ({ request }) => {
        const p = new URL(request.url).pathname;
        if (request.method === 'POST') {
          posts.push({ path: p, body: await request.clone().json() });
        }
        if (p.endsWith('/tokens/verify')) {
          return HttpResponse.json({
            owner: { id: '42', defaultBackend: 'snowflake' },
          });
        }
        // No existing workspace recorded in metadata.
        if (/\/branch\/[^/]+\/metadata$/.test(p) && request.method === 'GET') {
          return HttpResponse.json([]);
        }
        // POST metadata write-back.
        if (/\/branch\/[^/]+\/metadata$/.test(p) && request.method === 'POST') {
          return HttpResponse.json([{ key: WorkspaceManager.MCP_META_KEY, value: 123 }]);
        }
        // Create config under the billing component.
        if (/\/components\/[^/]+\/configs$/.test(p) && request.method === 'POST') {
          return HttpResponse.json({ id: 'cfg-1' });
        }
        // Create workspace -> returns an async job id.
        if (/\/configs\/[^/]+\/workspaces$/.test(p) && request.method === 'POST') {
          return HttpResponse.json({ id: 9001 });
        }
        // Poll the job: immediately successful.
        if (p.endsWith('/jobs/9001')) {
          return HttpResponse.json({ status: 'success', results: { id: 123 } });
        }
        // Resolve the created workspace by id.
        if (/\/branch\/[^/]+\/workspaces\/123$/.test(p)) {
          return HttpResponse.json(SNOWFLAKE_WS);
        }
        return undefined;
      }),
    );

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    expect(await manager.getWorkspaceId()).toBe(123);
    // A config + a workspace were created, and the id was written back to metadata.
    expect(posts.some((x) => /\/components\/[^/]+\/configs$/.test(x.path))).toBe(true);
    expect(posts.some((x) => /\/configs\/[^/]+\/workspaces$/.test(x.path))).toBe(true);
    const metaWrite = posts.find((x) => /\/branch\/[^/]+\/metadata$/.test(x.path));
    expect(metaWrite).toBeDefined();
    expect(JSON.stringify(metaWrite!.body)).toContain(WorkspaceManager.MCP_META_KEY);
  });

  it('cleans up the created config when workspace creation fails', async () => {
    const deleted: string[] = [];
    server.use(
      http.all(`${STORAGE_URL}/*`, ({ request }) => {
        const p = new URL(request.url).pathname;
        if (request.method === 'DELETE') deleted.push(p);
        if (p.endsWith('/tokens/verify')) {
          return HttpResponse.json({
            owner: { id: '42', defaultBackend: 'snowflake' },
          });
        }
        if (/\/branch\/[^/]+\/metadata$/.test(p) && request.method === 'GET') {
          return HttpResponse.json([]);
        }
        if (/\/components\/[^/]+\/configs$/.test(p) && request.method === 'POST') {
          return HttpResponse.json({ id: 'cfg-1' });
        }
        // Workspace creation fails hard.
        if (/\/configs\/[^/]+\/workspaces$/.test(p) && request.method === 'POST') {
          return new HttpResponse('boom', { status: 500 });
        }
        // Config cleanup.
        if (/\/components\/[^/]+\/configs\/cfg-1$/.test(p) && request.method === 'DELETE') {
          return new HttpResponse(null, { status: 204 });
        }
        return undefined;
      }),
    );

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    await expect(manager.getWorkspaceId()).rejects.toThrow();
    expect(deleted.some((p) => /\/components\/[^/]+\/configs\/cfg-1$/.test(p))).toBe(true);
  });
});

describe('WorkspaceManager — dialect quoting', () => {
  it('Snowflake quotes identifiers with double quotes', async () => {
    server.use(recordingStorageHandler([], {}));
    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    expect(await manager.getQuotedName('foo')).toBe('"foo"');
  });

  it('BigQuery quotes identifiers with backticks', async () => {
    server.use(recordingStorageHandler([], { wsDetail: BIGQUERY_WS }));
    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    expect(await manager.getQuotedName('foo')).toBe('`foo`');
    expect(await manager.getSqlDialect()).toBe('BigQuery');
  });

  it('rejects a BigQuery workspace without a project id in credentials', async () => {
    server.use(
      recordingStorageHandler([], {
        wsDetail: {
          id: 123,
          connection: { backend: 'bigquery', schema: 'ds', user: '{}' },
          readOnlyStorageAccess: true,
        },
      }),
    );
    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    await expect(manager.getSqlDialect()).rejects.toThrow(/no project ID/i);
  });
});

describe('WorkspaceManager.executeQuery — Query Service submit/poll/paginate', () => {
  it('runs a SELECT and returns columns/rows + selected-rows message', async () => {
    server.use(
      recordingStorageHandler([], {}),
      ...querySuccessHandlers({
        columns: [{ name: 'id' }, { name: 'name' }],
        data: [
          ['1', 'Alice'],
          ['2', 'Bob'],
        ],
        numberOfRows: 2,
      }),
    );

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    const result = await manager.executeQuery('SELECT * FROM t');

    expect(result.status).toBe('ok');
    expect(result.data?.columns).toEqual(['id', 'name']);
    expect(result.data?.rows).toEqual([
      { id: '1', name: 'Alice' },
      { id: '2', name: 'Bob' },
    ]);
    expect(result.message).toContain('Returning 2 of 2 selected rows.');
  });

  it('invokes on_job_submitted once with the full job info', async () => {
    server.use(recordingStorageHandler([], {}), ...querySuccessHandlers({}));

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    const received: JobSubmittedInfo[] = [];
    await manager.executeQuery('SELECT 1', {
      onJobSubmitted: async (info) => {
        received.push(info);
      },
    });

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual({
      job_id: 'job-1',
      cancellation_url: `${QUERY_URL}/api/v1/queries/job-1/cancel`,
      backend: 'snowflake',
    });
  });

  it('swallows a callback exception and still completes the query', async () => {
    server.use(recordingStorageHandler([], {}), ...querySuccessHandlers({}));

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    const result = await manager.executeQuery('SELECT 1', {
      onJobSubmitted: async () => {
        throw new Error('progress send failed');
      },
    });

    expect(result.status).toBe('ok');
  });

  it('rejects non-positive max_rows / max_chars', async () => {
    server.use(recordingStorageHandler([], {}), ...querySuccessHandlers({}));
    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    await expect(manager.executeQuery('SELECT 1', { maxRows: 0 })).rejects.toThrow(/max_rows/);
    await expect(manager.executeQuery('SELECT 1', { maxChars: 0 })).rejects.toThrow(/max_chars/);
  });

  it('truncates results to max_rows', async () => {
    server.use(
      recordingStorageHandler([], {}),
      ...querySuccessHandlers({
        columns: [{ name: 'n' }],
        data: [['1'], ['2'], ['3'], ['4'], ['5']],
        numberOfRows: 5,
      }),
    );
    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    const result = await manager.executeQuery('SELECT n', { maxRows: 2 });
    expect(result.data?.rows).toHaveLength(2);
    expect(result.message).toContain('Returning 2 of 5 selected rows.');
  });

  it('truncates results to max_chars on the first row that does not fit', async () => {
    server.use(
      recordingStorageHandler([], {}),
      ...querySuccessHandlers({
        columns: [{ name: 'v' }],
        data: [['aaa'], ['bbb'], ['ccc']], // 3 chars each
        numberOfRows: 3,
      }),
    );
    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);

    // Room for two rows (6 chars), not the third.
    const result = await manager.executeQuery('SELECT v', { maxChars: 6 });
    expect(result.data?.rows).toEqual([{ v: 'aaa' }, { v: 'bbb' }]);
  });
});

describe('WorkspaceManager.executeQuery — error handling', () => {
  it('returns an error result when the query fails', async () => {
    server.use(
      recordingStorageHandler([], {}),
      ...querySuccessHandlers({
        jobStatus: 'failed',
        resultsStatus: 'failed',
        columns: [],
        data: [],
        message: 'boom syntax error',
      }),
    );

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    const result = await manager.executeQuery('SELECT bad');

    expect(result.status).toBe('error');
    expect(result.data).toBeFalsy();
    expect(result.message).toContain('boom syntax error');
  });

  it('short-circuits a cancelled job with a clean message', async () => {
    server.use(
      recordingStorageHandler([], {}),
      http.post(`${QUERY_URL}/api/v1/branches/:bid/workspaces/:wid/queries`, () =>
        HttpResponse.json({ queryJobId: 'job-1' }),
      ),
      http.get(`${QUERY_URL}/api/v1/queries/job-1`, () =>
        HttpResponse.json({
          status: 'canceled',
          statements: [{ id: 'stmt-1' }],
        }),
      ),
    );

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    const result = await manager.executeQuery('SELECT 1');

    expect(result.status).toBe('error');
    expect(result.data).toBeNull();
    expect(result.message).toBe('Query was cancelled');
  });

  it('normalizes BigQuery error messages to the Message: "..." part', async () => {
    server.use(
      recordingStorageHandler([], { wsDetail: BIGQUERY_WS }),
      ...querySuccessHandlers({
        jobStatus: 'failed',
        resultsStatus: 'failed',
        columns: [],
        data: [],
        message:
          'Location: "query"; Message: "Syntax error: Unexpected identifier"; Reason: "invalidQuery"',
      }),
    );

    const cfg = new Config({ storageApiUrl: STORAGE_URL, storageToken: 'tok' });
    const manager = await makeManager(cfg);
    const result = await manager.executeQuery('SELECT bad');

    expect(result.status).toBe('error');
    expect(result.message).toBe('Syntax error: Unexpected identifier');
    expect(result.message).not.toContain('Reason:');
  });
});
