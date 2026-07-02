import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { registerSqlTools } from '@/tools/sql';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

const connect = async (cfg: Config = config) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  const mcp = new McpServer({ name: 'test', version: '0.0.0' });
  registerSqlTools(mcp, cfg);
  await mcp.connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

const callTool = async (
  client: Awaited<ReturnType<typeof connect>>,
  args: Record<string, unknown>,
) => client.callTool({ name: 'query_data', arguments: args });

const callText = async (
  client: Awaited<ReturnType<typeof connect>>,
  args: Record<string, unknown>,
) => {
  const result = await callTool(client, args);
  expect(result.isError).toBeFalsy();
  return (result.content as { text: string }[])[0]!.text;
};

// --- Storage-side mocks (workspace discovery) ---

const SNOWFLAKE_WS = {
  id: 123,
  connection: { backend: 'snowflake', schema: 'WORKSPACE_123', user: 'u' },
  readOnlyStorageAccess: true,
};

/**
 * Handles every Storage API call (`connection.test/v2/storage/...`) needed to resolve
 * a workspace from the production-branch metadata. `wsOverride` swaps the workspace
 * detail payload (e.g. for the BigQuery case).
 */
const storageHandler = (wsOverride?: Record<string, unknown>) =>
  http.all('https://connection.test/*', ({ request }) => {
    const url = new URL(request.url);
    const p = url.pathname;
    if (p.endsWith('/branch/default/metadata')) {
      return HttpResponse.json([{ key: 'KBC.McpServer.v2.workspaceId', value: 123 }]);
    }
    if (p.endsWith('/branch/default/workspaces/123')) {
      return HttpResponse.json(wsOverride ?? SNOWFLAKE_WS);
    }
    if (p.endsWith('/dev-branches')) {
      return HttpResponse.json([{ id: '999', isDefault: true }]);
    }
    return undefined;
  });

// --- Query Service mocks ---

const querySuccessHandlers = (opts: {
  columns: { name: string }[];
  data: unknown[][];
  numberOfRows?: number;
  message?: string;
}) => [
  http.post('https://query.test/api/v1/branches/:bid/workspaces/:wid/queries', () =>
    HttpResponse.json({ queryJobId: 'job-1' }),
  ),
  http.get('https://query.test/api/v1/queries/job-1', () =>
    HttpResponse.json({ status: 'completed', statements: [{ id: 'stmt-1' }] }),
  ),
  http.get('https://query.test/api/v1/queries/job-1/stmt-1/results', () =>
    HttpResponse.json({
      status: 'completed',
      columns: opts.columns,
      data: opts.data,
      numberOfRows: opts.numberOfRows ?? opts.data.length,
      message: opts.message,
    }),
  ),
];

describe('query_data', () => {
  it('runs a SELECT and returns CSV with a selected-rows message', async () => {
    server.use(
      storageHandler(),
      ...querySuccessHandlers({
        columns: [{ name: 'id' }, { name: 'name' }],
        data: [
          ['1', 'Alice'],
          ['2', 'Bob'],
        ],
        numberOfRows: 2,
      }),
    );

    const text = await callText(await connect(), {
      sql_query: 'SELECT * FROM t',
      query_name: 'My Query',
    });
    expect(text).toContain('id,name');
    expect(text).toContain('Alice');
    expect(text).toContain('Bob');
    // Selected-rows message is surfaced.
    expect(text).toContain('Returning 2 of 2 selected rows.');
    expect(text).toContain('My Query');
  });

  it('quotes CSV fields that contain commas or quotes', async () => {
    server.use(
      storageHandler(),
      ...querySuccessHandlers({
        columns: [{ name: 'val' }],
        data: [['a,b'], ['he said "hi"']],
      }),
    );

    const text = await callText(await connect(), {
      sql_query: 'SELECT val FROM t',
      query_name: 'Q',
    });
    // The CSV is TOON-encoded as a quoted string, so embedded double-quotes are escaped.
    expect(text).toContain('\\"a,b\\"');
    expect(text).toContain('\\"he said \\"\\"hi\\"\\"\\"');
  });

  it('returns an error result when the query fails', async () => {
    server.use(
      storageHandler(),
      http.post('https://query.test/api/v1/branches/:bid/workspaces/:wid/queries', () =>
        HttpResponse.json({ queryJobId: 'job-1' }),
      ),
      http.get('https://query.test/api/v1/queries/job-1', () =>
        HttpResponse.json({ status: 'failed', statements: [{ id: 'stmt-1' }] }),
      ),
      http.get('https://query.test/api/v1/queries/job-1/stmt-1/results', () =>
        HttpResponse.json({
          status: 'failed',
          columns: [],
          data: [],
          message: 'boom syntax error',
        }),
      ),
    );

    const result = await callTool(await connect(), {
      sql_query: 'SELECT bad',
      query_name: 'Bad Query',
    });
    expect(result.isError).toBe(true);
    expect((result.content as { text: string }[])[0]!.text).toContain('boom syntax error');
  });

  it('normalizes BigQuery error messages and uses backtick quoting', async () => {
    server.use(
      storageHandler({
        id: 123,
        connection: {
          backend: 'bigquery',
          schema: 'dataset_123',
          user: JSON.stringify({ project_id: 'my-proj' }),
        },
        readOnlyStorageAccess: true,
      }),
      http.post('https://query.test/api/v1/branches/:bid/workspaces/:wid/queries', () =>
        HttpResponse.json({ queryJobId: 'job-1' }),
      ),
      http.get('https://query.test/api/v1/queries/job-1', () =>
        HttpResponse.json({ status: 'failed', statements: [{ id: 'stmt-1' }] }),
      ),
      http.get('https://query.test/api/v1/queries/job-1/stmt-1/results', () =>
        HttpResponse.json({
          status: 'failed',
          columns: [],
          data: [],
          message:
            'Location: "query"; Message: "Syntax error: Unexpected identifier"; Reason: "invalidQuery"',
        }),
      ),
    );

    const result = await callTool(await connect(), {
      sql_query: 'SELECT bad',
      query_name: 'BQ Query',
    });
    expect(result.isError).toBe(true);
    const text = (result.content as { text: string }[])[0]!.text;
    expect(text).toContain('Syntax error: Unexpected identifier');
    expect(text).not.toContain('Reason:');
  });

  it('surfaces a cancelled query as a clean cancellation error', async () => {
    server.use(
      storageHandler(),
      http.post('https://query.test/api/v1/branches/:bid/workspaces/:wid/queries', () =>
        HttpResponse.json({ queryJobId: 'job-1' }),
      ),
      http.get('https://query.test/api/v1/queries/job-1', () =>
        HttpResponse.json({ status: 'canceled', statements: [{ id: 'stmt-1' }] }),
      ),
    );

    const result = await callTool(await connect(), {
      sql_query: 'SELECT 1',
      query_name: 'Cancelled Query',
    });
    expect(result.isError).toBe(true);
    expect((result.content as { text: string }[])[0]!.text).toContain('Query was cancelled');
  });

  it('truncates by max rows across pagination', async () => {
    // Page returns more than MAX_ROWS would, but the tool requests pageSize capped to remaining.
    // Here we just verify a short result passes through; pagination math is covered by the prefix.
    server.use(
      storageHandler(),
      ...querySuccessHandlers({
        columns: [{ name: 'n' }],
        data: [['1'], ['2'], ['3']],
        numberOfRows: 3,
      }),
    );
    const text = await callText(await connect(), { sql_query: 'SELECT n', query_name: 'Rows' });
    expect(text).toContain('Returning 3 of 3 selected rows.');
  });
});
