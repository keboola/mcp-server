import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { createServer } from '@/server';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

const connect = async () => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

const call = async (updates: { item_id: string; description: string }[]) => {
  const client = await connect();
  const result = await client.callTool({ name: 'update_descriptions', arguments: { updates } });
  const text = (result.content as { text: string }[])[0]!.text;
  await client.close();
  return text;
};

// Token verify backs the links manager (project id).
const verifyHandler = () =>
  http.get('https://connection.test/*', ({ request }) => {
    if (new URL(request.url).pathname.endsWith('/tokens/verify')) {
      return HttpResponse.json({ owner: { id: '42' } });
    }
    return undefined;
  });

const callTool = async (name: string, args: Record<string, unknown>) => {
  const client = await connect();
  const result = await client.callTool({ name, arguments: args });
  expect(result.isError).toBeFalsy();
  const text = (result.content as { text: string }[])[0]!.text;
  await client.close();
  return text;
};

describe('update_descriptions', () => {
  it('updates a bucket description via the bucket metadata endpoint', async () => {
    let captured: { path: string; body: unknown } | undefined;
    server.use(
      http.post('https://connection.test/*', async ({ request }) => {
        captured = { path: new URL(request.url).pathname, body: await request.json() };
        return HttpResponse.json([
          { key: 'KBC.description', value: 'desc', timestamp: '2026-01-01' },
        ]);
      }),
    );

    const text = await call([{ item_id: 'in.c-main', description: 'desc' }]);
    expect(captured?.path).toMatch(/\/v2\/storage\/buckets\/in\.c-main\/metadata$/);
    expect(JSON.stringify(captured?.body)).toContain('KBC.description');
    expect(text).toContain('successful: 1');
  });

  it('updates a column description via the table metadata endpoint', async () => {
    let body: { columnsMetadata?: Record<string, unknown> } | undefined;
    server.use(
      http.post('https://connection.test/*', async ({ request }) => {
        body = (await request.json()) as { columnsMetadata?: Record<string, unknown> };
        return HttpResponse.json({
          columnsMetadata: {
            age: [{ key: 'KBC.description', value: 'years', timestamp: '2026-01-02' }],
          },
        });
      }),
    );

    const text = await call([{ item_id: 'in.c-main.users.age', description: 'years' }]);
    expect(body?.columnsMetadata).toBeDefined();
    expect(text).toContain('successful: 1');
  });

  it('reports invalid item ids without calling the API', async () => {
    // No handlers registered -> any HTTP call would error (onUnhandledRequest: 'error').
    const text = await call([{ item_id: 'bad-id', description: 'x' }]);
    expect(text).toContain('failed: 1');
    expect(text).toContain('Invalid item_id format');
  });
});

describe('get_buckets', () => {
  it('lists all buckets with stage counts and the dashboard link', async () => {
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify'))
          return HttpResponse.json({ owner: { id: '42' } });
        if (url.pathname.endsWith('/buckets')) {
          return HttpResponse.json([
            {
              id: 'in.c-main',
              name: 'main',
              displayName: 'Main',
              stage: 'in',
              created: '2026-01-01T00:00:00+0000',
              dataSizeBytes: 100,
              description: 'legacy',
            },
            {
              id: 'out.c-result',
              name: 'result',
              displayName: 'Result',
              stage: 'out',
              created: '2026-01-02T00:00:00+0000',
            },
          ]);
        }
        return undefined;
      }),
    );

    const text = await callTool('get_buckets', { bucket_ids: [] });
    expect(text).toContain('in.c-main');
    expect(text).toContain('out.c-result');
    expect(text).toContain('total_buckets: 2');
    expect(text).toContain('input_buckets: 1');
    expect(text).toContain('output_buckets: 1');
    // metadata description takes precedence over legacy, but absent here -> legacy used
    expect(text).toContain('legacy');
    // bucket dashboard link
    expect(text).toContain('/storage');
  });

  it('prefers KBC.description metadata over the legacy description and reports missing ids', async () => {
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify'))
          return HttpResponse.json({ owner: { id: '42' } });
        if (url.pathname.endsWith('/buckets/in.c-main')) {
          return HttpResponse.json({
            id: 'in.c-main',
            name: 'main',
            displayName: 'Main',
            stage: 'in',
            created: '2026-01-01T00:00:00+0000',
            description: 'legacy auto-generated',
            metadata: [
              {
                key: 'KBC.description',
                value: 'curated description',
                timestamp: '2026-01-03T00:00:00+0000',
              },
            ],
          });
        }
        if (url.pathname.endsWith('/buckets/in.c-missing')) {
          return new HttpResponse(JSON.stringify({ error: 'not found' }), { status: 404 });
        }
        return undefined;
      }),
    );

    const text = await callTool('get_buckets', { bucket_ids: ['in.c-main', 'in.c-missing'] });
    expect(text).toContain('curated description');
    expect(text).not.toContain('legacy auto-generated');
    expect(text).toContain('in.c-missing'); // buckets_not_found
  });
});

describe('get_tables', () => {
  it('lists table summaries for a bucket (no FQN / columns)', async () => {
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify'))
          return HttpResponse.json({ owner: { id: '42' } });
        if (url.pathname.endsWith('/buckets/in.c-main')) {
          return HttpResponse.json({ id: 'in.c-main', name: 'main', stage: 'in', created: 'c' });
        }
        if (url.pathname.endsWith('/buckets/in.c-main/tables')) {
          return HttpResponse.json([
            {
              id: 'in.c-main.users',
              name: 'users',
              displayName: 'Users',
              primaryKey: ['id', 'email'],
              rowsCount: 5,
            },
          ]);
        }
        return undefined;
      }),
    );

    const text = await callTool('get_tables', { bucket_ids: ['in.c-main'] });
    expect(text).toContain('in.c-main.users');
    expect(text).toContain('id|email'); // primary key serialized as joined string
    expect(text).not.toContain('fullyQualifiedName');
  });

  it('returns full table detail with columns and a Snowflake FQN from backendPath', async () => {
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify'))
          return HttpResponse.json({ owner: { id: '42' } });
        if (url.pathname.endsWith('/tables/in.c-main.users')) {
          return HttpResponse.json({
            id: 'in.c-main.users',
            name: 'users',
            displayName: 'Users',
            created: 'c',
            bucket: { id: 'in.c-main', backendPath: ['DB', 'in.c-main'] },
            columns: ['id', 'name'],
            columnMetadata: {
              id: [
                { key: 'KBC.datatype.type', value: 'NUMBER', timestamp: 't' },
                { key: 'KBC.datatype.nullable', value: '0', timestamp: 't' },
                { key: 'KBC.description', value: 'identifier', timestamp: 't' },
              ],
              name: [{ key: 'KBC.datatype.nullable', value: '1', timestamp: 't' }],
            },
          });
        }
        return undefined;
      }),
    );

    const text = await callTool('get_tables', { table_ids: ['in.c-main.users'] });
    expect(text).toContain('fullyQualifiedName'); // FQN derived from backendPath
    expect(text).toContain('DB'); // Snowflake-quoted backendPath parts present
    expect(text).toContain('NUMBER');
    expect(text).toContain('identifier');
    // name column has no datatype.type -> defaults to VARCHAR
    expect(text).toContain('VARCHAR');
  });

  it('reports tables_not_found for unknown table ids', async () => {
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify'))
          return HttpResponse.json({ owner: { id: '42' } });
        if (url.pathname.includes('/tables/')) {
          return new HttpResponse(JSON.stringify({ error: 'nope' }), { status: 404 });
        }
        return undefined;
      }),
    );

    const text = await callTool('get_tables', { table_ids: ['in.c-main.ghost'] });
    expect(text).toContain('in.c-main.ghost');
  });
});
