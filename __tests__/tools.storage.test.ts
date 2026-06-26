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
