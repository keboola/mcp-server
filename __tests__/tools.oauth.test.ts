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

describe('create_oauth_url', () => {
  it('mints a scoped short-lived token and builds the external OAuth URL', async () => {
    let body: Record<string, unknown> = {};
    server.use(
      http.post('https://connection.test/*', async ({ request }) => {
        expect(new URL(request.url).pathname).toMatch(/\/v2\/storage\/tokens$/);
        body = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({ token: 'short-lived-123' });
      }),
    );

    const [clientT, serverT] = InMemoryTransport.createLinkedPair();
    await createServer(config).connect(serverT);
    const client = new Client({ name: 't', version: '0' });
    await client.connect(clientT);

    const result = await client.callTool({
      name: 'create_oauth_url',
      arguments: { component_id: 'keboola.ex-gmail', config_id: 'cfg1' },
    });
    const url = (result.content as { text: string }[])[0]!.text;

    expect(body).toMatchObject({ componentAccess: ['keboola.ex-gmail'], expiresIn: 3600 });
    expect(url).toBe(
      'https://external.keboola.com/oauth/index.html?token=short-lived-123&sapiUrl=https%3A%2F%2Fconnection.test#/keboola.ex-gmail/cfg1',
    );
    await client.close();
  });
});
