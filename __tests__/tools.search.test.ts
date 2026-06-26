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

describe('find_component_id', () => {
  it('returns suggested component ids with scores and a dashboard link', async () => {
    let body: unknown;
    server.use(
      http.get('https://connection.test/*', ({ request }) =>
        new URL(request.url).pathname.endsWith('/tokens/verify')
          ? HttpResponse.json({ owner: { id: '42' } })
          : undefined,
      ),
      http.post('https://ai.test/*', async ({ request }) => {
        expect(new URL(request.url).pathname).toBe('/suggest/component');
        body = await request.json();
        return HttpResponse.json({
          components: [{ componentId: 'keboola.ex-salesforce', score: 0.9, source: 'x' }],
        });
      }),
    );

    const [clientT, serverT] = InMemoryTransport.createLinkedPair();
    await createServer(config).connect(serverT);
    const client = new Client({ name: 't', version: '0' });
    await client.connect(clientT);
    const result = await client.callTool({
      name: 'find_component_id',
      arguments: { query: 'salesforce extractor' },
    });
    const text = (result.content as { text: string }[])[0]!.text;

    expect(body).toEqual({ prompt: 'salesforce extractor' });
    expect(text).toContain('keboola.ex-salesforce');
    expect(text).toContain('components/keboola.ex-salesforce'); // dashboard link
    await client.close();
  });
});
