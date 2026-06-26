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

describe('docs_query', () => {
  it('posts the query and returns the answer text + source urls', async () => {
    let body: unknown;
    server.use(
      http.post('https://ai.test/*', async ({ request }) => {
        expect(new URL(request.url).pathname).toBe('/docs/question');
        body = await request.json();
        return HttpResponse.json({
          text: 'Use the API.',
          sourceUrls: ['https://help.keboola.com/x'],
        });
      }),
    );

    const [clientT, serverT] = InMemoryTransport.createLinkedPair();
    await createServer(config).connect(serverT);
    const client = new Client({ name: 't', version: '0' });
    await client.connect(clientT);
    const result = await client.callTool({ name: 'docs_query', arguments: { query: 'how to?' } });
    const text = (result.content as { text: string }[])[0]!.text;

    expect(body).toEqual({ query: 'how to?' });
    expect(text).toContain('Use the API.');
    expect(text).toContain('https://help.keboola.com/x');
    await client.close();
  });
});
