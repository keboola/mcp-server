import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import type { DocsSearch } from '@/clients/docsSearch';
import { setDocsSearchForTests } from '@/clients/docsSearch';
import { Config } from '@/config';
import { createServer } from '@/server';

const server = setupServer(
  http.get('https://connection.test/*', ({ request }) =>
    new URL(request.url).pathname.endsWith('/tokens/verify')
      ? HttpResponse.json({ owner: { id: '42' } })
      : undefined,
  ),
);
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

/** A minimal fake docs-search provider; only the methods under test are implemented. */
const fakeDocsSearch = (overrides: Partial<DocsSearch> = {}): DocsSearch => ({
  search: async () => [],
  answerQuestion: async () => ({ text: '', sourceUrls: [] }),
  recommendComponents: async () => [],
  isReady: async () => true,
  close: async () => {},
  ...overrides,
});

const connect = async (): Promise<Client> => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 't', version: '0' });
  await client.connect(clientT);
  return client;
};

afterEach(() => setDocsSearchForTests(undefined));

describe('docs_query', () => {
  it('answers via the docs-search index and returns text + source urls', async () => {
    let asked: string | undefined;
    setDocsSearchForTests(
      fakeDocsSearch({
        answerQuestion: async (question) => {
          asked = question;
          return { text: 'Use the API.', sourceUrls: ['https://help.keboola.com/x'] };
        },
      }),
    );

    const client = await connect();
    const result = await client.callTool({ name: 'docs_query', arguments: { query: 'how to?' } });
    const text = (result.content as { text: string }[])[0]!.text;

    expect(asked).toBe('how to?');
    expect(text).toContain('Use the API.');
    expect(text).toContain('https://help.keboola.com/x');
    await client.close();
  });

  it('is filtered out of tools/list when no docs index is configured', async () => {
    setDocsSearchForTests(null);
    const client = await connect();
    const { tools } = await client.listTools();
    expect(tools.map((t) => t.name)).not.toContain('docs_query');
    await client.close();
  });
});
