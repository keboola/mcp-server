import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import type { DocsSearch, RetrievedDoc } from '@/clients/docsSearch';
import { setDocsSearchForTests } from '@/clients/docsSearch';
import { Config } from '@/config';
import { createServer } from '@/server';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => {
  server.resetHandlers();
  setDocsSearchForTests(undefined);
});
afterAll(() => server.close());

/** A minimal fake docs-search provider; only the methods under test are implemented. */
const fakeDocsSearch = (overrides: Partial<DocsSearch> = {}): DocsSearch => ({
  search: async () => [],
  answerQuestion: async () => ({ text: '', sourceUrls: [] }),
  recommendComponents: async () => [],
  isReady: async () => true,
  close: async () => {},
  ...overrides,
});

const retrieved = (sourceKey: string, score: number): RetrievedDoc => ({
  id: 'doc-1',
  sourceKey,
  sourceUrl: 'https://components.keboola.com/x',
  title: 'x',
  content: 'x',
  componentType: 'extractor',
  score,
});

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

const callSearch = async (args: Record<string, unknown>): Promise<string> => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 't', version: '0' });
  await client.connect(clientT);
  const result = await client.callTool({ name: 'search', arguments: args });
  const text = (result.content as { text: string }[])[0]!.text;
  await client.close();
  return text;
};

describe('search (global textual)', () => {
  it('runs server-side global search when the feature is enabled and maps hits', async () => {
    let searchQuery: URL | undefined;
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify')) {
          return HttpResponse.json({ owner: { id: '42', features: ['global-search'] } });
        }
        if (url.pathname.endsWith('/global-search')) {
          searchQuery = url;
          return HttpResponse.json({
            all: 1,
            byType: { table: 1 },
            items: [
              {
                id: 'in.c-main.customers',
                name: 'customers',
                type: 'table',
                fullPath: { bucket: { id: 'in.c-main' } },
                created: '2024-01-01T00:00:00Z',
              },
            ],
          });
        }
        return undefined;
      }),
    );

    const text = await callSearch({ patterns: ['customer'], item_types: ['table'] });

    // Production branch context -> branchTypes[]=production, scoped to the project.
    expect(searchQuery?.searchParams.get('branchTypes[]')).toBe('production');
    expect(searchQuery?.searchParams.get('projectIds[]')).toBe('42');
    expect(searchQuery?.searchParams.get('query')).toBe('customer');
    expect(text).toContain('in.c-main.customers');
    expect(text).toContain('current-branch');
    // Link to the table detail page.
    expect(text).toContain('storage/in.c-main/table/customers');
  });

  it('widens to all branches when the current branch context returns nothing', async () => {
    const scopes: string[] = [];
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify')) {
          return HttpResponse.json({ owner: { id: '42', features: ['global-search'] } });
        }
        if (url.pathname.endsWith('/global-search')) {
          const hasBranch = url.searchParams.has('branchTypes[]');
          scopes.push(hasBranch ? 'current' : 'all');
          if (hasBranch) {
            return HttpResponse.json({ all: 0, byType: {}, items: [] });
          }
          return HttpResponse.json({
            all: 1,
            byType: { bucket: 1 },
            items: [
              {
                id: 'in.c-dev',
                name: 'dev-bucket',
                type: 'bucket',
                fullPath: { branch: { id: '789', name: 'feature-x' } },
                created: '2024-02-02T00:00:00Z',
              },
            ],
          });
        }
        return undefined;
      }),
    );

    const text = await callSearch({ patterns: ['dev'], item_types: ['bucket'] });

    expect(scopes).toEqual(['current', 'all']);
    expect(text).toContain('all-branches');
    expect(text).toContain('feature-x');
    expect(text).toContain('in.c-dev');
  });
});

describe('search (enumeration fallback)', () => {
  it('config-based search matches inside configuration JSON and reports scopes', async () => {
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify')) {
          return HttpResponse.json({ owner: { id: '42', features: [] } });
        }
        if (url.pathname.endsWith('/components')) {
          return HttpResponse.json([
            {
              id: 'keboola.snowflake-transformation',
              type: 'transformation',
              configurations: [
                {
                  id: '123',
                  name: 'My SQL',
                  description: 'desc',
                  created: '2024-03-03T00:00:00Z',
                  configuration: {
                    storage: { input: { tables: [{ source: 'in.c-prod.customers' }] } },
                  },
                  rows: [],
                },
              ],
            },
          ]);
        }
        return undefined;
      }),
    );

    const text = await callSearch({
      patterns: ['in.c-prod.customers'],
      item_types: ['transformation'],
      search_type: 'config-based',
    });

    expect(text).toContain('123');
    expect(text).toContain('keboola.snowflake-transformation');
    // The matched JSONPath scope is reported.
    expect(text).toContain('storage.input.tables[0].source');
  });

  it('textual search without the global-search feature enumerates configurations', async () => {
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify')) {
          return HttpResponse.json({ owner: { id: '42', features: [] } });
        }
        if (url.pathname.endsWith('/components')) {
          return HttpResponse.json([
            {
              id: 'keboola.ex-db',
              type: 'extractor',
              configurations: [
                {
                  id: '7',
                  name: 'Sales report',
                  description: null,
                  created: '2024-04-04T00:00:00Z',
                  rows: [],
                },
              ],
            },
          ]);
        }
        return undefined;
      }),
    );

    const text = await callSearch({ patterns: ['sales'], item_types: ['configuration'] });
    expect(text).toContain('Sales report');
    expect(text).toContain('keboola.ex-db');
  });
});

describe('find_component_id', () => {
  it('recommends component ids from the docs index with scores and a dashboard link', async () => {
    let asked: string | undefined;
    server.use(
      http.get('https://connection.test/*', ({ request }) =>
        new URL(request.url).pathname.endsWith('/tokens/verify')
          ? HttpResponse.json({ owner: { id: '42' } })
          : undefined,
      ),
    );
    setDocsSearchForTests(
      fakeDocsSearch({
        recommendComponents: async (query) => {
          asked = query;
          return [
            retrieved('component:keboola.ex-salesforce', 0.9),
            // A non-component doc (no `component:` prefix) is dropped from the result.
            retrieved('help:some-doc', 0.8),
          ];
        },
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

    expect(asked).toBe('salesforce extractor');
    expect(text).toContain('keboola.ex-salesforce');
    expect(text).toContain('components/keboola.ex-salesforce'); // dashboard link
    expect(text).not.toContain('help:some-doc');
    await client.close();
  });

  it('is denied on call when no docs index is configured', async () => {
    server.use(
      http.get('https://connection.test/*', ({ request }) =>
        new URL(request.url).pathname.endsWith('/tokens/verify')
          ? HttpResponse.json({ owner: { id: '42' } })
          : undefined,
      ),
    );
    setDocsSearchForTests(null);

    const [clientT, serverT] = InMemoryTransport.createLinkedPair();
    await createServer(config).connect(serverT);
    const client = new Client({ name: 't', version: '0' });
    await client.connect(clientT);
    await expect(
      client.callTool({ name: 'find_component_id', arguments: { query: 'salesforce' } }),
    ).rejects.toThrow(/documentation index is not/);
    await client.close();
  });
});
