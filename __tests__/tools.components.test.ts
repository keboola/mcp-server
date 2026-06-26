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

const callTool = async (name: string, args: Record<string, unknown>) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 't', version: '0' });
  await client.connect(clientT);
  const result = await client.callTool({ name, arguments: args });
  const text = (result.content as { text: string }[])[0]!.text;
  await client.close();
  return { text, isError: result.isError };
};

describe('get_config_examples', () => {
  it('renders root and row configuration examples as markdown', async () => {
    server.use(
      http.get('https://ai.test/*', ({ request }) => {
        expect(new URL(request.url).pathname).toBe('/docs/components/keboola.ex-aws-s3');
        return HttpResponse.json({
          rootConfigurationExamples: [{ foo: 'bar' }],
          rowConfigurationExamples: [{ baz: 1 }],
        });
      }),
    );

    const { text } = await callTool('get_config_examples', { component_id: 'keboola.ex-aws-s3' });
    expect(text).toContain('# Configuration Examples for `keboola.ex-aws-s3`');
    expect(text).toContain('## Root Configuration Examples');
    expect(text).toContain('"foo": "bar"');
    expect(text).toContain('## Row Configuration Examples');
  });

  it('returns an empty string when the component lookup fails', async () => {
    server.use(http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 })));
    const { text, isError } = await callTool('get_config_examples', { component_id: 'nope' });
    expect(isError).toBeFalsy();
    expect(text).toBe('');
  });
});

const verify = () =>
  http.get('https://connection.test/*', ({ request }) => {
    const { pathname } = new URL(request.url);
    if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
    return undefined;
  });

describe('get_components', () => {
  it('merges AI catalog metadata with Storage data and derives capabilities', async () => {
    server.use(
      verify(),
      http.get('https://ai.test/*', () =>
        HttpResponse.json({
          id: 'keboola.ex-aws-s3',
          name: 'AWS S3',
          type: 'extractor',
          flags: ['genericDockerUI-rows', 'genericDockerUI-tableOutput'],
          documentation: 'docs here',
        }),
      ),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.includes('/components/')) {
          return HttpResponse.json({ data: { synchronous_actions: ['testConnection'] } });
        }
        return undefined;
      }),
    );

    const { text } = await callTool('get_components', { component_ids: ['keboola.ex-aws-s3'] });
    expect(text).toContain('AWS S3');
    expect(text).toContain('is_row_based: true');
    expect(text).toContain('testConnection');
    expect(text).toContain('docs here');
  });

  it('falls back to the Storage API when the AI catalog returns 404', async () => {
    server.use(
      verify(),
      http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 })),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        return HttpResponse.json({ id: 'priv.comp', name: 'Private', type: 'writer', flags: [] });
      }),
    );

    const { text } = await callTool('get_components', { component_ids: ['priv.comp'] });
    expect(text).toContain('Private');
  });
});

describe('get_configs', () => {
  it('lists configs by component id grouped under the component', async () => {
    server.use(
      verify(),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/components/keboola.ex-db-mysql/configs')) {
          return HttpResponse.json([{ id: '100', name: 'My DB', isDisabled: false }]);
        }
        if (pathname.endsWith('/components/keboola.ex-db-mysql')) {
          return HttpResponse.json({
            id: 'keboola.ex-db-mysql',
            name: 'MySQL',
            type: 'extractor',
            flags: [],
          });
        }
        return undefined;
      }),
    );

    const { text } = await callTool('get_configs', { component_ids: ['keboola.ex-db-mysql'] });
    expect(text).toContain('My DB');
    expect(text).toContain('MySQL');
  });

  it('returns full details with redacted secrets for specific configs', async () => {
    server.use(
      verify(),
      http.get('https://ai.test/*', () => new HttpResponse(null, { status: 404 })),
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (pathname.endsWith('/configs/100')) {
          return HttpResponse.json({
            id: '100',
            name: 'My DB',
            version: 3,
            configuration: { parameters: { host: 'db', '#password': 'plaintext' } },
          });
        }
        if (pathname.endsWith('/components/keboola.ex-db-mysql')) {
          return HttpResponse.json({
            id: 'keboola.ex-db-mysql',
            name: 'MySQL',
            type: 'extractor',
            flags: [],
          });
        }
        return undefined;
      }),
    );

    const { text } = await callTool('get_configs', {
      configs: [{ component_id: 'keboola.ex-db-mysql', configuration_id: '100' }],
    });
    expect(text).toContain('[REDACTED]');
    expect(text).not.toContain('plaintext');
    expect(text).toContain('host');
  });
});

describe('run_sync_action', () => {
  it('merges row config over root and posts to the sync-actions endpoint', async () => {
    let actionBody: { configData?: Record<string, unknown>; action?: string } | undefined;
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const { pathname } = new URL(request.url);
        if (pathname.endsWith('/configs/cfg/rows/r1')) {
          return HttpResponse.json({
            configuration: { parameters: { row: 1 }, storage: { input: 'r' } },
          });
        }
        if (pathname.endsWith('/configs/cfg')) {
          return HttpResponse.json({
            configuration: {
              parameters: { base: 1 },
              storage: {},
              authorization: { oauth_api: { id: 'a' } },
            },
          });
        }
        return undefined;
      }),
      http.post('https://sync-actions.test/*', async ({ request }) => {
        expect(new URL(request.url).pathname).toBe('/actions');
        actionBody = (await request.json()) as {
          configData?: Record<string, unknown>;
          action?: string;
        };
        return HttpResponse.json({ status: 'success', tables: [] });
      }),
    );

    const { text } = await callTool('run_sync_action', {
      action_name: 'getTables',
      component_id: 'keboola.ex-db-mysql',
      configuration_id: 'cfg',
      configuration_row_id: 'r1',
    });

    expect(actionBody?.action).toBe('getTables');
    // row parameters merged on top of root; authorization carried from root.
    expect(actionBody?.configData).toMatchObject({
      parameters: { base: 1, row: 1 },
      authorization: { oauth_api: { id: 'a' } },
    });
    expect(text).toContain('success');
  });
});
