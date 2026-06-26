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
