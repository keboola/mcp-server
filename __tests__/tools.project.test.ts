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

const connect = async (config: Config) => {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverTransport);
  const client = new Client({ name: 'test-client', version: '0.0.0' });
  await client.connect(clientTransport);
  return client;
};

const config = new Config({
  storageApiUrl: 'https://connection.test',
  storageToken: 'test-token',
});

describe('update_project_description', () => {
  it('posts the description to the current branch metadata endpoint', async () => {
    let captured: { pathname: string; body: unknown } | undefined;
    server.use(
      http.post('https://connection.test/*', async ({ request }) => {
        captured = { pathname: new URL(request.url).pathname, body: await request.json() };
        return HttpResponse.json([{ key: 'KBC.projectDescription', value: 'New desc' }]);
      }),
    );

    const client = await connect(config);
    const result = await client.callTool({
      name: 'update_project_description',
      arguments: { description: 'New desc' },
    });

    // Branch resolution: production maps to the `default` branch alias.
    expect(captured?.pathname).toMatch(/\/branch\/default\/metadata$/);
    expect(JSON.stringify(captured?.body)).toContain('New desc');

    const content = result.content as { type: string; text: string }[];
    expect(content[0]!.text).toContain('updated successfully');
    expect(result.isError).toBeFalsy();
    await client.close();
  });

  it('targets a development branch when one is configured', async () => {
    let pathname: string | undefined;
    server.use(
      http.post('https://connection.test/*', ({ request }) => {
        pathname = new URL(request.url).pathname;
        return HttpResponse.json([]);
      }),
    );

    const client = await connect(config.replaceBy({ branchId: '567' }));
    await client.callTool({ name: 'update_project_description', arguments: { description: 'x' } });
    expect(pathname).toMatch(/\/branch\/567\/metadata$/);
    await client.close();
  });
});
