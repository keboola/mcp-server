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

// Token verify backs the links manager (project id). Always available.
const verifyHandler = () =>
  http.get('https://connection.test/*', ({ request }) => {
    if (new URL(request.url).pathname.endsWith('/tokens/verify')) {
      return HttpResponse.json({ owner: { id: '42' } });
    }
    return undefined;
  });

const callText = async (
  client: Awaited<ReturnType<typeof connect>>,
  args: Record<string, unknown>,
) => {
  const result = await client.callTool({ name: 'get_jobs', arguments: args });
  expect(result.isError).toBeFalsy();
  return (result.content as { text: string }[])[0]!.text;
};

describe('get_jobs', () => {
  it('lists job summaries and maps component/config aliases', async () => {
    server.use(
      verifyHandler(),
      http.get('https://queue.test/*', () =>
        HttpResponse.json([
          {
            id: '1',
            status: 'success',
            component: 'keboola.ex-aws-s3',
            config: 'c1',
            isFinished: true,
          },
        ]),
      ),
    );

    const text = await callText(await connect(), { job_ids: [] });
    expect(text).toContain('keboola.ex-aws-s3');
    expect(text).toContain('componentId');
    // Listing surfaces the jobs dashboard link.
    expect(text).toContain('queue');
  });

  it('returns full details for specific job ids with a job link', async () => {
    server.use(
      verifyHandler(),
      http.get('https://queue.test/*', () =>
        HttpResponse.json({
          id: '99',
          status: 'error',
          component: 'x',
          config: 'c',
          url: 'https://job/99',
        }),
      ),
    );

    const text = await callText(await connect(), { job_ids: ['99'] });
    expect(text).toContain('https://job/99');
    expect(text).toContain('/queue/99'); // job detail link
  });

  it('includes filtered, chronologically-ordered logs when requested', async () => {
    server.use(
      verifyHandler(),
      http.get('https://queue.test/*', () =>
        HttpResponse.json({ id: '7', status: 'error', component: 'x', config: 'c', url: 'u' }),
      ),
    );
    // Events come newest-first; the tool filters by type then reverses to chronological.
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const url = new URL(request.url);
        if (url.pathname.endsWith('/tokens/verify'))
          return HttpResponse.json({ owner: { id: '42' } });
        if (url.pathname.endsWith('/events')) {
          return HttpResponse.json([
            { message: 'boom', type: 'error', created: 't2' },
            { message: 'starting', type: 'info', created: 't1' },
          ]);
        }
        return undefined;
      }),
    );

    const text = await callText(await connect(), {
      job_ids: ['7'],
      include_logs: true,
      log_event_types: ['error'],
    });
    expect(text).toContain('boom');
    expect(text).not.toContain('starting');
  });
});

describe('run_job', () => {
  const runJob = async (args: Record<string, unknown>, capture: (body: unknown) => void) => {
    server.use(
      verifyHandler(),
      http.post('https://queue.test/*', async ({ request }) => {
        capture(await request.json());
        return HttpResponse.json({
          id: '500',
          status: 'created',
          component: args.component_id,
          config: args.configuration_id,
          url: 'https://job/500',
        });
      }),
    );
    const client = await connect();
    const result = await client.callTool({ name: 'run_job', arguments: args });
    expect(result.isError).toBeFalsy();
    const text = (result.content as { text: string }[])[0]!.text;
    await client.close();
    return text;
  };

  it('creates a job with mode=run and returns its details + link', async () => {
    let body: Record<string, unknown> = {};
    const text = await runJob(
      { component_id: 'keboola.ex-aws-s3', configuration_id: 'c1' },
      (b) => {
        body = b as Record<string, unknown>;
      },
    );
    expect(body).toMatchObject({ component: 'keboola.ex-aws-s3', config: 'c1', mode: 'run' });
    expect(body.branchId).toBeUndefined(); // production: no branchId in payload
    expect(text).toContain('https://job/500');
    expect(text).toContain('/queue/500');
  });

  it('passes config row ids and the branch id on a development branch', async () => {
    let body: Record<string, unknown> = {};
    const [clientT, serverT] = InMemoryTransport.createLinkedPair();
    await createServer(config.replaceBy({ branchId: '789' })).connect(serverT);
    const client = new Client({ name: 't', version: '0' });
    await client.connect(clientT);
    server.use(
      verifyHandler(),
      http.post('https://queue.test/*', async ({ request }) => {
        body = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({
          id: '501',
          status: 'created',
          component: 'x',
          config: 'c',
          url: 'u',
        });
      }),
    );
    await client.callTool({
      name: 'run_job',
      arguments: { component_id: 'x', configuration_id: 'c', configuration_row_ids: ['r1', 'r2'] },
    });
    expect(body.branchId).toBe('789');
    expect(body.configRowIds).toEqual(['r1', 'r2']);
    await client.close();
  });
});
