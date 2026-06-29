import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { registerFlowTools } from '@/tools/flow';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const config = new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok' });

// A standalone server registering only the flow tools (the real server.ts is not edited).
const makeServer = (cfg = config) => {
  const mcp = new McpServer({ name: 'test', version: '0.0.0' });
  registerFlowTools(mcp, cfg);
  return mcp;
};

const connect = async (cfg = config) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await makeServer(cfg).connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

const call = async (name: string, args: Record<string, unknown>, cfg = config) => {
  const client = await connect(cfg);
  const result = await client.callTool({ name, arguments: args });
  const text = (result.content as { text: string }[])[0]!.text;
  await client.close();
  return { text, isError: result.isError };
};

// Token verify backs links manager (project id) + project context (name/features).
const verifyOwner = (features: unknown = []) =>
  http.get('https://connection.test/*', ({ request }) => {
    if (new URL(request.url).pathname.endsWith('/tokens/verify')) {
      return HttpResponse.json({ owner: { id: '42', name: 'Proj', features } });
    }
    return undefined;
  });

// Scheduler list (used by get_flows). Returns [] unless overridden.
const schedulerEmpty = () => http.get('https://scheduler.test/*', () => HttpResponse.json([]));

describe('get_flow_examples', () => {
  it('renders legacy examples as markdown', async () => {
    server.use(verifyOwner());
    const { text, isError } = await call('get_flow_examples', {
      flow_type: 'keboola.orchestrator',
    });
    expect(isError).toBeFalsy();
    expect(text).toContain('# Flow Configuration Examples for `keboola.orchestrator`');
    expect(text).toContain('1. Flow Configuration:');
    expect(text).toContain('```json');
  });

  it('refuses conditional examples when the feature is disabled', async () => {
    server.use(verifyOwner(['hide-conditional-flows']));
    const { text, isError } = await call('get_flow_examples', { flow_type: 'keboola.flow' });
    expect(isError).toBeTruthy();
    expect(text).toContain('Conditional flows are not supported');
    expect(text).toContain('legacy flow examples');
  });

  it('renders conditional examples when enabled', async () => {
    server.use(verifyOwner([]));
    const { text, isError } = await call('get_flow_examples', { flow_type: 'keboola.flow' });
    expect(isError).toBeFalsy();
    expect(text).toContain('# Flow Configuration Examples for `keboola.flow`');
  });
});

describe('get_flow_schema', () => {
  it('returns the bundled legacy schema as markdown', async () => {
    server.use(verifyOwner());
    const { text, isError } = await call('get_flow_schema', {
      flow_type: 'keboola.orchestrator',
    });
    expect(isError).toBeFalsy();
    expect(text).toContain('"phases"');
    expect(text).toContain('"tasks"');
    expect(text).toContain('componentId');
  });

  it('fetches the live conditional schema from the AI catalog', async () => {
    server.use(
      verifyOwner([]),
      http.get('https://ai.test/*', () =>
        HttpResponse.json({
          id: 'keboola.flow',
          name: 'Flow',
          type: 'application',
          configurationSchema: { type: 'object', properties: { phases: { type: 'array' } } },
        }),
      ),
      http.get('https://connection.test/v2/storage/*', () => HttpResponse.json({ data: {} })),
    );
    const { text, isError } = await call('get_flow_schema', { flow_type: 'keboola.flow' });
    expect(isError).toBeFalsy();
    expect(text).toContain('"phases"');
  });

  it('refuses conditional schema when the feature is disabled', async () => {
    server.use(verifyOwner(['hide-conditional-flows']));
    const { text, isError } = await call('get_flow_schema', { flow_type: 'keboola.flow' });
    expect(isError).toBeTruthy();
    expect(text).toContain('conditional_flows=false');
  });
});

describe('create_flow (legacy)', () => {
  it('creates a flow, normalizes ids, sets MCP metadata, returns links', async () => {
    let createdBody: Record<string, unknown> = {};
    server.use(
      verifyOwner(),
      // create config
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        async ({ request }) => {
          const url = new URL(request.url);
          if (url.pathname.endsWith('/configs')) {
            createdBody = (await request.json()) as Record<string, unknown>;
            return HttpResponse.json({
              id: '123',
              name: 'My Flow',
              description: 'desc',
              version: 1,
            });
          }
          return undefined;
        },
      ),
      // metadata POST (createdBy)
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/123/metadata',
        () => HttpResponse.json([]),
      ),
      // folder search + config list (folder hint, returns few configs -> no hint)
      http.get(
        'https://connection.test/v2/storage/branch/default/search/component-configurations',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        () => HttpResponse.json([{ id: '123' }]),
      ),
    );

    const { text, isError } = await call('create_flow', {
      name: 'My Flow',
      description: 'desc',
      phases: [{ name: 'Phase 1' }],
      tasks: [{ name: 'T', phase: 1, task: { componentId: 'keboola.ex-aws-s3', configId: 'c1' } }],
    });
    expect(isError).toBeFalsy();
    expect(text).toContain('123');
    expect(text).toContain('keboola.orchestrator');
    expect(text).toContain('/flows/123'); // flow detail link
    // phase got id=1, task got id=20001 and mode=run
    const cfg = createdBody.configuration as {
      phases: { id: number }[];
      tasks: { id: number; task: { mode: string } }[];
    };
    expect(cfg.phases[0]!.id).toBe(1);
    expect(cfg.tasks[0]!.id).toBe(20001);
    expect(cfg.tasks[0]!.task.mode).toBe('run');
  });

  it('rejects a task referencing a non-existent phase', async () => {
    server.use(verifyOwner());
    const { text, isError } = await call('create_flow', {
      name: 'Bad',
      description: 'd',
      phases: [{ id: 1, name: 'P1' }],
      tasks: [{ id: 5, name: 'T', phase: 99, task: { componentId: 'x' } }],
    });
    expect(isError).toBeTruthy();
    expect(text).toContain('non-existent phase');
  });
});

describe('create_conditional_flow', () => {
  const conditionalSchema = () =>
    http.get('https://ai.test/*', () =>
      HttpResponse.json({
        id: 'keboola.flow',
        name: 'Flow',
        type: 'application',
        configurationSchema: { type: 'object' },
      }),
    );

  it('validates structure (entry/reachability) and creates the flow', async () => {
    let body: Record<string, unknown> = {};
    server.use(
      verifyOwner([]),
      conditionalSchema(),
      http.get('https://connection.test/v2/storage/branch/default/components/keboola.flow', () =>
        HttpResponse.json({ data: {} }),
      ),
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.flow/configs',
        async ({ request }) => {
          body = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({ id: 'f1', name: 'CF', description: 'd', version: 1 });
        },
      ),
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.flow/configs/f1/metadata',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/search/component-configurations',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.flow/configs',
        () => HttpResponse.json([]),
      ),
    );

    const { text, isError } = await call('create_conditional_flow', {
      name: 'CF',
      description: 'd',
      phases: [
        { id: 'a', name: 'A', next: [{ id: 't1', goto: 'b' }] },
        { id: 'b', name: 'B', next: [] },
      ],
      tasks: [
        { id: 't', name: 'Task', phase: 'a', task: { type: 'job', componentId: 'x', mode: 'run' } },
      ],
    });
    expect(isError).toBeFalsy();
    expect(text).toContain('keboola.flow');
    expect(text).toContain('/flows-v2/f1');
    // ending phase 'b' had empty next -> dropped from serialized config
    const cfg = body.configuration as { phases: Record<string, unknown>[] };
    expect(cfg.phases[1]!.next).toBeUndefined();
  });

  it('rejects multiple entry phases', async () => {
    server.use(verifyOwner([]), conditionalSchema());
    const { text, isError } = await call('create_conditional_flow', {
      name: 'CF',
      description: 'd',
      phases: [
        { id: 'a', name: 'A', next: [] },
        { id: 'b', name: 'B', next: [] },
      ],
      tasks: [],
    });
    expect(isError).toBeTruthy();
    expect(text).toContain('entry phase');
  });

  it('fails when the conditional feature is disabled', async () => {
    server.use(verifyOwner(['hide-conditional-flows']));
    const { text, isError } = await call('create_conditional_flow', {
      name: 'CF',
      description: 'd',
      phases: [{ id: 'a', name: 'A', next: [] }],
      tasks: [],
    });
    expect(isError).toBeTruthy();
    expect(text).toContain('Conditional flows are not supported');
  });
});

describe('get_flows', () => {
  it('lists all flow summaries with dashboard links', async () => {
    server.use(
      verifyOwner(),
      schedulerEmpty(),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.flow/configs',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        () =>
          HttpResponse.json([
            {
              id: '7',
              name: 'Flow7',
              version: 2,
              configuration: { phases: [{ id: 1 }], tasks: [{ id: 2 }, { id: 3 }] },
            },
          ]),
      ),
    );
    const { text, isError } = await call('get_flows', { flow_ids: [] });
    expect(isError).toBeFalsy();
    expect(text).toContain('Flow7');
    expect(text).toContain('phases_count');
    expect(text).toContain('/flows'); // dashboard link
  });

  it('returns full details for a specific flow id, resolving its type', async () => {
    server.use(
      verifyOwner(),
      schedulerEmpty(),
      // keboola.flow lookup 404s -> falls back to orchestrator
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.flow/configs/7',
        () => new HttpResponse(null, { status: 404 }),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/7',
        () =>
          HttpResponse.json({
            id: '7',
            name: 'Flow7',
            version: 2,
            configuration: { phases: [{ id: 1, name: 'P' }], tasks: [] },
          }),
      ),
    );
    const { text, isError } = await call('get_flows', { flow_ids: ['7'] });
    expect(isError).toBeFalsy();
    expect(text).toContain('Flow7');
    expect(text).toContain('keboola.orchestrator');
    expect(text).toContain('phases');
  });
});

describe('modify_flow', () => {
  it('updates phases/tasks and sets update metadata', async () => {
    let putBody: Record<string, unknown> = {};
    server.use(
      verifyOwner([]),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/9',
        () =>
          HttpResponse.json({
            id: '9',
            name: 'F9',
            version: 3,
            configuration: { phases: [{ id: 1, name: 'Old' }], tasks: [] },
          }),
      ),
      http.put(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/9',
        async ({ request }) => {
          putBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({ id: '9', name: 'F9', description: 'd', version: 4 });
        },
      ),
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/9/metadata',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/search/component-configurations',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        () => HttpResponse.json([{ id: '9' }]),
      ),
    );
    const { text, isError } = await call('modify_flow', {
      configuration_id: '9',
      flow_type: 'keboola.orchestrator',
      change_description: 'update phases',
      phases: [{ id: 1, name: 'New Phase' }],
      tasks: [{ id: 2, name: 'T', phase: 1, task: { componentId: 'x' } }],
    });
    expect(isError).toBeFalsy();
    expect(text).toContain('version: 4');
    const cfg = putBody.configuration as { phases: { name: string }[] };
    expect(cfg.phases[0]!.name).toBe('New Phase');
    expect(putBody.changeDescription).toBe('update phases');
  });

  it('processes an add-schedule request and appends a scheduler link', async () => {
    server.use(
      verifyOwner([]),
      // no config changes -> just detail fetch
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/9',
        () =>
          HttpResponse.json({
            id: '9',
            name: 'F9',
            description: 'd',
            version: 3,
            configuration: {},
          }),
      ),
      // folder hint
      http.get(
        'https://connection.test/v2/storage/branch/default/search/component-configurations',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        () => HttpResponse.json([{ id: '9' }]),
      ),
      // scheduler list (current schedules) -> empty
      http.get('https://scheduler.test/schedules', () => HttpResponse.json([])),
      // create scheduler config in storage
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.scheduler/configs',
        () => HttpResponse.json({ id: 'sched1', version: 1 }),
      ),
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.scheduler/configs/sched1/metadata',
        () => HttpResponse.json([]),
      ),
      // activate schedule
      http.post('https://scheduler.test/schedules', () =>
        HttpResponse.json({
          id: 'act1',
          configurationId: 'sched1',
          schedule: { cronTab: '0 8 * * 1', timezone: 'UTC', state: 'enabled' },
        }),
      ),
    );
    const { text, isError } = await call('modify_flow', {
      configuration_id: '9',
      flow_type: 'keboola.orchestrator',
      change_description: 'add schedule',
      schedules: [{ action: 'add', cron_tab: '0 8 * * 1', state: 'enabled', timezone: 'UTC' }],
    });
    expect(isError).toBeFalsy();
    expect(text).toContain('Schedules request processed successfully');
    expect(text).toContain('Created schedule: sched1');
    expect(text).toContain('/schedules'); // scheduler detail link
  });

  it('rejects an invalid cron expression', async () => {
    server.use(
      verifyOwner([]),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/9',
        () =>
          HttpResponse.json({
            id: '9',
            name: 'F9',
            description: 'd',
            version: 3,
            configuration: {},
          }),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/search/component-configurations',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        () => HttpResponse.json([{ id: '9' }]),
      ),
      http.get('https://scheduler.test/schedules', () => HttpResponse.json([])),
    );
    const { text, isError } = await call('modify_flow', {
      configuration_id: '9',
      flow_type: 'keboola.orchestrator',
      change_description: 'bad cron',
      schedules: [{ action: 'add', cron_tab: '99 99 * *', state: 'enabled' }],
    });
    expect(isError).toBeTruthy();
    expect(text).toContain('Invalid cron tab expression');
  });
});

describe('update_flow', () => {
  it('delegates to modify_flow without schedules', async () => {
    let putBody: Record<string, unknown> = {};
    server.use(
      verifyOwner([]),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/5',
        () =>
          HttpResponse.json({
            id: '5',
            name: 'F5',
            version: 1,
            configuration: { phases: [{ id: 1, name: 'P' }], tasks: [] },
          }),
      ),
      http.put(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/5',
        async ({ request }) => {
          putBody = (await request.json()) as Record<string, unknown>;
          return HttpResponse.json({ id: '5', name: 'Renamed', description: 'd', version: 2 });
        },
      ),
      http.post(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs/5/metadata',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/search/component-configurations',
        () => HttpResponse.json([]),
      ),
      http.get(
        'https://connection.test/v2/storage/branch/default/components/keboola.orchestrator/configs',
        () => HttpResponse.json([{ id: '5' }]),
      ),
    );
    const { text, isError } = await call('update_flow', {
      configuration_id: '5',
      flow_type: 'keboola.orchestrator',
      change_description: 'rename',
      name: 'Renamed',
    });
    expect(isError).toBeFalsy();
    expect(putBody.name).toBe('Renamed');
    expect(text).toContain('Renamed');
  });
});
