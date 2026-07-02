import { serve } from '@hono/node-server';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import type { AddressInfo } from 'node:net';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { runPreviewConfigDiff } from '@/preview';
import { createHttpApp } from '@/transports/http';

// ---------------------------------------------------------------------------
// HTTP mocks (msw). The preview path issues: tokens/verify, configuration_detail,
// component fetch (ai + storage). Handlers are registered per-test via configure().
// ---------------------------------------------------------------------------

const msw = setupServer();
beforeAll(() =>
  msw.listen({
    onUnhandledRequest: (request, print) => {
      // Let the real Hono test server (localhost) through; error on anything else.
      if (new URL(request.url).hostname === 'localhost') return;
      print.error();
    },
  }),
);
afterEach(() => msw.resetHandlers());
afterAll(() => msw.close());

const baseConfig = (over: Record<string, string> = {}) =>
  new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok', ...over });

/** A loosely-typed config record shape for asserting on nested diff fields. */
type Cfg = {
  name?: string;
  description?: string;
  changeDescription?: string;
  configuration: { parameters: Record<string, unknown> };
};
const asCfg = (value: unknown): Cfg => value as Cfg;

type ConfigureOpts = {
  role?: string;
  features?: string[];
  config?: Record<string, unknown> | (() => never);
  component?: Record<string, unknown>;
};

/**
 * Registers msw handlers for the storage + ai services. `config` is the value returned
 * by configuration_detail / configuration_row_detail; pass a function to make it throw.
 */
const configure = (opts: ConfigureOpts = {}) => {
  const role = opts.role ?? 'admin';
  const features = opts.features ?? [];
  const component = opts.component ?? {
    id: 'keboola.ex-test',
    name: 'Test Extractor',
    type: 'extractor',
    configurationSchema: {},
    flags: [],
  };

  msw.use(
    http.get('https://connection.test/*', ({ request }) => {
      const path = new URL(request.url).pathname;
      if (path.endsWith('/tokens/verify')) {
        return HttpResponse.json({ owner: { features }, admin: { role } });
      }
      if (path.includes('/components/') && path.includes('/configs/')) {
        if (typeof opts.config === 'function') {
          return HttpResponse.json({ error: 'Invalid configuration ID' }, { status: 404 });
        }
        return HttpResponse.json(opts.config ?? {});
      }
      if (path.includes('/components/')) {
        return HttpResponse.json(component);
      }
      return undefined;
    }),
    // Component docs from the AI service — 404 so fetchComponent falls back to storage.
    http.get('https://ai.test/*', () => HttpResponse.json({ error: 'nope' }, { status: 404 })),
  );
};

// ---------------------------------------------------------------------------
// runPreviewConfigDiff (logic) — port of tests/test_preview.py.
// ---------------------------------------------------------------------------

describe('runPreviewConfigDiff — authorization', () => {
  it.each<[Record<string, string>, number]>([
    [{}, 200],
    [{ allowedTools: 'update_config,get_tables' }, 200],
    [{ allowedTools: 'get_tables,get_buckets' }, 403],
    [{ disallowedTools: 'update_config' }, 403],
    [{ readOnlyMode: 'true' }, 403],
  ])('header auth %j -> %s', async (over, expectedStatus) => {
    configure({ config: { id: 'config-123', name: 'C', configuration: { parameters: {} } } });
    const rq = {
      toolName: 'update_config',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'config-123',
        change_description: 'Test change',
      },
    };
    if (expectedStatus === 200) {
      const resp = await runPreviewConfigDiff(baseConfig(over), rq);
      expect(resp.isValid).toBe(true);
    } else {
      await expect(runPreviewConfigDiff(baseConfig(over), rq)).rejects.toMatchObject({
        status: 403,
        message: expect.stringContaining('not authorized'),
      });
    }
  });

  it.each<[string, string, string | undefined, string]>([
    ['update_config', 'readOnly', undefined, 'read-only operations'],
    ['modify_streamlit_data_app', 'admin', 'dev-123', 'main production branch'],
    ['update_flow', 'admin', undefined, 'admin/OAuth'],
  ])('project/role/branch gate: %s role=%s -> %s', async (toolName, role, branchId, fragment) => {
    configure({ role });
    const cfg = baseConfig(branchId ? { branchId } : {});
    await expect(
      runPreviewConfigDiff(cfg, { toolName, toolParams: { configuration_id: 'cfg-1' } }),
    ).rejects.toMatchObject({ status: 403, message: expect.stringContaining(fragment) });
  });
});

describe('runPreviewConfigDiff — update_config diff', () => {
  const originalConfigData = {
    id: 'config-123',
    name: 'Original Config Name',
    description: 'Original description',
    configuration: { parameters: { foo: 'bar', baz: 42 } },
  };

  it('previews update_config with parameter updates, name and description', async () => {
    configure({ config: structuredClone(originalConfigData) });
    const resp = await runPreviewConfigDiff(baseConfig(), {
      toolName: 'update_config',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'config-123',
        change_description: 'Test change',
        name: 'Updated Config Name',
        description: 'Updated description',
        parameter_updates: [
          { op: 'set', path: 'foo', value: 'updated_bar' },
          { op: 'set', path: 'new_param', value: 'new_value' },
        ],
      },
    });

    expect(resp.coordinates).toMatchObject({
      componentId: 'keboola.ex-test',
      configurationId: 'config-123',
    });
    expect(resp.coordinates).not.toHaveProperty('configurationRowId');
    expect(resp.isValid).toBe(true);
    expect(resp).not.toHaveProperty('validationErrors');

    const original = asCfg(resp.originalConfig);
    const updated = asCfg(resp.updatedConfig);
    expect(original.name).toBe('Original Config Name');
    expect(original.configuration.parameters.foo).toBe('bar');
    expect(updated.name).toBe('Updated Config Name');
    expect(updated.description).toBe('Updated description');
    expect(updated.configuration.parameters.foo).toBe('updated_bar');
    expect(updated.configuration.parameters.new_param).toBe('new_value');
    expect(updated.configuration.parameters.baz).toBe(42);
    expect(updated.changeDescription).toBe('Test change');
  });

  it('returns isValid=false with empty configs when the mutator throws', async () => {
    configure({ config: () => undefined as never });
    const resp = await runPreviewConfigDiff(baseConfig(), {
      toolName: 'update_config',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'invalid-config',
        change_description: 'Test change',
      },
    });
    expect(resp.isValid).toBe(false);
    expect((resp.validationErrors as string[]).length).toBeGreaterThan(0);
    expect(resp.originalConfig).toEqual({});
    expect(resp.updatedConfig).toEqual({});
  });

  it('leaves name/description unchanged when only required params are given', async () => {
    configure({
      config: {
        id: 'config-123',
        name: 'Original Config',
        description: 'Original description',
        configuration: { parameters: { foo: 'bar' } },
      },
    });
    const resp = await runPreviewConfigDiff(baseConfig(), {
      toolName: 'update_config',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'config-123',
        change_description: 'Test change',
      },
    });
    const updated = asCfg(resp.updatedConfig);
    expect(resp.isValid).toBe(true);
    expect(updated.name).toBe('Original Config');
    expect(updated.description).toBe('Original description');
    expect(updated.configuration.parameters.foo).toBe('bar');
  });
});

describe('runPreviewConfigDiff — update_config_row diff', () => {
  it('previews update_config_row including the row coordinate', async () => {
    configure({
      config: {
        id: 'row-456',
        name: 'Original Row Name',
        description: 'Original row description',
        configuration: { parameters: { foo: 'bar', baz: 42 } },
      },
    });
    const resp = await runPreviewConfigDiff(baseConfig(), {
      toolName: 'update_config_row',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'config-123',
        configuration_row_id: 'row-456',
        change_description: 'Row change',
        parameter_updates: [{ op: 'set', path: 'foo', value: 'updated_bar' }],
      },
    });
    expect(resp.isValid).toBe(true);
    expect(resp.coordinates).toMatchObject({
      componentId: 'keboola.ex-test',
      configurationId: 'config-123',
      configurationRowId: 'row-456',
    });
    expect(asCfg(resp.updatedConfig).configuration.parameters.foo).toBe('updated_bar');
  });
});

describe('runPreviewConfigDiff — schema validation', () => {
  it('returns isValid=false for a missing required param', async () => {
    configure();
    const resp = await runPreviewConfigDiff(baseConfig(), {
      toolName: 'update_config',
      toolParams: { component_id: 'keboola.ex-test', change_description: 'Test' },
    });
    expect(resp.isValid).toBe(false);
    expect(JSON.stringify(resp.validationErrors)).toContain('configuration_id');
    expect(resp.originalConfig).toEqual({});
    expect(resp.updatedConfig).toEqual({});
  });

  it('returns isValid=false for an invalid parameter_updates entry', async () => {
    configure();
    const resp = await runPreviewConfigDiff(baseConfig(), {
      toolName: 'update_config',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'config-123',
        change_description: 'Test change',
        parameter_updates: [
          { op: 'set', path: 'foo', value: 'x' },
          { op: 'foo', path: 'bar', value: 'y' },
        ],
      },
    });
    expect(resp.isValid).toBe(false);
    expect(JSON.stringify(resp.validationErrors)).toContain('parameter_updates');
  });
});

describe('runPreviewConfigDiff — unsupported tools', () => {
  it('rejects an unknown tool name with 400', async () => {
    configure();
    await expect(
      runPreviewConfigDiff(baseConfig(), {
        toolName: 'invalid_tool',
        toolParams: { component_id: 'keboola.ex-test', configuration_id: 'config-123' },
      }),
    ).rejects.toMatchObject({ status: 400 });
  });

  it.each(['update_sql_transformation', 'update_flow', 'modify_streamlit_data_app'])(
    'returns 400 for the not-yet-diffable config tool %s',
    async (toolName) => {
      // Use a role/branch that clears the project/role/branch gate for each tool.
      const role = toolName === 'update_flow' ? 'guest' : 'admin';
      configure({ role });
      const params: Record<string, unknown> = {
        configuration_id: 'cfg-1',
        change_description: 'x',
      };
      if (toolName === 'update_flow') params.flow_type = 'keboola.orchestrator';
      if (toolName === 'modify_streamlit_data_app') {
        params.name = 'My App';
        params.description = 'desc';
        params.source_code = 'print(1)';
        params.packages = ['streamlit'];
        params.authentication_type = 'default';
      }
      await expect(
        runPreviewConfigDiff(baseConfig(), { toolName, toolParams: params }),
      ).rejects.toMatchObject({ status: 400, message: expect.stringContaining('not supported') });
    },
  );
});

// ---------------------------------------------------------------------------
// Hono route — status codes + JSON body errors.
// ---------------------------------------------------------------------------

describe('POST /preview/configuration (Hono route)', () => {
  let httpServer: ReturnType<typeof serve>;
  let port: number;

  beforeAll(async () => {
    const app = createHttpApp(baseConfig());
    port = await new Promise<number>((resolve) => {
      httpServer = serve({ fetch: app.fetch, port: 0 }, (info: AddressInfo) => resolve(info.port));
    });
  });
  afterAll(() => httpServer.close());

  const post = (body: unknown, headers: Record<string, string> = {}) =>
    fetch(`http://localhost:${port}/preview/configuration`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: typeof body === 'string' ? body : JSON.stringify(body),
    });

  it('returns 400 for malformed JSON', async () => {
    const res = await post('{ not json');
    expect(res.status).toBe(400);
    expect(((await res.json()) as { message: string }).message).toContain('Invalid JSON');
  });

  it('returns 400 for a missing toolName', async () => {
    const res = await post({ toolParams: {} });
    expect(res.status).toBe(400);
  });

  it('returns 200 with a diff for a valid request', async () => {
    configure({ config: { id: 'config-123', name: 'C', configuration: { parameters: {} } } });
    const res = await post({
      toolName: 'update_config',
      toolParams: {
        component_id: 'keboola.ex-test',
        configuration_id: 'config-123',
        change_description: 'Test change',
      },
    });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { isValid: boolean; coordinates: { componentId: string } };
    expect(body.isValid).toBe(true);
    expect(body.coordinates.componentId).toBe('keboola.ex-test');
  });

  it('returns 403 when header authorization denies the tool', async () => {
    configure();
    const res = await post(
      {
        toolName: 'update_config',
        toolParams: { component_id: 'keboola.ex-test', configuration_id: 'config-123' },
      },
      { 'X-Disallowed-Tools': 'update_config' },
    );
    expect(res.status).toBe(403);
    expect(((await res.json()) as { message: string }).message).toContain('not authorized');
  });
});
