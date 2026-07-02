import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import { registerDataAppTools } from '@/tools/data_apps';

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const config = new Config({
  storageApiUrl: 'https://connection.test',
  storageToken: 'tok',
  workspaceSchema: 'WS_SCHEMA',
});

const connect = async (cfg: Config = config) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  const mcp = new McpServer({ name: 'test', version: '0.0.0' });
  registerDataAppTools(mcp, cfg);
  await mcp.connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

// tokens/verify backs both the links manager (project id) and feature checks.
const verifyHandler = (features: string[] = []) =>
  http.get('https://connection.test/*', ({ request }) => {
    if (new URL(request.url).pathname.endsWith('/tokens/verify')) {
      return HttpResponse.json({ owner: { id: '42', features } });
    }
    return undefined;
  });

const call = async (
  client: Awaited<ReturnType<typeof connect>>,
  name: string,
  args: Record<string, unknown>,
) => {
  const result = await client.callTool({ name, arguments: args });
  return result;
};

const text = (result: Awaited<ReturnType<typeof call>>): string =>
  (result.content as { text: string }[])[0]!.text;

describe('get_data_apps', () => {
  it('lists data app summaries filtered to the data-apps component', async () => {
    server.use(
      verifyHandler(),
      http.get('https://data-science.test/apps', () =>
        HttpResponse.json([
          {
            id: 'app1',
            projectId: '42',
            componentId: 'keboola.data-apps',
            branchId: null,
            configId: 'cfg1',
            configVersion: '3',
            type: 'streamlit',
            state: 'running',
            desiredState: 'running',
            url: 'https://app1.run',
          },
          {
            id: 'other',
            projectId: '42',
            componentId: 'keboola.other',
            branchId: null,
            configId: 'cfgX',
            configVersion: '1',
            type: 'streamlit',
            state: 'stopped',
            desiredState: 'stopped',
          },
        ]),
      ),
    );
    const result = await call(await connect(), 'get_data_apps', {});
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(out).toContain('cfg1');
    expect(out).not.toContain('cfgX'); // filtered out (wrong component)
    expect(out).toContain('data-apps'); // dashboard link
  });

  it('returns detail with deployment info and drafts for a python-js prod app', async () => {
    const prodConfig = {
      id: 'prod-cfg',
      name: 'My Prod',
      version: 5,
      configuration: { parameters: { id: 'prod-app' } },
      metadata: [],
    };
    const draftConfig = {
      id: 'draft-cfg',
      name: 'My Draft',
      version: 2,
      configuration: {
        parameters: {
          id: 'draft-app',
          dataApp: { isDraft: true, parentConfigurationId: 'prod-cfg' },
        },
      },
      metadata: [],
    };
    const appResponse = (id: string, cfgId: string) => ({
      id,
      projectId: '42',
      componentId: 'keboola.data-apps',
      branchId: null,
      configId: cfgId,
      configVersion: '5',
      type: 'python-js',
      state: 'running',
      desiredState: 'running',
      url: `https://${id}.run`,
    });

    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/prod-cfg')) return HttpResponse.json(prodConfig);
        if (path.endsWith('/configs/draft-cfg')) return HttpResponse.json(draftConfig);
        if (path.endsWith('/keboola.data-apps/configs'))
          return HttpResponse.json([prodConfig, draftConfig]);
        return undefined;
      }),
      http.get('https://data-science.test/apps/prod-app', () =>
        HttpResponse.json(appResponse('prod-app', 'prod-cfg')),
      ),
      http.get('https://data-science.test/apps/draft-app', () =>
        HttpResponse.json(appResponse('draft-app', 'draft-cfg')),
      ),
      http.get('https://data-science.test/apps/:id/git-repo', () =>
        HttpResponse.json({ httpsUrl: 'https://git.test/repo.git', isManagedGitRepo: true }),
      ),
      http.get('https://data-science.test/apps/:id/logs/tail', () =>
        HttpResponse.text('log line 1\nlog line 2'),
      ),
      http.get('https://data-science.test/apps/:id/runs', () =>
        HttpResponse.json([{ id: 'run1', state: 'running', appId: 'prod-app' }]),
      ),
    );

    const result = await call(await connect(), 'get_data_apps', {
      configuration_ids: ['prod-cfg'],
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(out).toContain('prod-cfg');
    expect(out).toContain('https://git.test/repo.git'); // repo_url on detail path
    expect(out).toContain('draft-cfg'); // inline drafts
    expect(out).toContain('log line 1'); // deployment logs
  });
});

describe('create_python_js_data_app_git_credential', () => {
  it('mints a token and returns an authenticated clone URL', async () => {
    const cfg = {
      id: 'prod-cfg',
      name: 'Prod',
      version: 1,
      configuration: { parameters: { id: 'prod-app' } },
      metadata: [],
    };
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/prod-cfg')) return HttpResponse.json(cfg);
        return undefined;
      }),
      http.get('https://data-science.test/apps/prod-app', () =>
        HttpResponse.json({
          id: 'prod-app',
          projectId: '42',
          componentId: 'keboola.data-apps',
          branchId: null,
          configId: 'prod-cfg',
          configVersion: '1',
          type: 'python-js',
          state: 'stopped',
          desiredState: 'stopped',
        }),
      ),
      http.get('https://data-science.test/apps/prod-app/git-repo', () =>
        HttpResponse.json({ httpsUrl: 'https://git.test/my/repo.git', isManagedGitRepo: true }),
      ),
      http.post('https://data-science.test/apps/prod-app/git-repo/credentials', () =>
        HttpResponse.json({
          id: 'cred1',
          type: 'http_token',
          permissions: 'readWrite',
          secret: 'sup3r/secret',
        }),
      ),
    );

    const result = await call(await connect(), 'create_python_js_data_app_git_credential', {
      configuration_id: 'prod-cfg',
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    // username embedded + secret URL-encoded.
    expect(out).toContain('https://kai:sup3r%2Fsecret@git.test/my/repo.git');
    expect(out).toContain('readWrite');
  });

  it('rejects a streamlit app', async () => {
    const cfg = {
      id: 'st-cfg',
      name: 'St',
      version: 1,
      configuration: { parameters: { id: 'st-app' } },
      metadata: [],
    };
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/st-cfg')) return HttpResponse.json(cfg);
        return undefined;
      }),
      http.get('https://data-science.test/apps/st-app', () =>
        HttpResponse.json({
          id: 'st-app',
          projectId: '42',
          componentId: 'keboola.data-apps',
          branchId: null,
          configId: 'st-cfg',
          configVersion: '1',
          type: 'streamlit',
          state: 'stopped',
          desiredState: 'stopped',
        }),
      ),
    );
    const result = await call(await connect(), 'create_python_js_data_app_git_credential', {
      configuration_id: 'st-cfg',
    });
    expect(result.isError).toBeTruthy();
    expect(text(result)).toContain('only supports python-js');
  });
});

describe('deploy_data_app', () => {
  const streamlitCfg = {
    id: 'st-cfg',
    name: 'St',
    version: 7,
    configuration: { parameters: { id: 'st-app' }, authorization: {} },
    metadata: [],
  };
  const stApp = (state: string) => ({
    id: 'st-app',
    projectId: '42',
    componentId: 'keboola.data-apps',
    branchId: null,
    configId: 'st-cfg',
    configVersion: '7',
    type: 'streamlit',
    state,
    desiredState: 'running',
    url: 'https://st.run',
  });

  it('deploys a streamlit app fetching the latest config version', async () => {
    let patchBody: Record<string, unknown> = {};
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/st-cfg/versions'))
          return HttpResponse.json([{ version: 6 }, { version: 7 }]);
        if (path.endsWith('/configs/st-cfg')) return HttpResponse.json(streamlitCfg);
        return undefined;
      }),
      http.get('https://data-science.test/apps/st-app', () => HttpResponse.json(stApp('running'))),
      http.patch('https://data-science.test/apps/st-app', async ({ request }) => {
        patchBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json(stApp('running'));
      }),
      http.get('https://data-science.test/apps/st-app/logs/tail', () => HttpResponse.text('hi')),
      http.get('https://data-science.test/apps/st-app/runs', () => HttpResponse.json([])),
    );
    const result = await call(await connect(), 'deploy_data_app', {
      action: 'deploy',
      configuration_id: 'st-cfg',
    });
    expect(result.isError).toBeFalsy();
    expect(patchBody.configVersion).toBe('7'); // latest version sent for streamlit
    expect(text(result)).toContain('running');
  });

  it('refuses to deploy an app that is stopping', async () => {
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/st-cfg')) return HttpResponse.json(streamlitCfg);
        return undefined;
      }),
      http.get('https://data-science.test/apps/st-app', () => HttpResponse.json(stApp('stopping'))),
    );
    const result = await call(await connect(), 'deploy_data_app', {
      action: 'deploy',
      configuration_id: 'st-cfg',
    });
    expect(result.isError).toBeTruthy();
    expect(text(result)).toContain('stopping');
  });
});

describe('delete_python_js_data_app_draft', () => {
  it('deletes a draft and surfaces the parent configuration id', async () => {
    const draftCfg = {
      id: 'draft-cfg',
      name: 'Draft',
      version: 1,
      configuration: {
        parameters: {
          id: 'draft-app',
          dataApp: { isDraft: true, parentConfigurationId: 'prod-cfg' },
        },
      },
      metadata: [],
    };
    let deleted = false;
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/draft-cfg')) return HttpResponse.json(draftCfg);
        return undefined;
      }),
      http.get('https://data-science.test/apps/draft-app', () =>
        HttpResponse.json({
          id: 'draft-app',
          projectId: '42',
          componentId: 'keboola.data-apps',
          branchId: null,
          configId: 'draft-cfg',
          configVersion: '1',
          type: 'python-js',
          state: 'stopped',
          desiredState: 'stopped',
        }),
      ),
      http.get('https://data-science.test/apps/draft-app/git-repo', () =>
        HttpResponse.json({ httpsUrl: 'https://git.test/repo.git', isManagedGitRepo: true }),
      ),
      http.delete('https://data-science.test/apps/draft-app', () => {
        deleted = true;
        return new HttpResponse(null, { status: 204 });
      }),
    );
    const result = await call(await connect(), 'delete_python_js_data_app_draft', {
      configuration_id: 'draft-cfg',
    });
    expect(result.isError).toBeFalsy();
    expect(deleted).toBe(true);
    const out = text(result);
    expect(out).toContain('deleted');
    expect(out).toContain('prod-cfg');
  });

  it('refuses to delete a prod (non-draft) app', async () => {
    const prodCfg = {
      id: 'prod-cfg',
      name: 'Prod',
      version: 1,
      configuration: { parameters: { id: 'prod-app' } },
      metadata: [],
    };
    server.use(
      verifyHandler(),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/configs/prod-cfg')) return HttpResponse.json(prodCfg);
        return undefined;
      }),
      http.get('https://data-science.test/apps/prod-app', () =>
        HttpResponse.json({
          id: 'prod-app',
          projectId: '42',
          componentId: 'keboola.data-apps',
          branchId: null,
          configId: 'prod-cfg',
          configVersion: '1',
          type: 'python-js',
          state: 'stopped',
          desiredState: 'stopped',
        }),
      ),
      http.get('https://data-science.test/apps/prod-app/git-repo', () =>
        HttpResponse.json({ httpsUrl: 'https://git.test/repo.git', isManagedGitRepo: true }),
      ),
    );
    const result = await call(await connect(), 'delete_python_js_data_app_draft', {
      configuration_id: 'prod-cfg',
    });
    expect(result.isError).toBeTruthy();
    expect(text(result)).toContain('prod** app');
  });
});

describe('modify_python_js_data_app', () => {
  it('rejects branch on the update path', async () => {
    const result = await call(await connect(), 'modify_python_js_data_app', {
      name: 'X',
      description: 'd',
      configuration_id: 'cfg1',
      branch: 'feature',
    });
    expect(result.isError).toBeTruthy();
    expect(text(result)).toContain('branch is only valid');
  });

  it('requires slug on create', async () => {
    const result = await call(await connect(), 'modify_python_js_data_app', {
      name: 'X',
      description: 'd',
    });
    expect(result.isError).toBeTruthy();
    expect(text(result)).toContain('slug is required');
  });

  it('creates a prod app with a managed repo (feature enabled, no workspace secret lookup)', async () => {
    let createBody: Record<string, unknown> = {};
    server.use(
      verifyHandler(['data-apps-storage-workspace']),
      http.post('https://encryption.test/encrypt', async ({ request }) =>
        HttpResponse.json((await request.json()) as Record<string, unknown>),
      ),
      http.post('https://data-science.test/apps', async ({ request }) => {
        createBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({
          id: 'new-app',
          projectId: '42',
          componentId: 'keboola.data-apps',
          branchId: null,
          configId: 'new-cfg',
          configVersion: '1',
          type: 'python-js',
          state: 'created',
          desiredState: 'stopped',
        });
      }),
      http.get('https://data-science.test/apps/new-app/git-repo', () =>
        HttpResponse.json({ httpsUrl: 'https://git.test/new.git', isManagedGitRepo: true }),
      ),
      http.post('https://connection.test/*', ({ request }) => {
        if (new URL(request.url).pathname.endsWith('/metadata')) return HttpResponse.json([]);
        return undefined;
      }),
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify'))
          return HttpResponse.json({
            owner: { id: '42', features: ['data-apps-storage-workspace'] },
          });
        if (path.endsWith('/keboola.data-apps/configs')) return HttpResponse.json([]);
        return undefined;
      }),
    );

    const result = await call(await connect(), 'modify_python_js_data_app', {
      name: 'My App',
      description: 'desc',
      slug: 'my-app',
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(createBody.useManagedGitRepo).toBe(true);
    expect(createBody.type).toBe('python-js');
    expect(out).toContain('created');
    expect(out).toContain('https://git.test/new.git');
  });
});

describe('modify_streamlit_data_app', () => {
  it('creates a streamlit app, injecting the query function and encrypting the config', async () => {
    let createBody: Record<string, unknown> = {};
    server.use(
      http.get('https://connection.test/*', ({ request }) => {
        const path = new URL(request.url).pathname;
        if (path.endsWith('/tokens/verify')) return HttpResponse.json({ owner: { id: '42' } });
        if (path.endsWith('/dev-branches'))
          return HttpResponse.json([{ id: 'main-branch', isDefault: true }]);
        if (path.endsWith('/workspaces'))
          return HttpResponse.json([
            { id: 'ws-1', connection: { schema: 'WS_SCHEMA', backend: 'snowflake' } },
          ]);
        if (path.endsWith('/keboola.data-apps/configs')) return HttpResponse.json([]);
        return undefined;
      }),
      http.post('https://connection.test/*', ({ request }) => {
        if (new URL(request.url).pathname.endsWith('/metadata')) return HttpResponse.json([]);
        return undefined;
      }),
      http.post('https://encryption.test/encrypt', async ({ request }) =>
        HttpResponse.json((await request.json()) as Record<string, unknown>),
      ),
      http.post('https://data-science.test/apps', async ({ request }) => {
        createBody = (await request.json()) as Record<string, unknown>;
        return HttpResponse.json({
          id: 'st-app',
          projectId: '42',
          componentId: 'keboola.data-apps',
          branchId: null,
          configId: 'st-cfg',
          configVersion: '1',
          type: 'streamlit',
          state: 'created',
          desiredState: 'stopped',
          url: 'https://st.run',
        });
      }),
    );

    const result = await call(await connect(), 'modify_streamlit_data_app', {
      name: 'Dashboard App',
      description: 'desc',
      source_code: 'import streamlit as st\n{QUERY_DATA_FUNCTION}\nst.write("hi")',
      packages: ['plotly'],
      authentication_type: 'default',
    });
    expect(result.isError).toBeFalsy();
    const out = text(result);
    expect(out).toContain('created');
    // injected query_data function replaced the placeholder.
    const script = (
      (createBody.config as Record<string, unknown>).parameters as Record<string, unknown>
    ).script as string[];
    expect(script[0]).toContain('def query_data');
    expect(script[0]).not.toContain('{QUERY_DATA_FUNCTION}');
  });
});
