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

// ---------------------------------------------------------------------------
// get_project_info
// ---------------------------------------------------------------------------

type ProjectInfoHandlers = {
  verifyToken?: Record<string, unknown>;
  devBranches?: unknown[];
  branchMetadata?: { key: string; value: string }[];
  workspaces?: unknown[];
};

/** Registers GET handlers for the four storage endpoints get_project_info reads. */
const useProjectInfoHandlers = (opts: ProjectInfoHandlers = {}) => {
  const {
    verifyToken = {
      owner: { id: '42', name: 'My Project', features: [] },
      organization: { id: '7' },
      admin: { role: 'admin' },
    },
    devBranches = [{ id: 123, name: 'Main', isDefault: true }],
    branchMetadata = [
      { key: 'KBC.projectDescription', value: 'Some description' },
      { key: 'KBC.McpServer.v2.workspaceId', value: '999' },
    ],
    workspaces = [
      {
        id: 999,
        connection: { backend: 'snowflake', schema: 'WORKSPACE_SCHEMA' },
        readOnlyStorageAccess: true,
      },
    ],
  } = opts;

  server.use(
    http.get('https://connection.test/v2/storage/tokens/verify', () =>
      HttpResponse.json(verifyToken),
    ),
    http.get('https://connection.test/v2/storage/dev-branches', () =>
      HttpResponse.json(devBranches),
    ),
    http.get('https://connection.test/v2/storage/branch/:branchId/metadata', () =>
      HttpResponse.json(branchMetadata),
    ),
    http.get('https://connection.test/v2/storage/branch/:branchId/workspaces', () =>
      HttpResponse.json(workspaces),
    ),
  );
};

const callProjectInfo = async (cfg: Config) => {
  const client = await connect(cfg);
  const result = await client.callTool({ name: 'get_project_info', arguments: {} });
  await client.close();
  const content = (result.content as { type: string; text: string }[])[0]!.text;
  return { result, text: content };
};

describe('get_project_info', () => {
  it('returns unified project info on the default branch', async () => {
    useProjectInfoHandlers();
    const { result, text } = await callProjectInfo(config);

    expect(result.isError).toBeFalsy();
    expect(text).toContain('project_id: "42"');
    expect(text).toContain('My Project');
    expect(text).toContain('Some description');
    expect(text).toContain('organization_id: "7"');
    expect(text).toContain('sql_dialect: Snowflake');
    expect(text).toContain('workspace_id: 999');
    expect(text).toContain('user_role: admin');
    // Default branch resolution.
    expect(text).toContain('branch_id: 123');
    expect(text).toContain('branch_name: Main');
    expect(text).toContain('is_development_branch: false');
    // Conditional flows enabled when the feature flag is absent.
    expect(text).toContain('conditional_flows: true');
    // The base system prompt (and the Snowflake dialect section) is embedded.
    expect(text).toContain('### SQL Identifiers');
    expect(text).toContain('Snowflake');
    expect(text).toContain('Finding Items');
    // admin role => no toolset restrictions (null dropped from compact TOON output).
    expect(text).not.toContain('toolset_restrictions');
  });

  it('resolves the configured development branch and surfaces role restrictions', async () => {
    useProjectInfoHandlers({
      verifyToken: {
        owner: { id: '42', name: 'My Project', features: ['hide-conditional-flows'] },
        organization: { id: '7' },
        admin: { role: 'readonly' },
      },
      devBranches: [
        { id: 123, name: 'Main', isDefault: true },
        { id: 456, name: 'feature-x', isDefault: false },
      ],
    });

    const { text } = await callProjectInfo(config.replaceBy({ branchId: '456' }));

    expect(text).toContain('branch_id: 456');
    expect(text).toContain('branch_name: feature-x');
    expect(text).toContain('is_development_branch: true');
    // hide-conditional-flows feature present => conditional flows disabled.
    expect(text).toContain('conditional_flows: false');
    expect(text).toContain('user_role: readonly');
    expect(text).toContain('read-only tools are available');
  });

  it('selects a BigQuery workspace by the configured workspace schema', async () => {
    useProjectInfoHandlers({
      workspaces: [
        {
          id: 111,
          connection: { backend: 'snowflake', schema: 'OTHER' },
          readOnlyStorageAccess: true,
        },
        {
          id: 222,
          connection: { backend: 'bigquery', schema: 'TARGET_SCHEMA' },
          readOnlyStorageAccess: true,
        },
      ],
    });

    const { text } = await callProjectInfo(config.replaceBy({ workspaceSchema: 'TARGET_SCHEMA' }));

    expect(text).toContain('sql_dialect: BigQuery');
    expect(text).toContain('workspace_id: 222');
    // BigQuery dialect section uses backtick FQNs.
    expect(text).toContain('`project`.`dataset`.`table`');
  });

  it('errors when no workspace can be resolved', async () => {
    useProjectInfoHandlers({ branchMetadata: [], workspaces: [] });
    const { result, text } = await callProjectInfo(config);
    expect(result.isError).toBe(true);
    expect(text).toContain('Failed to initialize Keboola Workspace.');
  });
});
