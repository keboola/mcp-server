import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { http, HttpResponse } from 'msw';
import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { Config } from '@/config';
import {
  authorizeToolCall,
  filterToolsList,
  type GatedTool,
  type GatingContext,
  getProjectFeatures,
  getTokenRole,
} from '@/mcp/filtering';
import { createServer } from '@/server';

// --- Pure gating functions (port of tests/test_mcp.py TestToolsFilteringMiddleware) ---

const tool = (name: string, readOnly = false): GatedTool => ({ name, readOnly });

const ctx = (over: Partial<GatingContext> = {}): GatingContext => ({
  tokenRole: '',
  features: new Set(),
  isOauth: false,
  isMainBranch: true,
  docsIndexAvailable: true,
  ...over,
});

describe('getProjectFeatures / getTokenRole', () => {
  it('reads features and role from token info', () => {
    const info = { owner: { features: ['a', '', 'b'] }, admin: { role: 'admin' } };
    expect(getProjectFeatures(info)).toEqual(new Set(['a', 'b']));
    expect(getTokenRole(info)).toBe('admin');
  });

  it('defaults to empty when missing', () => {
    expect(getProjectFeatures({})).toEqual(new Set());
    expect(getTokenRole({})).toBe('');
  });
});

describe('filterToolsList — data app tools by branch', () => {
  const dataAppTools = [
    'modify_streamlit_data_app',
    'get_data_apps',
    'deploy_data_app',
    'delete_python_js_data_app_draft',
  ];
  const tools = [...dataAppTools.map((n) => tool(n)), tool('other_tool')];

  it.each<[boolean, boolean]>([
    [false, true], // non-main branch -> filtered out
    [true, false], // main branch -> kept
  ])('isMainBranch=%s filters=%s', (isMainBranch, expectFiltered) => {
    const names = new Set(filterToolsList(tools, ctx({ isMainBranch })).map((t) => t.name));
    for (const n of dataAppTools) {
      expect(names.has(n)).toBe(!expectFiltered);
    }
    expect(names.has('other_tool')).toBe(true);
  });
});

describe('filterToolsList — flow tools by role / oauth', () => {
  const tools = [
    tool('modify_flow'),
    tool('update_flow'),
    tool('other_tool'),
    tool('read_only_tool', true),
  ];

  it.each<[string, boolean, string[], string[]]>([
    ['admin', false, ['update_flow'], ['modify_flow', 'read_only_tool']],
    ['share', false, ['update_flow'], ['modify_flow', 'read_only_tool']],
    ['', false, ['modify_flow'], ['update_flow', 'read_only_tool']],
    ['readOnly', false, ['modify_flow', 'update_flow'], ['read_only_tool']],
    ['guest', false, ['modify_flow'], ['update_flow', 'read_only_tool']],
    // OAuth regular user gets modify_flow (different from SAPI regular).
    ['', true, ['update_flow'], ['modify_flow', 'read_only_tool']],
  ])('role=%j oauth=%s', (tokenRole, isOauth, hidden, visible) => {
    const names = new Set(filterToolsList(tools, ctx({ tokenRole, isOauth })).map((t) => t.name));
    for (const n of hidden) expect(names.has(n)).toBe(false);
    for (const n of visible) expect(names.has(n)).toBe(true);
  });
});

describe('filterToolsList — semantic tools by feature', () => {
  const tools = [
    tool('search_semantic_context'),
    tool('get_semantic_context'),
    tool('get_semantic_schema'),
    tool('validate_semantic_query'),
    tool('other_tool'),
  ];

  it.each<[string[], string, boolean]>([
    [[], 'search_semantic_context', true],
    [[], 'get_semantic_schema', true],
    [['mcp-semantic-tooling'], 'search_semantic_context', false],
    [['mcp-semantic-tooling'], 'get_semantic_schema', false],
    [['other-feature'], 'search_semantic_context', true],
  ])('features=%j tool=%s filtered=%s', (features, name, expectFiltered) => {
    const names = new Set(
      filterToolsList(tools, ctx({ features: new Set(features) })).map((t) => t.name),
    );
    expect(names.has(name)).toBe(!expectFiltered);
    expect(names.has('other_tool')).toBe(true);
  });
});

describe('filterToolsList — conditional vs legacy flow', () => {
  const tools = [tool('create_flow'), tool('create_conditional_flow'), tool('other_tool')];

  it('hides create_conditional_flow when hide-conditional-flows feature is on', () => {
    const names = new Set(
      filterToolsList(tools, ctx({ features: new Set(['hide-conditional-flows']) })).map(
        (t) => t.name,
      ),
    );
    expect(names.has('create_flow')).toBe(true);
    expect(names.has('create_conditional_flow')).toBe(false);
  });

  it('hides create_flow when the feature is off', () => {
    const names = new Set(filterToolsList(tools, ctx()).map((t) => t.name));
    expect(names.has('create_flow')).toBe(false);
    expect(names.has('create_conditional_flow')).toBe(true);
  });
});

describe('filterToolsList / authorizeToolCall — docs tools by index availability', () => {
  const docsTools = [tool('docs_query', true), tool('find_component_id', true), tool('get_jobs')];

  it('keeps docs tools when the index is available, drops them when not', () => {
    const withIndex = filterToolsList(docsTools, ctx({ docsIndexAvailable: true })).map(
      (t) => t.name,
    );
    expect(withIndex).toContain('docs_query');
    expect(withIndex).toContain('find_component_id');

    const withoutIndex = filterToolsList(docsTools, ctx({ docsIndexAvailable: false })).map(
      (t) => t.name,
    );
    expect(withoutIndex).not.toContain('docs_query');
    expect(withoutIndex).not.toContain('find_component_id');
    expect(withoutIndex).toContain('get_jobs'); // non-docs tools unaffected
  });

  it('denies a docs tool call when the index is unavailable', () => {
    expect(
      authorizeToolCall({
        toolName: 'docs_query',
        isReadOnly: true,
        isSemantic: false,
        tokenRole: 'admin',
        features: new Set(),
        isOauth: false,
        isMainBranch: true,
        docsIndexAvailable: false,
      }),
    ).toContain('documentation index is not');
  });
});

describe('authorizeToolCall — flow tools by role / oauth', () => {
  it.each<[string, boolean, string, boolean, boolean]>([
    ['admin', false, 'modify_flow', false, false],
    ['admin', false, 'update_flow', false, true],
    ['share', false, 'modify_flow', false, false],
    ['share', false, 'update_flow', false, true],
    ['', false, 'modify_flow', false, true],
    ['', false, 'update_flow', false, false],
    ['guest', false, 'write_tool', false, false],
    ['guest', false, 'read_only_tool', true, false],
    ['readOnly', false, 'write_tool', false, true],
    ['readOnly', false, 'read_only_tool', true, false],
    ['', true, 'modify_flow', false, false],
    ['', true, 'update_flow', false, true],
  ])('role=%j oauth=%s tool=%s', (tokenRole, isOauth, toolName, isReadOnly, expectDenied) => {
    const denial = authorizeToolCall({
      toolName,
      isReadOnly,
      isSemantic: false,
      tokenRole,
      features: new Set(),
      isOauth,
      isMainBranch: true,
      docsIndexAvailable: true,
    });
    expect(denial !== null).toBe(expectDenied);
  });
});

describe('authorizeToolCall — data apps by branch', () => {
  it.each<[boolean, boolean]>([
    [false, true],
    [true, false],
  ])('isMainBranch=%s denied=%s', (isMainBranch, expectDenied) => {
    const denial = authorizeToolCall({
      toolName: 'modify_streamlit_data_app',
      isReadOnly: false,
      isSemantic: false,
      tokenRole: 'admin',
      features: new Set(),
      isOauth: false,
      isMainBranch,
      docsIndexAvailable: true,
    });
    if (expectDenied) {
      expect(denial).toContain('main production branch');
    } else {
      expect(denial).toBeNull();
    }
  });
});

describe('authorizeToolCall — semantic tools by feature', () => {
  it.each<[string[], string, boolean]>([
    [[], 'search_semantic_context', true],
    [[], 'get_semantic_schema', true],
    [['mcp-semantic-tooling'], 'search_semantic_context', false],
    [['mcp-semantic-tooling'], 'get_semantic_schema', false],
    [[], 'other_tool', false],
  ])('features=%j tool=%s denied=%s', (features, toolName, expectDenied) => {
    const isSemantic = ['search_semantic_context', 'get_semantic_schema'].includes(toolName);
    const denial = authorizeToolCall({
      toolName,
      isReadOnly: true,
      isSemantic,
      tokenRole: 'admin',
      features: new Set(features),
      isOauth: false,
      isMainBranch: true,
      docsIndexAvailable: true,
    });
    if (expectDenied) {
      expect(denial).toContain('Semantic Layer Tooling');
    } else {
      expect(denial).toBeNull();
    }
  });
});

// --- Integration: gating wired into createServer (tools/list + tools/call) ---

const msw = setupServer();
beforeAll(() => msw.listen({ onUnhandledRequest: 'error' }));
afterEach(() => msw.resetHandlers());
afterAll(() => msw.close());

const verify = (body: unknown) =>
  http.get('https://connection.test/*', ({ request }) => {
    if (new URL(request.url).pathname.endsWith('/tokens/verify')) {
      return HttpResponse.json(body as object);
    }
    return undefined;
  });

const connect = async (config: Config) => {
  const [clientT, serverT] = InMemoryTransport.createLinkedPair();
  await createServer(config).connect(serverT);
  const client = new Client({ name: 'test', version: '0.0.0' });
  await client.connect(clientT);
  return client;
};

const baseConfig = (over: Record<string, string> = {}) =>
  new Config({ storageApiUrl: 'https://connection.test', storageToken: 'tok', ...over });

describe('createServer gating integration', () => {
  it('hides create_flow for a no-feature project and shows create_conditional_flow', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig());
    const names = (await client.listTools()).tools.map((t) => t.name);
    expect(names).toContain('create_conditional_flow');
    expect(names).not.toContain('create_flow');
    await client.close();
  });

  it('shows only modify_flow (not update_flow) for an admin token', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig());
    const names = (await client.listTools()).tools.map((t) => t.name);
    expect(names).toContain('modify_flow');
    expect(names).not.toContain('update_flow');
    await client.close();
  });

  it('shows update_flow (not modify_flow) for a guest token', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'guest' } }));
    const client = await connect(baseConfig());
    const names = (await client.listTools()).tools.map((t) => t.name);
    expect(names).toContain('update_flow');
    expect(names).not.toContain('modify_flow');
    await client.close();
  });

  it('keeps data app tools in tools/list even on a non-main branch (discovery forces main)', async () => {
    // Parity with Python: tools/list always discovers against the main branch, so data
    // app tools stay visible during discovery and are only blocked at call time.
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig({ branchId: '123' }));
    const names = (await client.listTools()).tools.map((t) => t.name);
    expect(names).toContain('get_data_apps');
    expect(names).toContain('deploy_data_app');
    await client.close();
  });

  it('blocks a data app tools/call on a non-main branch', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig({ branchId: '123' }));
    await expect(client.callTool({ name: 'get_data_apps', arguments: {} })).rejects.toThrow(
      /main production branch/,
    );
    await client.close();
  });

  it('restricts list to read-only tools for a readonly role', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'readonly' } }));
    const client = await connect(baseConfig());
    const tools = (await client.listTools()).tools;
    expect(tools.length).toBeGreaterThan(0);
    expect(tools.every((t) => t.annotations?.readOnlyHint === true)).toBe(true);
    await client.close();
  });

  it('hides semantic tools without the semantic feature, shows them with it', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    let client = await connect(baseConfig());
    let names = (await client.listTools()).tools.map((t) => t.name);
    expect(names).not.toContain('search_semantic_context');
    await client.close();

    msw.use(verify({ owner: { features: ['mcp-semantic-tooling'] }, admin: { role: 'admin' } }));
    client = await connect(baseConfig());
    names = (await client.listTools()).tools.map((t) => t.name);
    expect(names).toContain('search_semantic_context');
    await client.close();
  });

  it('blocks a tools/call that the project gating denies (update_flow for admin)', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig());
    await expect(client.callTool({ name: 'update_flow', arguments: {} })).rejects.toThrow(
      /update_flow/,
    );
    await client.close();
  });

  it('blocks a tools/call denied by X-Disallowed-Tools header authorization', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig({ disallowedTools: 'get_jobs' }));
    await expect(client.callTool({ name: 'get_jobs', arguments: { job_ids: [] } })).rejects.toThrow(
      /not authorized/,
    );
    await client.close();
  });

  it('hides tools excluded by X-Allowed-Tools header authorization in tools/list', async () => {
    msw.use(verify({ owner: { features: [] }, admin: { role: 'admin' } }));
    const client = await connect(baseConfig({ allowedTools: 'get_jobs,get_buckets' }));
    const names = (await client.listTools()).tools.map((t) => t.name);
    expect(names.sort()).toEqual(['get_buckets', 'get_jobs']);
    await client.close();
  });
});
