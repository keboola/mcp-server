import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from './helpers/mcp';
import { getTestProjectForTest } from './testproject/fixture';

// Ported from integtests/test_mcp_server.py.
//
// The Python suite spins up the server over stdio / streamable-http subprocesses and
// asserts: the expected tool set is present (tools/list), 6 prompts (prompts/list),
// 0 resources (resources/list), and a get_configs round-trip returns the seeded
// component config. Here we connect the in-process MCP server (built from a leased
// project's Config) over the in-memory transport and make the same assertions through
// the MCP client. The transport-matrix / multi-client / different-header cases are
// stdio/HTTP-subprocess concerns that do not apply to the in-memory harness, so they
// are not ported.

// The tool set the server must expose. Tools gated on project features (conditional /
// classic flows, search, semantic) are excluded from the strict comparison because
// whether they appear depends on the leased project's enabled features — mirroring the
// Python `exclude` set.
const EXCLUDE = new Set([
  'create_conditional_flow',
  'create_flow',
  'search',
  'update_flow',
  'modify_flow',
  'get_semantic_context',
  'get_semantic_schema',
  'search_semantic_context',
  'validate_semantic_query',
]);

const EXPECTED_TOOLS = new Set(
  [
    'add_config_row',
    'create_conditional_flow',
    'create_config',
    'create_flow',
    'create_oauth_url',
    'create_python_js_data_app_git_credential',
    'create_sql_transformation',
    'delete_python_js_data_app_draft',
    'deploy_data_app',
    'docs_query',
    'find_component_id',
    'get_buckets',
    'get_components',
    'get_config_examples',
    'get_configs',
    'get_data_apps',
    'get_flow_examples',
    'get_flow_schema',
    'get_flows',
    'get_jobs',
    'get_project_info',
    'get_tables',
    'modify_flow',
    'modify_python_js_data_app',
    'modify_streamlit_data_app',
    'query_data',
    'run_job',
    'run_sync_action',
    'search',
    'update_config',
    'update_config_row',
    'update_descriptions',
    'update_flow',
    'update_project_description',
    'update_sql_transformation',
    // The TS port carries a `get_server_info` scaffold tool that has no Python equivalent;
    // it is expected to be present until the scaffold is removed.
    'get_server_info',
  ].filter((name) => !EXCLUDE.has(name)),
);

describe('MCP server wiring (integration)', () => {
  it('exposes the expected tool set, 6 prompts, and no resources', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const { tools } = await session.client.listTools();
      const actual = new Set(tools.map((t) => t.name).filter((name) => !EXCLUDE.has(name)));

      const missing = [...EXPECTED_TOOLS].filter((name) => !actual.has(name));
      expect(missing, `Missing tools: ${missing.join(', ')}`).toEqual([]);

      const unexpected = [...actual].filter((name) => !EXPECTED_TOOLS.has(name));
      expect(unexpected, `Unexpected new tools: ${unexpected.join(', ')}`).toEqual([]);

      const { prompts } = await session.client.listPrompts();
      expect(prompts.length).toBe(6);

      // No resources are exposed. The SDK only registers a `resources/list` handler when
      // at least one resource is registered, so with none the call raises "Method not
      // found" (-32601) — which is itself proof the server exposes zero resources, the
      // same fact the Python `len(resources) == 0` assertion checked.
      const resourceCount = await session.client
        .listResources()
        .then((r) => r.resources.length)
        .catch((err: { code?: number }) => {
          if (err?.code === -32601) return 0;
          throw err;
        });
      expect(resourceCount).toBe(0);
    } finally {
      await session.close();
    }
  });

  it('round-trips a component configuration through create_config + get_configs', async () => {
    // Port of _assert_get_component_details_tool_call: the Python test seeds a component
    // config and fetches its detail through get_configs. We create the config through the
    // tool layer instead (no dependency on the clean+seed wipe path, which is gated on a
    // dedicated project), then fetch it back — the same create→read round-trip through the
    // in-process MCP server.
    const COMPONENT_ID = 'ex-generic-v2';
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const created = await callToolText(session.client, 'create_config', {
        name: `integ_roundtrip_${Date.now()}`,
        description: 'Created by the mcp_server integration round-trip test.',
        component_id: COMPONENT_ID,
        parameters: { api: { baseUrl: 'https://example.com' } },
      });
      // The create output carries the new configuration_id.
      const configurationId = created.match(/configuration_id:\s*"?([^\s"]+)"?/)?.[1];
      expect(configurationId, `create_config output had no configuration_id:\n${created}`).toBeTruthy();

      const detail = await callToolText(session.client, 'get_configs', {
        configs: [{ component_id: COMPONENT_ID, configuration_id: configurationId }],
      });

      // The detail output names the requested component + configuration. (Python validated
      // the GetConfigsDetailOutput model fields; we assert on the TOON text shape.)
      expect(detail).toContain(COMPONENT_ID);
      expect(detail).toContain(configurationId!);
      // Component metadata (type/name) is resolved and present.
      expect(detail).toMatch(/component_type|component_name/);
    } finally {
      await session.close();
    }
  });
});
