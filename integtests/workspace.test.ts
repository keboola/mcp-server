import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from './helpers/mcp';
import { getTestProjectForTest } from './testproject/fixture';

// Ported from integtests/test_workspace.py.
//
// The Python tests poke WorkspaceManager internals directly: test_static_workspace
// resolves a workspace by its configured schema and reads its backend; test_dynamic_workspace
// creates a workspace on demand and confirms it is recorded in the branch. The TS port has
// no configured workspace_schema in the pooled Config — the WorkspaceManager always resolves
// (or provisions) the read-only SQL workspace on first use. We therefore assert the
// observable end-to-end behavior the manager is responsible for, through the tools that use
// it: the workspace is provisioned, its backend is resolved, and SQL actually runs over it.

describe('workspace provisioning + SQL execution (integration)', () => {
  it('resolves the workspace backend dialect via get_project_info', async () => {
    // Port of test_static_workspace's `info.backend in ['snowflake', 'bigquery']`:
    // get_project_info surfaces the sql_dialect, which the WorkspaceManager derives from
    // the resolved workspace's backend.
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'get_project_info');
      expect(text).toMatch(/Snowflake|BigQuery/i);
    } finally {
      await session.close();
    }
  });

  it('provisions a workspace and runs a SELECT through it', async () => {
    // Port of test_dynamic_workspace: with no preconfigured workspace schema, the first
    // query_data call must provision/resolve the workspace and execute the SQL over it.
    // A literal SELECT proves the workspace is live and queryable end-to-end.
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'query_data', {
        sql_query: 'SELECT 1 AS one',
        query_name: 'Workspace Smoke Query',
      });
      expect(text).toContain('Workspace Smoke Query');
      expect(text).toMatch(/\b1\b/);
    } finally {
      await session.close();
    }
  });
});
