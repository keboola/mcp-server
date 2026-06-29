import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_project.py.
describe('get_project_info (integration)', () => {
  it('returns project id, sql dialect, links, and branch context', async () => {
    const { config, projectId } = await getTestProjectForTest();
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'get_project_info');

      // project_id of the leased project.
      expect(text).toContain(String(projectId));
      // sql_dialect is one of the two supported backends.
      expect(text).toMatch(/Snowflake|BigQuery/);
      // links list is present (ui-detail / ui-dashboard / docs).
      expect(text).toMatch(/ui-detail|ui-dashboard|docs/);
      // The pool runs on the default (production) branch.
      expect(text).toMatch(/is_development_branch[^\n]*false/i);
    } finally {
      await session.close();
    }
  });
});
