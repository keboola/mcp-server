import { describe, expect, it } from 'vitest';

import { callToolRaw, callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_sql.py. The seeded-table COUNT(*) case lives with the
// storage seeding suite; here we cover the workspace-only paths (a literal SELECT + the
// invalid-query error) which need no project data.
describe('query_data (integration)', () => {
  it('runs a literal query and returns CSV data', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'query_data', {
        sql_query: 'SELECT 1 AS one',
        query_name: 'Smoke Query',
      });
      expect(text).toContain('Smoke Query');
      expect(text).toMatch(/\b1\b/);
    } finally {
      await session.close();
    }
  });

  it('reports an error for invalid SQL', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const result = await callToolRaw(session.client, 'query_data', {
        sql_query: 'INVALID SQL SYNTAX SELECT * FROM',
        query_name: 'Invalid Query Test',
      });
      expect(result.isError).toBeTruthy();
      expect((result.content as { text: string }[])[0]!.text).toMatch(/Failed to run SQL query/i);
    } finally {
      await session.close();
    }
  });
});
