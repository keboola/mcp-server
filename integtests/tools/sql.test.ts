import { describe, expect, it } from 'vitest';

import { callToolRaw, callToolText, connectMcp } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_sql.py: a literal SELECT, the invalid-query error path,
// and the faithful seeded COUNT(*) case (get_buckets -> get_tables -> query_data against the
// table's fully-qualified name).
describe('query_data (integration)', () => {
  it('counts rows of a seeded table via its fully-qualified name', async () => {
    // The fully-qualified-name resolver is Snowflake-only (port of _SnowflakeWorkspace), so
    // pin to a Snowflake project where get_tables reliably exposes the FQN.
    const project = await getTestProjectForTest({ backend: 'snowflake' });
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      // Resolve the seeded table's FQN: list a bucket, then fetch table detail.
      const tablesListing = await callToolText(session.client, 'get_tables', {
        bucket_ids: ['in.c-test_bucket_01'],
      });
      expect(tablesListing).toContain('in.c-test_bucket_01.test_table_01');

      const detail = await callToolText(session.client, 'get_tables', {
        table_ids: ['in.c-test_bucket_01.test_table_01'],
      });
      // Pull the fully-qualified name out of the detail. The Snowflake FQN ("DB"."SCHEMA"."TBL")
      // contains quotes/dots, so TOON emits it as a double-quoted (JSON-escaped) scalar. Capture
      // the whole value on the line and JSON-decode it when quoted — a naive non-greedy match
      // would truncate at the first inner quote and produce invalid SQL.
      const fqnMatch = detail.match(/fullyQualifiedName:\s*(.+?)\s*$/m);
      expect(fqnMatch, `Table detail should expose a fullyQualifiedName. Got: ${detail}`).not.toBeNull();
      let fqn = fqnMatch![1]!.trim();
      if (fqn.startsWith('"') && fqn.endsWith('"')) fqn = JSON.parse(fqn) as string;
      expect(fqn.length).toBeGreaterThan(0);

      const text = await callToolText(session.client, 'query_data', {
        sql_query: `SELECT COUNT(*) as row_count FROM ${fqn}`,
        query_name: 'Row Count Query',
      });
      expect(text).toContain('Row Count Query');
      // CSV must have a header (ROW_COUNT on Snowflake / row_count on BigQuery) plus a numeric row.
      expect(text).toMatch(/row_count/i);
      expect(text).toMatch(/\b\d+\b/);
    } finally {
      await session.close();
    }
  });


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
