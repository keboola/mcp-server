import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_storage.py (get_buckets + get_tables). Each test leases a
// fresh project, resets it, seeds the standard fixtures, then exercises the tools.
describe('storage tools (integration)', () => {
  it('get_buckets lists the seeded buckets', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_buckets');
      expect(text).toContain('in.c-test_bucket_01');
      expect(text).toContain('in.c-test_bucket_02');
    } finally {
      await session.close();
    }
  });

  it('get_tables lists a bucket and returns table detail with an FQN', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const listed = await callToolText(session.client, 'get_tables', {
        bucket_ids: ['in.c-test_bucket_01'],
      });
      expect(listed).toContain('in.c-test_bucket_01.test_table_01');

      const detail = await callToolText(session.client, 'get_tables', {
        table_ids: ['in.c-test_bucket_01.test_table_01'],
      });
      // Detail includes the fully-qualified name + the seeded columns.
      expect(detail).toMatch(/fully_qualified_name|fullyQualifiedName/);
      expect(detail).toContain('item_count');
    } finally {
      await session.close();
    }
  });
});
