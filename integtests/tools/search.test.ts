import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_search.py.
describe('find_component_id (integration)', () => {
  it('suggests relevant component ids for a query', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'find_component_id', {
        query: 'generic extractor - extract data from many APIs',
      });
      // The generic extractor should be among the suggestions.
      expect(text).toContain('ex-generic-v2');
    } finally {
      await session.close();
    }
  });
});

describe('search (integration)', () => {
  it('finds seeded buckets, tables and configs end-to-end', async () => {
    const project = await getTestProjectForTest();
    const seeded = await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      // Unscoped textual search across all item types for the 'test' name prefix.
      const text = await callToolText(session.client, 'search', {
        patterns: ['test'],
        limit: 50,
        offset: 0,
      });

      // The seeded buckets and table appear by id.
      for (const bucket of seeded.buckets) expect(text).toContain(bucket.id);
      for (const table of seeded.tables) expect(text).toContain(table.id);
      // Both seeded configurations appear by id (ex-generic-v2 + snowflake-transformation).
      for (const config of seeded.configs) expect(text).toContain(config.configurationId);
    } finally {
      await session.close();
    }
  });

  it('config-based scoped search matches the ex-generic-v2 config by api.baseUrl', async () => {
    const project = await getTestProjectForTest();
    const seeded = await seedProject(project);
    const config = seeded.configs.find((c) => c.componentId === 'ex-generic-v2')!;
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'search', {
        patterns: ['wttr.in'],
        item_types: ['configuration'],
        search_type: 'config-based',
        scopes: ['parameters.api.baseUrl'],
        limit: 20,
        offset: 0,
      });
      expect(text).toContain('ex-generic-v2');
      expect(text).toContain(config.configurationId);
    } finally {
      await session.close();
    }
  });
});
