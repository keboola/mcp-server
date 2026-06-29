import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_search.py (find_component_id only — needs no project
// data). Verifies the typed ai.suggestComponent migration against the real AI service.
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
