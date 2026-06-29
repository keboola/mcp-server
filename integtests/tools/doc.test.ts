import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_doc.py.
describe('docs_query (integration)', () => {
  it('returns an answer with text and source URLs', async () => {
    const { config } = await getTestProjectForTest();
    const session = await connectMcp(config);
    try {
      const text = await callToolText(session.client, 'docs_query', {
        query: 'What is Keboola Connection?',
      });
      expect(text.length).toBeGreaterThan(0);
      // The answer carries source_urls — at least one http(s) link.
      expect(text).toMatch(/https?:\/\//);
    } finally {
      await session.close();
    }
  });
});
