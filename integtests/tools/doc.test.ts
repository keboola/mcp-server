import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/test_doc.py.
//
// docs_query is served by the pgvector docs-search index (RFC:
// feature_spec/docs-search-pgvector/). The index is process-level infrastructure — when
// DATABASE_URL (+ embedder creds) is not configured the tool is gated off, so this suite
// is skipped rather than failing. It runs (and asserts real retrieval) only where a
// seeded docs index is reachable.
const DOCS_INDEX_CONFIGURED = Boolean(
  (process.env.DATABASE_URL ?? '').trim() &&
    (process.env.DOCS_EMBEDDER_ENDPOINT ?? '').trim() &&
    (process.env.DOCS_EMBEDDER_API_KEY ?? '').trim() &&
    (process.env.DOCS_EMBEDDER_MODEL ?? '').trim(),
);

const describeDocs = DOCS_INDEX_CONFIGURED ? describe : describe.skip;
if (!DOCS_INDEX_CONFIGURED) {
  console.warn('SKIP: docs_query integration — no docs-search index configured (DATABASE_URL/DOCS_EMBEDDER_*).');
}

describeDocs('docs_query (integration)', () => {
  it('returns an answer with text and source URLs', async () => {
    const { config } = await getTestProjectForTest({ clean: false });
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
