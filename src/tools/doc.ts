import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { getDocsSearch } from '@/clients/docsSearch';
import { registerTool } from '@/mcp/tool';

// Ported from tools/doc.py. Backed by the pgvector docs-search index (RFC:
// feature_spec/docs-search-pgvector/) instead of the legacy AI docs service. The docs
// index is process-level infrastructure (no per-request Config needed).

export const registerDocTools = (server: McpServer): void => {
  registerTool(server, {
    name: 'docs_query',
    title: 'Query documentation',
    description: 'Answers a question using the Keboola documentation as a source.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      query: z.string().describe('Natural language query to search for in the documentation.'),
    },
    handler: async ({ query }) => {
      const docs = getDocsSearch();
      if (!docs) throw new Error('The documentation index is not available.');
      const answer = await docs.answerQuestion(query);
      return { text: answer.text, source_urls: answer.sourceUrls };
    },
  });
};
