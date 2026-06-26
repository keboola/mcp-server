import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';

// Ported from tools/doc.py.

export const registerDocTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'docs_query',
    title: 'Query documentation',
    description: 'Answers a question using the Keboola documentation as a source.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      query: z.string().describe('Natural language query to search for in the documentation.'),
    },
    handler: async ({ query }) => {
      const { rawAi } = createKeboolaClients(config);
      const answer = await rawAi.post<{ text: string; sourceUrls?: string[] }>('docs/question', {
        body: { query },
        headers: { Accept: 'application/json' },
      });
      return { text: answer.text, source_urls: answer.sourceUrls ?? [] };
    },
  });
};
