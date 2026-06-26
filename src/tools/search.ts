import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';

// Ported from tools/search.py (find_component_id; the global `search` tool follows later).

type SuggestedComponent = { componentId?: string; component_id?: string; score?: number };

export const registerSearchTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'find_component_id',
    title: 'Find component id',
    description: 'Returns a list of component IDs that match the given natural-language query.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      query: z.string().describe('Natural language query to find the requested component.'),
    },
    handler: async ({ query }) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const response = await clients.rawAi.post<{ components?: SuggestedComponent[] }>(
        'suggest/component',
        {
          body: { prompt: query },
          headers: { Accept: 'application/json' },
        },
      );

      return (response.components ?? []).map((component) => {
        const componentId = component.componentId ?? component.component_id ?? '';
        return {
          component_id: componentId,
          score: component.score ?? 0,
          links: [linksManager.getConfigDashboardLink(componentId, undefined)],
        };
      });
    },
  });
};
