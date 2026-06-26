import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';

// Ported from tools/oauth.py.

export const registerOAuthTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'create_oauth_url',
    title: 'Create OAuth URL',
    description: 'Generates an OAuth authorization URL for a Keboola component configuration.',
    inputSchema: {
      component_id: z
        .string()
        .describe('The component ID to grant access to (e.g., "keboola.ex-google-analytics-v4").'),
      config_id: z.string().describe('The configuration ID for the component.'),
    },
    handler: async ({ component_id, config_id }) => {
      const { rawStorage } = createKeboolaClients(config);

      // Short-lived (1h) token scoped to the component, used by the external OAuth page.
      const tokenResponse = await rawStorage.post<{ token: string }>('tokens', {
        body: {
          description: `Short-lived token for OAuth URL - ${component_id}/${config_id}`,
          componentAccess: [component_id],
          expiresIn: 3600,
        },
      });

      const query = new URLSearchParams({
        token: tokenResponse.token,
        sapiUrl: config.storageApiUrl ?? '',
      });
      return `https://external.keboola.com/oauth/index.html?${query.toString()}#/${component_id}/${config_id}`;
    },
  });
};
