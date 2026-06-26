import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

import type { Config } from '@/config';

// Read from package.json at build time would need JSON import assertions; keep a
// constant and bump alongside package.json until the build wiring lands.
export const SERVER_NAME = 'keboola';
export const SERVER_VERSION = '2.0.0-alpha.0';

/**
 * Builds the MCP server and registers all tools/prompts/resources.
 *
 * Phase 0 registers a single scaffold tool to prove the registration → schema →
 * transport path end to end. Real tools land in later phases (see
 * feature_spec/mcp-typescript-rewrite/PLAN.md §4).
 */
export const createServer = (config: Config): McpServer => {
  const server = new McpServer({ name: SERVER_NAME, version: SERVER_VERSION });

  // ponytail: scaffold tool, replaced when the real tool modules are ported.
  server.registerTool(
    'get_server_info',
    {
      title: 'Get server info',
      description: 'Returns basic information about the running Keboola MCP server.',
      inputSchema: {},
      annotations: { readOnlyHint: true },
    },
    async () => ({
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            name: SERVER_NAME,
            version: SERVER_VERSION,
            branchId: config.branchId ?? null,
            hasStorageToken: Boolean(config.storageToken),
          }),
        },
      ],
    }),
  );

  return server;
};
