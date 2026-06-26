import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';

import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';
import { registerComponentTools } from '@/tools/components';
import { registerDocTools } from '@/tools/doc';
import { registerJobTools } from '@/tools/jobs';
import { registerOAuthTools } from '@/tools/oauth';
import { registerProjectTools } from '@/tools/project';
import { registerStorageTools } from '@/tools/storage';

// Reading package.json at build time would need JSON import assertions; keep a
// constant and bump alongside package.json until the build wiring lands.
export const SERVER_NAME = 'keboola';
export const SERVER_VERSION = '2.0.0-alpha.0';

/**
 * Builds the MCP server and registers all tools/prompts/resources.
 *
 * A single scaffold tool proves the registration → schema → TOON-serialize →
 * transport path end to end. Real tool modules land in later phases (see
 * feature_spec/mcp-typescript-rewrite/PLAN.md §4).
 */
export const createServer = (config: Config): McpServer => {
  const server = new McpServer({ name: SERVER_NAME, version: SERVER_VERSION });

  // ponytail: scaffold tool, replaced when the real tool modules are ported.
  registerTool(server, {
    name: 'get_server_info',
    title: 'Get server info',
    description: 'Returns basic information about the running Keboola MCP server.',
    annotations: { readOnlyHint: true },
    handler: () => ({
      name: SERVER_NAME,
      version: SERVER_VERSION,
      branchId: config.branchId ?? null,
      hasStorageToken: Boolean(config.storageToken),
    }),
  });

  registerProjectTools(server, config);
  registerJobTools(server, config);
  registerStorageTools(server, config);
  registerOAuthTools(server, config);
  registerComponentTools(server, config);
  registerDocTools(server, config);

  return server;
};
