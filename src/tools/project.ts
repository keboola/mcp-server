import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';

/** Registers the project tools (Plan §4). Ported from tools/project.py. */
export const registerProjectTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'update_project_description',
    title: 'Update project description',
    description: 'Updates the description of the current Keboola project.',
    inputSchema: {
      description: z.string().describe('The new project description text.'),
    },
    handler: async ({ description }) => {
      const clients = createKeboolaClients(config);
      await clients.storage.branches.saveDevBranchMetadata(clients.branchId, [
        { key: MetadataField.PROJECT_DESCRIPTION, value: description },
      ]);
      logger.info('Project description updated successfully.');
      return { message: 'Project description updated successfully.' };
    },
  });
};
