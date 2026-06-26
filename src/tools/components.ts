import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';

// Ported from tools/components/tools.py.

const jsonBlock = (label: string, index: number, example: unknown): string =>
  `${index}. ${label}:\n\`\`\`json\n${JSON.stringify(example, null, 2)}\n\`\`\`\n\n`;

export const registerComponentTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'get_config_examples',
    title: 'Get config examples',
    description: 'Retrieves sample configuration examples for a specific component.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      component_id: z
        .string()
        .describe('The ID of the component to get configuration examples for.'),
    },
    handler: async ({ component_id }) => {
      const { rawAi } = createKeboolaClients(config);

      let detail: { rootConfigurationExamples?: unknown[]; rowConfigurationExamples?: unknown[] };
      try {
        detail = await rawAi.get(`docs/components/${component_id}`);
      } catch {
        // Mirrors the Python tool: unknown/erroring component -> empty string.
        return '';
      }

      const rootExamples = detail.rootConfigurationExamples ?? [];
      const rowExamples = detail.rowConfigurationExamples ?? [];

      let markdown = `# Configuration Examples for \`${component_id}\`\n\n`;
      if (rootExamples.length > 0) {
        markdown += '## Root Configuration Examples\n\n';
        rootExamples.forEach((example, i) => {
          markdown += jsonBlock('Root Configuration', i + 1, example);
        });
      }
      if (rowExamples.length > 0) {
        markdown += '## Row Configuration Examples\n\n';
        rowExamples.forEach((example, i) => {
          markdown += jsonBlock('Row Configuration', i + 1, example);
        });
      }
      return markdown;
    },
  });
};
