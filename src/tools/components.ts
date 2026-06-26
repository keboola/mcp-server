import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager, type KeboolaClients } from '@/clients/keboola';
import { RawHttpError } from '@/clients/raw';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';

// Ported from tools/components/tools.py.

const jsonBlock = (label: string, index: number, example: unknown): string =>
  `${index}. ${label}:\n\`\`\`json\n${JSON.stringify(example, null, 2)}\n\`\`\`\n\n`;

type RawComponent = Record<string, unknown>;

const pick = <T>(raw: RawComponent, ...keys: string[]): T | undefined => {
  for (const key of keys) {
    if (raw[key] !== undefined && raw[key] !== null) return raw[key] as T;
  }
  return undefined;
};

/** Capabilities derived from developer-portal flags (port of ComponentCapabilities.from_flags). */
const capabilitiesFromFlags = (flags: string[]) => ({
  is_row_based: flags.includes('genericDockerUI-rows'),
  has_table_input:
    flags.includes('genericDockerUI-tableInput') ||
    flags.includes('genericDockerUI-simpleTableInput'),
  has_table_output: flags.includes('genericDockerUI-tableOutput'),
  has_file_input: flags.includes('genericDockerUI-fileInput'),
  has_file_output: flags.includes('genericDockerUI-fileOutput'),
  requires_oauth: flags.includes('genericDockerUI-authorization'),
});

/** Maps a raw component (AI catalog or Storage API) to the Component output shape. */
const toComponent = (raw: RawComponent) => {
  const flags = (pick<string[]>(raw, 'flags', 'componentFlags') ?? []) as string[];
  const data = (pick<Record<string, unknown>>(raw, 'data') ?? {}) as Record<string, unknown>;
  return {
    component_id: pick<string>(raw, 'id', 'componentId', 'component_id') ?? '',
    component_name: pick<string>(raw, 'name', 'componentName', 'component_name') ?? '',
    component_type: pick<string>(raw, 'type', 'componentType', 'component_type') ?? '',
    component_categories: pick<string[]>(raw, 'categories', 'componentCategories') ?? [],
    capabilities: capabilitiesFromFlags(flags),
    documentation_url: pick<string>(raw, 'documentationUrl', 'documentation_url') ?? null,
    documentation: pick<string>(raw, 'documentation') ?? null,
    configuration_schema: pick<unknown>(raw, 'configurationSchema', 'configuration_schema') ?? null,
    configuration_row_schema:
      pick<unknown>(raw, 'configurationRowSchema', 'configuration_row_schema') ?? null,
    sync_actions: (data.synchronous_actions as string[] | undefined) ?? null,
    links: [] as unknown[],
  };
};

/**
 * Fetches a component, preferring the AI catalog (docs + schemas) and merging the
 * Storage API `data` (sync actions); falls back to Storage API on 404. Port of
 * components/utils.py `fetch_component`.
 */
export const fetchComponent = async (
  clients: KeboolaClients,
  componentId: string,
): Promise<RawComponent> => {
  try {
    const fromAi = await clients.rawAi.get<RawComponent>(`docs/components/${componentId}`);
    const fromStorage = await clients.rawStorage.get<RawComponent>(
      `branch/${clients.branchId}/components/${componentId}`,
    );
    fromAi.data = fromStorage.data ?? {};
    return fromAi;
  } catch (error) {
    if (error instanceof RawHttpError && error.status === 404) {
      return clients.rawStorage.get<RawComponent>(
        `branch/${clients.branchId}/components/${componentId}`,
      );
    }
    throw error;
  }
};

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

  registerTool(server, {
    name: 'get_components',
    title: 'Get components',
    description: 'Retrieves detailed information about one or more components by their IDs.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      component_ids: z.array(z.string()).describe('IDs of the components to retrieve.'),
    },
    handler: async ({ component_ids }) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const components = await Promise.all(
        component_ids.map(async (componentId) => {
          const component = toComponent(await fetchComponent(clients, componentId));
          component.links = [
            linksManager.getConfigDashboardLink(componentId, component.component_name),
          ];
          return component;
        }),
      );

      return { components, links: [linksManager.getUsedComponentsLink()] };
    },
  });

  registerTool(server, {
    name: 'run_sync_action',
    title: 'Run sync action',
    description:
      'Executes a synchronous action for a component configuration or a component row configuration.',
    inputSchema: {
      action_name: z
        .string()
        .describe('The sync action to execute (e.g., "testConnection", "getTables").'),
      component_id: z.string().describe('The ID of the component (e.g., "keboola.ex-db-mysql").'),
      configuration_id: z
        .string()
        .describe('The ID of the configuration to use for the sync action.'),
      configuration_row_id: z
        .string()
        .nullish()
        .describe(
          'Optional row ID; row parameters/storage are shallow-merged on top of root config.',
        ),
    },
    handler: async ({ action_name, component_id, configuration_id, configuration_row_id }) => {
      const clients = createKeboolaClients(config);
      const base = `branch/${clients.branchId}/components/${component_id}/configs/${configuration_id}`;

      const configDetail = await clients.rawStorage.get<{
        configuration?: Record<string, unknown>;
      }>(base);
      const root = configDetail.configuration ?? {};
      let parameters = (root.parameters as Record<string, unknown>) ?? {};
      let storage = (root.storage as Record<string, unknown>) ?? {};
      // runtime/authorization live only on the root config (docker-runner contract).
      const runtime = (root.runtime as Record<string, unknown>) ?? {};
      const authorization = (root.authorization as Record<string, unknown>) ?? {};

      if (configuration_row_id) {
        const rowDetail = await clients.rawStorage.get<{ configuration?: Record<string, unknown> }>(
          `${base}/rows/${configuration_row_id}`,
        );
        const rowConfig = rowDetail.configuration ?? {};
        parameters = {
          ...parameters,
          ...((rowConfig.parameters as Record<string, unknown>) ?? {}),
        };
        storage = { ...storage, ...((rowConfig.storage as Record<string, unknown>) ?? {}) };
      }

      const configData: Record<string, unknown> = { parameters, storage };
      if (Object.keys(runtime).length > 0) configData.runtime = runtime;
      if (Object.keys(authorization).length > 0) configData.authorization = authorization;

      const payload: Record<string, unknown> = {
        configData,
        componentId: component_id,
        action: action_name,
      };
      if (config.branchId) payload.branchId = config.branchId;

      return clients.rawSyncActions.post<unknown>('actions', { body: payload });
    },
  });
};
