import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import type { Config } from '@/config';
import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  FLOW_TYPES,
  type FlowType,
  ORCHESTRATOR_COMPONENT_ID,
} from '@/constants';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import {
  CREATE_CONDITIONAL_FLOW_DESCRIPTION,
  CREATE_FLOW_DESCRIPTION,
  GET_FLOW_EXAMPLES_DESCRIPTION,
  GET_FLOW_SCHEMA_DESCRIPTION,
  GET_FLOWS_DESCRIPTION,
  MODIFY_FLOW_DESCRIPTION,
  UPDATE_FLOW_DESCRIPTION,
} from './descriptions';
import {
  flowTypeSchema,
  normalizeScheduleRequests,
  type Phase,
  type RawConfig,
  resolveFlowSchema,
  type ScheduleRequest,
  scheduleRequestSchema,
  type Task,
  validateFlowConfigurationAgainstSchema,
  validateFlowStructure,
} from './model';
import { createSchedulerClient, listSchedulesForConfig, processScheduleRequest } from './scheduler';
import {
  assertConditionalAllowed,
  buildFlowToolOutput,
  buildFolderHint,
  clearConfigurationFolderMetadata,
  configurationCreate,
  configurationDetail,
  configurationList,
  configurationUpdate,
  EXAMPLE_FILES,
  flowLabel,
  folderFieldDescription,
  getConfigFolders,
  getFlowConfiguration,
  getProjectContext,
  loadLegacySchema,
  readResource,
  resolveFlowById,
  setCfgCreationMetadata,
  setCfgUpdateMetadata,
  setConfigurationFolderMetadata,
  toFlowDetail,
  toFlowSummary,
} from './utils';

// Ported from tools/flow/tools.py: the 7 tool handlers + the shared create/modify cores.

const getSchemaAsMarkdown = async (
  clients: ReturnType<typeof createKeboolaClients>,
  flowType: FlowType,
): Promise<string> => {
  const schema = await resolveFlowSchema(clients, flowType, loadLegacySchema);
  return `\`\`\`json\n${JSON.stringify(schema, null, 2)}\n\`\`\``;
};

// =============================================================================
// CREATE (shared between create_flow and create_conditional_flow)
// =============================================================================

const createFlowImpl = async (
  config: Config,
  flowType: FlowType,
  args: { name: string; description: string; phases: Phase[]; tasks: Task[]; folder: string },
) => {
  const clients = createKeboolaClients(config);
  const flowConfiguration = getFlowConfiguration(args.phases, args.tasks, flowType);

  // Structural validation (semantic), then schema validation (syntax).
  validateFlowStructure(flowConfiguration, flowType);
  const schema = await resolveFlowSchema(clients, flowType, loadLegacySchema);
  validateFlowConfigurationAgainstSchema(flowConfiguration, schema);

  const linksManager = await createLinksManager(config, clients);
  const newRaw = await configurationCreate(
    clients,
    flowType,
    args.name,
    args.description,
    flowConfiguration,
  );
  const configId = String(newRaw.id ?? '');
  await setCfgCreationMetadata(clients, flowType, configId);

  const folder = args.folder.trim();
  let changeSummary: string | null = null;
  if (folder) {
    try {
      await setConfigurationFolderMetadata(clients, flowType, configId, folder);
    } catch {
      logger.warn(
        `Unable to set folder metadata for component "${flowType}", configuration "${configId}".`,
      );
    }
  } else {
    try {
      const { total, folders, lowerBound } = await getConfigFolders(clients, flowType);
      changeSummary = buildFolderHint(
        total,
        folders,
        flowLabel(flowType),
        'modify_flow',
        lowerBound,
      );
    } catch {
      logger.warn(
        `Unable to fetch flow folders for component "${flowType}" when creating flow "${configId}".`,
      );
    }
  }

  const flowLinks = linksManager.getFlowLinks(configId, String(newRaw.name ?? ''), flowType);
  logger.info(
    `Created flow "${args.name}" with configuration ID "${configId}" (type: ${flowType})`,
  );
  return buildFlowToolOutput({
    configurationId: configId,
    componentId: flowType,
    description: (newRaw.description as string) || '',
    version: Number(newRaw.version ?? 0),
    links: flowLinks,
    changeSummary,
  });
};

// =============================================================================
// MODIFY (shared core behind modify_flow and update_flow)
// =============================================================================

const modifyFlowImpl = async (
  config: Config,
  args: {
    configuration_id: string;
    flow_type: FlowType;
    change_description: string;
    phases: Phase[] | null;
    tasks: Task[] | null;
    name: string;
    description: string;
    schedules: ScheduleRequest[];
    is_disabled: boolean | null;
    folder: string | null;
  },
) => {
  const clients = createKeboolaClients(config);
  const project = await getProjectContext(clients);
  assertConditionalAllowed(args.flow_type, project);

  let responseMessage: string | null = null;
  const hasConfigChanges =
    Boolean(args.name) ||
    Boolean(args.description) ||
    args.phases !== null ||
    args.tasks !== null ||
    args.is_disabled !== null;

  let apiConfig: RawConfig;
  if (hasConfigChanges) {
    logger.info(`Updating flow configuration: ${args.configuration_id} (type: ${args.flow_type})`);
    // update_flow_internal: deep-clone the existing config, replace phases/tasks, validate.
    const currentConfig = await configurationDetail(clients, args.flow_type, args.configuration_id);
    const flowConfiguration = structuredClone(
      (currentConfig.configuration as Record<string, unknown>) ?? {},
    );
    const updated = getFlowConfiguration(args.phases, args.tasks, args.flow_type);
    if ((updated.phases as unknown[]).length > 0) flowConfiguration.phases = updated.phases;
    if ((updated.tasks as unknown[]).length > 0) flowConfiguration.tasks = updated.tasks;

    validateFlowStructure(flowConfiguration, args.flow_type);
    const schema = await resolveFlowSchema(clients, args.flow_type, loadLegacySchema);
    validateFlowConfigurationAgainstSchema(flowConfiguration, schema);

    apiConfig = await configurationUpdate(
      clients,
      args.flow_type,
      args.configuration_id,
      flowConfiguration,
      args.change_description,
      args.name || undefined,
      args.description || undefined,
      args.is_disabled,
    );
    await setCfgUpdateMetadata(
      clients,
      args.flow_type,
      String(apiConfig.id ?? ''),
      Number(apiConfig.version ?? 0),
    );
  } else {
    apiConfig = await configurationDetail(clients, args.flow_type, args.configuration_id);
  }

  // Folder handling.
  let folderHint: string | null = null;
  if (args.folder === null) {
    try {
      const { total, folders, lowerBound } = await getConfigFolders(clients, args.flow_type);
      folderHint = buildFolderHint(
        total,
        folders,
        flowLabel(args.flow_type),
        'modify_flow',
        lowerBound,
      );
    } catch {
      logger.warn(
        `Unable to fetch flow folders for component "${args.flow_type}" when updating flow "${args.configuration_id}".`,
      );
    }
  } else {
    const folderStripped = args.folder.trim();
    if (folderStripped) {
      await setConfigurationFolderMetadata(
        clients,
        args.flow_type,
        args.configuration_id,
        folderStripped,
      );
    } else {
      await clearConfigurationFolderMetadata(clients, args.flow_type, args.configuration_id);
    }
  }

  const linksManager = await createLinksManager(config, clients);
  const flowLinks = linksManager.getFlowLinks(
    String(apiConfig.id ?? ''),
    String(apiConfig.name ?? ''),
    args.flow_type,
  );

  if (args.schedules.length > 0) {
    const scheduler = createSchedulerClient(config);
    const responses = await processScheduleRequest(
      clients,
      scheduler,
      args.flow_type,
      args.configuration_id,
      args.schedules,
    );
    responseMessage = 'Schedules request processed successfully: \n' + responses.join('\n');
    logger.info(
      `Successfully processed ${args.schedules.length} schedule request(s) for flow ${args.configuration_id}`,
    );
    flowLinks.push(linksManager.getSchedulerDetailLink(args.configuration_id, args.flow_type));
  }

  logger.info(`Updated flow configuration: ${apiConfig.id}`);
  return buildFlowToolOutput({
    configurationId: String(apiConfig.id ?? ''),
    componentId: args.flow_type,
    description: (apiConfig.description as string) || '',
    version: Number(apiConfig.version ?? 0),
    links: flowLinks,
    response: responseMessage,
    changeSummary: folderHint,
  });
};

// =============================================================================
// REGISTRATION
// =============================================================================

export const registerFlowTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'create_flow',
    title: 'Create flow',
    description: CREATE_FLOW_DESCRIPTION,
    annotations: { destructiveHint: false },
    inputSchema: {
      name: z.string().describe('A short, descriptive name for the flow.'),
      description: z.string().describe('Detailed description of the flow purpose.'),
      phases: z.array(z.record(z.string(), z.unknown())).describe('List of phase definitions.'),
      tasks: z.array(z.record(z.string(), z.unknown())).describe('List of task definitions.'),
      folder: z.string().default('').describe(folderFieldDescription('flow', 'flows')),
    },
    handler: (args) =>
      createFlowImpl(config, ORCHESTRATOR_COMPONENT_ID, {
        name: args.name,
        description: args.description,
        phases: args.phases as Phase[],
        tasks: args.tasks as Task[],
        folder: args.folder,
      }),
  });

  registerTool(server, {
    name: 'create_conditional_flow',
    title: 'Create conditional flow',
    description: CREATE_CONDITIONAL_FLOW_DESCRIPTION,
    annotations: { destructiveHint: false },
    inputSchema: {
      name: z.string().describe('A short, descriptive name for the flow.'),
      description: z.string().describe('Detailed description of the flow purpose.'),
      phases: z
        .array(z.record(z.string(), z.unknown()))
        .describe('List of phase definitions for conditional flows.'),
      tasks: z
        .array(z.record(z.string(), z.unknown()))
        .describe('List of task definitions for conditional flows.'),
      folder: z.string().default('').describe(folderFieldDescription('flow', 'flows')),
    },
    handler: async (args) => {
      // Conditional flows require the feature to be enabled (parity with the Python tool's
      // create_conditional_flow, which fails fast when the schema is unavailable).
      const clients = createKeboolaClients(config);
      const project = await getProjectContext(clients);
      assertConditionalAllowed(CONDITIONAL_FLOW_COMPONENT_ID, project);
      return createFlowImpl(config, CONDITIONAL_FLOW_COMPONENT_ID, {
        name: args.name,
        description: args.description,
        phases: args.phases as Phase[],
        tasks: args.tasks as Task[],
        folder: args.folder,
      });
    },
  });

  registerTool(server, {
    name: 'get_flows',
    title: 'Get flows',
    description: GET_FLOWS_DESCRIPTION,
    annotations: { readOnlyHint: true },
    inputSchema: {
      flow_ids: z
        .array(z.string())
        .default([])
        .describe(
          'IDs of flows to retrieve full details for. ' +
            'When provided (non-empty), returns full flow configurations including phases and tasks. ' +
            'When empty [], lists all flows in the project as summaries.',
        ),
    },
    handler: async ({ flow_ids }) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);
      const scheduler = createSchedulerClient(config);

      // Case 1: full details for specific flow ids.
      if (flow_ids.length > 0) {
        const flows = await Promise.all(
          flow_ids.map(async (flowId) => {
            const { raw, flowType } = await resolveFlowById(clients, flowId);
            logger.info(`Found flow ${flowId} under flow type ${flowType}.`);
            const configId = String(raw.id ?? '');
            const links = linksManager.getFlowLinks(configId, String(raw.name ?? ''), flowType);
            const schedules = await listSchedulesForConfig(scheduler, flowType, configId);
            const scheduleLink = linksManager.getSchedulerDetailLink(configId, flowType);
            return toFlowDetail(raw, flowType, links, schedules, [scheduleLink]);
          }),
        );
        logger.info(`Retrieved full details for ${flows.length} flows.`);
        return { flows };
      }

      // Case 2: list all flows as summaries.
      const flows: ReturnType<typeof toFlowSummary>[] = [];
      for (const flowType of FLOW_TYPES) {
        const rawFlows = await configurationList(clients, flowType);
        const summaries = await Promise.all(
          rawFlows.map(async (raw) => {
            let nSchedules = 0;
            try {
              const schedules = await listSchedulesForConfig(
                scheduler,
                flowType,
                String(raw.id ?? ''),
              );
              nSchedules = schedules.length;
            } catch (e) {
              logger.warn({ err: e }, `Failed to fetch schedules for flow ${raw.id}`);
            }
            return toFlowSummary(raw, flowType, nSchedules);
          }),
        );
        flows.push(...summaries);
      }
      logger.info(`Retrieved ${flows.length} flows.`);
      return {
        flows,
        links: [
          linksManager.getFlowsDashboardLink(ORCHESTRATOR_COMPONENT_ID),
          linksManager.getFlowsDashboardLink(CONDITIONAL_FLOW_COMPONENT_ID),
        ],
      };
    },
  });

  registerTool(server, {
    name: 'update_flow',
    title: 'Update flow',
    description: UPDATE_FLOW_DESCRIPTION,
    annotations: { destructiveHint: true },
    inputSchema: {
      configuration_id: z.string().describe('ID of the flow configuration.'),
      flow_type: flowTypeSchema.describe(
        'The type of flow to update. Use "keboola.flow" for conditional flows or ' +
          '"keboola.orchestrator" for legacy flows. This MUST match the existing flow type.',
      ),
      change_description: z.string().describe('Description of changes made.'),
      phases: z
        .array(z.record(z.string(), z.unknown()))
        .nullish()
        .describe('Updated list of phase definitions.'),
      tasks: z
        .array(z.record(z.string(), z.unknown()))
        .nullish()
        .describe('Updated list of task definitions.'),
      name: z.string().default('').describe('Updated flow name. Only updated if provided.'),
      description: z
        .string()
        .default('')
        .describe('Updated flow description. Only updated if provided.'),
      is_disabled: z
        .boolean()
        .nullish()
        .describe(
          "Enable or disable the flow. Set to True to disable execution (flow won't run), " +
            'False to enable execution (flow will run). Only provide if changing the status, ' +
            'leave as null to preserve current state.',
        ),
      folder: z.string().nullish().describe(folderFieldDescription('flow', 'flows')),
    },
    handler: (args) =>
      modifyFlowImpl(config, {
        configuration_id: args.configuration_id,
        flow_type: args.flow_type,
        change_description: args.change_description,
        phases: (args.phases as Phase[] | null | undefined) ?? null,
        tasks: (args.tasks as Task[] | null | undefined) ?? null,
        name: args.name,
        description: args.description,
        schedules: [],
        is_disabled: args.is_disabled ?? null,
        folder: args.folder ?? null,
      }),
  });

  registerTool(server, {
    name: 'modify_flow',
    title: 'Modify flow',
    description: MODIFY_FLOW_DESCRIPTION,
    annotations: { destructiveHint: true },
    inputSchema: {
      configuration_id: z.string().describe('ID of the flow configuration.'),
      flow_type: flowTypeSchema.describe(
        'The type of flow to update. Use "keboola.flow" for conditional flows or ' +
          '"keboola.orchestrator" for legacy flows. This MUST match the existing flow type.',
      ),
      change_description: z.string().describe('Description of changes made.'),
      phases: z
        .array(z.record(z.string(), z.unknown()))
        .nullish()
        .describe('Updated list of phase definitions.'),
      tasks: z
        .array(z.record(z.string(), z.unknown()))
        .nullish()
        .describe('Updated list of task definitions.'),
      name: z.string().default('').describe('Updated flow name. Only updated if provided.'),
      description: z
        .string()
        .default('')
        .describe('Updated flow description. Only updated if provided.'),
      schedules: z
        .array(scheduleRequestSchema)
        .default([])
        .describe(
          'Optional sequence of schedule requests to add/update/remove schedules for this flow. ' +
            'Each request must have "action": "add"|"update"|"remove". ' +
            'For add: include "cron_tab", "state" ("enabled"|"disabled"), "timezone". ' +
            'For update/remove: include "schedule_id". ' +
            'Example: [{"action": "add", "cron_tab": "0 8 * * 1-5", "state": "enabled", "timezone": "UTC"}]',
        ),
      is_disabled: z
        .boolean()
        .nullish()
        .describe(
          "Enable or disable the flow. Set to True to disable execution (flow won't run), " +
            'False to enable execution (flow will run). Only provide if changing the status, ' +
            'leave as null to preserve current state.',
        ),
      folder: z.string().nullish().describe(folderFieldDescription('flow', 'flows')),
    },
    handler: (args) =>
      modifyFlowImpl(config, {
        configuration_id: args.configuration_id,
        flow_type: args.flow_type,
        change_description: args.change_description,
        phases: (args.phases as Phase[] | null | undefined) ?? null,
        tasks: (args.tasks as Task[] | null | undefined) ?? null,
        name: args.name,
        description: args.description,
        schedules: normalizeScheduleRequests(args.schedules),
        is_disabled: args.is_disabled ?? null,
        folder: args.folder ?? null,
      }),
  });

  registerTool(server, {
    name: 'get_flow_schema',
    title: 'Get flow schema',
    description: GET_FLOW_SCHEMA_DESCRIPTION,
    annotations: { readOnlyHint: true },
    inputSchema: {
      flow_type: flowTypeSchema.describe('The type of flow for which to fetch schema.'),
    },
    handler: async ({ flow_type }) => {
      const clients = createKeboolaClients(config);
      const project = await getProjectContext(clients);
      assertConditionalAllowed(flow_type, project);
      logger.info(`Returning flow configuration schema for flow type: ${flow_type}`);
      return getSchemaAsMarkdown(clients, flow_type);
    },
  });

  registerTool(server, {
    name: 'get_flow_examples',
    title: 'Get flow examples',
    description: GET_FLOW_EXAMPLES_DESCRIPTION,
    annotations: { readOnlyHint: true },
    inputSchema: {
      flow_type: flowTypeSchema.describe('The type of the flow to retrieve examples for.'),
    },
    handler: async ({ flow_type }) => {
      const clients = createKeboolaClients(config);
      const project = await getProjectContext(clients);
      assertConditionalAllowed(flow_type, project, true);

      const content = readResource(EXAMPLE_FILES[flow_type]);
      let markdown = `# Flow Configuration Examples for \`${flow_type}\`\n\n`;
      const lines = content.split('\n').filter((line) => line.trim().length > 0);
      lines.forEach((line, i) => {
        const data = JSON.parse(line);
        markdown += `${i + 1}. Flow Configuration:\n\`\`\`json\n${JSON.stringify(data, null, 2)}\n\`\`\`\n\n`;
      });
      return markdown;
    },
  });

  logger.info('Flow tools initialized.');
};
