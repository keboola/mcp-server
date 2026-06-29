import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager, type KeboolaClients } from '@/clients/keboola';
import { RawHttpError } from '@/clients/raw';
import type { Config } from '@/config';
import { ALL_COMPONENT_TYPES, type ComponentType } from '@/constants';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import {
  type JsonDict,
  validateProcessorsConfiguration,
  validateRootParametersConfiguration,
  validateRootStorageConfiguration,
  validateRowParametersConfiguration,
  validateRowStorageConfiguration,
} from '@/tools/validation';
import {
  buildFolderHint,
  checkSuitable,
  type ConfigParamUpdate,
  configParamUpdateSchema,
  createTransformationConfiguration,
  FOLDER_SUPPORTING_COMPONENT_IDS,
  folderFieldDescription,
  getSqlTransformationIdFromSqlDialect,
  setNestedValue,
  tfCodeSchema,
  type TfParamUpdate,
  tfParamUpdateSchema,
  toRawParameters,
  toSimplifiedParameters,
  updateParams,
  updateTransformationParameters,
  variableDefinitionSchema,
} from './model';
import {
  applyConfigurationVariables,
  applyVarsToParentCfg,
  clearFolderMetadata,
  configurationCreate,
  configurationDetail,
  configurationList,
  configurationRowCreate,
  configurationRowDetail,
  configurationRowUpdate,
  configurationUpdate,
  deleteVariablesConfig,
  fetchComponent,
  getConfigFolders,
  jsonBlock,
  nowIso,
  pick,
  type RawComponent,
  type RawConfig,
  resolveSqlDialect,
  setCfgCreationMetadata,
  setCfgUpdateMetadata,
  setFolderMetadata,
  toComponent,
  toComponentForValidation,
  toComponentSummary,
  toConfigSummary,
  toConfiguration,
} from './utils';

// Ported from tools/components/tools.py.

type ConfigToolOutput = {
  component_id: string;
  configuration_id: string;
  description: string;
  version: number;
  timestamp: string;
  success: boolean;
  links: Link[];
  change_summary?: string | null;
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
        // KEPT RAW: the AI catalog `docs/components/{id}` endpoint has no typed equivalent.
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

      const configDetail = await configurationDetail(clients, component_id, configuration_id);
      const root = (configDetail.configuration as Record<string, unknown>) ?? {};
      let parameters = (root.parameters as Record<string, unknown>) ?? {};
      let storage = (root.storage as Record<string, unknown>) ?? {};
      // runtime/authorization live only on the root config (docker-runner contract).
      const runtime = (root.runtime as Record<string, unknown>) ?? {};
      const authorization = (root.authorization as Record<string, unknown>) ?? {};

      if (configuration_row_id) {
        const rowDetail = await configurationRowDetail(
          clients,
          component_id,
          configuration_id,
          configuration_row_id,
        );
        const rowConfig = (rowDetail.configuration as Record<string, unknown>) ?? {};
        parameters = {
          ...parameters,
          ...((rowConfig.parameters as Record<string, unknown>) ?? {}),
        };
        storage = { ...storage, ...((rowConfig.storage as Record<string, unknown>) ?? {}) };
      }

      const configData: Record<string, unknown> = { parameters, storage };
      if (Object.keys(runtime).length > 0) configData.runtime = runtime;
      if (Object.keys(authorization).length > 0) configData.authorization = authorization;

      // MIGRATED → syncActions.sendSyncAction (POST /actions).
      const payload: {
        configData: Record<string, unknown>;
        componentId: string;
        action: string;
        branchId?: string;
      } = {
        configData,
        componentId: component_id,
        action: action_name,
      };
      if (config.branchId) payload.branchId = config.branchId;

      return clients.syncActions.sendSyncAction(payload);
    },
  });

  registerTool(server, {
    name: 'get_configs',
    title: 'Get configs',
    description: 'Retrieves component configurations in the project with optional filtering.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      component_types: z
        .array(z.enum(ALL_COMPONENT_TYPES))
        .default([])
        .describe(
          'Filter by component types; empty = all. Ignored when configs/component_ids given.',
        ),
      component_ids: z
        .array(z.string())
        .default([])
        .describe('Filter by specific component IDs. Ignored when configs is given.'),
      configs: z
        .array(z.object({ component_id: z.string(), configuration_id: z.string() }))
        .default([])
        .describe('Specific configs to retrieve full details for (grouped by component).'),
    },
    handler: async ({ component_types, component_ids, configs }) => {
      const clients = createKeboolaClients(config);
      const links = await createLinksManager(config, clients);

      // Case 1: full details for specific configs.
      if (configs.length > 0) {
        const fetched = await Promise.all(
          configs.map(async ({ component_id, configuration_id }) => {
            const raw = (await configurationDetail(
              clients,
              component_id,
              configuration_id,
            )) as RawConfig;
            const component = toComponentSummary(await fetchComponent(clients, component_id));
            const cfgLinks = links.getConfigurationLinks(
              component_id,
              configuration_id,
              String(raw.name ?? ''),
            );
            return toConfiguration(raw, component_id, component, cfgLinks);
          }),
        );
        return { configs: fetched };
      }

      // Case 2/3: list summaries grouped by component.
      const componentsWithConfigs: unknown[] = [];

      const buildGroup = async (rawComponent: RawComponent, rawConfigs: RawConfig[]) => {
        const componentId = (pick<string>(rawComponent, 'id', 'componentId') ?? '') as string;
        const component = toComponentSummary(rawComponent);
        component.links = [links.getConfigDashboardLink(componentId, component.component_name)];
        const configSummaries = rawConfigs.map((raw) =>
          toConfigSummary(raw, componentId, [
            links.getComponentConfigLink(componentId, String(raw.id ?? ''), String(raw.name ?? '')),
          ]),
        );
        componentsWithConfigs.push({ component, configs: configSummaries });
      };

      if (component_ids.length > 0) {
        for (const componentId of component_ids) {
          const rawConfigs = (await configurationList(clients, componentId)) as RawConfig[];
          // KEPT RAW: the component detail merge in fetchComponent has no typed equivalent;
          // here we only need the component summary, fetched via the raw Storage detail.
          const rawComponent = await clients.rawStorage.get<RawComponent>(
            `branch/${clients.branchId}/components/${componentId}`,
          );
          await buildGroup(rawComponent, rawConfigs);
        }
      } else {
        const types: readonly ComponentType[] =
          component_types.length > 0 ? component_types : ALL_COMPONENT_TYPES;
        for (const componentType of types) {
          // KEPT RAW: the listing uses `include=configuration` to embed configs in one
          // call; the typed getComponents does not expose the embedded-configuration shape.
          const rawComponents = await clients.rawStorage.get<RawComponent[]>(
            `branch/${clients.branchId}/components`,
            {
              params: { componentType, include: 'configuration' },
            },
          );
          for (const rawComponent of rawComponents) {
            await buildGroup(rawComponent, (rawComponent.configurations as RawConfig[]) ?? []);
          }
        }
      }

      return {
        components_with_configs: componentsWithConfigs,
        links: [links.getUsedComponentsLink(), links.getTransformationsDashboardLink()],
      };
    },
  });

  // ==========================================================================
  // CONFIGURATION MANAGEMENT WRITE TOOLS
  // ==========================================================================

  const processorsBeforeField = z
    .array(z.record(z.string(), z.any()))
    .nullish()
    .describe('The list of processors that will run before the configured component runs.');
  const processorsAfterField = z
    .array(z.record(z.string(), z.any()))
    .nullish()
    .describe('The list of processors that will run after the configured component runs.');

  registerTool(server, {
    name: 'create_config',
    title: 'Create config',
    description:
      'Creates a root component configuration using the specified name, component ID, configuration JSON, and ' +
      'description.',
    annotations: { destructiveHint: false },
    inputSchema: {
      name: z
        .string()
        .describe(
          'A short, descriptive name summarizing the purpose of the component configuration.',
        ),
      description: z
        .string()
        .describe(
          'The detailed description of the component configuration explaining its purpose and functionality.',
        ),
      component_id: z
        .string()
        .describe('The ID of the component for which to create the configuration.'),
      parameters: z
        .record(z.string(), z.any())
        .describe('The component configuration parameters, adhering to the configuration_schema'),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'The table and/or file input / output mapping of the component configuration. ' +
            'It is present only for components that have tables or file input mapping defined',
        ),
      processors_before: processorsBeforeField,
      processors_after: processorsAfterField,
      variables: z
        .array(variableDefinitionSchema)
        .nullish()
        .describe(
          'Variable definitions to attach to this configuration. ' +
            'Each entry specifies a name, type ("string" or "vault"), and an optional default value. ' +
            'On creation, both `None` (omitted) and `[]` (empty list) mean "do not attach variables" — ' +
            'no `keboola.variables` config is created. To remove variables from an existing configuration, ' +
            'use `update_config` with `variables=[]`.',
        ),
    },
    handler: async (args) => {
      checkSuitable('create_config', args.component_id);
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const component = toComponentForValidation(await fetchComponent(clients, args.component_id));

      const storageCfg = validateRootStorageConfiguration(
        args.storage as JsonDict | null | undefined,
        component,
        'The "storage" field is not valid.',
      );
      const parameters = validateRootParametersConfiguration(
        args.parameters as JsonDict,
        component,
        'The "parameters" field is not valid.',
      );

      const configurationPayload: JsonDict = { storage: storageCfg, parameters };

      const fetchForValidation = async (id: string) =>
        toComponentForValidation(await fetchComponent(clients, id));

      if (args.processors_before?.length) {
        const validated = await validateProcessorsConfiguration(
          fetchForValidation,
          args.processors_before as JsonDict[],
          'The "processors_before" field is not valid.',
        );
        setNestedValue(configurationPayload, 'processors.before', validated);
      }
      if (args.processors_after?.length) {
        const validated = await validateProcessorsConfiguration(
          fetchForValidation,
          args.processors_after as JsonDict[],
          'The "processors_after" field is not valid.',
        );
        setNestedValue(configurationPayload, 'processors.after', validated);
      }

      const newRaw = await configurationCreate(
        config,
        clients,
        args.component_id,
        args.name,
        args.description,
        configurationPayload,
      );
      const configurationId = String(newRaw.id);

      await setCfgCreationMetadata(clients, args.component_id, configurationId);

      let varsResult: JsonDict | null = null;
      if (args.variables && args.variables.length > 0) {
        varsResult = await applyConfigurationVariables(
          config,
          clients,
          args.component_id,
          configurationId,
          args.variables,
        );
        if (varsResult !== null) {
          await setCfgUpdateMetadata(
            clients,
            args.component_id,
            configurationId,
            varsResult.version as number,
          );
        }
      }

      const output: ConfigToolOutput = {
        component_id: args.component_id,
        configuration_id: configurationId,
        description: args.description,
        version: ((varsResult ?? newRaw).version as number) ?? 0,
        timestamp: nowIso(),
        success: true,
        links: linksManager.getConfigurationLinks(args.component_id, configurationId, args.name),
      };
      return output;
    },
  });

  registerTool(server, {
    name: 'add_config_row',
    title: 'Add config row',
    description:
      'Creates a component configuration row in the specified configuration_id, using the specified name, ' +
      'component ID, configuration JSON, and description.',
    annotations: { destructiveHint: false },
    inputSchema: {
      name: z
        .string()
        .describe(
          'A short, descriptive name summarizing the purpose of the component configuration.',
        ),
      description: z
        .string()
        .describe(
          'The detailed description of the component configuration explaining its purpose and functionality.',
        ),
      component_id: z
        .string()
        .describe('The ID of the component for which to create the configuration.'),
      configuration_id: z
        .string()
        .describe('The ID of the configuration for which to create the configuration row.'),
      parameters: z
        .record(z.string(), z.any())
        .describe(
          'The component row configuration parameters, adhering to the configuration_row_schema',
        ),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'The table and/or file input / output mapping of the component configuration. ' +
            'It is present only for components that have tables or file input mapping defined',
        ),
      processors_before: processorsBeforeField,
      processors_after: processorsAfterField,
    },
    handler: async (args) => {
      checkSuitable('add_config_row', args.component_id);
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const component = toComponentForValidation(await fetchComponent(clients, args.component_id));

      const storageCfg = validateRowStorageConfiguration(
        args.storage as JsonDict | null | undefined,
        component,
        'The "storage" field is not valid.',
        args.configuration_id,
      );
      const parameters = validateRowParametersConfiguration(
        args.parameters as JsonDict,
        component,
        'The "parameters" field is not valid.',
        args.configuration_id,
      );

      const configurationPayload: JsonDict = { storage: storageCfg, parameters };

      const fetchForValidation = async (id: string) =>
        toComponentForValidation(await fetchComponent(clients, id));

      if (args.processors_before?.length) {
        const validated = await validateProcessorsConfiguration(
          fetchForValidation,
          args.processors_before as JsonDict[],
          'The "processors_before" field is not valid.',
        );
        setNestedValue(configurationPayload, 'processors.before', validated);
      }
      if (args.processors_after?.length) {
        const validated = await validateProcessorsConfiguration(
          fetchForValidation,
          args.processors_after as JsonDict[],
          'The "processors_after" field is not valid.',
        );
        setNestedValue(configurationPayload, 'processors.after', validated);
      }

      const newRaw = await configurationRowCreate(
        config,
        clients,
        args.component_id,
        args.configuration_id,
        args.name,
        args.description,
        configurationPayload,
      );

      await setCfgUpdateMetadata(
        clients,
        args.component_id,
        args.configuration_id,
        newRaw.version as number,
      );

      const output: ConfigToolOutput = {
        component_id: args.component_id,
        configuration_id: args.configuration_id,
        description: args.description,
        version: (newRaw.version as number) ?? 0,
        timestamp: nowIso(),
        success: true,
        links: linksManager.getConfigurationLinks(
          args.component_id,
          args.configuration_id,
          args.name,
        ),
      };
      return output;
    },
  });

  registerTool(server, {
    name: 'update_config',
    title: 'Update config',
    description:
      'Updates an existing root component configuration by modifying its parameters, storage mappings, name or ' +
      'description. Updates are PARTIAL — only provide the fields you want to change; parameter_updates apply ' +
      'granular diff operations to the existing parameters.',
    annotations: { destructiveHint: true },
    inputSchema: {
      change_description: z
        .string()
        .describe(
          'A clear, human-readable summary of what changed in this update. ' +
            'Be specific: e.g., "Updated API key", "Added customers table to input mapping".',
        ),
      component_id: z.string().describe('The ID of the component the configuration belongs to.'),
      configuration_id: z.string().describe('The ID of the configuration to update.'),
      name: z
        .string()
        .default('')
        .describe(
          'New name for the configuration. Only provide if changing the name. ' +
            'Name should be short (typically under 50 characters) and descriptive.',
        ),
      description: z
        .string()
        .default('')
        .describe(
          'New detailed description for the configuration. Only provide if changing the description. ' +
            'Should explain the purpose, data sources, and behavior of this configuration. ' +
            'Leave empty to preserve the original description.',
        ),
      parameter_updates: z
        .array(configParamUpdateSchema)
        .nullish()
        .describe(
          'List of granular parameter update operations to apply. ' +
            'Each operation (set, str_replace, remove, list_append) modifies a specific ' +
            'value using JSONPath notation. Only provide if updating parameters - ' +
            'do not use for changing description, storage or processors. ' +
            'Paths are relative to the `parameters` object, not the configuration root ' +
            '(e.g. use `tables`, not `parameters.tables`). ' +
            'Prefer simple JSONPaths (e.g., "array_param[1]", "object_param.key") ' +
            'and make the smallest possible updates - only change what needs changing. ' +
            'In case you need to replace the whole parameters section, you can use the `set` operation ' +
            'with `$` as path.',
        ),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'Complete storage configuration containing input/output table and file mappings. ' +
            'Only provide if updating storage mappings - this replaces the ENTIRE storage configuration.',
        ),
      processors_before: processorsBeforeField,
      processors_after: processorsAfterField,
      folder: z
        .string()
        .nullish()
        .describe(folderFieldDescription('configuration', 'configurations')),
      variables: z
        .array(variableDefinitionSchema)
        .nullish()
        .describe(
          'Variable definitions for this configuration. ' +
            'Provide a non-empty list to create or replace all variable definitions. ' +
            'Provide an empty list ([]) to remove all variables. ' +
            'Omit (None) to leave existing variables unchanged.',
        ),
    },
    handler: async (args) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const configurationPayload = await buildUpdatedConfigPayload({
        config,
        clients,
        componentId: args.component_id,
        configurationId: args.configuration_id,
        parameterUpdates: args.parameter_updates ?? null,
        storage: (args.storage as JsonDict | undefined) ?? null,
        processorsBefore: (args.processors_before as JsonDict[] | undefined) ?? null,
        processorsAfter: (args.processors_after as JsonDict[] | undefined) ?? null,
        isRow: false,
      });

      let varsConfigIdToDelete: string | null = null;
      if (args.variables !== undefined && args.variables !== null) {
        const res = await applyVarsToParentCfg(
          config,
          clients,
          args.component_id,
          args.configuration_id,
          args.variables,
          configurationPayload,
        );
        varsConfigIdToDelete = res.varsConfigIdToDelete;
      }

      const updatedRaw = await configurationUpdate(
        config,
        clients,
        args.component_id,
        args.configuration_id,
        configurationPayload,
        args.change_description,
        args.name,
        args.description,
      );

      if (varsConfigIdToDelete) {
        await deleteVariablesConfig(clients, varsConfigIdToDelete);
      }

      let folderHint: string | null = null;
      if (FOLDER_SUPPORTING_COMPONENT_IDS.has(args.component_id)) {
        folderHint = await applyFolderMetadata(
          clients,
          args.component_id,
          args.configuration_id,
          (args.folder as string | null | undefined) ?? null,
          'configurations',
          'update_config',
        );
      }

      await setCfgUpdateMetadata(
        clients,
        args.component_id,
        args.configuration_id,
        updatedRaw.version as number,
      );

      const output: ConfigToolOutput = {
        component_id: args.component_id,
        configuration_id: args.configuration_id,
        description: (updatedRaw.description as string) || '',
        version: (updatedRaw.version as number) ?? 0,
        timestamp: nowIso(),
        success: true,
        links: linksManager.getConfigurationLinks(
          args.component_id,
          args.configuration_id,
          (updatedRaw.name as string) || '',
        ),
        change_summary: folderHint,
      };
      return output;
    },
  });

  registerTool(server, {
    name: 'update_config_row',
    title: 'Update config row',
    description:
      'Updates an existing component configuration row by modifying its parameters, storage mappings, name, or ' +
      'description. Updates are PARTIAL — only provide the fields you want to change; parameter_updates apply ' +
      'granular diff operations to the existing row parameters.',
    annotations: { destructiveHint: true },
    inputSchema: {
      change_description: z
        .string()
        .describe(
          'A clear, human-readable summary of what changed in this row update. Be specific.',
        ),
      component_id: z.string().describe('The ID of the component the configuration belongs to.'),
      configuration_id: z
        .string()
        .describe('The ID of the parent configuration containing the row to update.'),
      configuration_row_id: z
        .string()
        .describe('The ID of the specific configuration row to update.'),
      name: z
        .string()
        .default('')
        .describe(
          'New name for the configuration row. Only provide if changing the name. ' +
            'Name should be short (typically under 50 characters) and descriptive of this specific row.',
        ),
      description: z
        .string()
        .default('')
        .describe(
          'New detailed description for the configuration row. Only provide if changing the description. ' +
            'Should explain the specific purpose and behavior of this individual row.',
        ),
      parameter_updates: z
        .array(configParamUpdateSchema)
        .nullish()
        .describe(
          'List of granular parameter update operations to apply to this row. ' +
            'Each operation (set, str_replace, remove, list_append) modifies a specific ' +
            'parameter using JSONPath notation. Only provide if updating parameters - ' +
            'do not use for changing description or storage. ' +
            "Paths are relative to the row's `parameters` object, not the row root " +
            '(e.g. use `tables`, not `parameters.tables`). ' +
            'Prefer simple dot-delimited JSONPaths ' +
            'and make the smallest possible updates - only change what needs changing. ' +
            'In case you need to replace the whole parameters, you can use the `set` operation ' +
            'with `$` as path.',
        ),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'Complete storage configuration for this row containing input/output table and file mappings. ' +
            'Only provide if updating storage mappings - this replaces the ENTIRE storage configuration ' +
            'for this row.',
        ),
      processors_before: processorsBeforeField,
      processors_after: processorsAfterField,
      is_disabled: z
        .boolean()
        .nullish()
        .describe(
          "Enable or disable the configuration row. Set to True to disable execution (config row won't run), " +
            'False to enable execution (config row will run). Only provide if changing the status, ' +
            'leave as null to preserve current state.',
        ),
    },
    handler: async (args) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const configurationPayload = await buildUpdatedConfigPayload({
        config,
        clients,
        componentId: args.component_id,
        configurationId: args.configuration_id,
        configurationRowId: args.configuration_row_id,
        parameterUpdates: args.parameter_updates ?? null,
        storage: (args.storage as JsonDict | undefined) ?? null,
        processorsBefore: (args.processors_before as JsonDict[] | undefined) ?? null,
        processorsAfter: (args.processors_after as JsonDict[] | undefined) ?? null,
        isRow: true,
      });

      const updatedRaw = await configurationRowUpdate(
        config,
        clients,
        args.component_id,
        args.configuration_id,
        args.configuration_row_id,
        configurationPayload,
        args.change_description,
        args.name,
        args.description,
        args.is_disabled,
      );

      await setCfgUpdateMetadata(
        clients,
        args.component_id,
        args.configuration_id,
        updatedRaw.version as number,
      );

      const output: ConfigToolOutput = {
        component_id: args.component_id,
        configuration_id: args.configuration_id,
        description: (updatedRaw.description as string) || '',
        version: (updatedRaw.version as number) ?? 0,
        timestamp: nowIso(),
        success: true,
        links: linksManager.getConfigurationLinks(
          args.component_id,
          args.configuration_id,
          (updatedRaw.name as string) || '',
        ),
      };
      return output;
    },
  });

  // ==========================================================================
  // SQL TRANSFORMATION WRITE TOOLS
  // ==========================================================================

  registerTool(server, {
    name: 'create_sql_transformation',
    title: 'Create SQL transformation',
    description:
      'Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a ' +
      'detailed description, and a list of created table names.',
    annotations: { destructiveHint: false },
    inputSchema: {
      name: z
        .string()
        .describe('A short, descriptive name summarizing the purpose of the SQL transformation.'),
      description: z
        .string()
        .describe(
          'The detailed description of the SQL transformation capturing the user intent, explaining the ' +
            'SQL query, and the expected output.',
        ),
      sql_code_blocks: z
        .array(tfCodeSchema)
        .describe(
          'The SQL query code blocks, each containing a descriptive name and an executable SQL script ' +
            'written in the current SQL dialect. The query will be automatically reformatted to be more readable.',
        ),
      created_table_names: z
        .array(z.string())
        .default([])
        .describe(
          'A list of created table names if they are generated within the SQL query statements ' +
            '(e.g., using `CREATE TABLE ...`).',
        ),
      folder: z
        .string()
        .default('')
        .describe(folderFieldDescription('transformation', 'transformations')),
      variables: z
        .array(variableDefinitionSchema)
        .nullish()
        .describe(
          'Variable definitions to attach to this transformation. ' +
            'Each entry specifies a name, type ("string" or "vault"), and an optional default value. ' +
            'On creation, both `None` (omitted) and `[]` (empty list) mean "do not attach variables" — ' +
            'no `keboola.variables` config is created. To remove variables from an existing transformation, ' +
            'use `update_sql_transformation` with `variables=[]`.',
        ),
    },
    handler: async (args) => {
      const clients = createKeboolaClients(config);
      const sqlDialect = await resolveSqlDialect(clients);
      const componentId = getSqlTransformationIdFromSqlDialect(sqlDialect);

      const payload = createTransformationConfiguration(
        args.sql_code_blocks,
        args.name,
        args.created_table_names,
      );

      const linksManager = await createLinksManager(config, clients);

      const newRaw = await configurationCreate(
        config,
        clients,
        componentId,
        args.name,
        args.description,
        payload,
      );
      const configurationId = String(newRaw.id);

      await setCfgCreationMetadata(clients, componentId, configurationId);

      const folder = args.folder.trim();
      let changeSummary: string | null = null;
      if (folder) {
        try {
          await setFolderMetadata(clients, componentId, configurationId, folder);
        } catch {
          logger.warn(`Unable to set folder metadata for "${componentId}"/"${configurationId}".`);
        }
      } else {
        try {
          const [total, existingFolders, lowerBound] = await getConfigFolders(clients, componentId);
          changeSummary = buildFolderHint(
            total,
            existingFolders,
            'SQL transformations',
            'update_sql_transformation',
            lowerBound,
          );
        } catch {
          logger.warn(`Unable to fetch transformation folders for "${componentId}".`);
        }
      }

      let varsResult: JsonDict | null = null;
      if (args.variables && args.variables.length > 0) {
        varsResult = await applyConfigurationVariables(
          config,
          clients,
          componentId,
          configurationId,
          args.variables,
        );
        if (varsResult !== null) {
          await setCfgUpdateMetadata(
            clients,
            componentId,
            configurationId,
            varsResult.version as number,
          );
        }
      }

      const output: ConfigToolOutput = {
        component_id: componentId,
        configuration_id: configurationId,
        description: args.description,
        version: ((varsResult ?? newRaw).version as number) ?? 0,
        timestamp: nowIso(),
        success: true,
        links: linksManager.getTransformationLinks(componentId, configurationId, args.name),
        change_summary: changeSummary,
      };
      return output;
    },
  });

  registerTool(server, {
    name: 'update_sql_transformation',
    title: 'Update SQL transformation',
    description:
      'Updates an existing SQL transformation configuration by modifying its SQL code, storage mappings, name or ' +
      'description. parameter_updates apply PARTIAL, granular diff operations to the transformation blocks/codes; ' +
      'storage is a complete replacement.',
    annotations: { destructiveHint: true },
    inputSchema: {
      change_description: z
        .string()
        .describe(
          'A clear, human-readable summary of what changed in this transformation update. ' +
            'Be specific: e.g., "Added JOIN with customers table", "Updated WHERE clause to filter active records".',
        ),
      configuration_id: z
        .string()
        .describe('The ID of the transformation configuration to update.'),
      name: z
        .string()
        .default('')
        .describe(
          'New name for the transformation. Only provide if changing the name. ' +
            'Name should be short (typically under 50 characters) and descriptive.',
        ),
      description: z
        .string()
        .default('')
        .describe(
          'New detailed description for the transformation. Only provide if changing the description. ' +
            'Should explain what the transformation does, data sources, and business logic. ' +
            'Leave empty to preserve the original description.',
        ),
      parameter_updates: z
        .array(tfParamUpdateSchema)
        .nullish()
        .describe(
          'List of operations to apply to the transformation structure (blocks, codes, SQL scripts). ' +
            'Each operation modifies specific elements using block_id and code_id identifiers. ' +
            'Only provide if updating SQL code or block structure - do not use for description or storage changes. ' +
            'Use get_configs first to retrieve the current transformation structure and identify the block_id and ' +
            'code_id values needed for your operations. IDs are automatically assigned. Available operations: ' +
            'add_block, remove_block, rename_block, add_code, remove_code, rename_code, set_code, add_script, ' +
            'str_replace.',
        ),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'Complete storage configuration for transformation input/output table mappings. ' +
            'Only provide if updating storage mappings - this replaces the ENTIRE storage configuration.',
        ),
      folder: z
        .string()
        .nullish()
        .describe(folderFieldDescription('transformation', 'transformations')),
      variables: z
        .array(variableDefinitionSchema)
        .nullish()
        .describe(
          'Variable definitions for this transformation. ' +
            'Provide a non-empty list to create or replace all variable definitions. ' +
            'Provide an empty list ([]) to remove all variables. ' +
            'Omit (None) to leave existing variables unchanged.',
        ),
    },
    handler: async (args) => {
      const clients = createKeboolaClients(config);
      const sqlDialect = await resolveSqlDialect(clients);
      const sqlTransformationId = getSqlTransformationIdFromSqlDialect(sqlDialect);
      const linksManager = await createLinksManager(config, clients);

      let configDetails: JsonDict;
      try {
        configDetails = await configurationDetail(
          clients,
          sqlTransformationId,
          args.configuration_id,
        );
      } catch (error) {
        if (error instanceof RawHttpError && error.status === 404) {
          throw new Error(
            `Configuration '${args.configuration_id}' was not found under SQL transformation component ` +
              `'${sqlTransformationId}'. If this is a Python or R transformation, use 'update_config' ` +
              `with component_id 'keboola.python-transformation-v2' or 'keboola.r-transformation-v2' ` +
              `instead of 'update_sql_transformation'.`,
          );
        }
        throw error;
      }

      const transformation = toComponentForValidation(
        await fetchComponent(clients, sqlTransformationId),
      );

      const updatedConfiguration = structuredClone(
        (configDetails.configuration as JsonDict) ?? {},
      ) as JsonDict;

      let msg = '';
      if (args.parameter_updates && args.parameter_updates.length > 0) {
        const currentRaw = (updatedConfiguration.parameters as
          | {
              blocks?: { name: string; codes: { name: string; script: string[] }[] }[];
            }
          | undefined) ?? { blocks: [] };
        const simplified = toSimplifiedParameters({ blocks: currentRaw.blocks ?? [] });
        const [updatedParams, message] = updateTransformationParameters(
          simplified,
          args.parameter_updates as TfParamUpdate[],
        );
        msg = message;
        const updatedRawParams = toRawParameters(updatedParams);
        const parametersCfg = validateRootParametersConfiguration(
          updatedRawParams as unknown as JsonDict,
          transformation,
          'Applying the "parameter_updates" resulted in an invalid configuration.',
          args.configuration_id,
        );
        updatedConfiguration.parameters = parametersCfg;
      }

      if (args.storage !== undefined && args.storage !== null) {
        updatedConfiguration.storage = validateRootStorageConfiguration(
          args.storage as JsonDict,
          transformation,
          'The "storage" field is not valid.',
          args.configuration_id,
        );
      }

      let varsConfigIdToDelete: string | null = null;
      if (args.variables !== undefined && args.variables !== null) {
        const res = await applyVarsToParentCfg(
          config,
          clients,
          sqlTransformationId,
          args.configuration_id,
          args.variables,
          updatedConfiguration,
        );
        varsConfigIdToDelete = res.varsConfigIdToDelete;
      }

      const updatedRaw = await configurationUpdate(
        config,
        clients,
        sqlTransformationId,
        args.configuration_id,
        updatedConfiguration,
        args.change_description,
        args.name,
        args.description,
      );

      if (varsConfigIdToDelete) {
        await deleteVariablesConfig(clients, varsConfigIdToDelete);
      }

      let folderHint: string | null = null;
      if (args.folder === undefined || args.folder === null) {
        try {
          const [total, existingFolders, lowerBound] = await getConfigFolders(
            clients,
            sqlTransformationId,
          );
          folderHint = buildFolderHint(
            total,
            existingFolders,
            'SQL transformations',
            'update_sql_transformation',
            lowerBound,
          );
        } catch {
          logger.warn(`Unable to fetch transformation folders for "${sqlTransformationId}".`);
        }
      } else {
        const folderStripped = args.folder.trim();
        if (folderStripped) {
          try {
            await setFolderMetadata(
              clients,
              sqlTransformationId,
              args.configuration_id,
              folderStripped,
            );
          } catch {
            logger.warn(`Unable to set folder metadata for "${sqlTransformationId}".`);
          }
        } else {
          await clearFolderMetadata(clients, sqlTransformationId, args.configuration_id);
        }
      }

      await setCfgUpdateMetadata(
        clients,
        sqlTransformationId,
        args.configuration_id,
        updatedRaw.version as number,
      );

      const changeSummary = [msg, folderHint].filter(Boolean).join(' ') || null;

      const output: ConfigToolOutput = {
        component_id: sqlTransformationId,
        configuration_id: args.configuration_id,
        description: (updatedRaw.description as string) || '',
        version: (updatedRaw.version as number) ?? 0,
        timestamp: nowIso(),
        success: true,
        links: linksManager.getTransformationLinks(
          sqlTransformationId,
          args.configuration_id,
          (updatedRaw.name as string) || '',
        ),
        change_summary: changeSummary,
      };
      return output;
    },
  });
};

// ============================================================================
// Shared update-payload builder for update_config / update_config_row.
// ============================================================================

const buildUpdatedConfigPayload = async (opts: {
  config: Config;
  clients: KeboolaClients;
  componentId: string;
  configurationId: string;
  configurationRowId?: string;
  parameterUpdates: ConfigParamUpdate[] | null;
  storage: JsonDict | null;
  processorsBefore: JsonDict[] | null;
  processorsAfter: JsonDict[] | null;
  isRow: boolean;
}): Promise<JsonDict> => {
  const { clients, componentId, configurationId, isRow } = opts;
  checkSuitable(isRow ? 'update_config_row' : 'update_config', componentId);

  const current = isRow
    ? await configurationRowDetail(clients, componentId, configurationId, opts.configurationRowId!)
    : await configurationDetail(clients, componentId, configurationId);
  const component = toComponentForValidation(await fetchComponent(clients, componentId));

  const payload = structuredClone((current.configuration as JsonDict) ?? {}) as JsonDict;

  if (opts.storage !== null) {
    payload.storage = isRow
      ? validateRowStorageConfiguration(
          opts.storage,
          component,
          'The "storage" field is not valid.',
          configurationId,
          opts.configurationRowId,
        )
      : validateRootStorageConfiguration(
          opts.storage,
          component,
          'The "storage" field is not valid.',
          configurationId,
        );
  }

  const fetchForValidation = async (id: string) =>
    toComponentForValidation(await fetchComponent(clients, id));

  if (opts.processorsBefore !== null) {
    const validated = await validateProcessorsConfiguration(
      fetchForValidation,
      opts.processorsBefore,
      'The "processors_before" field is not valid.',
    );
    setNestedValue(payload, 'processors.before', validated);
  }
  if (opts.processorsAfter !== null) {
    const validated = await validateProcessorsConfiguration(
      fetchForValidation,
      opts.processorsAfter,
      'The "processors_after" field is not valid.',
    );
    setNestedValue(payload, 'processors.after', validated);
  }

  if (opts.parameterUpdates && opts.parameterUpdates.length > 0) {
    const currentParams = (payload.parameters as JsonDict) ?? {};
    const updated = updateParams(currentParams, opts.parameterUpdates);
    const initial = isRow
      ? 'Applying the "parameter_updates" resulted in an invalid row configuration.'
      : 'Applying the "parameter_updates" resulted in an invalid configuration.';
    payload.parameters = isRow
      ? validateRowParametersConfiguration(
          updated,
          component,
          initial,
          configurationId,
          opts.configurationRowId,
        )
      : validateRootParametersConfiguration(updated, component, initial, configurationId);
  }

  return payload;
};

/**
 * Additive re-export of the config-mutation internals the `/preview/configuration`
 * endpoint reuses to build a config diff WITHOUT writing (port of the Python
 * `_prepare_mutator` reuse of `update_config_internal` / `update_config_row_internal`).
 *
 * These are the same functions the `update_config` / `update_config_row` handlers call:
 * `buildUpdatedConfigPayload` only issues GET requests (config + component fetch +
 * pure validation), so it is safe to run against a read-only client. Exposed here so
 * `preview.ts` can compute the original/updated configuration pair without duplicating
 * the validation logic; the tool handlers and their behavior are unchanged.
 */
export const configPreviewInternals = {
  buildUpdatedConfigPayload,
  configurationDetail,
  configurationRowDetail,
};

/** Sets/clears folder metadata or returns a hint (port of apply_folder_metadata). */
const applyFolderMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  folder: string | null,
  kind: string,
  toolName: string,
): Promise<string | null> => {
  if (folder === null) {
    try {
      const [total, existingFolders, lowerBound] = await getConfigFolders(clients, componentId);
      return buildFolderHint(total, existingFolders, kind, toolName, lowerBound);
    } catch {
      logger.warn(`Unable to fetch ${kind} folders for component "${componentId}".`);
      return null;
    }
  }
  const normalized = folder.trim();
  if (normalized) {
    try {
      await setFolderMetadata(clients, componentId, configurationId, normalized);
    } catch {
      logger.warn(`Unable to set folder metadata for "${componentId}"/"${configurationId}".`);
    }
  } else {
    await clearFolderMetadata(clients, componentId, configurationId);
  }
  return null;
};
