import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import type { KeboolaClients } from '@/clients/keboola';
import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  FLOW_TYPES,
  type FlowType,
  MetadataField,
  ORCHESTRATOR_COMPONENT_ID,
} from '@/constants';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { resourcePath } from '@/resource-path';
import {
  CREATED_BY_MCP,
  type MetadataItem,
  normalizeDependsOn,
  type Phase,
  type RawConfig,
  type Task,
  UPDATED_BY_MCP_PREFIX,
} from './model';
import type { toScheduleDetail } from './scheduler';

// Ported from tools/flow/utils.py + the storage/metadata helpers from components/utils.py.

// =============================================================================
// RESOURCES
// =============================================================================
//
// The bundled legacy flow schema and the example files are copied from the Python
// `keboola_mcp_server/resources` tree into `src/resources/flow/` (a path this module
// owns) and read at runtime via `fs` relative to the module's own location. This keeps
// them on disk (not inlined) and works under both vitest (running from `src/`) and a
// built `dist/` once the build copies the folder.

const RESOURCES_DIR = resourcePath('flow');

export const readResource = (filename: string): string =>
  readFileSync(join(RESOURCES_DIR, filename), 'utf-8');

let cachedLegacySchema: Record<string, unknown> | undefined;
export const loadLegacySchema = (): Record<string, unknown> => {
  cachedLegacySchema ??= JSON.parse(readResource('flow-schema.json')) as Record<string, unknown>;
  return cachedLegacySchema;
};

export const EXAMPLE_FILES: Record<FlowType, string> = {
  [CONDITIONAL_FLOW_COMPONENT_ID]: 'conditional_flow_examples.jsonl',
  [ORCHESTRATOR_COMPONENT_ID]: 'legacy_flow_examples.jsonl',
};

// =============================================================================
// STORAGE CONFIGURATION HELPERS
// =============================================================================
//
// Component-config CRUD uses the typed `@keboola/api-client` storage client
// (`clients.storage.componentsAndConfigurations.*`). Configuration *metadata*
// (GET/POST/DELETE `.../configs/{id}/metadata`) has no typed subpath in api-client,
// so those calls stay on the raw Storage client (mirrors Python's storage_client).

export const configurationCreate = (
  clients: KeboolaClients,
  componentId: string,
  name: string,
  description: string,
  configuration: Record<string, unknown>,
): Promise<RawConfig> =>
  clients.storage.componentsAndConfigurations.createConfiguration({
    branchId: clients.branchId,
    componentId,
    name,
    description,
    configuration,
  }) as Promise<RawConfig>;

export const configurationUpdate = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  configuration: Record<string, unknown>,
  changeDescription: string,
  updatedName?: string,
  updatedDescription?: string,
  isDisabled?: boolean | null,
): Promise<RawConfig> =>
  clients.storage.componentsAndConfigurations.updateConfiguration({
    branchId: clients.branchId,
    componentId,
    configId: configurationId,
    configuration,
    changeDescription,
    ...(updatedName ? { name: updatedName } : {}),
    ...(updatedDescription ? { description: updatedDescription } : {}),
    ...(isDisabled !== undefined && isDisabled !== null ? { isDisabled } : {}),
  }) as Promise<RawConfig>;

export const configurationDetail = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<RawConfig> =>
  clients.storage.componentsAndConfigurations.getConfiguration({
    branchId: clients.branchId,
    componentId,
    configId: configurationId,
  }) as Promise<RawConfig>;

export const configurationList = (
  clients: KeboolaClients,
  componentId: string,
): Promise<RawConfig[]> =>
  clients.storage.componentsAndConfigurations.getConfigurations({
    branchId: clients.branchId,
    componentId,
  }) as Promise<RawConfig[]>;

export const configurationDelete = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<void> => {
  await clients.storage.componentsAndConfigurations.deleteConfiguration({
    branchId: clients.branchId,
    componentId,
    configId: configurationId,
  });
};

// --- Configuration metadata (KEPT RAW: no typed api-client subpath for config metadata) ---

const configBase = (clients: KeboolaClients, componentId: string): string =>
  `branch/${clients.branchId}/components/${componentId}/configs`;

const configurationMetadataGet = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<MetadataItem[]> =>
  clients.rawStorage.get<MetadataItem[]>(
    `${configBase(clients, componentId)}/${configurationId}/metadata`,
  );

const configurationMetadataUpdate = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  metadata: Record<string, string>,
): Promise<MetadataItem[]> =>
  clients.rawStorage.post<MetadataItem[]>(
    `${configBase(clients, componentId)}/${configurationId}/metadata`,
    { body: { metadata: Object.entries(metadata).map(([key, value]) => ({ key, value })) } },
  );

const configurationMetadataDelete = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  metadataId: string,
): Promise<void> => {
  await clients.rawStorage.delete(
    `${configBase(clients, componentId)}/${configurationId}/metadata/${metadataId}`,
  );
};

const componentConfigurationsSearch = async (
  clients: KeboolaClients,
  componentId: string | undefined,
  metadataKeys: string[],
): Promise<RawConfig[]> => {
  if (!componentId && metadataKeys.length === 0) return [];
  return clients.storage.componentsAndConfigurations.searchComponentConfigurations(
    { branchId: clients.branchId },
    {
      ...(componentId ? { componentId } : {}),
      ...(metadataKeys.length > 0 ? { metadataKeys } : {}),
    },
  ) as Promise<RawConfig[]>;
};

export const metadataProperty = (
  metadata: MetadataItem[] | undefined,
  key: string,
): string | undefined => {
  // Most-recent wins (mirrors get_metadata_property): iterate and keep the last match.
  let value: string | undefined;
  for (const item of metadata ?? []) {
    if (item.key === key) value = item.value;
  }
  return value;
};

// --- MCP creation/update + folder metadata helpers (port of components/utils.py) ---

export const setCfgCreationMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<void> => {
  try {
    await configurationMetadataUpdate(clients, componentId, configurationId, {
      [CREATED_BY_MCP]: 'true',
    });
  } catch (e) {
    logger.error(
      { err: e },
      `Failed to set "${CREATED_BY_MCP}" metadata for configuration ${configurationId}`,
    );
  }
};

export const setCfgUpdateMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  configurationVersion: number,
): Promise<void> => {
  const key = `${UPDATED_BY_MCP_PREFIX}${configurationVersion}`;
  try {
    await configurationMetadataUpdate(clients, componentId, configurationId, { [key]: 'true' });
  } catch (e) {
    logger.error(
      { err: e },
      `Failed to set "${key}" metadata for configuration ${configurationId}`,
    );
  }
};

const setConfigurationFolderMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  folder: string,
): Promise<void> => {
  const normalized = folder.trim();
  if (!normalized) return;
  await configurationMetadataUpdate(clients, componentId, configurationId, {
    [MetadataField.CONFIGURATION_FOLDER_NAME]: normalized,
  });
};

const clearConfigurationFolderMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<void> => {
  const metadata = await configurationMetadataGet(clients, componentId, configurationId);
  for (const entry of metadata) {
    if (entry.key === MetadataField.CONFIGURATION_FOLDER_NAME) {
      if (entry.id === undefined) {
        logger.warn(
          `Unable to clear folder metadata for component "${componentId}", configuration "${configurationId}": metadata entry is missing "id".`,
        );
        continue;
      }
      await configurationMetadataDelete(clients, componentId, configurationId, entry.id);
    }
  }
};

export { setConfigurationFolderMetadata, clearConfigurationFolderMetadata };

export const getConfigFolders = async (
  clients: KeboolaClients,
  componentId: string,
): Promise<{ total: number; folders: string[]; lowerBound: boolean }> => {
  const folderConfigs = await componentConfigurationsSearch(clients, componentId, [
    MetadataField.CONFIGURATION_FOLDER_NAME,
  ]);
  const seen = new Set<string>();
  const folders: string[] = [];
  for (const cfg of folderConfigs) {
    for (const meta of (cfg.metadata as MetadataItem[]) ?? []) {
      if (meta.key === MetadataField.CONFIGURATION_FOLDER_NAME) {
        const name = (meta.value ?? '').trim();
        if (name && !seen.has(name)) {
          seen.add(name);
          folders.push(name);
        }
      }
    }
  }
  if (folderConfigs.length >= 20) {
    return { total: folderConfigs.length, folders, lowerBound: true };
  }
  const rawConfigs = await configurationList(clients, componentId);
  const total = rawConfigs.length;
  if (total < 20) return { total, folders: [], lowerBound: false };
  return { total, folders, lowerBound: false };
};

export const folderFieldDescription = (singular: string, plural: string): string =>
  `Folder name to organize this ${singular} in the Keboola UI. ` +
  `Pass an empty string to remove an existing folder assignment. ` +
  `Existing folder names are returned in the response change_summary when no folder is provided ` +
  `and there are 20 or more ${plural} in the project. ` +
  `If there are 20 or more ${plural}, you should assign one of the existing folders or ` +
  `create a new one that clearly reflects the ${singular} purpose.`;

export const buildFolderHint = (
  total: number,
  existingFolders: string[],
  configLabel: string,
  updateTool: string,
  lowerBound = false,
): string | null => {
  if (total < 20) return null;
  const countStr = lowerBound ? `at least ${total}` : String(total);
  let hint = `Note: This project already has ${countStr} ${configLabel}. Consider organizing them with folders. `;
  if (existingFolders.length > 0) {
    hint +=
      `Existing folders: ${existingFolders.join(', ')}. ` +
      `Call ${updateTool} with a folder= parameter to assign this to one.`;
  } else {
    hint += `No folders have been created yet. Call ${updateTool} with a folder= parameter to start organizing.`;
  }
  return hint;
};

// =============================================================================
// PROJECT CONTEXT (only the bits the flow tools need: name + conditional_flows)
// =============================================================================

export type ProjectContext = { projectName: string; conditionalFlows: boolean };

export const getProjectContext = async (clients: KeboolaClients): Promise<ProjectContext> => {
  const token = (await clients.storage.tokens.verify()) as {
    owner?: { name?: string; features?: unknown };
  };
  const owner = token.owner ?? {};
  const features = owner.features;
  // `conditional_flows` is enabled unless the project carries the `hide-conditional-flows` feature.
  let hidden = false;
  if (Array.isArray(features)) {
    hidden = features.includes('hide-conditional-flows');
  } else if (features && typeof features === 'object') {
    hidden = 'hide-conditional-flows' in (features as Record<string, unknown>);
  }
  return { projectName: owner.name ?? '', conditionalFlows: !hidden };
};

export const assertConditionalAllowed = (
  flowType: FlowType,
  project: ProjectContext,
  examples = false,
): void => {
  if (flowType === CONDITIONAL_FLOW_COMPONENT_ID && !project.conditionalFlows) {
    throw new Error(
      `Conditional flows are not supported in this project. ` +
        `Project "${project.projectName}" has conditional_flows=false. ` +
        `If you want to use conditional flows, please enable them in your project settings. ` +
        `Otherwise, use flow_type="${ORCHESTRATOR_COMPONENT_ID}" for legacy flow${examples ? ' examples' : 's'} instead.`,
    );
  }
};

// =============================================================================
// FLOW CONFIGURATION BUILDING (port of utils.get_flow_configuration + ensure_*)
// =============================================================================

/** Legacy phase normalization (port of ensure_legacy_phase_ids). */
const ensureLegacyPhaseIds = (phases: Phase[]): Phase[] => {
  const processed: Phase[] = [];
  const usedIds = new Set<string | number>();

  phases.forEach((phase, i) => {
    const data: Phase = { ...phase };
    if (data.id === undefined || data.id === null || data.id === '' || data.id === 0) {
      let phaseId = i + 1;
      while (usedIds.has(phaseId)) phaseId += 1;
      data.id = phaseId;
    }
    if (data.name === undefined) data.name = `Phase ${data.id}`;

    if (typeof data.name !== 'string' || data.name.length < 1) {
      throw new Error(`Invalid phase configuration: phase ${data.id} has an invalid name.`);
    }
    const normalized: Phase = {
      id: data.id,
      name: data.name,
      description: typeof data.description === 'string' ? data.description : '',
      dependsOn: normalizeDependsOn(data),
    };
    usedIds.add(normalized.id as string | number);
    processed.push(normalized);
  });

  return processed;
};

/** Legacy task normalization (port of ensure_legacy_task_ids). */
const ensureLegacyTaskIds = (tasks: Task[]): Task[] => {
  const processed: Task[] = [];
  const usedIds = new Set<string | number>();
  // Phase IDs are small sequential numbers; task IDs start at 20001 to avoid collisions.
  let taskCounter = 20001;

  for (const task of tasks) {
    const data: Task = { ...task };
    if (data.id === undefined || data.id === null || data.id === '' || data.id === 0) {
      while (usedIds.has(taskCounter)) taskCounter += 1;
      data.id = taskCounter;
      taskCounter += 1;
    }
    if (data.name === undefined) data.name = `Task ${data.id}`;
    if (data.task === undefined) {
      throw new Error(`Task ${data.id} missing 'task' configuration`);
    }
    const taskObj = (data.task as Record<string, unknown>) ?? {};
    if (taskObj.componentId === undefined) {
      throw new Error(`Task ${data.id} missing componentId in task configuration`);
    }
    if (taskObj.mode === undefined) taskObj.mode = 'run';
    data.task = taskObj;

    if (data.phase === undefined || data.phase === null) {
      throw new Error(`Invalid task configuration: task ${data.id} missing phase.`);
    }

    const normalized: Task = {
      id: data.id,
      name: data.name,
      phase: data.phase,
      enabled: data.enabled ?? true,
      continueOnFailure:
        data.continueOnFailure ?? data.continue_on_failure ?? data['continue-on-failure'] ?? false,
      task: data.task,
    };
    usedIds.add(normalized.id as string | number);
    processed.push(normalized);
  }

  return processed;
};

/**
 * Conditional phase `model_dump(exclude_unset=True)` semantics: drop `next` when it is empty
 * or a single transition with goto=null (lets the Designer UI render ending phases cleanly).
 */
const dumpConditionalPhase = (phase: Phase): Phase => {
  const out: Phase = { ...phase };
  // Normalize a `next` provided as null/undefined to omitted.
  if (out.next === undefined || out.next === null) {
    delete out.next;
  } else if (Array.isArray(out.next)) {
    const next = out.next as Record<string, unknown>[];
    if (next.length === 0) {
      delete out.next;
    } else if (next.length === 1 && (next[0]?.goto === null || next[0]?.goto === undefined)) {
      delete out.next;
    }
  }
  return out;
};

/** Port of utils.get_flow_configuration. */
export const getFlowConfiguration = (
  phases: Phase[] | null,
  tasks: Task[] | null,
  flowType: FlowType,
): Record<string, unknown> => {
  if (flowType === ORCHESTRATOR_COMPONENT_ID) {
    return {
      phases: ensureLegacyPhaseIds(phases ?? []),
      tasks: ensureLegacyTaskIds(tasks ?? []),
    };
  }
  return {
    phases: (phases ?? []).map(dumpConditionalPhase),
    tasks: tasks ?? [],
  };
};

// =============================================================================
// READ-PATH MODELS (port of model.Flow / FlowSummary)
// =============================================================================

export const toFlowSummary = (raw: RawConfig, flowComponentId: FlowType, nSchedules: number) => {
  const config = (raw.configuration as Record<string, unknown>) ?? {};
  const metadata = (raw.metadata as MetadataItem[]) ?? [];
  return {
    component_id: flowComponentId,
    configuration_id: String(raw.id ?? ''),
    name: raw.name ?? '',
    description: raw.description ?? null,
    version: raw.version ?? 0,
    is_disabled: raw.isDisabled ?? false,
    is_deleted: raw.isDeleted ?? false,
    phases_count: ((config.phases as unknown[]) ?? []).length,
    tasks_count: ((config.tasks as unknown[]) ?? []).length,
    schedules_count: nSchedules,
    folder: metadataProperty(metadata, MetadataField.CONFIGURATION_FOLDER_NAME) ?? '',
    created: raw.created ?? null,
    updated: raw.updated ?? null,
  };
};

export const toFlowDetail = (
  raw: RawConfig,
  flowComponentId: FlowType,
  links: Link[],
  schedules: ReturnType<typeof toScheduleDetail>[],
  scheduleLinks: Link[],
) => {
  const config = (raw.configuration as Record<string, unknown>) ?? {};
  const metadata = (raw.metadata as MetadataItem[]) ?? [];
  return {
    component_id: flowComponentId,
    configuration_id: String(raw.id ?? ''),
    name: raw.name ?? '',
    description: raw.description ?? null,
    version: raw.version ?? 0,
    is_disabled: raw.isDisabled ?? false,
    is_deleted: raw.isDeleted ?? false,
    configuration: {
      phases: (config.phases as unknown[]) ?? [],
      tasks: (config.tasks as unknown[]) ?? [],
    },
    change_description: raw.changeDescription ?? null,
    configuration_metadata: metadata,
    folder: metadataProperty(metadata, MetadataField.CONFIGURATION_FOLDER_NAME) ?? '',
    created: raw.created ?? null,
    updated: raw.updated ?? null,
    schedules: {
      schedules,
      n_schedules: schedules.length,
      links: scheduleLinks,
    },
    links,
  };
};

/** Resolve a flow across all flow types (port of utils.resolve_flow_by_id). */
export const resolveFlowById = async (
  clients: KeboolaClients,
  flowId: string,
): Promise<{ raw: RawConfig; flowType: FlowType }> => {
  for (const flowType of FLOW_TYPES) {
    try {
      const raw = await configurationDetail(clients, flowType, flowId);
      return { raw, flowType };
    } catch {
      continue;
    }
  }
  throw new Error(`Flow configuration "${flowId}" not found`);
};

// =============================================================================
// TOOL OUTPUT BUILDING
// =============================================================================

export const buildFlowToolOutput = (opts: {
  configurationId: string;
  componentId: string;
  description: string;
  version: number;
  links: Link[];
  response?: string | null;
  changeSummary?: string | null;
}) => ({
  configuration_id: opts.configurationId,
  component_id: opts.componentId,
  description: opts.description,
  version: opts.version,
  timestamp: new Date().toISOString(),
  response: opts.response ?? null,
  change_summary: opts.changeSummary ?? null,
  success: true,
  links: opts.links,
});

export const flowLabel = (flowType: FlowType): string =>
  flowType === ORCHESTRATOR_COMPONENT_ID ? 'legacy flows' : 'conditional flows';
