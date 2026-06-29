import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { Ajv } from 'ajv';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager, type KeboolaClients } from '@/clients/keboola';
import { createRawClient, type RawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  FLOW_TYPES,
  type FlowType,
  MetadataField,
  ORCHESTRATOR_COMPONENT_ID,
} from '@/constants';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import { resourcePath } from '@/resource-path';
import { fetchComponent } from '@/tools/components';

// Ported from tools/flow/{tools,model,utils,scheduler,scheduler_model}.py and
// clients/scheduler.py. The scheduler service has no @keboola/api-client subpath,
// so it is built locally as a raw client (see schedulerClient below).

// =============================================================================
// RESOURCES
// =============================================================================
//
// The bundled legacy flow schema and the example files are copied from the Python
// `keboola_mcp_server/resources` tree into `src/resources/flow/` (a path this module
// owns) and read at runtime via `fs` relative to the module's own location
// (`import.meta.url`). This keeps them on disk (not inlined) and works under both
// vitest (running from `src/`) and a built `dist/` once the build copies the folder.

// MCP tracking metadata keys (not yet in the shared constants module; kept local to avoid
// editing a cross-module file). Mirror config.py CREATED_BY_MCP /
// UPDATED_BY_MCP_PREFIX.
const CREATED_BY_MCP = 'KBC.MCP.createdBy';
const UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.';

const RESOURCES_DIR = resourcePath('flow');

const readResource = (filename: string): string =>
  readFileSync(join(RESOURCES_DIR, filename), 'utf-8');

let cachedLegacySchema: Record<string, unknown> | undefined;
const loadLegacySchema = (): Record<string, unknown> => {
  cachedLegacySchema ??= JSON.parse(readResource('flow-schema.json')) as Record<string, unknown>;
  return cachedLegacySchema;
};

const EXAMPLE_FILES: Record<FlowType, string> = {
  [CONDITIONAL_FLOW_COMPONENT_ID]: 'conditional_flow_examples.jsonl',
  [ORCHESTRATOR_COMPONENT_ID]: 'legacy_flow_examples.jsonl',
};

// =============================================================================
// SCHEDULER CLIENT (local raw client; no api-client subpath exists)
// =============================================================================

type ScheduleApiResponse = {
  id: string;
  configurationId?: string;
  configuration_id?: string;
  schedule: { cronTab?: string; cron_tab?: string; timezone: string; state: string };
  target?: Record<string, unknown>;
  executions?: {
    jobId?: string;
    job_id?: string;
    executionTime?: string;
    execution_time?: string;
  }[];
};

type SchedulerClient = {
  activateSchedule: (scheduleConfigId: string) => Promise<ScheduleApiResponse>;
  listSchedulesByConfigId: (
    componentId: string,
    configurationId: string,
  ) => Promise<ScheduleApiResponse[]>;
  deleteSchedule: (scheduleConfigId: string) => Promise<void>;
};

/** Builds a Scheduler API client against `deriveServiceUrls(...).scheduler`. */
const createSchedulerClient = (config: Config): SchedulerClient => {
  const urls = deriveServiceUrls(config.storageApiUrl ?? '');
  const raw: RawClient = createRawClient({
    baseUrl: urls.scheduler,
    token: config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken,
  });
  return {
    activateSchedule: (scheduleConfigId) =>
      raw.post<ScheduleApiResponse>('schedules', { body: { configurationId: scheduleConfigId } }),
    listSchedulesByConfigId: (componentId, configurationId) =>
      raw.get<ScheduleApiResponse[]>('schedules', {
        params: { componentId, configurationId },
      }),
    deleteSchedule: async (scheduleConfigId) => {
      await raw.delete(`configurations/${scheduleConfigId}`);
    },
  };
};

// =============================================================================
// STORAGE CONFIGURATION HELPERS (raw Storage client, mirrors Python storage_client)
// =============================================================================

type RawConfig = Record<string, unknown>;
type MetadataItem = { id?: string; key?: string; value?: string };

const configBase = (clients: KeboolaClients, componentId: string): string =>
  `branch/${clients.branchId}/components/${componentId}/configs`;

const configurationCreate = (
  clients: KeboolaClients,
  componentId: string,
  name: string,
  description: string,
  configuration: Record<string, unknown>,
): Promise<RawConfig> =>
  clients.rawStorage.post<RawConfig>(configBase(clients, componentId), {
    body: { name, description, configuration },
  });

const configurationUpdate = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  configuration: Record<string, unknown>,
  changeDescription: string,
  updatedName?: string,
  updatedDescription?: string,
  isDisabled?: boolean | null,
): Promise<RawConfig> => {
  const body: Record<string, unknown> = { configuration, changeDescription };
  if (updatedName) body.name = updatedName;
  if (updatedDescription) body.description = updatedDescription;
  if (isDisabled !== undefined && isDisabled !== null) body.isDisabled = isDisabled;
  return clients.rawStorage.put<RawConfig>(
    `${configBase(clients, componentId)}/${configurationId}`,
    { body },
  );
};

const configurationDetail = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<RawConfig> =>
  clients.rawStorage.get<RawConfig>(`${configBase(clients, componentId)}/${configurationId}`);

const configurationList = (clients: KeboolaClients, componentId: string): Promise<RawConfig[]> =>
  clients.rawStorage.get<RawConfig[]>(configBase(clients, componentId));

const configurationDelete = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<void> => {
  await clients.rawStorage.delete(`${configBase(clients, componentId)}/${configurationId}`);
};

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
  const params: Record<string, string> = {};
  if (componentId) params.componentId = componentId;
  metadataKeys.forEach((key, i) => {
    params[`metadataKeys[${i}]`] = key;
  });
  return clients.rawStorage.get<RawConfig[]>(
    `branch/${clients.branchId}/search/component-configurations`,
    { params },
  );
};

const metadataProperty = (
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

const setCfgCreationMetadata = async (
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

const setCfgUpdateMetadata = async (
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

const getConfigFolders = async (
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

const folderFieldDescription = (singular: string, plural: string): string =>
  `Folder name to organize this ${singular} in the Keboola UI. ` +
  `Pass an empty string to remove an existing folder assignment. ` +
  `Existing folder names are returned in the response change_summary when no folder is provided ` +
  `and there are 20 or more ${plural} in the project. ` +
  `If there are 20 or more ${plural}, you should assign one of the existing folders or ` +
  `create a new one that clearly reflects the ${singular} purpose.`;

const buildFolderHint = (
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

type ProjectContext = { projectName: string; conditionalFlows: boolean };

const getProjectContext = async (clients: KeboolaClients): Promise<ProjectContext> => {
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

const assertConditionalAllowed = (
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

type Phase = Record<string, unknown>;
type Task = Record<string, unknown>;

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

const normalizeDependsOn = (phase: Phase): (string | number)[] => {
  const value = phase.dependsOn ?? phase.depends_on ?? phase['depends-on'] ?? [];
  if (!Array.isArray(value)) {
    throw new Error(`Invalid phase configuration: dependsOn must be a list.`);
  }
  return value as (string | number)[];
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
const getFlowConfiguration = (
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
// STRUCTURAL VALIDATION (port of utils._validate_*_flow_structure)
// =============================================================================

const checkCircularDependencies = (
  edges: Map<string | number, (string | number)[]>,
  allNodeIds: Set<string | number>,
): void => {
  const visited = new Set<string | number>();

  const hasCycle = (
    nodeId: string | number,
    recStack: Set<string | number>,
    path: (string | number)[],
  ): (string | number)[] | null => {
    visited.add(nodeId);
    recStack.add(nodeId);
    path.push(nodeId);
    for (const target of edges.get(nodeId) ?? []) {
      if (!visited.has(target)) {
        const cycle = hasCycle(target, recStack, path);
        if (cycle) return cycle;
      } else if (recStack.has(target)) {
        const idx = path.indexOf(target);
        return idx >= 0 ? [...path.slice(idx), target] : [nodeId, target];
      }
    }
    path.pop();
    recStack.delete(nodeId);
    return null;
  };

  for (const nodeId of allNodeIds) {
    if (!visited.has(nodeId)) {
      const cyclePath = hasCycle(nodeId, new Set(), []);
      if (cyclePath) {
        throw new Error(`Circular dependency detected: ${cyclePath.map(String).join(' -> ')}`);
      }
    }
  }
};

const validateLegacyFlowStructure = (phases: Phase[], tasks: Task[]): void => {
  const phaseIds = new Set(phases.map((p) => p.id as string | number));
  for (const phase of phases) {
    for (const dep of normalizeDependsOn(phase)) {
      if (!phaseIds.has(dep)) {
        throw new Error(`Phase ${phase.id} depends on non-existent phase ${dep}`);
      }
    }
  }
  for (const task of tasks) {
    if (!phaseIds.has(task.phase as string | number)) {
      throw new Error(`Task ${task.id} references non-existent phase ${task.phase}`);
    }
  }
  const edges = new Map<string | number, (string | number)[]>(
    phases.map((p) => [p.id as string | number, normalizeDependsOn(p)]),
  );
  checkCircularDependencies(edges, phaseIds);
};

const reachableIds = (
  start: string,
  edges: Map<string, Set<string>>,
  visited: Set<string>,
): Set<string> => {
  visited.add(start);
  for (const target of edges.get(start) ?? []) {
    if (!visited.has(target)) reachableIds(target, edges, visited);
  }
  return visited;
};

const validateConditionalFlowStructure = (phases: Phase[], tasks: Task[]): void => {
  // Duplicate phase / task ids.
  const countOccurrences = (ids: unknown[]): Map<unknown, number> => {
    const counter = new Map<unknown, number>();
    for (const id of ids) counter.set(id, (counter.get(id) ?? 0) + 1);
    return counter;
  };
  const phaseCounter = countOccurrences(phases.map((p) => p.id));
  const dupPhases = [...phaseCounter.entries()].filter(([, c]) => c > 1).map(([id]) => id);
  if (dupPhases.length > 0) {
    throw new Error(`Flow contains duplicate phase IDs: ${JSON.stringify(dupPhases)}.`);
  }
  const taskCounter = countOccurrences(tasks.map((t) => t.id));
  const dupTasks = [...taskCounter.entries()].filter(([, c]) => c > 1).map(([id]) => id);
  if (dupTasks.length > 0) {
    throw new Error(`Flow contains duplicate task IDs: ${JSON.stringify(dupTasks)}.`);
  }

  const phaseIds = new Set(phases.map((p) => String(p.id)));
  for (const task of tasks) {
    if (!phaseIds.has(String(task.phase))) {
      throw new Error(`Task ${task.id} references non-existent phase ${task.phase}`);
    }
  }

  const succPhases = new Map<string, Set<string>>();
  const predPhases = new Map<string, Set<string>>();
  const endingPhases = new Set<string>();
  const ensure = (map: Map<string, Set<string>>, key: string): Set<string> => {
    let set = map.get(key);
    if (!set) {
      set = new Set();
      map.set(key, set);
    }
    return set;
  };

  for (const phase of phases) {
    const pid = String(phase.id);
    const next = (phase.next as { goto?: string | null }[] | undefined) ?? [];
    if (next.length === 0) {
      endingPhases.add(pid);
    } else {
      for (const transition of next) {
        if (transition.goto === null || transition.goto === undefined) {
          endingPhases.add(pid);
        } else {
          if (!phaseIds.has(transition.goto)) {
            throw new Error(
              `Phase ${phase.id} has a transition that references non-existent phase ${transition.goto}`,
            );
          }
          ensure(succPhases, pid).add(transition.goto);
          ensure(predPhases, transition.goto).add(pid);
        }
      }
    }
  }

  if (endingPhases.size === 0) {
    throw new Error(
      'Flow has no ending phases. Each conditional flow must have at least one ending phase. Any ending phase ' +
        'has either no transitions at all or contains transition with goto: null referencing end of the flow.',
    );
  }

  const entryPhases = [...phaseIds].filter((pid) => (predPhases.get(pid)?.size ?? 0) === 0);
  if (entryPhases.length === 0) {
    throw new Error(
      'Flow has no entry phase. Each conditional flow must have exactly one entry phase. An entry phase has no ' +
        'incoming transitions; no transition from another phase leads to it.',
    );
  }
  if (entryPhases.length > 1) {
    throw new Error(
      `Flow has multiple entry phases (${entryPhases.length}): ${JSON.stringify(entryPhases)}. Each conditional flow must have ` +
        'exactly one entry phase. Either merge the entry phases into one or redefine the transitions to form a ' +
        'single entry phase.',
    );
  }

  const reachable = reachableIds(entryPhases[0]!, succPhases, new Set());
  if (reachable.size !== phaseIds.size || [...phaseIds].some((id) => !reachable.has(id))) {
    const unreachable = [...phaseIds].filter((id) => !reachable.has(id));
    throw new Error(
      `Flow has phases that are not reachable from the entry phase (${entryPhases[0]}): ` +
        `${JSON.stringify(unreachable)}. All phases must be reachable from the entry phase by a valid path of ` +
        'transitions.',
    );
  }

  const edges = new Map<string | number, (string | number)[]>();
  for (const [pid, targets] of succPhases) edges.set(pid, [...targets]);
  checkCircularDependencies(edges, phaseIds);
};

const validateFlowStructure = (
  flowConfiguration: Record<string, unknown>,
  flowType: FlowType,
): void => {
  const phases = (flowConfiguration.phases as Phase[]) ?? [];
  const tasks = (flowConfiguration.tasks as Task[]) ?? [];
  if (flowType === ORCHESTRATOR_COMPONENT_ID) {
    validateLegacyFlowStructure(phases, tasks);
  } else {
    validateConditionalFlowStructure(phases, tasks);
  }
};

// =============================================================================
// SCHEMA VALIDATION (jsonschema, port of validation.validate_flow_configuration_against_schema)
// =============================================================================

// `strict: false` mirrors Python jsonschema's leniency (e.g. `minLength` on integer-or-string
// ids, `$ref` next to sibling keywords); invalid schemas there "continue as valid", so we never
// hard-fail on schema-author mistakes — only on data that violates a usable schema.
const ajv = new Ajv({ strict: false, allErrors: true });

const validateFlowConfigurationAgainstSchema = (
  flow: Record<string, unknown>,
  schema: Record<string, unknown>,
): void => {
  let validate;
  try {
    validate = ajv.compile(schema);
  } catch (e) {
    // Schema itself is unusable — Python logs and treats the data as valid.
    logger.error({ err: e }, 'The validation schema is not valid; skipping schema validation.');
    return;
  }
  if (!validate(flow)) {
    const message = (validate.errors ?? [])
      .map((err) => `${err.instancePath || '<root>'} ${err.message}`)
      .join('; ');
    throw new Error(`Flow configuration does not follow the schema: ${message}`);
  }
};

/**
 * Resolve the JSON schema for a flow type. Legacy orchestrator stays bundled; conditional
 * (`keboola.flow`) is fetched live from the Developer Portal (AI catalog) via fetchComponent.
 */
const resolveFlowSchema = async (
  clients: KeboolaClients,
  flowType: FlowType,
): Promise<Record<string, unknown>> => {
  if (flowType !== CONDITIONAL_FLOW_COMPONENT_ID) {
    return loadLegacySchema();
  }
  const failureMessage =
    'Could not retrieve the conditional flow (keboola.flow) configuration schema from the ' +
    'Developer Portal. The schema is required to create or validate conditional flows. ' +
    'Please retry; if this persists the keboola.flow component schema may be unavailable on ' +
    'this stack.';
  let component: Record<string, unknown>;
  try {
    component = await fetchComponent(clients, CONDITIONAL_FLOW_COMPONENT_ID);
  } catch (e) {
    throw new Error(failureMessage, { cause: e });
  }
  const schema = (component.configurationSchema ?? component.configuration_schema) as
    | Record<string, unknown>
    | undefined;
  if (!schema || Object.keys(schema).length === 0) {
    throw new Error(failureMessage);
  }
  return schema;
};

const getSchemaAsMarkdown = async (
  clients: KeboolaClients,
  flowType: FlowType,
): Promise<string> => {
  const schema = await resolveFlowSchema(clients, flowType);
  return `\`\`\`json\n${JSON.stringify(schema, null, 2)}\n\`\`\``;
};

// =============================================================================
// READ-PATH MODELS (port of model.Flow / FlowSummary)
// =============================================================================

const toFlowSummary = (raw: RawConfig, flowComponentId: FlowType, nSchedules: number) => {
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

const toFlowDetail = (
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
const resolveFlowById = async (
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
// SCHEDULER MODELS + LOGIC (port of scheduler.py + scheduler_model.py)
// =============================================================================

const toScheduleDetail = (api: ScheduleApiResponse) => ({
  scheduleId: api.configurationId ?? api.configuration_id ?? '',
  timezone: api.schedule.timezone,
  state: api.schedule.state,
  cronTab: api.schedule.cronTab ?? api.schedule.cron_tab ?? '',
  target_executions: (api.executions ?? []).map((exec) => ({
    jobId: exec.jobId ?? exec.job_id ?? null,
    executionTime: exec.executionTime ?? exec.execution_time ?? null,
  })),
});

const SCHEDULER_COMPONENT_ID = 'keboola.scheduler';

const CRON_TAB_INSTRUCTIONS = `
Cron Tab Expression should be in the format: \`* * * * *\`.
Field order:
1. Minute (0-59)
2. Hour (0-23)
3. Day of month (1-31, or L for last day of month)
4. Month (1-12)
5. Day of week (0-6, where 0 = Sunday)

Examples:
1. schedule daily at 1:00 PM and 1:00 AM would be \`0 1,13 * * *\`
2. schedule weekly on Monday at 9:00 AM would be \`0 9 * * 1\`
3. schedule monthly on the 1st and 20th day of the month at 10:00 AM would be \`0 10 1,20 * *\`
4. schedule yearly on the 1st of january and august at 11:00 AM would be \`0 11 1 1,8 *\`
5. schedule hourly every 15 minutes would be \`0,15,30,45 * * * *\`
6. schedule monthly on the last day of the month at 10:00 AM would be \`0 10 L * *\`
`;

/** Port of scheduler.validate_cron_tab. */
const validateCronTab = (cronTab: string | null | undefined): void => {
  if (cronTab === null || cronTab === undefined) return;
  try {
    const parts = cronTab.trim().split(/\s+/);
    if (parts.length !== 5) {
      throw new Error(
        `Cron expression must have exactly 5 parts got: ${cronTab} which has ${parts.length} parts.`,
      );
    }
    const toIntList = (field: string, allowL = false): { parts: number[]; hasL: boolean } => {
      if (field === '*') return { parts: [], hasL: false };
      let hasL = false;
      const nums: number[] = [];
      for (let x of field.split(',')) {
        x = x.trim();
        if (allowL && x.toUpperCase() === 'L') {
          hasL = true;
        } else if (/^-?\d+$/.test(x)) {
          nums.push(Number(x));
        } else {
          throw new Error(`Cron expression must have only digits got: ${field} in "${cronTab}".`);
        }
      }
      if (allowL && hasL && nums.length > 0) {
        throw new Error('Day of month must use either `L` or numeric values, not both.');
      }
      return { parts: nums, hasL };
    };

    const { parts: minutes } = toIntList(parts[0]!.trim());
    const { parts: hours } = toIntList(parts[1]!.trim());
    const { parts: days, hasL: hasLastDay } = toIntList(parts[2]!.trim(), true);
    const { parts: months } = toIntList(parts[3]!.trim());
    const { parts: weekdays } = toIntList(parts[4]!.trim());

    if (minutes.some((x) => x < 0 || x > 59)) {
      throw new Error(`Minutes of hour \`M _ _ _ _\` must be between 0 and 59, got: ${parts[0]}`);
    }
    if (hours.some((x) => x < 0 || x > 23)) {
      throw new Error(`Hours of day \`_ H _ _ _\` must be between 0 and 23, got: ${parts[1]}`);
    }
    if (days.some((x) => x < 1 || x > 31)) {
      throw new Error(`Days of month \`_ _ D _ _\`must be between 1 and 31, got: ${parts[2]}`);
    }
    if (months.some((x) => x < 1 || x > 12)) {
      throw new Error(`Months of year \`_ _ _ M _\` must be between 1 and 12, got: ${parts[3]}`);
    }
    if (weekdays.some((x) => x < 0 || x > 6)) {
      throw new Error(
        `Days of week \`_ _ _ _ W\` must be between 0=Sunday and 6=Saturday, got: ${parts[4]}`,
      );
    }
    if (months.length > 0 && days.length === 0 && !hasLastDay) {
      throw new Error(
        'Months of year must be specified with days of month. Example: `35 12 31 1,3 *`',
      );
    }
    if ((days.length > 0 || hasLastDay) && hours.length === 0) {
      throw new Error('Days of month must be specified with hours of day. Example: `55 12 31 * *`');
    }
    if (hours.length > 0 && minutes.length === 0) {
      throw new Error(
        'Hours of day must be specified with minutes of hour. Example: `55 12 * * *`',
      );
    }
    if (weekdays.length > 0 && hours.length === 0) {
      throw new Error('Days of week must be specified with hours of day. Example: `55 12 * * 0`');
    }
    if (weekdays.length > 0 && (days.length > 0 || months.length > 0 || hasLastDay)) {
      throw new Error('Days of week must not be specified with days of month nor months of year.');
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`Invalid cron tab expression: ${msg}.\n${CRON_TAB_INSTRUCTIONS}`);
  }
};

type ScheduleRequest = {
  action: 'add' | 'update' | 'remove';
  schedule_id?: string | null;
  timezone?: string | null;
  cron_tab?: string | null;
  state?: 'enabled' | 'disabled' | null;
};

type SimplifiedSchedule = {
  scheduleId: string | null;
  cronTab: string;
  timezone: string;
  state: string;
};

const listSchedulesForConfig = async (
  scheduler: SchedulerClient,
  componentId: string,
  configurationId: string,
): Promise<ReturnType<typeof toScheduleDetail>[]> => {
  const apiSchedules = await scheduler.listSchedulesByConfigId(componentId, configurationId);
  return apiSchedules.map(toScheduleDetail);
};

/** Compute original/updated/new schedulers (port of scheduler._update_schedulers_internal). */
const updateSchedulersInternal = async (
  scheduler: SchedulerClient,
  configurationId: string,
  componentId: string,
  schedules: ScheduleRequest[],
): Promise<{
  original: Map<string, SimplifiedSchedule>;
  updated: Map<string, SimplifiedSchedule | null>;
  added: SimplifiedSchedule[];
}> => {
  const current = await listSchedulesForConfig(scheduler, componentId, configurationId);
  const original = new Map<string, SimplifiedSchedule>();
  for (const s of current) {
    original.set(s.scheduleId, {
      scheduleId: s.scheduleId,
      cronTab: s.cronTab,
      timezone: s.timezone,
      state: s.state,
    });
  }
  const added: SimplifiedSchedule[] = [];
  const updated = new Map<string, SimplifiedSchedule | null>();

  for (const request of schedules) {
    if (request.action === 'add') {
      if (request.cron_tab == null) {
        throw new Error('cron_tab is required to add a schedule.');
      }
      validateCronTab(request.cron_tab);
      added.push({
        scheduleId: request.schedule_id ?? null,
        cronTab: request.cron_tab,
        timezone: request.timezone ?? 'UTC',
        state: request.state ?? 'enabled',
      });
    } else if (request.action === 'update') {
      const id = request.schedule_id ?? '';
      const existing = original.get(id);
      if (!existing) {
        throw new Error(
          `Schedule (ID: ${request.schedule_id}) cannot be updated because it was not found in the existing schedulers.`,
        );
      }
      if (request.cron_tab != null) validateCronTab(request.cron_tab);
      updated.set(id, {
        scheduleId: existing.scheduleId,
        cronTab: request.cron_tab ?? existing.cronTab,
        timezone: request.timezone ?? existing.timezone,
        state: request.state ?? existing.state,
      });
    } else if (request.action === 'remove') {
      const id = request.schedule_id ?? '';
      if (!original.has(id)) {
        throw new Error(
          `Schedule (ID: ${request.schedule_id}) cannot be removed because it was not found in the existing schedulers.`,
        );
      }
      updated.set(id, null);
    } else {
      throw new Error(`Invalid action for schedulers: ${(request as { action: string }).action}.`);
    }
  }
  return { original, updated, added };
};

const createSchedule = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  targetComponentId: string,
  targetConfigurationId: string,
  cronTab: string,
  timezone: string,
  state: string,
): Promise<ReturnType<typeof toScheduleDetail>> => {
  const scheduleName = `Schedule for ${targetConfigurationId}`;
  const schedulerConfig = {
    schedule: { cronTab, timezone, state },
    target: { componentId: targetComponentId, configurationId: targetConfigurationId, mode: 'run' },
  };
  const storageResponse = await configurationCreate(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleName,
    `Automated schedule for ${targetConfigurationId}`,
    schedulerConfig,
  );
  const scheduleConfigId = String(storageResponse.id ?? '');
  logger.info(`Created schedule configuration in Storage API: ${scheduleConfigId}`);
  const scheduleResponse = await scheduler.activateSchedule(scheduleConfigId);
  logger.info(`Activated schedule in Scheduler API: ${scheduleResponse.id}`);
  await setCfgCreationMetadata(clients, SCHEDULER_COMPONENT_ID, scheduleConfigId);
  return toScheduleDetail(scheduleResponse);
};

const updateSchedule = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  scheduleConfigId: string,
  cronTab: string | null,
  timezone: string | null,
  state: string | null,
): Promise<void> => {
  const currentConfig = await configurationDetail(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleConfigId,
  );
  const schedulerConfig = (currentConfig.configuration as Record<string, unknown>) ?? {};
  const schedule = (schedulerConfig.schedule as Record<string, unknown>) ?? {};
  if (cronTab !== null) schedule.cronTab = cronTab;
  if (timezone !== null) schedule.timezone = timezone;
  if (state !== null) schedule.state = state;
  schedulerConfig.schedule = schedule;

  const updated = await configurationUpdate(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleConfigId,
    schedulerConfig,
    'Schedule Updated',
  );
  logger.info(`Updated schedule configuration in Storage API: ${scheduleConfigId}`);
  await scheduler.activateSchedule(scheduleConfigId);
  await setCfgUpdateMetadata(
    clients,
    SCHEDULER_COMPONENT_ID,
    scheduleConfigId,
    Number(updated.version ?? 0),
  );
};

const removeSchedule = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  scheduleConfigId: string,
): Promise<void> => {
  await scheduler.deleteSchedule(scheduleConfigId);
  await configurationDelete(clients, SCHEDULER_COMPONENT_ID, scheduleConfigId);
};

/** Port of scheduler.process_schedule_request. */
const processScheduleRequest = async (
  clients: KeboolaClients,
  scheduler: SchedulerClient,
  targetComponentId: string,
  targetConfigurationId: string,
  requests: ScheduleRequest[],
): Promise<string[]> => {
  const { updated, added } = await updateSchedulersInternal(
    scheduler,
    targetConfigurationId,
    targetComponentId,
    requests,
  );
  const responses: string[] = [];
  try {
    for (const [scheduleId, schedule] of updated) {
      if (schedule === null) {
        await removeSchedule(clients, scheduler, scheduleId);
        responses.push(`Removed schedule: ${scheduleId}`);
      } else {
        await updateSchedule(
          clients,
          scheduler,
          scheduleId,
          schedule.cronTab,
          schedule.timezone,
          schedule.state,
        );
        responses.push(`Updated schedule: ${scheduleId}`);
      }
    }
    for (const newScheduler of added) {
      const response = await createSchedule(
        clients,
        scheduler,
        targetComponentId,
        targetConfigurationId,
        newScheduler.cronTab,
        newScheduler.timezone,
        newScheduler.state,
      );
      responses.push(`Created schedule: ${response.scheduleId}`);
    }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`Error processing schedule requests: ${msg}`);
  }
  return responses;
};

// =============================================================================
// TOOL OUTPUT BUILDING
// =============================================================================

const buildFlowToolOutput = (opts: {
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

const flowLabel = (flowType: FlowType): string =>
  flowType === ORCHESTRATOR_COMPONENT_ID ? 'legacy flows' : 'conditional flows';

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
  const schema = await resolveFlowSchema(clients, flowType);
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
    const schema = await resolveFlowSchema(clients, args.flow_type);
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
// TOOL DESCRIPTIONS (preserved verbatim from Python docstrings)
// =============================================================================

const CREATE_FLOW_DESCRIPTION = `Creates a new legacy (non-conditional) flow using \`keboola.orchestrator\`.

PRE-REQUISITES:
- Always use \`get_flow_schema\` with flow_type="keboola.orchestrator" and review \`get_flow_examples\` if unknown
- Collect component configuration IDs for every task you include

RULES:
- \`phases\` and \`tasks\` must follow the orchestrator schema; each entry must include \`id\` and \`name\`
- Phases run sequentially; tasks inside a phase run in parallel
- Use \`dependsOn\` on phases to sequence them; reference other phase ids
- Always share the returned links with the user

WHEN TO USE:
- Simple/linear orchestrations without branching or conditions
- ETL/ELT pipelines where phases just need ordering and parallel task groups`;

const CREATE_CONDITIONAL_FLOW_DESCRIPTION = `Creates a new conditional flow configuration using \`keboola.flow\`.

PRE-REQUISITES:
- Always use \`get_flow_schema\` with flow_type="keboola.flow" and review \`get_flow_examples\` if unknown
- Gather component configuration IDs for all tasks you include

RULES:
- \`phases\` and \`tasks\` must follow the keboola.flow schema; each entry needs \`id\` and \`name\`
- Exactly one entry phase (no incoming transitions); all phases must be reachable
- Connect phases via \`next\` transitions; no cycles or dangling phases; empty \`next\` means flow end
- Task/phase failures already stop the flow; add retries/conditions only if the user requests them
- Always share the returned links with the user

WHEN TO USE:
- Flows needing branching, conditions, retries, or notifications
- Default choice when user simply says "create a flow," unless they explicitly want legacy orchestrator behavior`;

const UPDATE_FLOW_DESCRIPTION = `Updates an existing flow configuration (either legacy \`keboola.orchestrator\` or conditional \`keboola.flow\`).

PRE-REQUISITES:
- Always use \`get_flow_schema\` (and \`get_flow_examples\`) for that flow type you want to update to follow the
required structure and see the examples if unknown
- Only pass \`phases\`/\`tasks\` when you want to replace them; omit to keep the existing ones unchanged

RULES (ALL FLOWS):
- \`flow_type\` must match the stored component id of the flow; do not switch flow types during update
- \`phases\` and \`tasks\` must follow the schema for the selected flow type; include at least \`id\` and \`name\`
- Tasks must reference existing component configurations; keep dependencies consistent
- Always provide a clear \`change_description\` and surface any links returned in the response to the user

CONDITIONAL FLOWS (\`keboola.flow\`):
- Maintain a single entry phase and ensure every phase is reachable; connect phases via \`next\` transitions
- No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

LEGACY FLOWS (\`keboola.orchestrator\`):
- Phases run sequentially; tasks inside a phase run in parallel; \`dependsOn\` references other phase ids
- Use \`continueOnFailure\` or best-effort patterns only when the user explicitly asks for them

WHEN TO USE:
- Renaming a flow, updating descriptions, adding/removing phases or tasks, adjusting dependencies,
or enabling/disabling flow execution`;

const MODIFY_FLOW_DESCRIPTION = `Updates an existing flow configuration (either legacy \`keboola.orchestrator\` or conditional \`keboola.flow\`) or
manages schedules for this flow.

PRE-REQUISITES:
- Always use \`get_flow_schema\` (and \`get_flow_examples\`) for that flow type you want to update to follow the
required structure and see the examples if unknown
- Only pass \`phases\`/\`tasks\` when you want to replace them; omit to keep the existing ones unchanged

RULES (ALL FLOWS):
- \`flow_type\` must match the stored component id of the flow; do not switch flow types during update
- \`phases\` and \`tasks\` must follow the schema for the selected flow type; include at least \`id\` and \`name\`
- Tasks must reference existing component configurations; keep dependencies consistent
- Always provide a clear \`change_description\` and surface any links returned in the response to the user
- A flow can have multiple schedules for automation runs. Add/update/remove schedules only if requested.
- When updating a flow or a schedule, specify only the fields you want to update, others will be kept unchanged.

CONDITIONAL FLOWS (\`keboola.flow\`):
- Maintain a single entry phase and ensure every phase is reachable; connect phases via \`next\` transitions
- No cycles or dangling phases; failed tasks already stop the flow, so only add retries/conditions if requested

LEGACY FLOWS (\`keboola.orchestrator\`):
- Phases run sequentially; tasks inside a phase run in parallel; \`dependsOn\` references other phase ids
- Use \`continueOnFailure\` or best-effort patterns only when the user explicitly asks for them

WHEN TO USE:
- Renaming a flow, updating descriptions, adding/removing phases or tasks, updating schedules,
adjusting dependencies, or enabling/disabling flow execution`;

const GET_FLOWS_DESCRIPTION = `Lists flows or retrieves full details for specific flows.

WHEN NOT TO USE:
- Do NOT call with \`flow_ids=[]\` just to find a flow by name. Use \`search\` with
  item_types=["flow"] instead.
- Only use \`flow_ids=[]\` when you need a complete list of all flows in the project.

OPTIONS:
- \`flow_ids=[]\` → summaries of all flows in the project
- \`flow_ids=["id1", ...]\` → full details (including phases/tasks) for those flows`;

const GET_FLOW_SCHEMA_DESCRIPTION = `Returns the JSON schema for the given flow type (markdown).

PRE-REQUISITES:
- Unknown schema for the target flow type: \`keboola.flow\` (conditional) or \`keboola.orchestrator\` (legacy)

RULES:
- Projects without conditional flows enabled cannot request \`keboola.flow\` schema
- Use the returned schema to shape \`phases\` and \`tasks\` for \`create_flow\` / \`create_conditional_flow\` /
\`update_flow\``;

const GET_FLOW_EXAMPLES_DESCRIPTION = `Retrieves examples of valid flow configurations.

PRE-REQUISITES:
- Unknown examples for the target flow type: \`keboola.flow\` (conditional) or \`keboola.orchestrator\` (legacy) to help
build the specific flow configuration by mirroring the structure/fields.

RULES:
- Conditional-flow examples require conditional flows to be enabled; otherwise use legacy orchestrator examples
- Present the examples or cite unavailability to the user`;

// =============================================================================
// SCHEDULE REQUEST ZOD SCHEMA (snake_case params, mirrors ScheduleRequest)
// =============================================================================

const scheduleRequestSchema = z.object({
  action: z.enum(['add', 'update', 'remove']).describe('Action to perform on the schedule.'),
  schedule_id: z
    .string()
    .nullish()
    .describe('ID of the schedule configuration to update. None if creating a new schedule.'),
  timezone: z
    .string()
    .nullish()
    .describe('Timezone for the schedule. Default UTC if None provided.'),
  cron_tab: z
    .string()
    .nullish()
    .describe(
      'Cron expression for the schedule following the format: `* * * * *`.' +
        'Where 1. minutes, 2. hours, 3. days of month, 4. months, 5. days of week. Example: `15,45 1,13 * * 0`',
    ),
  state: z.enum(['enabled', 'disabled']).nullish().describe('Enable or disable the schedule.'),
});

const normalizeScheduleRequests = (
  raw: z.infer<typeof scheduleRequestSchema>[],
): ScheduleRequest[] =>
  raw.map((r) => ({
    action: r.action,
    schedule_id: r.schedule_id ?? null,
    timezone: r.timezone ?? null,
    cron_tab: r.cron_tab ?? null,
    state: r.state ?? null,
  }));

// =============================================================================
// REGISTRATION
// =============================================================================

const flowTypeSchema = z.enum([CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID]);

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
