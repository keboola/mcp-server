import { Ajv } from 'ajv';
import { z } from 'zod';

import { fetchComponent } from '../components';

import type { KeboolaClients } from '@/clients/keboola';
import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  type FlowType,
  ORCHESTRATOR_COMPONENT_ID,
} from '@/constants';
import { logger } from '@/logger';

// Ported from tools/flow/{model,utils}.py and clients/validation.py: the zod input
// schemas + structural/JSON-schema flow validation live here.

// =============================================================================
// SHARED ALIASES
// =============================================================================

export type RawConfig = Record<string, unknown>;
export type MetadataItem = { id?: string; key?: string; value?: string };
export type Phase = Record<string, unknown>;
export type Task = Record<string, unknown>;

// MCP tracking metadata keys (not yet in the shared constants module; kept local to avoid
// editing a cross-module file). Mirror config.py CREATED_BY_MCP / UPDATED_BY_MCP_PREFIX.
export const CREATED_BY_MCP = 'KBC.MCP.createdBy';
export const UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.';

// =============================================================================
// SCHEDULE REQUEST MODEL (snake_case params, mirrors ScheduleRequest)
// =============================================================================

export type ScheduleRequest = {
  action: 'add' | 'update' | 'remove';
  schedule_id?: string | null;
  timezone?: string | null;
  cron_tab?: string | null;
  state?: 'enabled' | 'disabled' | null;
};

export const scheduleRequestSchema = z.object({
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

export const normalizeScheduleRequests = (
  raw: z.infer<typeof scheduleRequestSchema>[],
): ScheduleRequest[] =>
  raw.map((r) => ({
    action: r.action,
    schedule_id: r.schedule_id ?? null,
    timezone: r.timezone ?? null,
    cron_tab: r.cron_tab ?? null,
    state: r.state ?? null,
  }));

export const flowTypeSchema = z.enum([CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID]);

// =============================================================================
// STRUCTURAL VALIDATION (port of utils._validate_*_flow_structure)
// =============================================================================

const normalizeDependsOn = (phase: Phase): (string | number)[] => {
  const value = phase.dependsOn ?? phase.depends_on ?? phase['depends-on'] ?? [];
  if (!Array.isArray(value)) {
    throw new Error(`Invalid phase configuration: dependsOn must be a list.`);
  }
  return value as (string | number)[];
};

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

export const validateFlowStructure = (
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

export { normalizeDependsOn };

// =============================================================================
// SCHEMA VALIDATION (jsonschema, port of validation.validate_flow_configuration_against_schema)
// =============================================================================

// `strict: false` mirrors Python jsonschema's leniency (e.g. `minLength` on integer-or-string
// ids, `$ref` next to sibling keywords); invalid schemas there "continue as valid", so we never
// hard-fail on schema-author mistakes — only on data that violates a usable schema.
const ajv = new Ajv({ strict: false, allErrors: true });

export const validateFlowConfigurationAgainstSchema = (
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
export const resolveFlowSchema = async (
  clients: KeboolaClients,
  flowType: FlowType,
  loadLegacySchema: () => Record<string, unknown>,
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
