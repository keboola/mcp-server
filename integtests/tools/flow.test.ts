import { describe, expect, it } from 'vitest';

import { callToolRaw, callToolText, connectMcp, type McpSession } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest, type TestProject } from '../testproject/fixture';

import { CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID } from '@/constants';

// Ported from integtests/tools/flow/test_tools.py (10 tests) and
// integtests/tools/flow/test_scheduler.py (2 tests).
//
// The Python suite drove the tool *functions* directly (and mocked out the tool-filtering
// middleware so every tool was always callable). Here we go through the real MCP server over an
// in-memory transport, so two server-side gates apply that the Python tests bypassed:
//
//   1. Conditional-vs-legacy mutual exclusivity (feature `hide-conditional-flows`):
//        - conditional-enabled project  -> ONLY `create_conditional_flow` is callable;
//          `create_flow` is blocked ("...use create_conditional_flow tool instead").
//        - legacy-only project          -> ONLY `create_flow` is callable;
//          `create_conditional_flow` is blocked.
//      So a given leased project supports exactly one create variant. Each create/update test
//      detects the project's variant via get_project_info (`conditional_flows`) and returns
//      early with a logged SKIP when the leased project is the wrong variant. (Vitest has no
//      late `it.skip`, so the early-return-with-warning is the equivalent.)
//
//   2. update_flow vs modify_flow depends on the token role: admin/OAuth tokens may call
//      ONLY `modify_flow`; other tokens may call ONLY `update_flow`. We pick the right tool
//      from get_project_info (`user_role`).
//
// Positive assertions are on the TOON text the client returns (substrings/regex); negative
// paths assert on the raw CallToolResult (isError + message).

// =============================================================================
// HELPERS
// =============================================================================

/** Pulls the first `configuration_id: <value>` out of a tool's TOON text. */
const extractConfigId = (text: string): string => {
  const m = text.match(/configuration_id:\s*(\S+)/);
  if (!m) throw new Error(`No configuration_id found in tool output:\n${text}`);
  return m[1]!;
};

const projectInfo = (session: McpSession): Promise<string> =>
  callToolText(session.client, 'get_project_info');

/** conditional_flows == true (no `hide-conditional-flows` feature). */
const isConditionalProject = (info: string): boolean => /conditional_flows:\s*true/i.test(info);

/** The update tool callable for this token: admin/OAuth -> modify_flow, else update_flow. */
const updateToolName = (info: string): 'modify_flow' | 'update_flow' =>
  /user_role:\s*(admin|share)/i.test(info) ? 'modify_flow' : 'update_flow';

const isAdmin = (info: string): boolean => /user_role:\s*admin/i.test(info);

const skip = (reason: string): void => {
  // eslint-disable-next-line no-console
  console.warn(`SKIP: ${reason}`);
};

/** Seeds the standard fixtures and returns the first real config (component + config id). */
const seedFirstConfig = async (
  project: TestProject,
): Promise<{ componentId: string; configurationId: string }> => {
  const seeded = await seedProject(project);
  const c = seeded.configs[0]!;
  return { componentId: c.componentId, configurationId: c.configurationId };
};

/** Creates the standard "Initial Test Flow" legacy flow (port of conftest.initial_lf). */
const createInitialLegacyFlow = async (
  session: McpSession,
  componentId: string,
  configurationId: string,
): Promise<string> => {
  const text = await callToolText(session.client, 'create_flow', {
    name: 'Initial Test Flow',
    description: 'Initial test flow created by automated test',
    phases: [{ name: 'Phase1', dependsOn: [], description: 'First phase' }],
    tasks: [
      {
        id: 20001,
        name: 'Task1',
        phase: 1,
        continueOnFailure: false,
        enabled: false,
        task: { componentId, configId: configurationId, mode: 'run' },
      },
    ],
  });
  return extractConfigId(text);
};

/** Creates the standard "Initial Test Flow" conditional flow (port of conftest.initial_cf). */
const createInitialConditionalFlow = async (
  session: McpSession,
  componentId: string,
  configurationId: string,
): Promise<string> => {
  const text = await callToolText(session.client, 'create_conditional_flow', {
    name: 'Initial Test Flow',
    description: 'Initial test flow created by automated test',
    phases: [
      {
        id: 'phase1',
        name: 'Phase1',
        description: 'First phase',
        next: [{ id: 'phase1_end', name: 'End Flow', goto: null }],
      },
    ],
    tasks: [
      {
        id: 'task1',
        name: 'Task1',
        phase: 'phase1',
        task: { type: 'job', componentId, configId: configurationId, mode: 'run' },
      },
    ],
  });
  return extractConfigId(text);
};

// Flow tests are backend-agnostic; pin to snowflake (the bigquery pool entries currently fail
// the harness's synchronous-bucket-drop reset, unrelated to flows).
const leaseProject = (clean = true) => getTestProjectForTest({ backend: 'snowflake', clean });

describe('flow tools (integration)', () => {
  // ===========================================================================
  // test_create_and_retrieve_flow (legacy) — runs only on a legacy-only project
  // ===========================================================================
  it('create_flow creates a legacy flow and get_flows retrieves it', async () => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      if (isConditionalProject(await projectInfo(session))) {
        return skip('project is conditional-enabled; create_flow (legacy) is not available.');
      }
      const flowName = 'Integration Test Flow';
      const created = await callToolText(session.client, 'create_flow', {
        name: flowName,
        description: 'Flow created by integration test.',
        phases: [
          { name: 'Extract', dependsOn: [], description: 'Extract data' },
          { name: 'Transform', dependsOn: [1], description: 'Transform data' },
        ],
        tasks: [
          { name: 'Extract Task', phase: 1, task: { componentId, configId: configurationId } },
          { name: 'Transform Task', phase: 2, task: { componentId, configId: configurationId } },
        ],
      });
      expect(created).toContain(ORCHESTRATOR_COMPONENT_ID);
      expect(created).toContain('Flow created by integration test.');
      expect(created).toMatch(/success:\s*true/i);
      expect(created).toMatch(/version:/);
      expect(created).toContain('https://help.keboola.com/flows/');
      const flowId = extractConfigId(created);

      const list = await callToolText(session.client, 'get_flows');
      expect(list).toContain(flowName);
      expect(list).toContain(flowId);

      const detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toContain(ORCHESTRATOR_COMPONENT_ID);
      expect(detail).toContain('Extract');
      expect(detail).toContain('Transform');
      expect(detail).toContain(componentId);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_create_and_retrieve_conditional_flow — runs only on a conditional project
  // ===========================================================================
  it('create_conditional_flow creates a conditional flow and get_flows retrieves it', async () => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      if (!isConditionalProject(await projectInfo(session))) {
        return skip('project is legacy-only; create_conditional_flow is not available.');
      }
      const flowName = 'Integration Test Conditional Flow';
      const created = await callToolText(session.client, 'create_conditional_flow', {
        name: flowName,
        description: 'Conditional flow created by integration test.',
        phases: [
          {
            id: 'extract_phase',
            name: 'Extract',
            description: 'Extract data',
            next: [{ id: 'extract_to_transform', name: 'Extract to Transform', goto: 'transform_phase' }],
          },
          {
            id: 'transform_phase',
            name: 'Transform',
            description: 'Transform data',
            next: [{ id: 'transform_end', name: 'End Flow', goto: null }],
          },
        ],
        tasks: [
          {
            id: 'extract_task',
            name: 'Extract Task',
            phase: 'extract_phase',
            task: { type: 'job', componentId, configId: configurationId, mode: 'run' },
          },
          {
            id: 'transform_task',
            name: 'Transform Task',
            phase: 'transform_phase',
            task: { type: 'job', componentId, configId: configurationId, mode: 'run' },
          },
        ],
      });
      expect(created).toContain(CONDITIONAL_FLOW_COMPONENT_ID);
      expect(created).toContain('Conditional flow created by integration test.');
      expect(created).toMatch(/success:\s*true/i);
      expect(created).toMatch(/version:/);
      const flowId = extractConfigId(created);

      const list = await callToolText(session.client, 'get_flows');
      expect(list).toContain(flowName);
      expect(list).toContain(flowId);

      const detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toContain(CONDITIONAL_FLOW_COMPONENT_ID);
      expect(detail).toContain('Extract');
      expect(detail).toContain('Transform');
      expect(detail).toContain(componentId);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_update_flow — legacy parametrized cases (legacy-only projects)
  // ===========================================================================
  const legacyUpdateCases: { label: string; updates: Record<string, unknown> }[] = [
    {
      label: 'phases + tasks + name + description',
      updates: {
        phases: [
          { id: 1, name: 'Phase1', dependsOn: [], description: 'First phase updated' },
          { id: 2, name: 'Phase2', dependsOn: [], description: 'Second phase added' },
        ],
        tasks: [
          {
            id: 20001,
            name: 'Task1 - Updated',
            phase: 1,
            continueOnFailure: false,
            enabled: false,
            task: { componentId: 'ex-generic-v2', configId: 'test_config_001', mode: 'run' },
          },
          {
            id: 20002,
            name: 'Task2 - Added',
            phase: 2,
            continueOnFailure: false,
            enabled: false,
            task: { componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
          },
        ],
        name: 'Updated Test Flow',
        description: 'The test flow updated by an automated test.',
      },
    },
    {
      label: 'phases only',
      updates: {
        phases: [
          { id: 1, name: 'Phase1', dependsOn: [], description: 'First phase updated' },
          { id: 2, name: 'Phase2', dependsOn: [], description: 'Second phase added' },
        ],
      },
    },
    {
      label: 'tasks only',
      updates: {
        tasks: [
          {
            id: 20001,
            name: 'Task1 - Updated',
            phase: 1,
            continueOnFailure: false,
            enabled: false,
            task: { componentId: 'ex-generic-v2', configId: 'test_config_001', mode: 'run' },
          },
          {
            id: 20002,
            name: 'Task2 - Added',
            phase: 1,
            continueOnFailure: false,
            enabled: false,
            task: { componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
          },
        ],
      },
    },
    { label: 'name only', updates: { name: 'Updated just name' } },
    { label: 'description only', updates: { description: 'Updated just description' } },
    { label: 'is_disabled true', updates: { is_disabled: true } },
  ];

  it.each(legacyUpdateCases)('update legacy flow ($label)', async ({ updates }) => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      const info = await projectInfo(session);
      if (isConditionalProject(info)) {
        return skip('project is conditional-enabled; legacy create_flow is not available.');
      }
      const tool = updateToolName(info);
      const flowId = await createInitialLegacyFlow(session, componentId, configurationId);

      const result = await callToolText(session.client, tool, {
        configuration_id: flowId,
        flow_type: ORCHESTRATOR_COMPONENT_ID,
        change_description: 'Integration test update',
        ...updates,
      });
      expect(result).toContain(flowId);
      expect(result).toContain(ORCHESTRATOR_COMPONENT_ID);
      expect(result).toMatch(/success:\s*true/i);
      expect(result).toMatch(/timestamp:/);
      expect(result).toMatch(/version:/);

      const expectedName = (updates.name as string) ?? 'Initial Test Flow';
      const expectedDescription =
        (updates.description as string) ?? 'Initial test flow created by automated test';

      const detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toContain(expectedName);
      expect(detail).toContain(expectedDescription);
      // The update bumps the configuration to version 2. (The Python test also asserted the
      // KBC.MCP.updatedBy/createdBy metadata, but that is read via the Storage client, not
      // surfaced by the get_flows MCP tool, so it is not assertable through this surface.)
      expect(detail).toMatch(/version:\s*2/);
      if (updates.is_disabled === true) expect(detail).toMatch(/is_disabled:\s*true/i);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_update_flow — conditional parametrized cases (conditional projects)
  // ===========================================================================
  const conditionalUpdateCases: { label: string; updates: Record<string, unknown> }[] = [
    {
      label: 'phases + tasks',
      updates: {
        phases: [
          {
            id: 'phase1',
            name: 'Phase1',
            description: 'First phase updated',
            next: [{ id: 'phase1_phase2', name: 'End Flow', goto: 'phase2' }],
          },
          {
            id: 'phase2',
            name: 'Phase2',
            description: 'Second phase added',
            next: [{ id: 'phase2_end', name: 'End Flow', goto: null }],
          },
        ],
        tasks: [
          {
            id: 'task1',
            name: 'Task1 - Updated',
            phase: 'phase1',
            task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_001', mode: 'run' },
          },
          {
            id: 'task2',
            name: 'Task2 - Added',
            phase: 'phase2',
            task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
          },
        ],
      },
    },
    {
      label: 'phases only',
      updates: {
        phases: [
          {
            id: 'phase1',
            name: 'Phase1',
            description: 'First phase updated',
            next: [{ id: 'phase1_phase2', name: 'End Flow', goto: 'phase2' }],
          },
          {
            id: 'phase2',
            name: 'Phase2',
            description: 'Second phase added',
            next: [{ id: 'phase2_end', name: 'End Flow', goto: null }],
          },
        ],
      },
    },
    {
      label: 'tasks only',
      updates: {
        tasks: [
          {
            id: 'task1',
            name: 'Task1 - Updated',
            phase: 'phase1',
            task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_001', mode: 'run' },
          },
          {
            id: 'task2',
            name: 'Task2 - Added',
            phase: 'phase1',
            task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
          },
        ],
      },
    },
    { label: 'name only', updates: { name: 'Updated just name' } },
    { label: 'description only', updates: { description: 'Updated just description' } },
    { label: 'is_disabled true', updates: { is_disabled: true } },
  ];

  it.each(conditionalUpdateCases)('update conditional flow ($label)', async ({ updates }) => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      const info = await projectInfo(session);
      if (!isConditionalProject(info)) {
        return skip('project is legacy-only; create_conditional_flow is not available.');
      }
      const tool = updateToolName(info);
      const flowId = await createInitialConditionalFlow(session, componentId, configurationId);

      const result = await callToolText(session.client, tool, {
        configuration_id: flowId,
        flow_type: CONDITIONAL_FLOW_COMPONENT_ID,
        change_description: 'Integration test update',
        ...updates,
      });
      expect(result).toContain(flowId);
      expect(result).toContain(CONDITIONAL_FLOW_COMPONENT_ID);
      expect(result).toMatch(/success:\s*true/i);

      const expectedName = (updates.name as string) ?? 'Initial Test Flow';
      const expectedDescription =
        (updates.description as string) ?? 'Initial test flow created by automated test';

      const detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toContain(expectedName);
      expect(detail).toContain(expectedDescription);
      // version bumps to 2 on update; MCP-tracking metadata is not surfaced by get_flows.
      expect(detail).toMatch(/version:\s*2/);
      if (updates.is_disabled === true) expect(detail).toMatch(/is_disabled:\s*true/i);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_get_flows_empty
  // ===========================================================================
  it('get_flows returns an empty list when no flows exist', async () => {
    const project = await leaseProject();
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_flows');
      expect(text).not.toMatch(/configuration_id:/);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_get_flows_list — creates whichever flow variant the project supports
  // ===========================================================================
  it('get_flows lists created flows with dashboard links', async () => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      const info = await projectInfo(session);
      const id = isConditionalProject(info)
        ? await createInitialConditionalFlow(session, componentId, configurationId)
        : await createInitialLegacyFlow(session, componentId, configurationId);

      const list = await callToolText(session.client, 'get_flows');
      expect(list).toContain(id);
      // Dashboard links for both flow surfaces are always present in the list output.
      expect(list).toContain('/flows');
      expect(list).toContain('/flows-v2');
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_get_flow_schema (read-only; no project reset needed)
  // ===========================================================================
  it('get_flow_schema returns the legacy (and conditional) JSON schema', async () => {
    const project = await leaseProject(false);
    const session = await connectMcp(project.config);
    try {
      const legacy = await callToolText(session.client, 'get_flow_schema', {
        flow_type: ORCHESTRATOR_COMPONENT_ID,
      });
      expect(legacy.startsWith('```json\n')).toBe(true);
      expect(legacy.endsWith('\n```')).toBe(true);
      expect(legacy).toContain('dependsOn');
      const legacyParsed = JSON.parse(legacy.slice(8, -4));
      expect(legacyParsed).toHaveProperty('$schema');
      expect(legacyParsed.properties).toHaveProperty('phases');
      expect(legacyParsed.properties).toHaveProperty('tasks');

      if (isConditionalProject(await projectInfo(session))) {
        const conditional = await callToolText(session.client, 'get_flow_schema', {
          flow_type: CONDITIONAL_FLOW_COMPONENT_ID,
        });
        expect(conditional.startsWith('```json\n')).toBe(true);
        expect(conditional).not.toBe(legacy);
        const conditionalParsed = JSON.parse(conditional.slice(8, -4));
        expect(conditionalParsed.properties.phases.items.properties).toHaveProperty('next');
        expect(conditionalParsed.properties.tasks.items.properties.task).toHaveProperty('oneOf');
      } else {
        const result = await callToolRaw(session.client, 'get_flow_schema', {
          flow_type: CONDITIONAL_FLOW_COMPONENT_ID,
        });
        expect(result.isError).toBeTruthy();
        expect((result.content as { text: string }[])[0]!.text).toMatch(
          /conditional flows are not supported/i,
        );
      }
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // get_flow_examples (read-only)
  // ===========================================================================
  it('get_flow_examples returns example configurations for the legacy flow type', async () => {
    const project = await leaseProject(false);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_flow_examples', {
        flow_type: ORCHESTRATOR_COMPONENT_ID,
      });
      expect(text).toContain(`Flow Configuration Examples for \`${ORCHESTRATOR_COMPONENT_ID}\``);
      expect(text).toContain('```json');
      expect(text).toContain('phases');
      expect(text).toContain('tasks');
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_create_legacy_flow_invalid_structure
  // (legacy validation runs only on legacy-only projects, where create_flow is callable)
  // ===========================================================================
  it('create_flow rejects a legacy flow that depends on a non-existent phase', async () => {
    const project = await leaseProject(false);
    const session = await connectMcp(project.config);
    try {
      if (isConditionalProject(await projectInfo(session))) {
        return skip('project is conditional-enabled; create_flow (legacy) is not available.');
      }
      const result = await callToolRaw(session.client, 'create_flow', {
        name: 'Invalid Legacy Flow',
        description: 'Should fail',
        phases: [{ name: 'Phase1', dependsOn: [99], description: 'Depends on non-existent phase' }],
        tasks: [{ name: 'Task1', phase: 1, task: { componentId: 'ex-generic-v2', configId: 'x' } }],
      });
      expect(result.isError).toBeTruthy();
      expect((result.content as { text: string }[])[0]!.text).toMatch(/non-existent phase/i);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_create_conditional_flow_invalid_structure (type/schema validation)
  // ===========================================================================
  it('create_conditional_flow rejects structurally invalid phases/tasks', async () => {
    const project = await leaseProject(false);
    const session = await connectMcp(project.config);
    try {
      if (!isConditionalProject(await projectInfo(session))) {
        return skip('project is legacy-only; create_conditional_flow is not available.');
      }
      const result = await callToolRaw(session.client, 'create_conditional_flow', {
        name: 'Invalid Conditional Flow',
        description: 'Should fail',
        phases: [
          {
            id: 123, // invalid: should be a string
            name: '', // invalid: empty
            next: [{ id: 'transition-1', goto: 'phase-2' }],
          },
        ],
        tasks: [
          {
            id: 'task-1',
            name: 'Task1',
            phase: 'phase-1',
            enabled: true,
            task: { type: 'invalid_type', componentId: 'ex-generic-v2', configId: 'x', mode: 'invalid_mode' },
          },
        ],
      });
      expect(result.isError).toBeTruthy();
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_create_conditional_flow_semantically_invalid_structure
  // ===========================================================================
  const semanticInvalidCases: {
    label: string;
    phases: Record<string, unknown>[];
    tasks: Record<string, unknown>[];
    expected: RegExp;
  }[] = [
    {
      label: 'multiple entry phases',
      phases: [
        { id: 'phase-1', name: 'Phase1', next: [{ id: 'transition-1', goto: null }] },
        { id: 'phase-2', name: 'Phase2', next: [{ id: 'transition-2', goto: null }] },
      ],
      tasks: [
        {
          id: 'task-1',
          name: 'Task1',
          phase: 'phase-1',
          task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
        },
      ],
      expected: /multiple entry phases/i,
    },
    {
      label: 'no ending phases',
      phases: [
        { id: 'phase-1', name: 'Phase1', next: [{ id: 'transition-1', goto: 'phase-2' }] },
        { id: 'phase-2', name: 'Phase2', next: [{ id: 'transition-2', goto: 'phase-1' }] },
      ],
      tasks: [
        {
          id: 'task-1',
          name: 'Task1',
          phase: 'phase-1',
          task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
        },
        {
          id: 'task-2',
          name: 'Task2',
          phase: 'phase-2',
          task: { type: 'job', componentId: 'ex-generic-v2', configId: 'test_config_002', mode: 'run' },
        },
      ],
      expected: /no ending phases/i,
    },
  ];

  it.each(semanticInvalidCases)(
    'create_conditional_flow rejects a semantically invalid flow ($label)',
    async ({ phases, tasks, expected }) => {
      const project = await leaseProject(false);
      const session = await connectMcp(project.config);
      try {
        if (!isConditionalProject(await projectInfo(session))) {
          return skip('project is legacy-only; create_conditional_flow is not available.');
        }
        const result = await callToolRaw(session.client, 'create_conditional_flow', {
          name: 'Invalid Conditional Flow',
          description: 'Should fail',
          phases,
          tasks,
        });
        expect(result.isError).toBeTruthy();
        expect((result.content as { text: string }[])[0]!.text).toMatch(expected);
      } finally {
        await session.close();
      }
    },
  );

  // ===========================================================================
  // test_flow_lifecycle_integration — create the supported variant, retrieve, list
  // ===========================================================================
  it('full flow lifecycle: create, retrieve individually, list', async () => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      const info = await projectInfo(session);
      const created: { type: string; id: string }[] = [];

      if (isConditionalProject(info)) {
        const text = await callToolText(session.client, 'create_conditional_flow', {
          name: 'Integration Test Conditional Flow',
          description: 'Conditional flow created by integration test',
          phases: [
            {
              id: 'phase-1',
              name: 'Extract',
              description: 'Extract data from source',
              next: [{ id: 'transition-1', goto: 'phase-2' }],
            },
            { id: 'phase-2', name: 'Load', description: 'Load data to destination', next: [] },
          ],
          tasks: [
            {
              id: 'task-1',
              name: 'Extract from API',
              phase: 'phase-1',
              enabled: true,
              task: { type: 'job', componentId, configId: configurationId, mode: 'run' },
            },
            {
              id: 'task-2',
              name: 'Load to Warehouse',
              phase: 'phase-2',
              enabled: true,
              task: { type: 'job', componentId, configId: configurationId, mode: 'run' },
            },
          ],
        });
        expect(text).toMatch(/success:\s*true/i);
        created.push({ type: CONDITIONAL_FLOW_COMPONENT_ID, id: extractConfigId(text) });
      } else {
        const text = await callToolText(session.client, 'create_flow', {
          name: 'Integration Test Orchestrator Flow',
          description: 'Orchestrator flow created by integration test',
          phases: [
            { id: 1, name: 'Extract', description: 'Extract data from source', dependsOn: [] },
            { id: 2, name: 'Load', description: 'Load data to destination', dependsOn: [1] },
          ],
          tasks: [
            {
              id: 20001,
              name: 'Extract from API',
              phase: 1,
              enabled: true,
              continueOnFailure: false,
              task: { componentId, configId: configurationId, mode: 'run' },
            },
            {
              id: 20002,
              name: 'Load to Warehouse',
              phase: 2,
              enabled: true,
              continueOnFailure: false,
              task: { componentId, configId: configurationId, mode: 'run' },
            },
          ],
        });
        expect(text).toMatch(/success:\s*true/i);
        created.push({ type: ORCHESTRATOR_COMPONENT_ID, id: extractConfigId(text) });
      }

      // Retrieve each individually.
      for (const { type, id } of created) {
        const detail = await callToolText(session.client, 'get_flows', { flow_ids: [id] });
        expect(detail).toContain(id);
        expect(detail).toContain(type);
        expect(detail).toContain('Extract');
        expect(detail).toContain('Load');
      }

      // List all and verify presence.
      const list = await callToolText(session.client, 'get_flows');
      for (const { id } of created) expect(list).toContain(id);
    } finally {
      await session.close();
    }
  });

  // ===========================================================================
  // test_scheduler_lifecycle_tooling — modify_flow add/update/remove schedules
  // (admin token only; admins must use modify_flow)
  // ===========================================================================
  it('modify_flow manages a flow schedule via tooling (add, update, remove)', async () => {
    const project = await leaseProject();
    const { componentId, configurationId } = await seedFirstConfig(project);
    const session = await connectMcp(project.config);
    try {
      const info = await projectInfo(session);
      if (!isAdmin(info)) return skip('scheduler tooling requires an admin token (modify_flow).');

      // Create whatever flow variant the project supports, schedule that one.
      const conditional = isConditionalProject(info);
      const flowType = conditional ? CONDITIONAL_FLOW_COMPONENT_ID : ORCHESTRATOR_COMPONENT_ID;
      const flowId = conditional
        ? await createInitialConditionalFlow(session, componentId, configurationId)
        : await createInitialLegacyFlow(session, componentId, configurationId);

      // Add a schedule.
      const added = await callToolText(session.client, 'modify_flow', {
        configuration_id: flowId,
        flow_type: flowType,
        change_description: 'Add scheduler via tooling',
        schedules: [{ action: 'add', cron_tab: '0 8 * * *', timezone: 'UTC', state: 'enabled' }],
      });
      expect(added).toMatch(/success:\s*true/i);

      let detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toMatch(/n_schedules:\s*1/);
      expect(detail).toContain('0 8 * * *');
      expect(detail).toContain('UTC');
      expect(detail).toMatch(/state:\s*enabled/);
      const scheduleId = detail.match(/scheduleId:\s*(\S+)/)?.[1];
      expect(scheduleId).toBeTruthy();

      // Update the schedule.
      const updated = await callToolText(session.client, 'modify_flow', {
        configuration_id: flowId,
        flow_type: flowType,
        change_description: 'Update scheduler via tooling',
        schedules: [
          {
            action: 'update',
            schedule_id: scheduleId,
            cron_tab: '0 12 * * *',
            timezone: 'America/New_York',
            state: 'disabled',
          },
        ],
      });
      expect(updated).toMatch(/success:\s*true/i);

      detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toMatch(/n_schedules:\s*1/);
      expect(detail).toContain('0 12 * * *');
      expect(detail).toContain('America/New_York');
      expect(detail).toMatch(/state:\s*disabled/);

      // Remove the schedule.
      const removed = await callToolText(session.client, 'modify_flow', {
        configuration_id: flowId,
        flow_type: flowType,
        change_description: 'Remove scheduler via tooling',
        schedules: [{ action: 'remove', schedule_id: scheduleId }],
      });
      expect(removed).toMatch(/success:\s*true/i);

      detail = await callToolText(session.client, 'get_flows', { flow_ids: [flowId] });
      expect(detail).toMatch(/n_schedules:\s*0/);
    } finally {
      await session.close();
    }
  });
});
