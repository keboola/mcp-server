import { describe, expect, it } from 'vitest';

import { callToolText, connectMcp } from '../helpers/mcp';
import { seedProject } from '../helpers/seed';
import { getTestProjectForTest } from '../testproject/fixture';

// Ported from integtests/tools/components/test_tools.py. The Python suite calls the tool
// functions directly and asserts on typed Pydantic models; here the tools return TOON text
// via the MCP client, so we assert on substrings/regex. Read tests seed the standard fixtures
// (two configs: ex-generic-v2 extractor + keboola.snowflake-transformation transformation);
// write tests lease a clean project and create their own data.

const EXTRACTOR_COMPONENT = 'ex-generic-v2';
const TRANSFORMATION_COMPONENT = 'keboola.snowflake-transformation';

describe('components tools (integration)', () => {
  // --- get_configs ---------------------------------------------------------

  it('get_configs returns detailed configuration for specific configs', async () => {
    const project = await getTestProjectForTest();
    const seeded = await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      for (const cfg of seeded.configs) {
        const text = await callToolText(session.client, 'get_configs', {
          configs: [
            { component_id: cfg.componentId, configuration_id: cfg.configurationId },
          ],
        });
        // Detailed Configuration output carries the component, the configuration_root with the
        // matching IDs, and a non-empty links list.
        expect(text).toContain(cfg.componentId);
        expect(text).toContain(cfg.configurationId);
        expect(text).toMatch(/component_type/);
        expect(text).toMatch(/component_name/);
        expect(text).toMatch(/links/);
      }
    } finally {
      await session.close();
    }
  });

  it('get_configs lists components filtered by component IDs', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_configs', {
        component_ids: [EXTRACTOR_COMPONENT, TRANSFORMATION_COMPONENT],
      });
      expect(text).toContain(EXTRACTOR_COMPONENT);
      expect(text).toContain(TRANSFORMATION_COMPONENT);
      expect(text).toMatch(/components_with_configs|component/);
    } finally {
      await session.close();
    }
  });

  // Mirrors the Python parametrized test_get_configs_list_by_types: the seeded project has one
  // extractor and one transformation config.
  it.each([
    { types: ['extractor'], expected: [EXTRACTOR_COMPONENT], absent: [TRANSFORMATION_COMPONENT] },
    {
      types: ['transformation'],
      expected: [TRANSFORMATION_COMPONENT],
      absent: [EXTRACTOR_COMPONENT],
    },
    {
      types: ['application', 'extractor', 'transformation'],
      expected: [EXTRACTOR_COMPONENT, TRANSFORMATION_COMPONENT],
      absent: [],
    },
    { types: [], expected: [EXTRACTOR_COMPONENT, TRANSFORMATION_COMPONENT], absent: [] },
  ])('get_configs filters by component types %o', async ({ types, expected, absent }) => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_configs', { component_types: types });
      for (const id of expected) expect(text).toContain(id);
      for (const id of absent) expect(text).not.toContain(id);
    } finally {
      await session.close();
    }
  });

  // --- get_components ------------------------------------------------------

  it('get_components returns details for multiple components', async () => {
    const project = await getTestProjectForTest();
    await seedProject(project);
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_components', {
        component_ids: [EXTRACTOR_COMPONENT, TRANSFORMATION_COMPONENT],
      });
      expect(text).toContain(EXTRACTOR_COMPONENT);
      expect(text).toContain(TRANSFORMATION_COMPONENT);
      expect(text).toMatch(/component_name/);
      expect(text).toMatch(/component_type/);
      // both per-component links and output-level links should be present
      expect(text).toMatch(/links/);
    } finally {
      await session.close();
    }
  });

  // --- get_config_examples -------------------------------------------------

  it('get_config_examples returns markdown examples for a component', async () => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_config_examples', {
        component_id: EXTRACTOR_COMPONENT,
      });
      expect(text).toContain(`# Configuration Examples for \`${EXTRACTOR_COMPONENT}\``);
      expect(text).toContain('parameters');
    } finally {
      await session.close();
    }
  });

  it('get_config_examples returns empty string for an invalid component', async () => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'get_config_examples', {
        component_id: 'completely-non-existent-component-12345',
      });
      expect(text).toBe('');
    } finally {
      await session.close();
    }
  });

  // --- create_config -------------------------------------------------------

  it('create_config creates a configuration with success metadata and links', async () => {
    const project = await getTestProjectForTest();
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'create_config', {
        name: 'Test Configuration',
        description: 'Test configuration created by automated test',
        component_id: EXTRACTOR_COMPONENT,
        parameters: {},
        storage: {},
      });
      expect(text).toContain(EXTRACTOR_COMPONENT);
      expect(text).toContain('Test configuration created by automated test');
      expect(text).toMatch(/success/);
      expect(text).toMatch(/true/);
      // links: ui-detail + ui-dashboard for the created configuration
      expect(text).toContain(`/components/${EXTRACTOR_COMPONENT}`);
      expect(text).toContain('Configuration: Test Configuration');
    } finally {
      await session.close();
    }
  });

  // --- add_config_row ------------------------------------------------------

  it('add_config_row adds a row to a configuration', async () => {
    const project = await getTestProjectForTest();
    const session = await connectMcp(project.config);
    try {
      const rootText = await callToolText(session.client, 'create_config', {
        name: 'Root Configuration for Row Test',
        description: 'Root configuration created for row configuration test',
        component_id: EXTRACTOR_COMPONENT,
        parameters: {},
        storage: {},
      });
      const configurationId = extractConfigurationId(rootText);

      const rowText = await callToolText(session.client, 'add_config_row', {
        name: 'Test Row Configuration',
        description: 'Test row configuration created by automated test',
        component_id: EXTRACTOR_COMPONENT,
        configuration_id: configurationId,
        parameters: { row_param: 'row_value' },
        storage: {},
      });
      expect(rowText).toContain(EXTRACTOR_COMPONENT);
      expect(rowText).toContain(configurationId);
      expect(rowText).toContain('Test row configuration created by automated test');
      expect(rowText).toMatch(/success/);
      expect(rowText).toContain('Configuration: Test Row Configuration');
    } finally {
      await session.close();
    }
  });

  // --- update_config -------------------------------------------------------

  // Mirrors Python test_update_config parametrize cases (only the fields-affecting-output ones
  // are asserted via text, since backend re-fetch is not available through the MCP client).
  it.each([
    {
      label: 'all fields',
      updates: {
        name: 'Updated Test Configuration',
        description: 'Updated test configuration by automated test',
        parameter_updates: [{ op: 'set', path: 'updated_param', value: 'updated_value' }],
        storage: { output: { tables: [{ source: 'output.csv', destination: 'out.c-bucket.table' }] } },
      },
      expectName: 'Updated Test Configuration',
      expectDescription: 'Updated test configuration by automated test',
    },
    {
      label: 'just name',
      updates: { name: 'Updated just name' },
      expectName: 'Updated just name',
      expectDescription: 'Initial test configuration created by automated test',
    },
    {
      label: 'just description',
      updates: { description: 'Updated just description' },
      expectName: 'Initial Test Configuration',
      expectDescription: 'Updated just description',
    },
    {
      label: 'just parameters',
      updates: {
        parameter_updates: [{ op: 'set', path: 'updated_param', value: 'Updated just parameters' }],
      },
      expectName: 'Initial Test Configuration',
      expectDescription: 'Initial test configuration created by automated test',
    },
    {
      label: 'just storage',
      updates: {
        storage: { output: { tables: [{ source: 'output.csv', destination: 'out.c-bucket.table' }] } },
      },
      expectName: 'Initial Test Configuration',
      expectDescription: 'Initial test configuration created by automated test',
    },
  ])('update_config updates a configuration ($label)', async ({ updates, expectName, expectDescription }) => {
    const project = await getTestProjectForTest();
    const session = await connectMcp(project.config);
    try {
      const createText = await callToolText(session.client, 'create_config', {
        name: 'Initial Test Configuration',
        description: 'Initial test configuration created by automated test',
        component_id: EXTRACTOR_COMPONENT,
        parameters: { initial_param: 'initial_value' },
        storage: { input: { tables: [{ source: 'in.c-bucket.table', destination: 'input.csv' }] } },
      });
      const configurationId = extractConfigurationId(createText);

      const text = await callToolText(session.client, 'update_config', {
        change_description: 'Integration test update',
        component_id: EXTRACTOR_COMPONENT,
        configuration_id: configurationId,
        ...updates,
      });
      expect(text).toContain(EXTRACTOR_COMPONENT);
      expect(text).toContain(configurationId);
      expect(text).toMatch(/success/);
      expect(text).toContain(expectDescription);
      expect(text).toContain(`Configuration: ${expectName}`);
    } finally {
      await session.close();
    }
  });

  // --- update_config_row ---------------------------------------------------

  it.each([
    {
      label: 'all fields',
      updates: {
        name: 'Updated Row Configuration',
        description: 'Updated row configuration by automated test',
        parameter_updates: [{ op: 'set', path: '$', value: { updated_row_param: 'updated_row_value' } }],
        storage: {},
      },
      expectName: 'Updated Row Configuration',
      expectDescription: 'Updated row configuration by automated test',
    },
    {
      label: 'just name',
      updates: { name: 'Updated just name' },
      expectName: 'Updated just name',
      expectDescription: 'Initial row configuration for update test',
    },
    {
      label: 'just description',
      updates: { description: 'Updated just description' },
      expectName: 'Initial Test Row Configuration',
      expectDescription: 'Updated just description',
    },
    {
      label: 'is_disabled',
      updates: { is_disabled: true },
      expectName: 'Initial Test Row Configuration',
      expectDescription: 'Initial row configuration for update test',
    },
  ])('update_config_row updates a row configuration ($label)', async ({ updates, expectName, expectDescription }) => {
    const project = await getTestProjectForTest();
    const session = await connectMcp(project.config);
    try {
      const createText = await callToolText(session.client, 'create_config', {
        name: 'Initial Test Configuration',
        description: 'Initial test configuration created by automated test',
        component_id: EXTRACTOR_COMPONENT,
        parameters: { initial_param: 'initial_value' },
        storage: {},
      });
      const configurationId = extractConfigurationId(createText);

      // create the initial row
      await callToolText(session.client, 'add_config_row', {
        name: 'Initial Test Row Configuration',
        description: 'Initial row configuration for update test',
        component_id: EXTRACTOR_COMPONENT,
        configuration_id: configurationId,
        parameters: { initial_row_param: 'initial_row_value' },
        storage: {},
      });

      // fetch the row id from the configuration detail (raw Storage API)
      const rowId = await fetchFirstRowId(project, EXTRACTOR_COMPONENT, configurationId);

      const text = await callToolText(session.client, 'update_config_row', {
        change_description: 'Integration test update',
        component_id: EXTRACTOR_COMPONENT,
        configuration_id: configurationId,
        configuration_row_id: rowId,
        ...updates,
      });
      expect(text).toContain(EXTRACTOR_COMPONENT);
      expect(text).toContain(configurationId);
      expect(text).toMatch(/success/);
      expect(text).toContain(expectDescription);
      expect(text).toContain(`Configuration: ${expectName}`);
    } finally {
      await session.close();
    }
  });

  // --- create_sql_transformation -------------------------------------------

  it('create_sql_transformation creates a SQL transformation', async () => {
    const project = await getTestProjectForTest({ backend: 'snowflake' });
    const session = await connectMcp(project.config);
    try {
      const text = await callToolText(session.client, 'create_sql_transformation', {
        name: 'Test SQL Transformation',
        description: 'Test SQL transformation created by automated test',
        sql_code_blocks: [
          {
            name: 'Main transformation',
            script: 'SELECT 1 as test_column; SELECT 2 as another_column;',
          },
        ],
        created_table_names: ['test_output_table'],
      });
      expect(text).toContain('Test SQL transformation created by automated test');
      expect(text).toContain('keboola.snowflake-transformation');
      expect(text).toMatch(/success/);
      expect(text).toContain('Transformation: Test SQL Transformation');
      expect(text).toContain('Transformations dashboard');
    } finally {
      await session.close();
    }
  });

  // --- update_sql_transformation -------------------------------------------

  it.each([
    {
      label: 'all fields',
      updates: {
        name: 'Updated SQL transformation name',
        description: 'Updated SQL transformation description',
        parameter_updates: [
          { op: 'rename_block', block_id: 'b0', block_name: 'Updated block' },
          { op: 'rename_code', block_id: 'b0', code_id: 'b0.c0', code_name: 'Updated code' },
          {
            op: 'set_code',
            block_id: 'b0',
            code_id: 'b0.c0',
            script:
              'SELECT 1 as updated_column;\n\nSELECT 2 as additional_column;\n\nSELECT 3 as third_column;\n\n',
          },
        ],
        storage: {
          input: { tables: [{ source: 'in.c-bucket.input_table', destination: 'input.csv' }] },
          output: {
            tables: [
              { source: 'updated_output_table', destination: 'out.c-bucket.updated_output_table' },
              { source: 'second_output_table', destination: 'out.c-bucket.second_output_table' },
            ],
          },
        },
      },
      expectName: 'Updated SQL transformation name',
      expectDescription: 'Updated SQL transformation description',
    },
    {
      label: 'just name',
      updates: { name: 'Updated SQL transformation name' },
      expectName: 'Updated SQL transformation name',
      expectDescription: 'Initial SQL transformation for update test',
    },
    {
      label: 'just description',
      updates: { description: 'Updated SQL transformation description' },
      expectName: 'Initial Test SQL Transformation',
      expectDescription: 'Updated SQL transformation description',
    },
    {
      label: 'just parameters',
      updates: {
        parameter_updates: [
          {
            op: 'str_replace',
            block_id: 'b0',
            code_id: 'b0.c0',
            search_for: 'SELECT 1',
            replace_with: 'SELECT 12',
          },
          {
            op: 'add_script',
            block_id: 'b0',
            code_id: 'b0.c0',
            script: 'SELECT 2 as additional_column',
            position: 'end',
          },
        ],
      },
      expectName: 'Initial Test SQL Transformation',
      expectDescription: 'Initial SQL transformation for update test',
    },
    {
      label: 'just storage',
      updates: {
        storage: {
          input: { tables: [{ source: 'in.c-bucket.input_table', destination: 'input.csv' }] },
          output: {
            tables: [
              { source: 'updated_output_table', destination: 'out.c-bucket.updated_output_table' },
              { source: 'second_output_table', destination: 'out.c-bucket.second_output_table' },
            ],
          },
        },
      },
      expectName: 'Initial Test SQL Transformation',
      expectDescription: 'Initial SQL transformation for update test',
    },
  ])('update_sql_transformation updates a transformation ($label)', async ({ updates, expectName, expectDescription }) => {
    const project = await getTestProjectForTest({ backend: 'snowflake' });
    const session = await connectMcp(project.config);
    try {
      const createText = await callToolText(session.client, 'create_sql_transformation', {
        name: 'Initial Test SQL Transformation',
        description: 'Initial SQL transformation for update test',
        sql_code_blocks: [{ name: 'Initial transformation', script: 'SELECT 1 as initial_column;' }],
        created_table_names: ['initial_output_table'],
      });
      const configurationId = extractConfigurationId(createText);

      const text = await callToolText(session.client, 'update_sql_transformation', {
        change_description: 'Integration test update',
        configuration_id: configurationId,
        ...updates,
      });
      expect(text).toContain('keboola.snowflake-transformation');
      expect(text).toContain(configurationId);
      expect(text).toMatch(/success/);
      expect(text).toContain(expectDescription);
      expect(text).toContain(`Transformation: ${expectName}`);
    } finally {
      await session.close();
    }
  });
});

/** Pulls the configuration_id out of a TOON ConfigToolOutput text blob. */
function extractConfigurationId(text: string): string {
  const match = text.match(/configuration_id:\s*([^\s,]+)/);
  if (!match) throw new Error(`Could not find configuration_id in tool output:\n${text}`);
  return match[1]!.replace(/["']/g, '');
}

/** Reads the first row id of a configuration via the raw Storage API. */
async function fetchFirstRowId(
  project: { storageApiUrl: string; storageApiToken: string },
  componentId: string,
  configurationId: string,
): Promise<string> {
  const res = await fetch(
    `${project.storageApiUrl}/v2/storage/branch/default/components/${componentId}/configs/${configurationId}`,
    { headers: { 'X-StorageApi-Token': project.storageApiToken } },
  );
  if (!res.ok) throw new Error(`Failed to fetch config detail: ${res.status} ${await res.text()}`);
  const detail = (await res.json()) as { rows?: { id: string }[] };
  const rows = detail.rows ?? [];
  if (rows.length === 0) throw new Error('No rows found in configuration');
  return String(rows[0]!.id);
}
