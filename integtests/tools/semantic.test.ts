import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';

import { callToolRaw, callToolText, connectMcp, type McpSession } from '../helpers/mcp';
import { getTestProjectForTest, type TestProject } from '../testproject/fixture';

import { createRawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';

// Ported from integtests/tools/semantic/test_tools.py.
//
// The four semantic tools (get_semantic_context, get_semantic_schema,
// search_semantic_context, validate_semantic_query) are gated behind the
// `mcp-semantic-tooling` project feature (src/mcp/filtering.ts). When the feature is
// absent the tools are FILTERED OUT of tools/list and any call is DENIED with an
// McpError -> the SDK surfaces that as a thrown/rejected promise carrying the message
// 'is not available in this project ... "Semantic Layer Tooling" feature'.
//
// The shared pool projects do NOT have this feature (verified live: tokens/verify returns
// no mcp-semantic-tooling for any pool project). So in this environment every semantic
// test is expected to take the "feature absent" branch: we assert the documented
// unavailable behavior (tool hidden + call denied) once, and skip the metastore-seeded
// happy-path tests with a clear reason. The happy-path assertions are kept inline (guarded
// behind the feature probe) so they run as soon as a project with the feature is leased.

const SEMANTIC_TOOL_NAMES = [
  'search_semantic_context',
  'get_semantic_context',
  'get_semantic_schema',
  'validate_semantic_query',
] as const;

/** True if the leased project has the semantic-tooling feature surfaced (tools listed). */
const semanticToolsAvailable = async (session: McpSession): Promise<boolean> => {
  const { tools } = await session.client.listTools();
  const names = new Set(tools.map((t) => t.name));
  return SEMANTIC_TOOL_NAMES.every((n) => names.has(n));
};

// ---------------------------------------------------------------------------
// Metastore seeding (port of the Python `semantic_test_setup` fixture). Only used when the
// feature is present; the raw metastore client mirrors keboola_client.metastore_client.
// ---------------------------------------------------------------------------

type MetastoreObject = { id: string; [k: string]: unknown };

const createMetastore = (config: Config) => {
  const urls = deriveServiceUrls(config.storageApiUrl!);
  const token = config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken!;
  const raw = createRawClient({ baseUrl: urls.metastore, token });
  return {
    createObject: async (
      objectType: string,
      name: string,
      data: Record<string, unknown>,
    ): Promise<MetastoreObject> =>
      raw.post<MetastoreObject>(`objects/${objectType}`, { body: { name, data } }),
    deleteObject: async (objectType: string, id: string): Promise<void> => {
      await raw.delete(`objects/${objectType}/${id}`);
    },
  };
};

type SemanticSetup = {
  slug: string;
  modelId: string;
  modelName: string;
  primaryDatasetId: string;
  secondaryDatasetId: string;
  primaryTableId: string;
  primaryFqn: string;
  metricId: string;
  metricName: string;
  relationshipId: string;
  constraintId: string;
  cleanup: () => Promise<void>;
};

/**
 * Seeds a full semantic model (model + 2 datasets + metric + relationship + constraint),
 * returning the created IDs plus a teardown that deletes them (twice, to clear the
 * soft-delete), mirroring the Python fixture. Only invoked when the feature is enabled.
 */
const seedSemanticModel = async (project: TestProject): Promise<SemanticSetup> => {
  const metastore = createMetastore(project.config);
  const uniqueId = randomUUID().slice(0, 8);
  const slug = `it-semantic-${uniqueId}`;
  const primaryTableId = `in.c-it-semantic.${slug}_orders`;
  const secondaryTableId = `in.c-it-semantic.${slug}_orders_aux`;
  const primaryFqn = `${slug}_orders`;
  const secondaryFqn = `${slug}_orders_aux`;
  const sqlDialect = project.backend === 'bigquery' ? 'bigquery' : 'snowflake';

  const created: [string, string][] = [];
  const cleanup = async (): Promise<void> => {
    for (const [objectType, id] of [...created].reverse()) {
      try {
        await metastore.deleteObject(objectType, id);
        await metastore.deleteObject(objectType, id);
      } catch {
        // best-effort (401/403/404 expected on the second pass).
      }
    }
  };

  try {
    const modelName = `${slug} model`;
    const model = await metastore.createObject('semantic-model', modelName, {
      name: modelName,
      description: `Semantic walkthrough model ${slug}`,
      sql_dialect: sqlDialect,
    });
    created.push(['semantic-model', model.id]);

    const primaryDataset = await metastore.createObject('semantic-dataset', `${slug} orders`, {
      name: `${slug} orders`,
      description: `Primary walkthrough dataset ${slug}`,
      tableId: primaryTableId,
      fqn: primaryFqn,
      modelUUID: model.id,
    });
    created.push(['semantic-dataset', primaryDataset.id]);

    const secondaryDataset = await metastore.createObject(
      'semantic-dataset',
      `${slug} orders aux`,
      {
        name: `${slug} orders aux`,
        description: `Secondary walkthrough dataset ${slug}`,
        tableId: secondaryTableId,
        fqn: secondaryFqn,
        modelUUID: model.id,
      },
    );
    created.push(['semantic-dataset', secondaryDataset.id]);

    const metricName = `${slug} total items`;
    const metric = await metastore.createObject('semantic-metric', metricName, {
      name: metricName,
      description: `Walkthrough metric ${slug}`,
      sql: 'SUM(item_count)',
      dataset: primaryTableId,
      modelUUID: model.id,
    });
    created.push(['semantic-metric', metric.id]);

    const relationship = await metastore.createObject(
      'semantic-relationship',
      `${slug} relationship`,
      {
        name: `${slug} relationship`,
        modelUUID: model.id,
        from: primaryTableId,
        to: secondaryTableId,
        type: 'left',
        on: 'orders.id = orders_aux.id',
      },
    );
    created.push(['semantic-relationship', relationship.id]);

    const constraintName = `it_semantic_${uniqueId}_constraint`;
    const constraint = await metastore.createObject('semantic-constraint', constraintName, {
      name: constraintName,
      description: `Walkthrough exclusion rule ${slug}`,
      modelUUID: model.id,
      constraintType: 'exclusion',
      severity: 'warning',
      rule: 'Do not combine both walkthrough datasets in one query.',
      metrics: [metricName],
      datasets: [primaryTableId, secondaryTableId],
    });
    created.push(['semantic-constraint', constraint.id]);

    return {
      slug,
      modelId: model.id,
      modelName,
      primaryDatasetId: primaryDataset.id,
      secondaryDatasetId: secondaryDataset.id,
      primaryTableId,
      primaryFqn,
      metricId: metric.id,
      metricName,
      relationshipId: relationship.id,
      constraintId: constraint.id,
      cleanup,
    };
  } catch (err) {
    await cleanup();
    throw err;
  }
};

describe('semantic tools (integration)', () => {
  // Documented unavailable-behavior: with the feature absent the tools are hidden from
  // tools/list and any call is denied. This keeps the suite meaningful (not pure skips)
  // against the feature-less pool. When the feature IS enabled this asserts availability.
  it('semantic tools follow the project-feature gate', async () => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      const available = await semanticToolsAvailable(session);
      if (available) {
        // Feature present: a call must NOT be denied by the gate (it may still need data,
        // but get_semantic_schema works with no project data).
        const schema = await callToolText(session.client, 'get_semantic_schema', {
          semantic_types: ['semantic-dataset'],
        });
        expect(schema).toContain('semantic-dataset');
        return;
      }
      // Feature absent: tool hidden from list AND call denied with the gating message.
      const result = await callToolRaw(session.client, 'get_semantic_schema', {
        semantic_types: ['semantic-dataset'],
      }).then(
        (r) => ({ thrown: false as const, r }),
        (e: unknown) => ({ thrown: true as const, message: (e as Error).message }),
      );
      expect(result.thrown).toBe(true);
      if (result.thrown) {
        expect(result.message).toMatch(/not available in this project/i);
        expect(result.message).toMatch(/Semantic Layer Tooling/i);
      }
    } finally {
      await session.close();
    }
  });

  // Port of test_search_semantic_context.
  it('search_semantic_context groups matches by semantic model', async (ctx) => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      if (!(await semanticToolsAvailable(session))) {
        // Skipped: project lacks the `mcp-semantic-tooling` feature (none of the shared pool
        // projects have it), so the tool is filtered out / denied. See the gate test above.
        return ctx.skip();
      }
      const setup = await seedSemanticModel(project);
      try {
        const text = await callToolText(session.client, 'search_semantic_context', {
          patterns: [setup.slug],
          max_results: 20,
        });
        // One model group, matching our seeded model, surfacing all object types.
        expect(text).toContain(setup.modelId);
        expect(text).toContain('semantic-model');
        expect(text).toContain('semantic-dataset');
        expect(text).toContain('semantic-metric');
        expect(text).toContain('semantic-relationship');
        expect(text).toContain('semantic-constraint');
      } finally {
        await setup.cleanup();
      }
    } finally {
      await session.close();
    }
  });

  // Port of test_get_semantic_context.
  it('get_semantic_context returns objects grouped by type', async (ctx) => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      if (!(await semanticToolsAvailable(session))) {
        return ctx.skip();
      }
      const setup = await seedSemanticModel(project);
      try {
        const text = await callToolText(session.client, 'get_semantic_context', {
          semantic_objects: [
            { object_type: 'semantic-model', ids: [setup.modelId] },
            {
              object_type: 'semantic-dataset',
              ids: [setup.primaryDatasetId, setup.secondaryDatasetId],
            },
            { object_type: 'semantic-metric', ids: [setup.metricId] },
            { object_type: 'semantic-relationship', ids: [setup.relationshipId] },
            { object_type: 'semantic-constraint', ids: [setup.constraintId] },
          ],
          semantic_model_ids: [setup.modelId],
        });
        expect(text).toContain(setup.modelId);
        expect(text).toContain(setup.primaryDatasetId);
        expect(text).toContain(setup.secondaryDatasetId);
        // ids-supplied selections return full objects with an `attributes` block.
        expect(text).toMatch(/attributes/);
      } finally {
        await setup.cleanup();
      }
    } finally {
      await session.close();
    }
  });

  // Port of test_get_semantic_schema. Needs no seeded data.
  it('get_semantic_schema returns JSON schemas for requested types', async (ctx) => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      if (!(await semanticToolsAvailable(session))) {
        return ctx.skip();
      }
      const text = await callToolText(session.client, 'get_semantic_schema', {
        semantic_types: ['semantic-dataset', 'semantic-metric'],
      });
      expect(text).toContain('semantic-dataset');
      expect(text).toContain('semantic-metric');
      expect(text).toMatch(/schema/);
    } finally {
      await session.close();
    }
  });

  // Port of test_validate_semantic_query.
  it('validate_semantic_query validates a query against the seeded model', async (ctx) => {
    const project = await getTestProjectForTest({ clean: false });
    const session = await connectMcp(project.config);
    try {
      if (!(await semanticToolsAvailable(session))) {
        return ctx.skip();
      }
      const setup = await seedSemanticModel(project);
      try {
        const text = await callToolText(session.client, 'validate_semantic_query', {
          sql_query: `SELECT SUM(item_count) AS total_items FROM ${setup.primaryFqn}`,
          semantic_model_ids: [setup.modelId],
          expected_semantic_objects: [
            { object_type: 'semantic-dataset', ids: [setup.primaryDatasetId] },
            { object_type: 'semantic-metric', ids: [setup.metricId] },
          ],
        });
        // Auto-detected validation is valid and resolves to our model.
        expect(text).toMatch(/valid:\s*true/i);
        expect(text).toContain(setup.modelId);
        expect(text).toContain(setup.metricId);
        expect(text).toContain(setup.primaryDatasetId);
      } finally {
        await setup.cleanup();
      }
    } finally {
      await session.close();
    }
  });
});
