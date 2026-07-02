import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createRawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';
import { getObjectById, validateSemanticQueryWithUsedObjects } from './detect';
import {
  type SemanticObjectRef,
  type SemanticObjectType,
  SemanticObjectTypeEnum,
  type SemanticObjectTypeSelection,
  SemanticObjectTypeSelectionSchema,
  type SemanticServiceData,
  type SemanticServiceDataTypeGroup,
  type SemanticValidationServiceOutput,
} from './model';
import {
  createMetastoreClient,
  loadSemanticContextForType,
  loadValidationContexts,
  type MetastoreClient,
  searchSemanticContext,
} from './service';

// Ported 1:1 from tools/semantic/tools.py.

const asString = (value: unknown): string | null => (typeof value === 'string' ? value : null);

// --- Tool-facing compact views (port of model classes in tools.py) -------------

const compactSemanticObject = (obj: SemanticServiceData): Record<string, unknown> => {
  const a = obj.attributes;
  switch (obj.semanticType) {
    case 'semantic-model':
      return {
        id: obj.id,
        name: obj.displayName,
        description: asString(a.description),
        sql_dialect: asString(a.sql_dialect),
      };
    case 'semantic-dataset':
      return {
        id: obj.id,
        name: obj.displayName,
        tableId: asString(a.tableId),
        description: asString(a.description),
        model_uuid: asString(a.modelUUID),
        fqn: asString(a.fqn),
      };
    case 'semantic-metric':
      return {
        id: obj.id,
        name: obj.displayName,
        description: asString(a.description),
        dataset: asString(a.dataset),
        model_uuid: asString(a.modelUUID),
      };
    case 'semantic-relationship':
      return {
        id: obj.id,
        name: obj.displayName,
        from_dataset: asString(a.from),
        to_dataset: asString(a.to),
        type: asString(a.type),
        on: asString(a.on),
        model_uuid: asString(a.modelUUID),
      };
    case 'semantic-glossary':
      return {
        id: obj.id,
        name: obj.displayName,
        term: asString(a.term),
        definition: asString(a.definition),
        model_uuid: asString(a.modelUUID),
      };
    case 'semantic-constraint':
      return {
        id: obj.id,
        name: obj.displayName,
        description: asString(a.description),
        type: asString(a.constraintType),
        rule: asString(a.rule),
        severity: asString(a.severity),
        model_uuid: asString(a.modelUUID),
      };
    default:
      throw new Error(`Unsupported semantic object type "${obj.semanticType}"`);
  }
};

const fullSemanticObject = (obj: SemanticServiceData): Record<string, unknown> => ({
  id: obj.id,
  name: obj.displayName,
  attributes: obj.attributes ?? {},
});

const compactName = (compact: Record<string, unknown>): string =>
  (compact.name as string | null) || (compact.id as string);

const usedDatasetView = (obj: SemanticServiceData): Record<string, unknown> => ({
  id: obj.id,
  name: obj.name || '',
  tableId: obj.tableId || '',
  description: obj.description || '',
  fqn: obj.fqn || '',
});

const usedMetricView = (obj: SemanticServiceData): Record<string, unknown> => ({
  id: obj.id,
  name: obj.name || '',
  description: obj.description || '',
  sql: obj.sql || '',
  dataset: obj.dataset || '',
});

const formatValidationResult = (
  rawResult: SemanticValidationServiceOutput,
  opts: { models?: SemanticServiceData[]; summaryNotes?: string[] } = {},
): Record<string, unknown> => {
  const models = opts.models ?? [];
  const summaryNotes = opts.summaryNotes ?? [];

  let usedDatasetObjects: SemanticServiceData[] = [];
  let usedMetricObjects: SemanticServiceData[] = [];
  for (const group of rawResult.usedObjectGroups) {
    if (group.objectType === 'semantic-dataset') usedDatasetObjects = group.objects;
    else if (group.objectType === 'semantic-metric') usedMetricObjects = group.objects;
  }

  const usedDatasets = usedDatasetObjects.map(usedDatasetView);
  const usedMetrics = usedMetricObjects.map(usedMetricView);

  const semanticModelOutputs = models.map((m) => compactSemanticObject(m));
  const sqlDialects = [
    ...new Set(models.map((m) => m.sqlDialect).filter((d): d is string => Boolean(d))),
  ].sort();

  const summaryParts: string[] = [];
  if (sqlDialects.length > 1) {
    summaryParts.push(
      `Warning: semantic models use different SQL dialects (${sqlDialects.join(', ')}). ` +
        'The query may not be portable across all models.',
    );
  }
  if (rawResult.violations.length) {
    summaryParts.push(
      'Semantic validation found pre-execution issues that should be fixed before running.',
    );
  }
  if (rawResult.postExecutionChecks.length) {
    summaryParts.push('Some checks should be verified after execution.');
  }
  summaryParts.push(...summaryNotes);

  const summary = summaryParts.length
    ? summaryParts.join('\n')
    : 'Semantic validation finished without relevant findings.';

  return {
    valid: rawResult.valid,
    semantic_models: semanticModelOutputs,
    sql_dialects: sqlDialects,
    used_datasets: usedDatasets,
    used_metrics: usedMetrics,
    matched_relationships: rawResult.matchedRelationships,
    violations: rawResult.violations,
    post_execution_checks: rawResult.postExecutionChecks,
    summary,
  };
};

const compareExpectedAndDetectedObjects = (
  expectedSemanticObjects: SemanticObjectTypeSelection[],
  usedObjectGroups: SemanticServiceDataTypeGroup[],
): {
  matched: SemanticObjectRef[];
  missing: SemanticObjectRef[];
  unexpected: Record<string, unknown>[];
} => {
  if (!expectedSemanticObjects.length) return { matched: [], missing: [], unexpected: [] };

  const expectedIdsByType = new Map<SemanticObjectType, Set<string>>();
  for (const selection of expectedSemanticObjects) {
    if (selection.ids.length) {
      const set = expectedIdsByType.get(selection.object_type) ?? new Set<string>();
      for (const id of selection.ids) set.add(id);
      expectedIdsByType.set(selection.object_type, set);
    }
  }
  const expectedTypes = new Set(expectedSemanticObjects.map((s) => s.object_type));

  const matched: SemanticObjectRef[] = [];
  const missing: SemanticObjectRef[] = [];
  const unexpected: Record<string, unknown>[] = [];

  for (const [objectType, expectedIds] of expectedIdsByType) {
    const detectedIds = new Set(
      usedObjectGroups
        .filter((g) => g.objectType === objectType)
        .flatMap((g) => g.objects.map((o) => o.id)),
    );
    const both = [...expectedIds].filter((id) => detectedIds.has(id)).sort();
    const onlyExpected = [...expectedIds].filter((id) => !detectedIds.has(id)).sort();
    matched.push(...both.map((id) => ({ object_type: objectType, id })));
    missing.push(...onlyExpected.map((id) => ({ object_type: objectType, id })));
  }

  for (const group of usedObjectGroups) {
    const selectionIds = expectedIdsByType.get(group.objectType);
    let unexpectedObjects: Record<string, unknown>[];
    if (!expectedTypes.has(group.objectType)) {
      unexpectedObjects = group.objects.map(compactSemanticObject);
    } else if (selectionIds && selectionIds.size) {
      unexpectedObjects = group.objects
        .filter((obj) => !selectionIds.has(obj.id))
        .map(compactSemanticObject);
    } else {
      unexpectedObjects = [];
    }
    if (unexpectedObjects.length) {
      unexpected.push({ object_type: group.objectType, objects: unexpectedObjects });
    }
  }

  return { matched, missing, unexpected };
};

// --- Tool registration ---------------------------------------------------------

export const registerSemanticTools = (server: McpServer, config: Config): void => {
  const buildClient = (): MetastoreClient => {
    if (!config.storageApiUrl) throw new Error('Storage API URL is not configured.');
    if (!config.storageToken) throw new Error('Storage API token is not configured.');
    const urls = deriveServiceUrls(config.storageApiUrl);
    const token = config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken;
    return createMetastoreClient(createRawClient({ baseUrl: urls.metastore, token }));
  };

  registerTool(server, {
    name: 'search_semantic_context',
    title: 'Search semantic context',
    annotations: { readOnlyHint: true },
    description:
      'Searches semantic models and semantic objects using regex patterns matched against their names, descriptions and\n' +
      'stringified JSON attributes.\n\n' +
      'Returns compact matches grouped by semantic model. Each match includes the semantic object type,\n' +
      'the paths where the patterns matched, and compact object view.\n\n' +
      'CONSIDERATIONS:\n' +
      '- The search is case-insensitive by default. Use `case_sensitive=True` when exact casing matters.\n' +
      '- The search is performed against semantic object names and data attributes which are stringified JSON objects\n' +
      'following their corresponding JSON schema.\n' +
      '- The search can be scoped to specific semantic models or semantic object types but prefer broader search without\n' +
      'scoping unless required by the context.\n\n' +
      'WHEN TO USE:\n' +
      '- When you need to discover which semantic objects are relevant to a user request.\n' +
      '- When you know business terms, column names, metric fragments, or rule names, but not exact object UUIDs.\n' +
      '- When you need to find semantic objects by keyword or values used in their attributes.\n\n' +
      'WHEN NOT TO USE:\n' +
      '- When you know the exact IDs.\n\n' +
      'EXAMPLES:\n' +
      '- Find semantic objects by business concepts for revenue or sales:\n' +
      '  `patterns=["revenue", "sales"]`\n' +
      '- Find semantic objects using a Keboola table ID:\n' +
      '  `patterns=["out.c-sales-main.fact_orders"]`\n' +
      '- Find semantic dataset for a certain table:\n' +
      '  `patterns=["in.c-sales-main.fact_orders"], semantic_types=["semantic-dataset"]`\n' +
      '- Find semantic datasets that mention a column name:\n' +
      '  `patterns=["column_name"], semantic_types=["semantic-dataset"]`\n' +
      '- Search semantic objects e.g. semantic metrics, relationships, and constraints using a certain semantic dataset:\n' +
      '  `patterns=["table-id-of-the-dataset"], semantic_types=["semantic-metric",`\n' +
      '  `"semantic-relationship", "semantic-constraint"]`\n' +
      '- Search semantic constraints using e.g. certain semantic metrics and certain semantic datasets:\n' +
      '  `patterns=["metric-name-1", "metric-name-2", "table-id-from-the-dataset"],`\n' +
      '  `semantic_types=["semantic-metric", "semantic-relationship"]`\n' +
      '- Search something within specific semantic models only:\n' +
      '  `patterns=["something"], semantic_model_ids=["<semantic-model-uuid-1>", "<semantic-model-uuid-2>"]`',
    inputSchema: {
      patterns: z
        .array(z.string())
        .describe(
          'One or more regex patterns used to search semantic metadata. ' +
            'The search checks semantic model names plus semantic object names and nested attribute values. ' +
            'Use multiple patterns when you need to find objects related to several business terms at once.',
        ),
      semantic_types: z
        .array(SemanticObjectTypeEnum)
        .default([])
        .describe(
          'Optional semantic object types to search. ' +
            'Empty list [] means ALL semantic object types are searched. ' +
            'Use this to narrow the search when you already know whether you want datasets, metrics, ' +
            'relationships, glossary terms, constraints, or models.',
        ),
      semantic_model_ids: z
        .array(z.string())
        .default([])
        .describe(
          'Optional list of semantic model IDs to restrict the search to specific models. ' +
            'Empty list [] means search across all semantic models.',
        ),
      case_sensitive: z
        .boolean()
        .default(false)
        .describe(
          'Whether regex matching should be case-sensitive. ' +
            'Leave false for normal discovery; set true only when exact casing matters.',
        ),
      max_results: z
        .number()
        .int()
        .default(100)
        .describe(
          'Maximum number of matched semantic objects to return. ' +
            'Use a smaller value for quick discovery and a larger value only when you need a broader result set.',
        ),
    },
    handler: async (args) => {
      const cleanedPatterns = args.patterns.filter((p) => p && p.trim()).map((p) => p.trim());
      if (!cleanedPatterns.length) throw new Error('At least one regex pattern must be provided.');
      if (args.max_results <= 0) throw new Error('max_results must be a positive integer.');

      const client = buildClient();
      const hits = await searchSemanticContext(client, cleanedPatterns, {
        semanticTypes: args.semantic_types,
        semanticModelIds: args.semantic_model_ids.length ? args.semantic_model_ids : null,
        caseSensitive: args.case_sensitive,
        maxResults: args.max_results,
      });

      const grouped = new Map<string, Record<string, unknown>[]>();
      for (const hit of hits) {
        const list = grouped.get(hit.semanticModelId) ?? [];
        list.push({
          object_type: hit.objectType,
          matched_paths: hit.matchedPaths,
          data: compactSemanticObject(hit.object),
        });
        grouped.set(hit.semanticModelId, list);
      }

      const modelResults = [...grouped.entries()].map(([modelId, matches]) => ({
        semantic_model_id: modelId,
        matches: [...matches].sort((a, b) =>
          compactName(a.data as Record<string, unknown>).localeCompare(
            compactName(b.data as Record<string, unknown>),
          ),
        ),
      }));
      modelResults.sort((a, b) => a.semantic_model_id.localeCompare(b.semantic_model_id));
      return modelResults;
    },
  });

  registerTool(server, {
    name: 'get_semantic_context',
    title: 'Get semantic context',
    annotations: { readOnlyHint: true },
    description:
      'Loads semantic objects grouped by semantic object type.\n\n' +
      'CONSIDERATIONS:\n' +
      '- If a selection has empty `ids`, the tool returns all objects of that type in compact form.\n' +
      '- If a selection has non-empty `ids`, the tool returns only those specific objects with full attributes.\n' +
      '- `semantic_model_ids` optionally narrows the lookup to specific semantic models.\n\n' +
      'WHEN TO USE:\n' +
      '- When you already know IDs of the semantic objects you want to load and want to inspect them in detail.\n' +
      '- When you want to list all semantic objects of certain types or specific semantic models.\n' +
      '- When you want to list semantic models.\n\n' +
      'WHEN NOT TO USE:\n' +
      '- When you need to discover semantic objects.\n\n' +
      'EXAMPLES:\n' +
      '- List all semantic models:\n' +
      '  `semantic_objects=[{"object_type": "semantic-model"}]`\n' +
      '- List semantic datasets and metrics for specific semantic models:\n' +
      '  `semantic_objects=[{"object_type": "semantic-dataset"}, {"object_type": "semantic-metric"}],`\n' +
      '  `semantic_model_ids=["model-uuid-1", "model-uuid-2"]`\n' +
      '- Get detailed context for specific semantic objects by their id:\n' +
      '  `semantic_objects=[{"object_type": "semantic-dataset", "ids": ["dataset-uuid-1"]},`\n' +
      '  `{"object_type": "semantic-metric", "ids": ["metric-uuid-1", "metric-uuid-2"]}]`\n' +
      '- List all constraints for specific semantic models:\n' +
      '  `semantic_objects=[{"object_type": "semantic-constraint"}], semantic_model_ids=["model-uuid-1"]`',
    inputSchema: {
      semantic_objects: z
        .array(SemanticObjectTypeSelectionSchema)
        .describe(
          'List of semantic object selections to load. ' +
            'Each item contains "object_type" and optional "ids". ' +
            'If "ids" is empty, all objects of that type are returned in compact form. ' +
            'If "ids" is non-empty, only those objects are returned with full attributes.',
        ),
      semantic_model_ids: z
        .array(z.string())
        .default([])
        .describe(
          'Optional list of semantic model IDs to restrict loading to specific models. ' +
            'Empty list [] means load across all semantic models.',
        ),
    },
    handler: async (args) => {
      if (!args.semantic_objects.length) {
        throw new Error('At least one semantic object type must be provided.');
      }
      const client = buildClient();
      const modelIds = args.semantic_model_ids.length ? args.semantic_model_ids : null;

      const groups = await Promise.all(
        args.semantic_objects.map((selection) =>
          loadSemanticContextForType(client, selection.object_type, {
            semanticModelIds: modelIds,
            ids: selection.ids,
          }),
        ),
      );

      return args.semantic_objects.map((selection, i) => {
        const context = groups[i]!;
        if (selection.ids.length) {
          return {
            object_type: context.objectType,
            objects: context.objects.map(fullSemanticObject),
          };
        }
        return {
          object_type: context.objectType,
          objects: context.objects.map(compactSemanticObject),
        };
      });
    },
  });

  registerTool(server, {
    name: 'get_semantic_schema',
    title: 'Get semantic schema',
    annotations: { readOnlyHint: true },
    description:
      'Returns JSON schemas for the requested semantic object types.\n\n' +
      'WHEN TO USE:\n' +
      '- When you want to know the JSON schema of a semantic object type, e.g. before searching something specific.',
    inputSchema: {
      semantic_types: z
        .array(SemanticObjectTypeEnum)
        .describe(
          'List of semantic object types for which JSON schemas should be returned. ' +
            'Each returned item contains the requested semantic type and its metastore schema.',
        ),
    },
    handler: async (args) => {
      if (!args.semantic_types.length) {
        throw new Error('At least one semantic type must be provided.');
      }
      const client = buildClient();
      const schemas = await Promise.all(
        args.semantic_types.map((semanticType) => client.getSchema(semanticType)),
      );
      return args.semantic_types.map((semanticType, i) => ({
        semantic_type: semanticType,
        schema: schemas[i]!,
      }));
    },
  });

  registerTool(server, {
    name: 'validate_semantic_query',
    title: 'Validate semantic query',
    annotations: { readOnlyHint: true },
    description:
      'Performs best-effort semantic validation of an SQL query against one or more semantic models and compares it with\n' +
      'the expected semantic objects provided.\n\n' +
      'RETURNS:\n' +
      '- `validation_auto_detected`: semantic validation built from objects heuristically detected in the SQL\n' +
      '- `validation_detected_from_expected`: semantic validation built only from explicitly provided expected object IDs\n' +
      '- expected semantic objects that were matched or missing in the auto-detected result\n' +
      '- unexpected auto-detected objects outside the expected semantic scope\n\n' +
      'LIMITATIONS:\n' +
      '- Detection is heuristic and based on string matching over SQL and semantic metadata.\n' +
      '- The tool does not parse SQL semantically and does not execute the query.\n' +
      '- Auto-detected objects, missing objects, and relationship matches may therefore be imperfect.\n' +
      '- Use the result as a best-effort semantic check, not as a formal proof that the query is correct.\n\n' +
      'CONSIDERATIONS:\n' +
      '-  Prefer calling this tool before executing any SQL that touches semantic objects.\n' +
      '- This tool confirms the SQL dialect, surfaces semantic constraint violations, and provides post-execution checks.\n' +
      '- Only proceed to query_data once this tool returns valid=True and violations is empty. If violations are found,\n' +
      'fix the query first or consider the limitations of this tool.\n\n' +
      'WHEN TO USE:\n' +
      '- Before generating or approving a query that should follow a semantic model.\n' +
      '- When you want to validate a SQL query against the semantic objects before executing it using "query_data" tool\n' +
      'or creating a new SQL transformation out of it, especially when investigating data quality issues.\n' +
      '- When you want to verify that a query uses the intended semantic objects.\n' +
      '- When you need to surface semantic business-rule violations or follow-up checks.\n\n' +
      'EXAMPLES:\n' +
      '- Validate a SQL query against one semantic model:\n' +
      '  `sql_query="SELECT SUM(\\"REVENUE\\") FROM ...", semantic_model_ids=["semantic-model-uuid"],`\n' +
      '  `expected_semantic_objects=[{"object_type": "semantic-dataset"}]`\n' +
      '- Validate a cross-model query against two semantic models:\n' +
      '  `sql_query="SELECT * FROM ...", semantic_model_ids=["model-uuid-1", "model-uuid-2"],`\n' +
      '  `expected_semantic_objects=[{"object_type": "semantic-dataset", "ids": ["dataset-uuid-1"]}]`\n' +
      '- Validate a query and compare it against expected objects:\n' +
      '  `sql_query="SELECT SUM(\\"REVENUE\\") FROM ...", semantic_model_ids=["semantic-model-uuid"],`\n' +
      '  `expected_semantic_objects=[{"object_type": "semantic-metric", "ids": ["metric-uuid-1"]}]`',
    inputSchema: {
      sql_query: z
        .string()
        .describe(
          'SQL query that should be checked against the semantic layer. ' +
            'The query is not executed; the tool performs best-effort semantic detection and rule validation ' +
            'using heuristic string matching, so the detected objects may be incomplete or imperfect.',
        ),
      semantic_model_ids: z
        .array(z.string())
        .describe(
          'One or more semantic model IDs against which the SQL should be validated. ' +
            'Contexts from all models are merged into a single universe for object detection. ' +
            'Constraint evaluation is performed per model to avoid cross-model rule contamination.',
        ),
      expected_semantic_objects: z
        .array(SemanticObjectTypeSelectionSchema)
        .default([])
        .describe(
          'Optional semantic object selections that define the expected semantic scope of the query. ' +
            'These expectations are compared with the objects actually detected in the SQL. ' +
            'Use `ids` when you want to assert that specific semantic objects should be present.',
        ),
    },
    handler: async (args) => {
      if (!args.sql_query.trim()) throw new Error('sql_query must not be empty.');
      const cleanedModelIds = [
        ...new Set(args.semantic_model_ids.filter((m) => m && m.trim()).map((m) => m.trim())),
      ];
      if (!cleanedModelIds.length) {
        throw new Error('At least one semantic_model_id must be provided.');
      }

      const client = buildClient();

      const models = await Promise.all(
        cleanedModelIds.map((modelId) => getObjectById(client, 'semantic-model', modelId)),
      );

      // Pre-load contexts once when both validation paths will run.
      let preLoadedContexts: Map<SemanticObjectType, SemanticServiceDataTypeGroup>[] | undefined;
      if (args.expected_semantic_objects.length) {
        preLoadedContexts = await loadValidationContexts(client, cleanedModelIds);
      }

      const rawAutoDetected = await validateSemanticQueryWithUsedObjects(
        client,
        args.sql_query,
        cleanedModelIds,
        { contextsPerModel: preLoadedContexts },
      );

      let matched: SemanticObjectRef[] = [];
      let missing: SemanticObjectRef[] = [];
      let unexpected: Record<string, unknown>[] = [];
      let rawFromExpected: SemanticValidationServiceOutput | null = null;

      if (args.expected_semantic_objects.length) {
        ({ matched, missing, unexpected } = compareExpectedAndDetectedObjects(
          args.expected_semantic_objects,
          rawAutoDetected.usedObjectGroups,
        ));
        const modelIds = args.semantic_model_ids.length ? args.semantic_model_ids : null;
        const expectedObjectGroups = await Promise.all(
          args.expected_semantic_objects.map((selection) =>
            loadSemanticContextForType(client, selection.object_type, {
              semanticModelIds: modelIds,
              ids: selection.ids,
            }),
          ),
        );
        if (expectedObjectGroups.length) {
          rawFromExpected = await validateSemanticQueryWithUsedObjects(
            client,
            args.sql_query,
            cleanedModelIds,
            { usedObjectGroups: expectedObjectGroups, contextsPerModel: preLoadedContexts },
          );
        }
      }

      const autoDetectedSummaryNotes: string[] = [];
      if (missing.length) {
        autoDetectedSummaryNotes.push(
          'Some expected semantic objects were not detected in the SQL query.',
        );
      }
      if (unexpected.length) {
        autoDetectedSummaryNotes.push(
          'Some detected semantic objects fall outside the expected semantic scope.',
        );
      }

      return {
        validation_auto_detected: formatValidationResult(rawAutoDetected, {
          models,
          summaryNotes: autoDetectedSummaryNotes,
        }),
        validation_detected_from_expected:
          rawFromExpected !== null ? formatValidationResult(rawFromExpected, { models }) : null,
        matched_expected_objects: matched,
        missing_expected_objects: missing,
        unexpected_detected_objects: unexpected,
      };
    },
  });
};
