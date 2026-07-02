import {
  type ConstraintValidationFinding,
  type SemanticObjectType,
  type SemanticServiceData,
  type SemanticServiceDataTypeGroup,
  type SemanticValidationServiceOutput,
} from './model';
import {
  getSemanticModelId,
  loadValidationContexts,
  metaName,
  type MetastoreClient,
  toSemanticServiceData,
} from './service';

// Ported 1:1 from tools/semantic/service.py — SQL-string heuristics, object
// detection, constraint evaluation, and validation orchestration.

const POST_QUERY_CONSTRAINT_TYPES = new Set([
  'inequality',
  'equality',
  'range',
  'temporal',
  'conditional',
]);

// Captures the single column name from a simple aggregate metric SQL expression,
// e.g. SUM("REVENUE_YTD") -> "REVENUE_YTD", AVG(margin_pct) -> "margin_pct".
const AGGREGATE_COLUMN_RE = /^\s*\w+\s*\(\s*"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\)\s*$/;

// SQL function names / keywords never treated as column identifiers in ON clauses.
const SQL_KEYWORDS_UPPER = new Set([
  'AND',
  'OR',
  'NOT',
  'IN',
  'IS',
  'NULL',
  'TRUE',
  'FALSE',
  'LEFT',
  'RIGHT',
  'INNER',
  'OUTER',
  'FULL',
  'CROSS',
  'JOIN',
  'ON',
  'WHERE',
  'SELECT',
  'FROM',
  'AS',
  'BY',
  'GROUP',
  'AVG',
  'SUM',
  'COUNT',
  'MIN',
  'MAX',
  'COALESCE',
  'NULLIF',
  'CAST',
  'CONCAT',
  'TRIM',
  'LENGTH',
  'UPPER',
  'LOWER',
  'IFF',
  'CASE',
  'WHEN',
  'THEN',
  'ELSE',
  'END',
]);

const matchesSql = (sqlQuery: string, candidate: string): boolean => {
  if (!candidate) return false;
  const candidateLower = candidate.toLowerCase();
  const sqlLower = sqlQuery.toLowerCase();
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(candidate)) {
    const escaped = candidateLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const pattern = new RegExp(`(?<![a-zA-Z0-9_])${escaped}(?![a-zA-Z0-9_])`);
    return pattern.test(sqlLower);
  }
  return sqlLower.includes(candidateLower);
};

const pickValidationQuery = (
  constraint: SemanticServiceData,
  sqlDialect: string | null | undefined,
): string | null => {
  const validationQuery = constraint.validationQuery;
  if (validationQuery == null) return null;

  const dialectKey = (sqlDialect ?? '').trim().toLowerCase();
  if (dialectKey === 'snowflake' && typeof validationQuery.snowflake === 'string') {
    return validationQuery.snowflake;
  }
  if (dialectKey === 'bigquery' && typeof validationQuery.bigquery === 'string') {
    return validationQuery.bigquery;
  }
  const defaultQuery = validationQuery.default;
  return typeof defaultQuery === 'string' ? defaultQuery : null;
};

const constraintMessage = (constraint: SemanticServiceData, defaultMessage: string): string => {
  const err = constraint.errorMessage?.trim();
  const rem = constraint.remediation?.trim();
  if (err) {
    return rem ? `${err} Remediation: ${rem}` : err;
  }
  if (rem) return `${defaultMessage} Remediation: ${rem}`;
  return defaultMessage;
};

const datasetIdentifiers = (dataset: SemanticServiceData): string[] =>
  [dataset.fqn]
    .filter((c): c is string => typeof c === 'string' && c.trim().length > 0)
    .map((c) => c.trim());

const extractMetricColumn = (sql: string): string | null => {
  const m = AGGREGATE_COLUMN_RE.exec(sql);
  return m ? m[1]! : null;
};

const metricIdentifiers = (metric: SemanticServiceData): string[] => {
  const candidates: string[] = [];
  if (metric.sql) {
    candidates.push(metric.sql);
    const col = extractMetricColumn(metric.sql);
    if (col) candidates.push(col);
  }
  return candidates.map((c) => c.trim()).filter((c) => c.length > 0);
};

const detectUsedDatasets = (
  sqlQuery: string,
  datasets: SemanticServiceData[],
): SemanticServiceData[] =>
  datasets.filter((dataset) =>
    datasetIdentifiers(dataset).some((candidate) => matchesSql(sqlQuery, candidate)),
  );

const detectUsedMetricsForDatasets = (
  sqlQuery: string,
  metrics: SemanticServiceData[],
  usedDatasetIds: Set<string>,
): SemanticServiceData[] => {
  const matches: SemanticServiceData[] = [];
  for (const metric of metrics) {
    if (metric.dataset == null || !usedDatasetIds.has(metric.dataset)) continue;
    if (metricIdentifiers(metric).some((candidate) => matchesSql(sqlQuery, candidate))) {
      matches.push(metric);
    }
  }
  return matches;
};

const extractJoinColumns = (onClause: string): string[] => {
  const cleaned = onClause.replace(/'[^']*'/g, '');
  const tokens = cleaned.match(/\b[A-Z][A-Z0-9_]{2,}\b/g) ?? [];
  const seen = new Set<string>();
  const result: string[] = [];
  for (const token of tokens) {
    if (!SQL_KEYWORDS_UPPER.has(token) && !seen.has(token)) {
      seen.add(token);
      result.push(token);
    }
  }
  return result;
};

const detectUsedRelationships = (
  sqlQuery: string,
  relationships: SemanticServiceData[],
  usedDatasetIds: Set<string>,
): SemanticServiceData[] => {
  const matches: SemanticServiceData[] = [];
  for (const relationship of relationships) {
    if (relationship.fromDataset == null || relationship.toDataset == null) continue;
    if (
      !usedDatasetIds.has(relationship.fromDataset) ||
      !usedDatasetIds.has(relationship.toDataset)
    ) {
      continue;
    }
    if (relationship.on && relationship.on.trim()) {
      const colNames = extractJoinColumns(relationship.on);
      if (colNames.length) {
        if (!colNames.every((col) => matchesSql(sqlQuery, col))) continue;
      } else if (!matchesSql(sqlQuery, relationship.on)) {
        continue;
      }
    }
    matches.push(relationship);
  }
  return matches;
};

const constraintIsRelevant = (
  constraint: SemanticServiceData,
  usedMetricNames: Set<string>,
  usedDatasetIds: Set<string>,
): boolean => {
  const constraintMetrics = new Set(
    (constraint.metrics ?? []).map((m) => m.trim()).filter((m) => m.length > 0),
  );
  const constraintDatasets = new Set(
    (constraint.datasets ?? []).map((d) => d.trim()).filter((d) => d.length > 0),
  );
  if (constraintMetrics.size && [...usedMetricNames].some((m) => constraintMetrics.has(m)))
    return true;
  if (constraintDatasets.size && [...usedDatasetIds].some((d) => constraintDatasets.has(d)))
    return true;
  return constraintMetrics.size === 0 && constraintDatasets.size === 0;
};

// --- Detect + evaluate ---------------------------------------------------------

const emptyGroup = (objectType: SemanticObjectType): SemanticServiceDataTypeGroup => ({
  objectType,
  objects: [],
});

const detectUsedObjectsFromContext = (
  sqlQuery: string,
  contextByType: Map<SemanticObjectType, SemanticServiceDataTypeGroup>,
  usedObjectsByType: Map<SemanticObjectType, SemanticServiceDataTypeGroup>,
): Map<SemanticObjectType, SemanticServiceDataTypeGroup> => {
  const datasets = contextByType.get('semantic-dataset') ?? emptyGroup('semantic-dataset');
  const metrics = contextByType.get('semantic-metric') ?? emptyGroup('semantic-metric');
  const relationships =
    contextByType.get('semantic-relationship') ?? emptyGroup('semantic-relationship');

  let usedDatasetObjects = detectUsedDatasets(sqlQuery, datasets.objects);
  const expectedDatasets = usedObjectsByType.get('semantic-dataset');
  if (expectedDatasets) {
    const ids = new Set(usedDatasetObjects.map((o) => o.id));
    usedDatasetObjects = usedDatasetObjects.concat(
      expectedDatasets.objects.filter((o) => !ids.has(o.id)),
    );
  }
  const usedDatasetIds = new Set(
    usedDatasetObjects.map((item) => (item.tableId ?? '').trim()).filter((id) => id.length > 0),
  );

  let usedMetricObjects = detectUsedMetricsForDatasets(sqlQuery, metrics.objects, usedDatasetIds);
  const expectedMetrics = usedObjectsByType.get('semantic-metric');
  if (expectedMetrics) {
    const ids = new Set(usedMetricObjects.map((o) => o.id));
    usedMetricObjects = usedMetricObjects.concat(
      expectedMetrics.objects.filter((o) => !ids.has(o.id)),
    );
  }

  let usedRelationshipObjects = detectUsedRelationships(
    sqlQuery,
    relationships.objects,
    usedDatasetIds,
  );
  const expectedRelationships = usedObjectsByType.get('semantic-relationship');
  if (expectedRelationships) {
    const ids = new Set(usedRelationshipObjects.map((o) => o.id));
    usedRelationshipObjects = usedRelationshipObjects.concat(
      expectedRelationships.objects.filter((o) => !ids.has(o.id)),
    );
  }

  const usedGroups = new Map<SemanticObjectType, SemanticServiceDataTypeGroup>();
  if (usedDatasetObjects.length) {
    usedGroups.set('semantic-dataset', {
      objectType: 'semantic-dataset',
      objects: usedDatasetObjects,
    });
  }
  if (usedMetricObjects.length) {
    usedGroups.set('semantic-metric', {
      objectType: 'semantic-metric',
      objects: usedMetricObjects,
    });
  }
  if (usedRelationshipObjects.length) {
    usedGroups.set('semantic-relationship', {
      objectType: 'semantic-relationship',
      objects: usedRelationshipObjects,
    });
  }
  return usedGroups;
};

const relationshipNames = (objects: SemanticServiceData[]): string[] =>
  objects.map((item) => item.name || metaName(item.data) || item.id).sort();

const evaluateConstraintsFromContext = (
  contextByType: Map<SemanticObjectType, SemanticServiceDataTypeGroup>,
  usedObjectGroupsByType: Map<SemanticObjectType, SemanticServiceDataTypeGroup>,
): SemanticValidationServiceOutput => {
  const modelGroup = contextByType.get('semantic-model') ?? emptyGroup('semantic-model');
  const model = modelGroup.objects[0] ?? null;
  const constraints = (
    contextByType.get('semantic-constraint') ?? emptyGroup('semantic-constraint')
  ).objects;

  const usedDatasetObjects = usedObjectGroupsByType.get('semantic-dataset')?.objects ?? [];
  const usedMetricObjects = usedObjectGroupsByType.get('semantic-metric')?.objects ?? [];
  const usedRelationshipObjects =
    usedObjectGroupsByType.get('semantic-relationship')?.objects ?? [];

  const usedDatasetIds = new Set(
    usedDatasetObjects.map((i) => (i.tableId ?? '').trim()).filter((i) => i.length > 0),
  );
  const usedMetricNames = new Set(
    usedMetricObjects.map((i) => (i.name ?? '').trim()).filter((i) => i.length > 0),
  );
  const matchedRelationships = relationshipNames(usedRelationshipObjects);

  const sqlDialectStr = model ? model.sqlDialect : null;
  const violations: ConstraintValidationFinding[] = [];
  const postExecutionChecks: ConstraintValidationFinding[] = [];
  let hasError = false;

  for (const constraint of constraints) {
    if (!constraintIsRelevant(constraint, usedMetricNames, usedDatasetIds)) continue;

    const constraintName = constraint.name || metaName(constraint.data) || constraint.id;
    const severity = constraint.severity || 'error';
    const constraintType = constraint.constraintType || 'unknown';
    const validationQuery = pickValidationQuery(constraint, sqlDialectStr);
    const constraintMetrics = (constraint.metrics ?? [])
      .map((m) => m.trim())
      .filter((m) => m.length > 0);
    const constraintDatasets = (constraint.datasets ?? [])
      .map((d) => d.trim())
      .filter((d) => d.length > 0);
    const preQueryCheck = constraint.preQueryCheck ?? false;

    if (constraintType === 'composition') {
      const missingMetrics = constraintMetrics.filter((m) => !usedMetricNames.has(m));
      if (missingMetrics.length) {
        if (severity === 'error') hasError = true;
        violations.push({
          constraint_id: constraint.id,
          constraint_name: constraintName,
          severity,
          status: 'missing_metrics',
          message: constraintMessage(
            constraint,
            `Constraint "${constraintName}" expects metrics present in the SQL: ${missingMetrics.join(', ')}.`,
          ),
          validation_query: validationQuery,
        });
      }
      continue;
    }

    if (constraintType === 'exclusion') {
      const usedExcludedMetrics = constraintMetrics.filter((m) => usedMetricNames.has(m));
      const usedExcludedDatasets = constraintDatasets.filter((d) => usedDatasetIds.has(d));
      if (usedExcludedMetrics.length > 1 || usedExcludedDatasets.length > 1) {
        if (severity === 'error') hasError = true;
        violations.push({
          constraint_id: constraint.id,
          constraint_name: constraintName,
          severity,
          status: 'excluded_combination',
          message: constraintMessage(
            constraint,
            `Constraint "${constraintName}" forbids this combination of semantic objects.`,
          ),
          validation_query: validationQuery,
        });
      }
      continue;
    }

    if (preQueryCheck) {
      if (severity === 'error') hasError = true;
      violations.push({
        constraint_id: constraint.id,
        constraint_name: constraintName,
        severity,
        status: 'pre_query_check',
        message: constraintMessage(
          constraint,
          `Constraint "${constraintName}" should be explicitly checked before trusting the query result.`,
        ),
        validation_query: validationQuery,
      });
      continue;
    }

    if (!POST_QUERY_CONSTRAINT_TYPES.has(constraintType) && validationQuery === null) {
      continue;
    }

    postExecutionChecks.push({
      constraint_id: constraint.id,
      constraint_name: constraintName,
      severity,
      status: 'post_query_check',
      message: constraintMessage(
        constraint,
        `Constraint "${constraintName}" is relevant for this SQL and should be verified against the result.`,
      ),
      validation_query: validationQuery,
    });
  }

  return {
    valid: !hasError,
    usedObjectGroups: [...usedObjectGroupsByType.values()],
    matchedRelationships,
    violations,
    postExecutionChecks,
  };
};

const mergeContexts = (
  contexts: Map<SemanticObjectType, SemanticServiceDataTypeGroup>[],
): Map<SemanticObjectType, SemanticServiceDataTypeGroup> => {
  const merged = new Map<SemanticObjectType, SemanticServiceData[]>();
  for (const context of contexts) {
    for (const [objectType, group] of context) {
      const list = merged.get(objectType) ?? [];
      list.push(...group.objects);
      merged.set(objectType, list);
    }
  }
  return new Map([...merged].map(([objectType, objects]) => [objectType, { objectType, objects }]));
};

const filterUsedObjectsByModel = (
  usedObjectGroupsByType: Map<SemanticObjectType, SemanticServiceDataTypeGroup>,
  modelId: string,
): Map<SemanticObjectType, SemanticServiceDataTypeGroup> => {
  const filtered = new Map<SemanticObjectType, SemanticServiceDataTypeGroup>();
  for (const [objectType, group] of usedObjectGroupsByType) {
    const modelObjects = group.objects.filter((obj) => getSemanticModelId(obj) === modelId);
    if (modelObjects.length) filtered.set(objectType, { objectType, objects: modelObjects });
  }
  return filtered;
};

const mergeUsedObjectGroups = (
  usedObjectGroups: SemanticServiceDataTypeGroup[],
): Map<SemanticObjectType, SemanticServiceDataTypeGroup> => {
  const merged = new Map<SemanticObjectType, SemanticServiceData[]>();
  for (const group of usedObjectGroups) {
    const list = merged.get(group.objectType) ?? [];
    list.push(...group.objects);
    merged.set(group.objectType, list);
  }
  return new Map([...merged].map(([objectType, objects]) => [objectType, { objectType, objects }]));
};

const evaluateUsedObjectsForContexts = (
  semanticModelIds: readonly string[],
  contextsPerModel: Map<SemanticObjectType, SemanticServiceDataTypeGroup>[],
  usedObjectGroupsByType: Map<SemanticObjectType, SemanticServiceDataTypeGroup>,
): SemanticValidationServiceOutput => {
  const allViolations: ConstraintValidationFinding[] = [];
  const allPostChecks: ConstraintValidationFinding[] = [];
  let hasError = false;

  for (let i = 0; i < semanticModelIds.length; i++) {
    const modelId = semanticModelIds[i]!;
    const contextByType = contextsPerModel[i]!;
    const modelUsedObjects = filterUsedObjectsByModel(usedObjectGroupsByType, modelId);
    const perModelResult = evaluateConstraintsFromContext(contextByType, modelUsedObjects);
    allViolations.push(...perModelResult.violations);
    allPostChecks.push(...perModelResult.postExecutionChecks);
    if (!perModelResult.valid) hasError = true;
  }

  const usedRelationships =
    usedObjectGroupsByType.get('semantic-relationship') ?? emptyGroup('semantic-relationship');
  const matchedRelationships = relationshipNames(usedRelationships.objects);

  return {
    valid: !hasError,
    usedObjectGroups: [...usedObjectGroupsByType.values()],
    matchedRelationships,
    violations: allViolations,
    postExecutionChecks: allPostChecks,
  };
};

export const validateSemanticQueryWithUsedObjects = async (
  client: MetastoreClient,
  sqlQuery: string,
  semanticModelIds: readonly string[],
  opts: {
    usedObjectGroups?: SemanticServiceDataTypeGroup[];
    contextsPerModel?: Map<SemanticObjectType, SemanticServiceDataTypeGroup>[];
  } = {},
): Promise<SemanticValidationServiceOutput> => {
  if (!sqlQuery.trim()) {
    throw new Error('sql_query must not be empty.');
  }
  const cleanedModelIds = [
    ...new Set(semanticModelIds.map((m) => m.trim()).filter((m) => m.length > 0)),
  ];
  if (!cleanedModelIds.length) {
    throw new Error('At least one semantic_model_id must be provided.');
  }

  const contextsPerModel =
    opts.contextsPerModel ?? (await loadValidationContexts(client, cleanedModelIds));
  const usedObjectGroups = opts.usedObjectGroups ?? [];

  const mergedContext = mergeContexts(contextsPerModel);
  const usedByType = detectUsedObjectsFromContext(
    sqlQuery,
    mergedContext,
    mergeUsedObjectGroups(usedObjectGroups),
  );
  return evaluateUsedObjectsForContexts(cleanedModelIds, contextsPerModel, usedByType);
};

export const getObjectById = async (
  client: MetastoreClient,
  objectType: SemanticObjectType,
  objectId: string,
): Promise<SemanticServiceData> => {
  const rawObj = await client.getObject(objectType, objectId);
  if (rawObj.type !== objectType) {
    throw new Error(
      `Expected object "${objectId}" to be of type "${objectType}", got "${rawObj.type}" from the Metastore API.`,
    );
  }
  return toSemanticServiceData(objectType, rawObj);
};
