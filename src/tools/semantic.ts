import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createRawClient, type RawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { registerTool } from '@/mcp/tool';
import {
  SEMANTIC_OBJECT_TYPE,
  type SemanticObjectRef,
  type SemanticObjectType,
  SemanticObjectTypeEnum,
  type SemanticObjectTypeSelection,
  SemanticObjectTypeSelectionSchema,
} from './semantic.model';

// Ported 1:1 from tools/semantic/{tools,service,model}.py.
//
// The semantic tools read the Metastore service (the semantic-layer repository).
// The Python `MetastoreClient` issues plain JSON:API requests; we build an
// equivalent raw client locally from the derived Metastore service URL so we keep
// exact response-shape parity (`data` envelope with per-item attributes + meta).

// --- Metastore raw client + object shape ---------------------------------------

type MetaObjectMeta = {
  name?: string | null;
  [key: string]: unknown;
};

/** Single object from the Metastore JSON:API response (port of `MetastoreObject`). */
type MetastoreObject = {
  type?: string | null;
  id?: string | null;
  attributes?: Record<string, unknown> | null;
  relationships?: Record<string, unknown> | null;
  meta?: MetaObjectMeta | null;
};

const parseList = (response: unknown): MetastoreObject[] => {
  if (!response || typeof response !== 'object' || Array.isArray(response)) {
    throw new Error('Unexpected metastore response format: expected JSON object with "data" key.');
  }
  const data = (response as { data?: unknown }).data;
  if (!Array.isArray(data)) {
    throw new Error('Unexpected metastore response format: "data" is not an array.');
  }
  return data as MetastoreObject[];
};

const parseObject = (response: unknown): MetastoreObject => {
  if (!response || typeof response !== 'object') {
    throw new Error('Unexpected metastore response format: expected JSON object.');
  }
  const data = (response as { data?: unknown }).data ?? response;
  if (!data || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error('Unexpected metastore response format: "data" is not an object.');
  }
  return data as MetastoreObject;
};

type MetastoreClient = {
  getSchema: (objectType: string) => Promise<Record<string, unknown>>;
  listObjects: (
    objectType: string,
    opts?: { limit?: number; offset?: number },
  ) => Promise<MetastoreObject[]>;
  getObject: (objectType: string, uuid: string) => Promise<MetastoreObject>;
};

const createMetastoreClient = (raw: RawClient): MetastoreClient => ({
  getSchema: async (objectType) => {
    const response = await raw.get(`api/v1/schema/${objectType}`);
    if (!response || typeof response !== 'object' || Array.isArray(response)) {
      throw new Error('Unexpected metastore schema response format.');
    }
    return response as Record<string, unknown>;
  },
  listObjects: async (objectType, opts = {}) => {
    const params: Record<string, number | undefined> = {};
    if (opts.limit !== undefined) params.limit = opts.limit;
    if (opts.offset !== undefined) params.offset = opts.offset;
    const response = await raw.get(`api/v1/repository/${objectType}`, { params });
    return parseList(response);
  },
  getObject: async (objectType, uuid) => {
    const response = await raw.get(`api/v1/repository/${objectType}/${uuid}`);
    return parseObject(response);
  },
});

// --- Service constants ---------------------------------------------------------

const SEMANTIC_OBJECT_TYPES: readonly SemanticObjectType[] = SEMANTIC_OBJECT_TYPE;

const VALIDATION_OBJECT_TYPES: readonly SemanticObjectType[] = [
  'semantic-model',
  'semantic-dataset',
  'semantic-metric',
  'semantic-relationship',
  'semantic-constraint',
];

// Some metastore endpoints return 500 for large responses unless paged aggressively.
const DEFAULT_PAGE_LIMIT = 20;
const DEFAULT_PAGE_LIMITS: Partial<Record<SemanticObjectType, number>> = {
  'semantic-dataset': 1,
  'semantic-metric': 5,
};

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

// --- Typed service objects (port of SemanticServiceData hierarchy) -------------

type SemanticServiceData = {
  semanticType: SemanticObjectType;
  id: string;
  data: MetastoreObject;
  attributes: Record<string, unknown>;
  /** display_name: own `name`, else meta.name (glossary overrides with term). */
  displayName: string | null;
  // Type-specific fields used by the service heuristics:
  name?: string | null;
  sqlDialect?: string | null;
  tableId?: string | null;
  fqn?: string | null;
  modelUuid?: string | null;
  sql?: string | null;
  dataset?: string | null;
  fromDataset?: string | null;
  toDataset?: string | null;
  on?: string | null;
  term?: string | null;
  description?: string | null;
  constraintType?: string | null;
  severity?: string | null;
  metrics?: string[];
  datasets?: string[];
  errorMessage?: string | null;
  remediation?: string | null;
  preQueryCheck?: boolean;
  validationQuery?: Record<string, unknown> | null;
};

const asString = (value: unknown): string | null => (typeof value === 'string' ? value : null);

const metaName = (obj: MetastoreObject): string | null => {
  const name = obj.meta?.name;
  return typeof name === 'string' && name ? name : null;
};

const toSemanticServiceData = (
  objectType: SemanticObjectType,
  obj: MetastoreObject,
): SemanticServiceData => {
  const attributes = obj.attributes ?? {};
  const id = obj.id ?? '';
  const ownName = asString(attributes.name) || metaName(obj);

  const base: SemanticServiceData = {
    semanticType: objectType,
    id,
    data: obj,
    attributes,
    displayName: ownName || null,
  };

  switch (objectType) {
    case 'semantic-model':
      return {
        ...base,
        name: ownName,
        description: asString(attributes.description),
        sqlDialect: asString(attributes.sql_dialect),
      };
    case 'semantic-dataset':
      return {
        ...base,
        name: ownName,
        tableId: asString(attributes.tableId),
        fqn: asString(attributes.fqn),
        description: asString(attributes.description),
        modelUuid: asString(attributes.modelUUID),
      };
    case 'semantic-metric':
      return {
        ...base,
        name: ownName,
        sql: asString(attributes.sql),
        dataset: asString(attributes.dataset),
        description: asString(attributes.description),
        modelUuid: asString(attributes.modelUUID),
      };
    case 'semantic-relationship':
      return {
        ...base,
        name: ownName,
        fromDataset: asString(attributes.from),
        toDataset: asString(attributes.to),
        on: asString(attributes.on),
        modelUuid: asString(attributes.modelUUID),
      };
    case 'semantic-glossary': {
      const term = asString(attributes.term);
      return {
        ...base,
        // Glossary display name overrides with term.
        displayName: term || base.displayName,
        term,
        modelUuid: asString(attributes.modelUUID),
      };
    }
    case 'semantic-constraint': {
      const ai = attributes.ai;
      const validationQuery = attributes.validationQuery;
      return {
        ...base,
        name: ownName,
        description: asString(attributes.description),
        constraintType: asString(attributes.constraintType),
        severity: asString(attributes.severity),
        modelUuid: asString(attributes.modelUUID),
        metrics: asStringArray(attributes.metrics),
        datasets: asStringArray(attributes.datasets),
        errorMessage: asString(attributes.errorMessage),
        remediation: asString(attributes.remediation),
        preQueryCheck:
          typeof ai === 'object' &&
          ai !== null &&
          (ai as { preQueryCheck?: unknown }).preQueryCheck === true,
        validationQuery:
          validationQuery && typeof validationQuery === 'object' && !Array.isArray(validationQuery)
            ? (validationQuery as Record<string, unknown>)
            : null,
      };
    }
    default:
      throw new Error(`Unsupported semantic object type "${objectType}".`);
  }
};

const asStringArray = (value: unknown): string[] =>
  Array.isArray(value)
    ? value.filter((v): v is string => typeof v === 'string' && v.length > 0)
    : [];

type SemanticServiceDataTypeGroup = {
  objectType: SemanticObjectType;
  objects: SemanticServiceData[];
};

// --- Helpers -------------------------------------------------------------------

const getSemanticModelId = (obj: SemanticServiceData): string => {
  if (obj.semanticType === 'semantic-model') return obj.id;
  return obj.modelUuid ?? '';
};

/** Model id from a raw metastore object (used during paged listing). */
const getModelIdFromMeta = (obj: MetastoreObject): string => {
  if (obj.type === 'semantic-model') return obj.id ?? '';
  const modelId = (obj.attributes ?? {}).modelUUID;
  return modelId ? String(modelId) : '';
};

const stringifyValue = (value: unknown): string => {
  if (typeof value === 'string') return value;
  try {
    return stableStringify(value);
  } catch {
    return String(value);
  }
};

/** JSON stringify with sorted keys (port of json.dumps(..., sort_keys=True)). */
const stableStringify = (value: unknown): string => {
  return JSON.stringify(value, (_key, val) => {
    if (val && typeof val === 'object' && !Array.isArray(val)) {
      return Object.keys(val as Record<string, unknown>)
        .sort()
        .reduce<Record<string, unknown>>((acc, k) => {
          acc[k] = (val as Record<string, unknown>)[k];
          return acc;
        }, {});
    }
    return val;
  });
};

/** Walk every scalar leaf of an object/array, yielding [dottedPath, value]. */
function* walkLeaves(node: unknown, path: string): Generator<[string, unknown]> {
  if (Array.isArray(node)) {
    for (let i = 0; i < node.length; i++) {
      yield* walkLeaves(node[i], `${path}[${i}]`);
    }
  } else if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      yield* walkLeaves(value, path ? `${path}.${key}` : key);
    }
  } else {
    yield [path, node];
  }
}

const findMatches = (
  semanticObject: SemanticServiceData,
  compiledPatterns: RegExp[],
): { matchedPaths: string[]; matchedPatterns: string[] } => {
  const matchedPaths = new Set<string>();
  const matchedPatterns = new Set<string>();

  if (semanticObject.displayName) {
    for (const compiled of compiledPatterns) {
      if (compiled.test(semanticObject.displayName)) {
        matchedPaths.add('meta.name');
        matchedPatterns.add(compiled.source);
      }
    }
  }

  const attrs = semanticObject.attributes ?? {};
  const attrsStringified = stringifyValue(attrs);
  if (compiledPatterns.some((compiled) => compiled.test(attrsStringified))) {
    for (const [path, value] of walkLeaves(attrs, '')) {
      if (value && typeof value === 'object') continue;
      const haystack = stringifyValue(value);
      if (!haystack) continue;
      for (const compiled of compiledPatterns) {
        if (compiled.test(haystack)) {
          matchedPaths.add(path);
          matchedPatterns.add(compiled.source);
        }
      }
    }
  }

  return {
    matchedPaths: [...matchedPaths].sort(),
    matchedPatterns: [...matchedPatterns].sort(),
  };
};

const listSemanticTypeObjects = async (
  client: MetastoreClient,
  objectType: SemanticObjectType,
  semanticModelIds?: readonly string[] | null,
): Promise<SemanticServiceData[]> => {
  const limit = DEFAULT_PAGE_LIMITS[objectType] ?? DEFAULT_PAGE_LIMIT;
  let offset = 0;
  const data: SemanticServiceData[] = [];
  const modelIdSet = semanticModelIds && semanticModelIds.length ? new Set(semanticModelIds) : null;

  for (;;) {
    const page = await client.listObjects(objectType, { limit, offset });
    for (const obj of page) {
      if (modelIdSet === null || modelIdSet.has(getModelIdFromMeta(obj))) {
        data.push(toSemanticServiceData(objectType, obj));
      }
    }
    if (page.length < limit) return data;
    offset += limit;
  }
};

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

// --- Validation findings -------------------------------------------------------

type ConstraintValidationFinding = {
  constraint_id: string;
  constraint_name: string;
  severity: string;
  status: string;
  message: string;
  validation_query: string | null;
};

type SemanticValidationServiceOutput = {
  valid: boolean;
  usedObjectGroups: SemanticServiceDataTypeGroup[];
  matchedRelationships: string[];
  violations: ConstraintValidationFinding[];
  postExecutionChecks: ConstraintValidationFinding[];
};

// --- Service: search -----------------------------------------------------------

type SemanticSearchHit = {
  objectType: SemanticObjectType;
  object: SemanticServiceData;
  semanticModelId: string;
  matchedPatterns: string[];
  matchedPaths: string[];
};

const searchSemanticContext = async (
  client: MetastoreClient,
  patterns: string[],
  opts: {
    semanticTypes?: readonly SemanticObjectType[];
    semanticModelIds?: readonly string[] | null;
    caseSensitive?: boolean;
    maxResults?: number;
  },
): Promise<SemanticSearchHit[]> => {
  const cleanedPatterns = patterns.map((p) => p.trim()).filter((p) => p.length > 0);
  if (cleanedPatterns.length === 0) {
    throw new Error('At least one regex pattern must be provided.');
  }
  const maxResults = opts.maxResults ?? 50;
  if (maxResults <= 0) {
    throw new Error('max_results must be a positive integer.');
  }

  const targetTypes =
    opts.semanticTypes && opts.semanticTypes.length ? opts.semanticTypes : SEMANTIC_OBJECT_TYPES;
  const flags = opts.caseSensitive ? '' : 'i';
  const compiledPatterns: RegExp[] = [];
  for (const pattern of cleanedPatterns) {
    try {
      compiledPatterns.push(new RegExp(pattern, flags));
    } catch (e) {
      throw new Error(
        `Invalid regex pattern "${pattern}": ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  const matches: SemanticSearchHit[] = [];
  for (const objectType of targetTypes) {
    if (matches.length >= maxResults) break;
    const objects = await listSemanticTypeObjects(client, objectType, opts.semanticModelIds);
    for (const semanticObject of objects) {
      if (matches.length >= maxResults) break;
      const { matchedPaths, matchedPatterns } = findMatches(semanticObject, compiledPatterns);
      if (matchedPatterns.length === 0) continue;
      matches.push({
        objectType,
        semanticModelId: getSemanticModelId(semanticObject),
        object: semanticObject,
        matchedPatterns: [...matchedPatterns].sort(),
        matchedPaths: [...matchedPaths].sort(),
      });
    }
  }
  return matches.slice(0, maxResults);
};

// --- Service: load context -----------------------------------------------------

const loadSemanticContextForType = async (
  client: MetastoreClient,
  objectType: SemanticObjectType,
  opts: { ids?: readonly string[]; semanticModelIds?: readonly string[] | null } = {},
): Promise<SemanticServiceDataTypeGroup> => {
  let objects: SemanticServiceData[];
  if (opts.ids && opts.ids.length) {
    const raw = await Promise.all(opts.ids.map((id) => client.getObject(objectType, id)));
    objects = raw.map((obj) => toSemanticServiceData(objectType, obj));
  } else {
    objects = await listSemanticTypeObjects(client, objectType, opts.semanticModelIds);
  }
  return { objectType, objects };
};

const loadSemanticContextForModel = async (
  client: MetastoreClient,
  semanticModelId: string,
): Promise<Map<SemanticObjectType, SemanticServiceDataTypeGroup>> => {
  const groups = await Promise.all(
    VALIDATION_OBJECT_TYPES.map((objectType) =>
      loadSemanticContextForType(client, objectType, { semanticModelIds: [semanticModelId] }),
    ),
  );
  return new Map(groups.map((g) => [g.objectType, g]));
};

// --- Service: detect + evaluate ------------------------------------------------

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

const loadValidationContexts = async (
  client: MetastoreClient,
  semanticModelIds: readonly string[],
): Promise<Map<SemanticObjectType, SemanticServiceDataTypeGroup>[]> => {
  if (!semanticModelIds.length) {
    throw new Error('At least one semantic_model_id must be provided.');
  }
  return Promise.all(
    semanticModelIds.map((modelId) => loadSemanticContextForModel(client, modelId)),
  );
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

const validateSemanticQueryWithUsedObjects = async (
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

const getObjectById = async (
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
