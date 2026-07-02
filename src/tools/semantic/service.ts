import type { RawClient } from '@/clients/raw';
import {
  type MetastoreObject,
  SEMANTIC_OBJECT_TYPE,
  type SemanticObjectType,
  type SemanticSearchHit,
  type SemanticServiceData,
  type SemanticServiceDataTypeGroup,
} from './model';

// Ported 1:1 from tools/semantic/service.py.
//
// The semantic tools read the Metastore service (the semantic-layer repository).
// The Python `MetastoreClient` issues plain JSON:API requests; we build an
// equivalent raw client locally from the derived Metastore service URL so we keep
// exact response-shape parity (`data` envelope with per-item `attributes` + `meta`).
//
// NOTE: we deliberately KEEP this local raw client instead of the typed
// `@keboola/api-client` metastore client. The typed `getMetaObjects` exposes only a
// `filter` query parameter (no `limit`/`offset`), but these heuristics rely on
// aggressive offset paging with small per-type limits to avoid 500s on large
// responses; and the typed response nests user attributes under `attributes.data`
// and drops the `meta.name` field, which would break the shapes these heuristics
// (and the JSON:API `data` envelope parsing) depend on.

// --- Metastore raw client ------------------------------------------------------

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

export type MetastoreClient = {
  getSchema: (objectType: string) => Promise<Record<string, unknown>>;
  listObjects: (
    objectType: string,
    opts?: { limit?: number; offset?: number },
  ) => Promise<MetastoreObject[]>;
  getObject: (objectType: string, uuid: string) => Promise<MetastoreObject>;
};

export const createMetastoreClient = (raw: RawClient): MetastoreClient => ({
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

export const VALIDATION_OBJECT_TYPES: readonly SemanticObjectType[] = [
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

// --- Object mapping ------------------------------------------------------------

const asString = (value: unknown): string | null => (typeof value === 'string' ? value : null);

const asStringArray = (value: unknown): string[] =>
  Array.isArray(value)
    ? value.filter((v): v is string => typeof v === 'string' && v.length > 0)
    : [];

export const metaName = (obj: MetastoreObject): string | null => {
  const name = obj.meta?.name;
  return typeof name === 'string' && name ? name : null;
};

export const toSemanticServiceData = (
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

// --- Helpers -------------------------------------------------------------------

export const getSemanticModelId = (obj: SemanticServiceData): string => {
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

// --- Service: search -----------------------------------------------------------

export const searchSemanticContext = async (
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

export const loadSemanticContextForType = async (
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

// --- Service: validation contexts ----------------------------------------------

export const loadValidationContexts = async (
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
