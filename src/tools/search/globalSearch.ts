/**
 * Global (server-side) textual search and the client-side enumeration fallback.
 *
 * Port of the Python `tools/search_global.py` (`_global_textual_search`) and the
 * `tools/search.py` enumeration helpers (`_enumeration_search` and friends).
 */

import type { KeboolaClients } from '@/clients/keboola';
import { MetadataField } from '@/constants';
import { logger } from '@/logger';
import {
  type ApiItemType,
  DATA_APP_COMPONENT_ID,
  getFieldValue,
  getMetadataProperty,
  GLOBAL_SEARCH_FEATURE,
  makeHit,
  MAX_GLOBAL_SEARCH_LIMIT,
  ORCHESTRATOR_IDS,
  type PatternMatch,
  type RawDict,
  SEARCH_ITEM_TYPE_TO_API_TYPES,
  type SearchHit,
  type SearchItemType,
  type SearchOutput,
  type SearchSpec,
  setMatches,
  WORKSPACE_COMPONENT_ID,
} from './model';

// ---------------------------------------------------------------------------
// Global (server-side) textual search (port of search_global._global_textual_search)
// ---------------------------------------------------------------------------

type GlobalSearchItem = {
  id: string;
  name: string;
  type: string;
  fullPath?: RawDict;
  componentId?: string | null;
  created: string;
};

type GlobalSearchResponse = {
  all: number;
  items: GlobalSearchItem[];
  byType?: Record<string, number>;
};

const apiTypesFor = (itemTypes: SearchItemType[]): ApiItemType[] => {
  const apiTypes: ApiItemType[] = [];
  for (const itemType of itemTypes) {
    for (const apiType of SEARCH_ITEM_TYPE_TO_API_TYPES[itemType] ?? []) {
      if (!apiTypes.includes(apiType)) apiTypes.push(apiType);
    }
  }
  return apiTypes;
};

const retypeConfiguration = (componentId: string | null | undefined): SearchItemType => {
  if (componentId && ORCHESTRATOR_IDS.has(componentId)) return 'flow';
  if (componentId === DATA_APP_COMPONENT_ID) return 'data-app';
  if (componentId === WORKSPACE_COMPONENT_ID) return 'workspace';
  return 'configuration';
};

const branchInfo = (item: GlobalSearchItem): { id: string | null; name: string | null } => {
  const branch = item.fullPath?.branch;
  if (branch && typeof branch === 'object') {
    const b = branch as RawDict;
    return {
      id: b.id != null ? String(b.id) : null,
      name: b.name ? String(b.name) : null,
    };
  }
  return { id: null, name: null };
};

const globalSearchHit = (item: GlobalSearchItem): SearchHit | null => {
  const { id: branch_id, name: branch_name } = branchInfo(item);
  const common = { updated: item.created, name: item.name, branch_id, branch_name };

  if (item.type === 'bucket') {
    return makeHit({ bucket_id: item.id, item_type: 'bucket', ...common });
  }

  if (item.type === 'table') {
    const bucket = item.fullPath?.bucket;
    const bucketId =
      bucket && typeof bucket === 'object' && (bucket as RawDict).id
        ? String((bucket as RawDict).id)
        : null;
    return makeHit({ table_id: item.id, bucket_id: bucketId, item_type: 'table', ...common });
  }

  if (item.type === 'configuration-row' || item.type === 'rows') {
    const configuration = item.fullPath?.configuration;
    const configurationId =
      configuration && typeof configuration === 'object' && (configuration as RawDict).id
        ? String((configuration as RawDict).id)
        : null;
    if (!(item.componentId && configurationId)) {
      logger.warn(
        `Skipping global-search row hit with no parent configuration in fullPath: ${item.id}`,
      );
      return null;
    }
    return makeHit({
      component_id: item.componentId,
      configuration_id: configurationId,
      configuration_row_id: item.id,
      item_type: 'configuration-row',
      ...common,
    });
  }

  const componentId =
    item.componentId ?? (item.type === 'workspace' ? WORKSPACE_COMPONENT_ID : null);
  if (!componentId) {
    logger.warn(`Skipping global-search hit with no component id: ${item.type} ${item.id}`);
    return null;
  }
  const itemType =
    item.type === 'configuration'
      ? retypeConfiguration(componentId)
      : (item.type as SearchItemType);
  return makeHit({
    component_id: componentId,
    configuration_id: item.id,
    item_type: itemType,
    ...common,
  });
};

export const globalTextualSearch = async (
  clients: KeboolaClients,
  spec: SearchSpec,
  limit: number,
  offset: number,
): Promise<SearchOutput> => {
  const apiTypes = apiTypesFor(spec.itemTypes);
  const requestedTypes = new Set<SearchItemType>(
    spec.itemTypes
      .filter((t) => t !== 'component')
      .map((t) => (t === 'rows' ? 'configuration-row' : t)),
  );

  const needsOverfetch = requestedTypes.size > 0 && apiTypes.includes('configuration');
  const fetchLimit = needsOverfetch ? MAX_GLOBAL_SEARCH_LIMIT : limit;

  const projectId = await clients.storage.tokens
    .verify()
    .then((t) => String((t.owner as { id: string | number }).id));

  const query = async (branchScope: 'current' | 'all'): Promise<GlobalSearchResponse[]> => {
    return Promise.all(
      spec.patterns.map((pattern) => {
        // Issued via the raw Storage client (not the typed `storage.search.globalSearch`) so array
        // params serialize as repeated `projectIds[]=...` keys — the typed client's serializer
        // nests them as `projectIds[][0]`, which SAPI rejects.
        const params: Record<string, string | number | string[] | undefined> = {
          query: pattern,
          'projectIds[]': [projectId],
          'types[]': apiTypes.length > 0 ? apiTypes : undefined,
          limit: fetchLimit,
          offset: offset || undefined,
        };
        if (branchScope === 'current') {
          if (clients.branchId === 'default') {
            params['branchTypes[]'] = 'production';
          } else {
            params['branchTypes[]'] = 'development';
            params['branchIds[]'] = clients.branchId;
          }
        }
        return clients.rawStorage.get<GlobalSearchResponse>('global-search', { params });
      }),
    );
  };

  const collect = (responses: GlobalSearchResponse[]): SearchHit[] => {
    const hitsByKey = new Map<string, SearchHit>();
    for (const response of responses) {
      for (const item of response.items ?? []) {
        const hit = globalSearchHit(item);
        if (hit === null) continue;
        if (requestedTypes.size > 0 && !requestedTypes.has(hit.item_type)) continue;
        const key = `${item.type} ${item.id}`;
        if (!hitsByKey.has(key)) hitsByKey.set(key, hit);
      }
    }
    return [...hitsByKey.values()];
  };

  let branchScope: 'current' | 'all' = 'current';
  let responses = await query(branchScope);
  let hits = collect(responses);
  if (hits.length === 0 && offset === 0) {
    branchScope = 'all';
    responses = await query(branchScope);
    hits = collect(responses);
  }

  sortHits(hits);

  const byType: Record<string, number> = {};
  for (const response of responses) {
    for (const [type, count] of Object.entries(response.byType ?? {})) {
      byType[type] = (byType[type] ?? 0) + count;
    }
  }

  return {
    hits: hits.slice(0, limit),
    total: responses.reduce((sum, r) => sum + (r.all ?? 0), 0),
    by_type: byType,
    branch_scope: branchScope === 'current' ? 'current-branch' : 'all-branches',
  };
};

const sortKey = (hit: SearchHit): string =>
  hit.bucket_id ??
  hit.table_id ??
  hit.component_id ??
  hit.configuration_id ??
  hit.configuration_row_id ??
  '';

/** Sorts by (updated, id) descending — same tuple comparison the Python code uses. */
const sortHits = (hits: SearchHit[]): void => {
  hits.sort((a, b) => {
    if (a.updated !== b.updated) return a.updated < b.updated ? 1 : -1;
    const ka = sortKey(a);
    const kb = sortKey(b);
    if (ka === kb) return 0;
    return ka < kb ? 1 : -1;
  });
};

// ---------------------------------------------------------------------------
// Client-side enumeration (port of search._enumeration_search and helpers)
// ---------------------------------------------------------------------------

const fetchBuckets = async (clients: KeboolaClients, spec: SearchSpec): Promise<SearchHit[]> => {
  const buckets = await clients.rawStorage.get<RawDict[]>(`branch/${clients.branchId}/buckets`, {
    params: { include: 'metadata' },
  });
  const hits: SearchHit[] = [];
  for (const bucket of buckets ?? []) {
    const bucketId = bucket.id ? String(bucket.id) : null;
    if (!bucketId) continue;
    const name = (bucket.name as string) ?? null;
    const displayName = (bucket.displayName as string) ?? null;
    const description = getMetadataProperty(bucket.metadata, MetadataField.DESCRIPTION);

    const matches = spec.matchTexts([bucketId, name, displayName, description]);
    if (matches.length > 0) {
      hits.push(
        setMatches(
          makeHit({
            bucket_id: bucketId,
            item_type: 'bucket',
            updated: getFieldValue(bucket, ['lastChangeDate', 'updated', 'created']) ?? '',
            name,
            display_name: displayName,
            description,
          }),
          matches,
        ),
      );
    }
  }
  return hits;
};

const checkColumnMatch = (table: RawDict, spec: SearchSpec): PatternMatch[] => {
  const colNames = table.columns;
  if (Array.isArray(colNames) && colNames.length > 0) {
    const matched = spec.matchTexts(colNames as string[]);
    if (matched.length > 0) return matched;
  }
  const colMetadata = table.columnMetadata;
  if (colMetadata && typeof colMetadata === 'object') {
    const descs = Object.values(colMetadata as RawDict)
      .map((meta) => getMetadataProperty(meta, MetadataField.DESCRIPTION))
      .filter((d): d is string => Boolean(d));
    const matched = spec.matchTexts(descs);
    if (matched.length > 0) return matched;
  }
  return [];
};

const fetchTables = async (clients: KeboolaClients, spec: SearchSpec): Promise<SearchHit[]> => {
  const buckets = await clients.rawStorage.get<RawDict[]>(`branch/${clients.branchId}/buckets`);
  const hits: SearchHit[] = [];
  for (const bucket of buckets ?? []) {
    const bucketId = bucket.id ? String(bucket.id) : null;
    if (!bucketId) continue;
    const tables = await clients.rawStorage.get<RawDict[]>(
      `branch/${clients.branchId}/buckets/${bucketId}/tables`,
      { params: { include: 'columns,columnMetadata' } },
    );
    for (const table of tables ?? []) {
      const tableId = table.id ? String(table.id) : null;
      if (!tableId) continue;
      const name = (table.name as string) ?? null;
      const displayName = (table.displayName as string) ?? null;
      const description = getMetadataProperty(table.metadata, MetadataField.DESCRIPTION);

      const matches = spec.matchTexts([tableId, name, displayName, description]);
      matches.push(...checkColumnMatch(table, spec));
      if (matches.length > 0) {
        hits.push(
          setMatches(
            makeHit({
              table_id: tableId,
              item_type: 'table',
              updated: getFieldValue(table, ['lastChangeDate', 'created']) ?? '',
              name,
              display_name: displayName,
              description,
            }),
            matches,
          ),
        );
      }
    }
  }
  return hits;
};

const fetchConfigsForType = async (
  clients: KeboolaClients,
  spec: SearchSpec,
  componentType: string | null,
): Promise<SearchHit[]> => {
  const params: Record<string, string> = { include: 'configuration,rows' };
  if (componentType) params.componentType = componentType;
  const components = await clients.rawStorage.get<RawDict[]>(
    `branch/${clients.branchId}/components`,
    { params },
  );

  const allowedTransformations =
    spec.itemTypes.includes('transformation') || componentType === null;
  const allowedComponents =
    spec.itemTypes.includes('configuration') ||
    spec.itemTypes.includes('configuration-row') ||
    componentType === null;
  const allowedFlows = spec.itemTypes.includes('flow') || componentType === null;
  const allowedWorkspaces = spec.itemTypes.includes('workspace') || componentType === null;
  const allowedDataApps = spec.itemTypes.includes('data-app') || componentType === null;

  const hits: SearchHit[] = [];
  for (const component of components ?? []) {
    const componentId = component.id ? String(component.id) : null;
    if (!componentId) continue;
    const currentComponentType = component.type as string | undefined;

    let itemType: SearchItemType;
    if (ORCHESTRATOR_IDS.has(componentId)) {
      itemType = 'flow';
      if (!allowedFlows) continue;
    } else if (currentComponentType === 'transformation') {
      itemType = 'transformation';
      if (!allowedTransformations) continue;
    } else if (componentId === WORKSPACE_COMPONENT_ID) {
      itemType = 'workspace';
      if (!allowedWorkspaces) continue;
    } else if (componentId === DATA_APP_COMPONENT_ID) {
      itemType = 'data-app';
      if (!allowedDataApps) continue;
    } else if (
      currentComponentType === 'extractor' ||
      currentComponentType === 'writer' ||
      currentComponentType === 'application'
    ) {
      itemType = 'configuration';
      if (!allowedComponents) continue;
    } else {
      itemType = 'configuration';
    }

    for (const config of (component.configurations as RawDict[]) ?? []) {
      const configId = config.id ? String(config.id) : null;
      if (!configId) continue;
      const configName = (config.name as string) ?? null;
      const configDescription = (config.description as string) ?? null;
      const configUpdated = getFieldValue(config, ['currentVersion.created', 'created']) ?? '';

      if (spec.searchType === 'textual') {
        const matches = spec.matchTexts([configId, configName, configDescription]);
        if (matches.length > 0) {
          hits.push(
            setMatches(
              makeHit({
                component_id: componentId,
                configuration_id: configId,
                item_type: itemType,
                updated: configUpdated,
                name: configName,
                description: configDescription,
              }),
              matches,
            ),
          );
        }
      } else {
        const matches = spec.matchConfigurationScopes(config.configuration);
        if (matches.length > 0) {
          hits.push(
            setMatches(
              makeHit({
                component_id: componentId,
                configuration_id: configId,
                item_type: itemType,
                updated: configUpdated,
                name: configName,
                description: configDescription,
              }),
              matches,
            ),
          );
        }
      }

      for (const row of (config.rows as RawDict[]) ?? []) {
        const rowId = row.id ? String(row.id) : null;
        if (!rowId) continue;
        const rowName = (row.name as string) ?? null;
        const rowDescription = (row.description as string) ?? null;
        const rowUpdated = configUpdated || (getFieldValue(row, ['created']) ?? '');

        if (spec.searchType === 'textual') {
          const matches = spec.matchTexts([rowId, rowName, rowDescription]);
          if (matches.length > 0) {
            hits.push(
              setMatches(
                makeHit({
                  component_id: componentId,
                  configuration_id: configId,
                  configuration_row_id: rowId,
                  item_type: 'configuration-row',
                  updated: rowUpdated,
                  name: rowName,
                  description: rowDescription,
                }),
                matches,
              ),
            );
          }
        } else {
          const matches = spec.matchConfigurationScopes(row.configuration);
          if (matches.length > 0) {
            hits.push(
              setMatches(
                makeHit({
                  component_id: componentId,
                  configuration_id: configId,
                  configuration_row_id: rowId,
                  item_type: 'configuration-row',
                  updated: rowUpdated,
                  name: rowName,
                  description: rowDescription,
                }),
                matches,
              ),
            );
          }
        }
      }
    }
  }
  return hits;
};

const fetchConfigurations = async (
  clients: KeboolaClients,
  spec: SearchSpec,
): Promise<SearchHit[]> => {
  if (spec.componentTypes.length > 0) {
    const all = await Promise.all(
      spec.componentTypes.map((componentType) => fetchConfigsForType(clients, spec, componentType)),
    );
    return all.flat();
  }
  return fetchConfigsForType(clients, spec, null);
};

const CONFIG_TYPES = new Set<SearchItemType>([
  'configuration',
  'transformation',
  'flow',
  'configuration-row',
  'workspace',
  'data-app',
]);

export const enumerationSearch = async (
  clients: KeboolaClients,
  spec: SearchSpec,
  limit: number,
  offset: number,
): Promise<SearchOutput> => {
  const typesToFetch = new Set(spec.itemTypes);
  const tasks: Promise<SearchHit[]>[] = [];

  if (typesToFetch.size === 0 || typesToFetch.has('bucket')) {
    tasks.push(fetchBuckets(clients, spec));
  }
  if (typesToFetch.size === 0 || typesToFetch.has('table')) {
    tasks.push(fetchTables(clients, spec));
  }
  if (typesToFetch.size === 0) {
    tasks.push(fetchConfigurations(clients, spec));
  } else if ([...typesToFetch].some((t) => CONFIG_TYPES.has(t))) {
    tasks.push(fetchConfigurations(clients, spec));
  }

  const results = await Promise.allSettled(tasks);
  let allHits: SearchHit[] = [];
  for (const result of results) {
    if (result.status === 'rejected') {
      logger.warn(`Error fetching items: ${String(result.reason)}`);
      continue;
    }
    allHits.push(...result.value);
  }

  if (typesToFetch.size > 0) {
    allHits = allHits.filter((hit) => typesToFetch.has(hit.item_type));
  }

  sortHits(allHits);

  const byType: Record<string, number> = {};
  for (const hit of allHits) {
    byType[hit.item_type] = (byType[hit.item_type] ?? 0) + 1;
  }

  return {
    hits: allHits.slice(offset, offset + limit),
    total: allHits.length,
    by_type: byType,
    branch_scope: 'current-branch',
  };
};

export const isGlobalSearchEnabled = async (clients: KeboolaClients): Promise<boolean> => {
  const verified = await clients.storage.tokens.verify();
  const owner = verified.owner as { features?: string[] } | undefined;
  return Boolean(owner?.features?.includes(GLOBAL_SEARCH_FEATURE));
};
