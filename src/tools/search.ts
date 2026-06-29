import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager, type KeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import {
  type ApiItemType,
  DATA_APP_COMPONENT_ID,
  DEFAULT_GLOBAL_SEARCH_LIMIT,
  GLOBAL_SEARCH_FEATURE,
  makeHit,
  MAX_GLOBAL_SEARCH_LIMIT,
  ORCHESTRATOR_IDS,
  type PatternMatch,
  SEARCH_ITEM_TYPE_TO_API_TYPES,
  SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES,
  SEARCH_ITEM_TYPES,
  SEARCH_PATTERN_MODES,
  SEARCH_TYPES,
  type SearchHit,
  type SearchItemType,
  type SearchOutput,
  type SearchPatternMode,
  type SearchType,
  WORKSPACE_COMPONENT_ID,
} from './search.model';

// Ported from tools/search.py (find_component_id; the global `search` tool follows later).

type SuggestedComponent = { componentId?: string; component_id?: string; score?: number };

// ---------------------------------------------------------------------------
// Pattern / configuration matching (port of search_models.SearchSpec)
// ---------------------------------------------------------------------------

type JsonValue = unknown;

/** Compiled search specification, mirroring the Python `SearchSpec`. */
class SearchSpec {
  readonly patterns: string[];
  readonly itemTypes: SearchItemType[];
  readonly patternMode: SearchPatternMode;
  readonly searchType: SearchType;
  readonly searchScopes: string[];
  readonly returnAllMatchedPatterns: boolean;
  readonly componentTypes: string[];

  private readonly compiled: RegExp[];

  constructor(opts: {
    patterns: string[];
    itemTypes: SearchItemType[];
    patternMode?: SearchPatternMode;
    searchType?: SearchType;
    searchScopes?: string[];
    returnAllMatchedPatterns?: boolean;
  }) {
    const cleaned = opts.patterns
      .filter((p) => p != null)
      .map((p) => String(p).trim())
      .filter((p) => p.length > 0);
    if (cleaned.length === 0) {
      throw new Error('At least one search pattern must be provided.');
    }
    this.patterns = cleaned;
    this.patternMode = opts.patternMode ?? 'regex';
    this.searchType = opts.searchType ?? 'textual';
    this.searchScopes = [...(opts.searchScopes ?? [])];
    this.returnAllMatchedPatterns = opts.returnAllMatchedPatterns ?? false;

    // _validate_item_types: 'component' expands to configuration + configuration-row.
    let itemTypes = [...opts.itemTypes];
    if (itemTypes.includes('component')) {
      itemTypes = [
        ...new Set<SearchItemType>([...itemTypes, 'configuration', 'configuration-row']),
      ];
    }
    this.itemTypes = itemTypes;

    // _validate_component_args: derive component types fetched during enumeration.
    this.componentTypes = [
      ...new Set(itemTypes.flatMap((item) => SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES[item] ?? [])),
    ];

    // Case-insensitive by default (Python `case_sensitive` defaults to False).
    const reFlags = 'i';
    this.compiled = cleaned.map((pattern) =>
      this.patternMode === 'literal'
        ? new RegExp(escapeRegExp(pattern), reFlags)
        : new RegExp(pattern, reFlags),
    );
  }

  /** Returns the patterns that match a string or stringified JSON value. */
  matchPatterns(value: string | JsonValue | null | undefined): string[] {
    if (value === null || value === undefined) return [];
    const haystack = typeof value === 'string' ? value : stringify(value);
    if (!haystack) return [];

    const matches: string[] = [];
    for (let i = 0; i < this.patterns.length; i++) {
      if (this.compiled[i]!.test(haystack)) {
        matches.push(this.patterns[i]!);
        if (!this.returnAllMatchedPatterns) break;
      }
    }
    return matches;
  }

  /** Matches a list of texts (e.g. id/name/description); scope is null. */
  matchTexts(texts: (string | null | undefined)[]): PatternMatch[] {
    const matches: PatternMatch[] = [];
    for (const text of texts) {
      const matched = this.matchPatterns(text);
      if (matched.length > 0) {
        matches.push({ scope: null, patterns: matched });
        if (!this.returnAllMatchedPatterns) break;
      }
    }
    return matches;
  }

  /** Matches configuration JSON within the configured scopes (or all nodes). */
  matchConfigurationScopes(configuration: JsonValue | null | undefined): PatternMatch[] {
    if (configuration === null || configuration === undefined) return [];

    if (this.searchScopes.length > 0) {
      const all: PatternMatch[] = [];
      const seen = new Set<string | null>();
      for (const scope of this.searchScopes) {
        const selfNodes = selectScope(configuration, scope);
        // Scalar matches in the scope node first.
        let scopeMatches = this.findMatches(selfNodes, true);
        if (scopeMatches.length === 0) {
          const descNodes = selfNodes.flatMap((n) => descendants(n.value, n.path));
          scopeMatches = this.findMatches(descNodes, false);
        }
        for (const match of scopeMatches) {
          if (seen.has(match.scope)) continue;
          seen.add(match.scope);
          all.push(match);
          if (!this.returnAllMatchedPatterns) return all;
        }
      }
      return all;
    }

    // No scope provided — search all descendants, return exact match paths.
    const nodes = descendants(configuration, '$');
    return this.findMatches(nodes, false);
  }

  private findMatches(nodes: PathNode[], scalarOnly: boolean): PatternMatch[] {
    const matches: PatternMatch[] = [];
    for (const node of nodes) {
      if (scalarOnly && node.value !== null && typeof node.value === 'object') continue;
      const matched = this.matchPatterns(node.value);
      if (matched.length > 0) {
        matches.push({ scope: cleanJsonPath(node.path), patterns: matched });
        if (!this.returnAllMatchedPatterns) return matches;
      }
    }
    return matches;
  }
}

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** Deterministic JSON stringification with sorted keys (port of SearchSpec._stringify). */
const stringify = (value: JsonValue): string => {
  try {
    return stableStringify(value);
  } catch {
    return String(value);
  }
};

const stableStringify = (value: JsonValue): string => {
  const seen = new WeakSet<object>();
  const sort = (val: JsonValue): JsonValue => {
    if (val === null || typeof val !== 'object') return val;
    if (seen.has(val as object)) return val;
    seen.add(val as object);
    if (Array.isArray(val)) return val.map(sort);
    const out: Record<string, JsonValue> = {};
    for (const key of Object.keys(val as Record<string, JsonValue>).sort()) {
      out[key] = sort((val as Record<string, JsonValue>)[key]);
    }
    return out;
  };
  return JSON.stringify(sort(value));
};

type PathNode = { path: string; value: JsonValue };

/** Resolves a dot/bracket scope to the matching nodes (supports a single `[*]`/`[N]` step). */
const selectScope = (root: JsonValue, scope: string): PathNode[] => {
  const normalized = scope.startsWith('$') ? scope.slice(1).replace(/^\./, '') : scope;
  if (!normalized) return [{ path: '$', value: root }];

  let nodes: PathNode[] = [{ path: '$', value: root }];
  for (const token of tokenizePath(normalized)) {
    const next: PathNode[] = [];
    for (const node of nodes) {
      if (token.type === 'wildcard') {
        next.push(...childEntries(node));
      } else if (token.type === 'index') {
        if (Array.isArray(node.value) && token.index < node.value.length) {
          next.push({ path: `${node.path}[${token.index}]`, value: node.value[token.index] });
        }
      } else {
        if (node.value && typeof node.value === 'object' && !Array.isArray(node.value)) {
          const obj = node.value as Record<string, JsonValue>;
          if (token.key in obj) {
            next.push({ path: `${node.path}.${token.key}`, value: obj[token.key] });
          }
        }
      }
    }
    nodes = next;
  }
  return nodes;
};

type PathToken =
  | { type: 'key'; key: string }
  | { type: 'index'; index: number }
  | { type: 'wildcard' };

const tokenizePath = (path: string): PathToken[] => {
  const tokens: PathToken[] = [];
  const regex = /\[([^\]]*)\]|([^.[\]]+)/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(path)) !== null) {
    if (match[1] !== undefined) {
      const inner = match[1].replace(/^['"]|['"]$/g, '');
      if (inner === '*') tokens.push({ type: 'wildcard' });
      else if (/^\d+$/.test(inner)) tokens.push({ type: 'index', index: Number(inner) });
      else tokens.push({ type: 'key', key: inner });
    } else if (match[2] !== undefined) {
      if (match[2] === '*') tokens.push({ type: 'wildcard' });
      else tokens.push({ type: 'key', key: match[2] });
    }
  }
  return tokens;
};

const childEntries = (node: PathNode): PathNode[] => {
  const { value, path } = node;
  if (Array.isArray(value)) {
    return value.map((item, i) => ({ path: `${path}[${i}]`, value: item }));
  }
  if (value && typeof value === 'object') {
    return Object.entries(value as Record<string, JsonValue>).map(([k, v]) => ({
      path: `${path}.${k}`,
      value: v,
    }));
  }
  return [];
};

/** Recursive descent (`$..*`): every descendant node with its full path. */
const descendants = (root: JsonValue, basePath: string): PathNode[] => {
  const out: PathNode[] = [];
  const walk = (node: PathNode): void => {
    for (const child of childEntries(node)) {
      out.push(child);
      walk(child);
    }
  };
  walk({ path: basePath, value: root });
  return out;
};

/** Normalizes a path string: strips the leading `$.`/`$` and `.[` artifacts (port of _clean_jsonpath_path_str). */
const cleanJsonPath = (path: string): string => {
  let result = path.replace(/^\$\.?/, '');
  result = result.replace(/\.\[/g, '[');
  return result;
};

/** Assigns matches to a hit, keeping only the most specific scopes (port of SearchHit.set_matches). */
const setMatches = (hit: SearchHit, matches: PatternMatch[]): SearchHit => {
  const patternsByScope = new Map<string, Set<string>>();
  for (const match of matches) {
    if (!match.scope) continue;
    if (!patternsByScope.has(match.scope)) patternsByScope.set(match.scope, new Set());
    for (const p of match.patterns) patternsByScope.get(match.scope)!.add(p);
  }
  const scopes = [...patternsByScope.keys()];
  const mostSpecific = scopes.filter(
    (scope) =>
      !scopes.some(
        (other) =>
          other.startsWith(scope) &&
          other.length > scope.length &&
          (other[scope.length] === '.' || other[scope.length] === '['),
      ),
  );
  hit.matches = mostSpecific.map((scope) => ({
    scope,
    patterns: [...patternsByScope.get(scope)!].sort(),
  }));
  return hit;
};

// ---------------------------------------------------------------------------
// Metadata helpers (port of clients.client.get_metadata_property + get_nested)
// ---------------------------------------------------------------------------

type RawDict = Record<string, unknown>;

const getMetadataProperty = (metadata: unknown, key: string): string | null => {
  if (!Array.isArray(metadata)) return null;
  const filtered = (metadata as RawDict[]).filter((m) => m && m.key === key);
  // Most recent by timestamp.
  let best: RawDict | undefined;
  let bestTs = '';
  for (const m of filtered) {
    const ts = (m.timestamp as string) ?? '';
    if (best === undefined || ts >= bestTs) {
      best = m;
      bestTs = ts;
    }
  }
  const value = best ? best.value : undefined;
  return value != null ? String(value) : null;
};

const getNested = (obj: RawDict | null | undefined, key: string): unknown => {
  let cur: unknown = obj;
  for (const part of key.split('.')) {
    if (cur && typeof cur === 'object' && !Array.isArray(cur)) {
      cur = (cur as RawDict)[part];
    } else {
      return null;
    }
    if (cur === null || cur === undefined) return null;
  }
  return cur;
};

const getFieldValue = (item: RawDict, fields: string[]): string | null => {
  for (const field of fields) {
    const value = getNested(item, field);
    if (value) return String(value);
  }
  return null;
};

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

const globalTextualSearch = async (
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
        // Built manually (matching the Python client) so array params serialize as repeated
        // `projectIds[]=...` keys — the typed client's serializer nests them as `projectIds[][0]`.
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
        const key = `${item.type} ${item.id}`;
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

const enumerationSearch = async (
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

const isGlobalSearchEnabled = async (clients: KeboolaClients): Promise<boolean> => {
  const verified = await clients.storage.tokens.verify();
  const owner = verified.owner as { features?: string[] } | undefined;
  return Boolean(owner?.features?.includes(GLOBAL_SEARCH_FEATURE));
};

const SEARCH_DESCRIPTION =
  'Searches for Keboola items (tables, buckets, components, configurations, transformations, flows, ' +
  'data-apps, etc.) in the current project and returns matching IDs and metadata. Supports textual ' +
  'search (matches item names, server-side) and config-based search (matches patterns against the ' +
  'configuration JSON content, optionally narrowed by JSONPath scopes). THIS IS THE PRIMARY DISCOVERY ' +
  'TOOL — use it before any get_* tool when you need to find items by name or configuration content. ' +
  'Multiple patterns work as an OR condition. Textual search prefers the current branch and, when ' +
  'nothing is found there, automatically widens to all branches of the project.';

export const registerSearchTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'find_component_id',
    title: 'Find component id',
    description: 'Returns a list of component IDs that match the given natural-language query.',
    annotations: { readOnlyHint: true },
    inputSchema: {
      query: z.string().describe('Natural language query to find the requested component.'),
    },
    handler: async ({ query }) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);

      const response = await clients.rawAi.post<{ components?: SuggestedComponent[] }>(
        'suggest/component',
        {
          body: { prompt: query },
          headers: { Accept: 'application/json' },
        },
      );

      return (response.components ?? []).map((component) => {
        const componentId = component.componentId ?? component.component_id ?? '';
        return {
          component_id: componentId,
          score: component.score ?? 0,
          links: [linksManager.getConfigDashboardLink(componentId, undefined)],
        };
      });
    },
  });

  registerTool(server, {
    name: 'search',
    title: 'Search',
    description: SEARCH_DESCRIPTION,
    annotations: { readOnlyHint: true },
    inputSchema: {
      patterns: z
        .array(z.string())
        .describe(
          'One or more search patterns. For textual search they match item names (server-side, ' +
            'tokenized full-text); for config-based search they match the configuration JSON content. ' +
            'Case-insensitive by default. Examples: ["customer"], ["sales", "revenue"], ["my_bucket"]. ' +
            'Do not use empty strings or empty lists.',
        ),
      item_types: z
        .array(z.enum(SEARCH_ITEM_TYPES))
        .default([])
        .describe(
          'Filter for specific Keboola item types. Common values: "table" (data tables), "bucket" ' +
            '(table containers), "transformation" (SQL/Python transformations), "component" ' +
            '(extractor/writer/application components), "data-app" (data apps), "flow" (orchestration ' +
            "flows). Use when you know what type of item you're looking for or leave empty to search " +
            'all types.',
        ),
      search_type: z
        .enum(SEARCH_TYPES)
        .default('textual')
        .describe(
          'Search mode: "textual" (name/id/description) or "config-based" (stringified configuration ' +
            'payloads). (default: "textual")',
        ),
      scopes: z
        .array(z.string())
        .default([])
        .describe(
          'JSONPath expressions to narrow config-based search to specific parts of the configuration. ' +
            'Simple dot-notation (e.g. "parameters", "storage.input") and full JSONPath (e.g. ' +
            '"$.tasks[*]") are both supported (e.g. "parameters.host", "storage.input[0].source"). ' +
            'Leave empty to search the whole configuration.',
        ),
      mode: z
        .enum(SEARCH_PATTERN_MODES)
        .default('literal')
        .describe(
          'How to interpret patterns. Applies to config-based search only: "regex" for regular ' +
            'expressions or "literal" for exact text (default: "literal"). Ignored by textual search, ' +
            'which is always a tokenized full-text name query (not typo-corrected) and rejects "regex".',
        ),
      limit: z
        .number()
        .default(DEFAULT_GLOBAL_SEARCH_LIMIT)
        .describe(
          `Maximum number of items to return (default: ${DEFAULT_GLOBAL_SEARCH_LIMIT}, max: ${MAX_GLOBAL_SEARCH_LIMIT}).`,
        ),
      offset: z
        .number()
        .default(0)
        .describe('Number of matching items to skip for pagination (default: 0).'),
    },
    handler: async (args) => {
      const spec = new SearchSpec({
        patterns: args.patterns,
        itemTypes: args.item_types,
        patternMode: args.mode,
        searchType: args.search_type,
        searchScopes: args.scopes,
        returnAllMatchedPatterns: args.search_type === 'config-based',
      });

      const offset = Math.max(0, args.offset);
      let limit = args.limit;
      if (!(limit > 0 && limit <= MAX_GLOBAL_SEARCH_LIMIT)) {
        logger.warn(
          `The "limit" parameter is out of range (0, ${MAX_GLOBAL_SEARCH_LIMIT}], setting to default value ${DEFAULT_GLOBAL_SEARCH_LIMIT}.`,
        );
        limit = DEFAULT_GLOBAL_SEARCH_LIMIT;
      }

      const clients = createKeboolaClients(config);

      let output: SearchOutput;
      if (args.search_type === 'textual' && (await isGlobalSearchEnabled(clients))) {
        if (args.mode === 'regex') {
          throw new Error(
            'Regex patterns are not supported for textual search — it is a tokenized full-text name search. ' +
              'Pass the plain name as the pattern, or use search_type="config-based" for regex matching inside ' +
              'configurations.',
          );
        }
        // Global search is a fast path with a safety net: fall back to client-side enumeration on any
        // error, or when it finds nothing.
        try {
          output = await globalTextualSearch(clients, spec, limit, offset);
          if (output.hits.length === 0 && offset === 0) {
            logger.info('Global search returned no hits; falling back to client-side enumeration.');
            output = await enumerationSearch(clients, spec, limit, offset);
          }
        } catch (error) {
          logger.warn(
            { err: error },
            'Global search failed; falling back to client-side enumeration.',
          );
          output = await enumerationSearch(clients, spec, limit, offset);
        }
      } else {
        output = await enumerationSearch(clients, spec, limit, offset);
      }

      const linksManager = await createLinksManager(config, clients);
      for (const hit of output.hits) {
        hit.links.push(
          ...linksManager.getLinks({
            bucketId: hit.bucket_id ?? undefined,
            tableId: hit.table_id ?? undefined,
            componentId: hit.component_id ?? undefined,
            configurationId: hit.configuration_id ?? undefined,
            name: hit.name ?? undefined,
          }),
        );
      }

      return output;
    },
  });
};
