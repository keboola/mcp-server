/**
 * Shared models, type aliases, constants and the matching model (`SearchSpec`) for the
 * `search` tool.
 *
 * Port of the Python `tools/search_models.py` (and the parts of `search_global.py` /
 * `search.py` that the global `search` tool depends on). Kept in its own module so the
 * tool file stays focused on registration + handler wiring.
 */

import {
  CONDITIONAL_FLOW_COMPONENT_ID,
  DATA_APP_COMPONENT_ID,
  ORCHESTRATOR_COMPONENT_ID,
} from '@/constants';
import type { Link } from '@/links';
import {
  cleanJsonPath,
  descendants,
  escapeRegExp,
  type JsonValue,
  type PathNode,
  selectScope,
  stringify,
} from './jsonpath';

export const MAX_GLOBAL_SEARCH_LIMIT = 100;
export const DEFAULT_GLOBAL_SEARCH_LIMIT = 50;

export const GLOBAL_SEARCH_FEATURE = 'global-search';
export const WORKSPACE_COMPONENT_ID = 'keboola.sandboxes';

/** Item types the `search` tool accepts (and reports). */
export const SEARCH_ITEM_TYPES = [
  'bucket',
  'table',
  'data-app',
  'flow',
  'transformation',
  'component',
  'configuration',
  'configuration-row',
  'workspace',
  'shared-code',
  'rows',
  'state',
] as const;
export type SearchItemType = (typeof SEARCH_ITEM_TYPES)[number];

/** Item types reported by the SAPI global-search endpoint. */
export type ApiItemType =
  | 'flow'
  | 'bucket'
  | 'table'
  | 'transformation'
  | 'configuration'
  | 'configuration-row'
  | 'workspace'
  | 'shared-code'
  | 'rows'
  | 'state';

export const SEARCH_TYPES = ['textual', 'config-based'] as const;
export type SearchType = (typeof SEARCH_TYPES)[number];

export const SEARCH_PATTERN_MODES = ['regex', 'literal'] as const;
export type SearchPatternMode = (typeof SEARCH_PATTERN_MODES)[number];

export type SearchBranchScope = 'current-branch' | 'all-branches';

/** Maps a tool item type to the component types fetched during client-side enumeration. */
export const SEARCH_ITEM_TYPE_TO_COMPONENT_TYPES: Partial<Record<SearchItemType, string[]>> = {
  'data-app': ['other'],
  flow: ['other'],
  transformation: ['transformation'],
  configuration: ['extractor', 'writer', 'application'],
  'configuration-row': ['extractor', 'writer', 'application'],
  component: ['extractor', 'writer', 'application'],
  workspace: ['other'],
};

/**
 * Maps the tool's item types to the API types requested from the global-search endpoint. Some tool
 * types (data-app, flow, workspace) exist server-side as 'configuration' items distinguished only
 * by their component ID, so 'configuration' is over-fetched and narrowed client-side after re-typing.
 */
export const SEARCH_ITEM_TYPE_TO_API_TYPES: Record<SearchItemType, ApiItemType[]> = {
  bucket: ['bucket'],
  table: ['table'],
  transformation: ['transformation'],
  configuration: ['configuration'],
  'configuration-row': ['configuration-row'],
  component: ['configuration', 'configuration-row'],
  flow: ['flow', 'configuration'],
  'data-app': ['configuration'],
  workspace: ['workspace', 'configuration'],
  'shared-code': ['shared-code'],
  rows: ['rows'],
  state: ['state'],
};

export const ORCHESTRATOR_IDS = new Set<string>([
  ORCHESTRATOR_COMPONENT_ID,
  CONDITIONAL_FLOW_COMPONENT_ID,
]);
export { DATA_APP_COMPONENT_ID };

export type PatternMatch = {
  scope: string | null;
  patterns: string[];
};

/** A single search result. Shape mirrors the Python `SearchHit`. */
export type SearchHit = {
  bucket_id: string | null;
  table_id: string | null;
  component_id: string | null;
  configuration_id: string | null;
  configuration_row_id: string | null;
  item_type: SearchItemType;
  updated: string;
  name: string | null;
  display_name: string | null;
  description: string | null;
  branch_id: string | null;
  branch_name: string | null;
  matches: PatternMatch[];
  links: Link[];
};

export type SearchOutput = {
  hits: SearchHit[];
  total: number;
  by_type: Record<string, number>;
  branch_scope: SearchBranchScope;
};

/** Builds a SearchHit with the same field defaults the Python model declares. */
export const makeHit = (init: Partial<SearchHit> & { item_type: SearchItemType }): SearchHit => ({
  bucket_id: null,
  table_id: null,
  component_id: null,
  configuration_id: null,
  configuration_row_id: null,
  updated: '',
  name: null,
  display_name: null,
  description: null,
  branch_id: null,
  branch_name: null,
  matches: [],
  links: [],
  ...init,
});

// ---------------------------------------------------------------------------
// Pattern / configuration matching (port of search_models.SearchSpec)
// ---------------------------------------------------------------------------

/** Compiled search specification, mirroring the Python `SearchSpec`. */
export class SearchSpec {
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

/** Assigns matches to a hit, keeping only the most specific scopes (port of SearchHit.set_matches). */
export const setMatches = (hit: SearchHit, matches: PatternMatch[]): SearchHit => {
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

export type RawDict = Record<string, unknown>;

export const getMetadataProperty = (metadata: unknown, key: string): string | null => {
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

export const getFieldValue = (item: RawDict, fields: string[]): string | null => {
  for (const field of fields) {
    const value = getNested(item, field);
    if (value) return String(value);
  }
  return null;
};
