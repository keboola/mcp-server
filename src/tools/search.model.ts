/**
 * Shared models, type aliases and constants for the `search` tool.
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
