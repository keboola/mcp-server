import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import { type RawClient, RawHttpError } from '@/clients/raw';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import type { Link, ProjectLinksManager } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';

// Ported from tools/storage/tools.py (get_buckets, get_tables, update_descriptions),
// tools/storage_helpers.py and tools/storage/usage.py.

// ---------------------------------------------------------------------------
// Metadata helpers (ports of clients/client.py get_metadata_property and
// tools/components/utils.py get_nested, utils.py parse_iso_timestamp).
// ---------------------------------------------------------------------------

type RawObj = Record<string, unknown>;

const FAKE_DEVELOPMENT_BRANCH = 'KBC.fakeDevelopmentBranch';
const SHARED_DESCRIPTION = 'KBC.sharedDescription';
const DATATYPE_BASETYPE = 'KBC.datatype.basetype';
const DATATYPE_TYPE = 'KBC.datatype.type';
const DATATYPE_NULLABLE = 'KBC.datatype.nullable';
const CREATED_BY_COMPONENT_ID = 'KBC.createdBy.component.id';
const CREATED_BY_CONFIGURATION_ID = 'KBC.createdBy.configuration.id';
const CREATED_BY_CONFIGURATION_ROW_ID = 'KBC.createdBy.configurationRow.id';
const UPDATED_BY_COMPONENT_ID = 'KBC.lastUpdatedBy.component.id';
const UPDATED_BY_CONFIGURATION_ID = 'KBC.lastUpdatedBy.configuration.id';
const UPDATED_BY_CONFIGURATION_ROW_ID = 'KBC.lastUpdatedBy.configurationRow.id';

/** Parse an ISO 8601 timestamp into epoch millis, accepting `Z` and `+HHMM` offsets. */
const parseIsoTimestamp = (ts: string): number => {
  const normalized = ts.replace('Z', '+00:00').replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
  const millis = Date.parse(normalized);
  if (Number.isNaN(millis)) throw new Error(`Invalid ISO timestamp: ${ts}`);
  return millis;
};

/** Port of get_metadata_property: most-recent value for `key`, optionally provider-ranked. */
const getMetadataProperty = (
  metadata: unknown,
  key: string,
  preferredProviders?: string[],
): string | null => {
  if (!Array.isArray(metadata)) return null;
  const filtered = (metadata as RawObj[]).filter((m) => m && m.key === key);
  if (filtered.length === 0) return null;
  const sortKey = (m: RawObj): [number, string] => {
    const ts = (m.timestamp as string) ?? '';
    if (preferredProviders) {
      const p = m.provider as string | undefined;
      const idx =
        p && preferredProviders.includes(p)
          ? preferredProviders.indexOf(p)
          : preferredProviders.length;
      return [-1 * idx, ts];
    }
    return [0, ts];
  };
  let best: RawObj | undefined;
  let bestKey: [number, string] | undefined;
  for (const m of filtered) {
    const k = sortKey(m);
    if (!bestKey || k[0] > bestKey[0] || (k[0] === bestKey[0] && k[1] > bestKey[1])) {
      best = m;
      bestKey = k;
    }
  }
  const value = best?.value;
  return value != null ? String(value) : null;
};

/** Port of get_nested: dot-path lookup through nested objects. */
const getNested = (obj: unknown, path: string): unknown => {
  let cur: unknown = obj;
  for (const part of path.split('.')) {
    if (cur && typeof cur === 'object' && !Array.isArray(cur)) {
      cur = (cur as RawObj)[part];
    } else {
      return undefined;
    }
    if (cur == null) return undefined;
  }
  return cur;
};

/** Most recent of the given ISO timestamps (string-preserving), or null. */
const maxTimestamp = (...timestamps: (string | null | undefined)[]): string | null => {
  const valid = timestamps.filter((ts): ts is string => Boolean(ts));
  if (valid.length === 0) return null;
  const score = (ts: string): [number, number | string] => {
    try {
      return [1, parseIsoTimestamp(ts)];
    } catch {
      return [0, ts];
    }
  };
  let best = valid[0]!;
  let bestScore = score(best);
  for (const ts of valid.slice(1)) {
    const s = score(ts);
    if (s[0] > bestScore[0] || (s[0] === bestScore[0] && s[1] > bestScore[1])) {
      best = ts;
      bestScore = s;
    }
  }
  return best;
};

const asNumberOrNull = (value: unknown): number | null => {
  if (value == null) return null;
  const n = Number(value);
  return Number.isNaN(n) ? null : n;
};

// ---------------------------------------------------------------------------
// Lineage usage references (port of tools/storage/usage.py get_created_by /
// get_last_updated_by). find_id_usage depends on the search subsystem which is
// not yet ported, so include_usage is unavailable (see _find_id_usage_unavailable).
// ---------------------------------------------------------------------------

type ComponentUsageReference = {
  component_id: string;
  configuration_id: string;
  configuration_row_id: string | null;
  configuration_name: string | null;
  used_in: string | null;
  timestamp: string | null;
};

const latestMetadataTimestamp = (metadata: RawObj[], keys: string[]): string | null => {
  let latest: number | null = null;
  let latestRaw: string | null = null;
  for (const item of metadata) {
    if (!keys.includes(item.key as string)) continue;
    const rawTs = item.timestamp;
    if (typeof rawTs !== 'string') continue;
    let parsed: number;
    try {
      parsed = parseIsoTimestamp(rawTs);
    } catch {
      continue;
    }
    if (latest === null || parsed > latest) {
      latest = parsed;
      latestRaw = rawTs;
    }
  }
  return latestRaw;
};

const lineageReference = (
  metadata: unknown,
  componentKey: string,
  configKey: string,
  rowKey: string,
): ComponentUsageReference | null => {
  if (!Array.isArray(metadata)) return null;
  const items = metadata as RawObj[];
  const componentId = getMetadataProperty(items, componentKey);
  const configurationId = getMetadataProperty(items, configKey);
  const rowId = getMetadataProperty(items, rowKey);
  if (componentId === null || configurationId === null) return null;
  return {
    component_id: String(componentId),
    configuration_id: String(configurationId),
    configuration_row_id: rowId ? String(rowId) : null,
    configuration_name: null,
    used_in: null,
    timestamp: latestMetadataTimestamp(items, [componentKey, configKey, rowKey]),
  };
};

const getCreatedBy = (metadata: unknown): ComponentUsageReference | null =>
  lineageReference(
    metadata,
    CREATED_BY_COMPONENT_ID,
    CREATED_BY_CONFIGURATION_ID,
    CREATED_BY_CONFIGURATION_ROW_ID,
  );

const getLastUpdatedBy = (metadata: unknown): ComponentUsageReference | null =>
  lineageReference(
    metadata,
    UPDATED_BY_COMPONENT_ID,
    UPDATED_BY_CONFIGURATION_ID,
    UPDATED_BY_CONFIGURATION_ROW_ID,
  );

// ---------------------------------------------------------------------------
// Bucket / table models (ports of BucketDetail, TableSummary, TableDetail).
// ---------------------------------------------------------------------------

type Bucket = {
  id: string;
  name: string;
  displayName: string;
  description: string | null;
  stage: string;
  created: string;
  updated: string | null;
  dataSizeBytes: number | null;
  tablesCount: number | null;
  links: Link[] | null;
  source_project: string | null;
  created_by: ComponentUsageReference | null;
  last_updated_by: ComponentUsageReference | null;
  // internal, excluded from output
  branch_id: string | null;
  prod_id: string;
};

const validateBucket = (raw: RawObj): Bucket => {
  const id = String(raw.id ?? '');
  const metadata = raw.metadata;

  const branchId = getMetadataProperty(metadata, FAKE_DEVELOPMENT_BRANCH);
  const prodId = branchId ? id.replace(`c-${branchId}-`, 'c-') : id;

  const description =
    getMetadataProperty(metadata, SHARED_DESCRIPTION) ||
    getMetadataProperty(metadata, MetadataField.DESCRIPTION) ||
    (raw.description as string | undefined) ||
    null;

  const tables = raw.tables;
  const tablesCount = Array.isArray(tables) ? tables.length : null;

  let sourceProject: string | null = null;
  const sp = getNested(raw, 'sourceBucket.project') as RawObj | undefined;
  if (sp) sourceProject = `${sp.name} (ID: ${sp.id})`;

  const updated = (raw.updated as string | undefined) || maxTimestamp(raw.lastChangeDate as string);

  return {
    id,
    name: String(raw.name ?? ''),
    displayName: String(raw.displayName ?? raw.display_name ?? ''),
    description: description || null,
    stage: String(raw.stage ?? ''),
    created: String(raw.created ?? ''),
    updated: updated || null,
    dataSizeBytes: asNumberOrNull(raw.dataSizeBytes),
    tablesCount,
    links: null,
    source_project: sourceProject,
    created_by: null,
    last_updated_by: null,
    branch_id: branchId || null,
    prod_id: prodId,
  };
};

/** Port of BucketDetail.with_lineage_metadata. */
const withBucketLineage = (bucket: Bucket, raw: RawObj): Bucket => {
  const metadata = raw.metadata;
  if (!Array.isArray(metadata) || metadata.length === 0) return bucket;
  const lastUpdatedBy = getLastUpdatedBy(metadata);
  return {
    ...bucket,
    created_by: getCreatedBy(metadata),
    last_updated_by: lastUpdatedBy,
    updated: maxTimestamp(bucket.updated, lastUpdatedBy?.timestamp ?? null),
  };
};

type TableColumnInfo = {
  name: string;
  quotedName: string;
  database_native_type: string;
  nullable: boolean;
  keboola_base_type: string | null;
  description: string | null;
};

type Table = {
  id: string;
  name: string;
  displayName: string;
  description: string | null;
  primaryKey: string | null; // serialized as '|'-joined string (port of serialize_primary_key)
  created: string | null;
  updated: string | null;
  rowsCount: number | null;
  dataSizeBytes: number | null;
  links: Link[] | null;
  source_project: string | null;
  // detail-only fields (absent on summaries)
  columns?: TableColumnInfo[] | null;
  fullyQualifiedName?: string | null;
  used_by?: ComponentUsageReference[] | null;
  created_by?: ComponentUsageReference | null;
  last_updated_by?: ComponentUsageReference | null;
  // internal
  branch_id: string | null;
  prod_id: string;
  isDetail: boolean;
};

const validateTableCommon = (raw: RawObj): Omit<Table, 'isDetail'> => {
  const id = String(raw.id ?? '');
  const metadata = raw.metadata;

  const branchId = getMetadataProperty(metadata, FAKE_DEVELOPMENT_BRANCH);
  const prodId = branchId ? id.replace(`c-${branchId}-`, 'c-') : id;

  const description =
    getMetadataProperty(metadata, MetadataField.DESCRIPTION) ||
    getMetadataProperty(getNested(raw, 'sourceTable.metadata') ?? [], MetadataField.DESCRIPTION) ||
    (raw.description as string | undefined) ||
    null;

  let sourceProject: string | null = null;
  const sp = getNested(raw, 'sourceTable.project') as RawObj | undefined;
  if (sp) sourceProject = `${sp.name} (ID: ${sp.id})`;

  const updated =
    (raw.updated as string | undefined) ||
    maxTimestamp(raw.lastChangeDate as string, raw.lastImportDate as string);

  const pk = raw.primaryKey;
  const primaryKey = Array.isArray(pk) && pk.length ? (pk as string[]).join('|') : null;

  return {
    id,
    name: String(raw.name ?? ''),
    displayName: String(raw.displayName ?? raw.display_name ?? ''),
    description: description || null,
    primaryKey,
    created: (raw.created as string | undefined) ?? null,
    updated: updated || null,
    rowsCount: asNumberOrNull(raw.rowsCount),
    dataSizeBytes: asNumberOrNull(raw.dataSizeBytes),
    links: null,
    source_project: sourceProject,
    branch_id: branchId || null,
    prod_id: prodId,
  };
};

const withTableLineage = (table: Table, raw: RawObj): Table => {
  const metadata = raw.metadata;
  if (!Array.isArray(metadata) || metadata.length === 0) return table;
  const lastUpdatedBy = getLastUpdatedBy(metadata);
  return {
    ...table,
    created_by: getCreatedBy(metadata),
    last_updated_by: lastUpdatedBy,
    updated: maxTimestamp(table.updated, lastUpdatedBy?.timestamp ?? null),
  };
};

// Strip internal/absent fields before emitting (mirrors pydantic exclude + the
// TableSummary vs TableDetail field split). isDetail tables keep their detail fields.
const serializeBucket = (b: Bucket): RawObj => ({
  id: b.id,
  name: b.name,
  displayName: b.displayName,
  description: b.description,
  stage: b.stage,
  created: b.created,
  updated: b.updated,
  dataSizeBytes: b.dataSizeBytes,
  tablesCount: b.tablesCount,
  links: b.links,
  source_project: b.source_project,
  created_by: b.created_by,
  last_updated_by: b.last_updated_by,
});

const serializeTable = (t: Table): RawObj => {
  const base: RawObj = {
    id: t.id,
    name: t.name,
    displayName: t.displayName,
    description: t.description,
    primaryKey: t.primaryKey,
    created: t.created,
    updated: t.updated,
    rowsCount: t.rowsCount,
    dataSizeBytes: t.dataSizeBytes,
    links: t.links,
    source_project: t.source_project,
  };
  if (t.isDetail) {
    base.columns = t.columns ?? null;
    base.fullyQualifiedName = t.fullyQualifiedName ?? null;
    base.used_by = t.used_by ?? null;
    base.created_by = t.created_by ?? null;
    base.last_updated_by = t.last_updated_by ?? null;
  }
  return base;
};

// ---------------------------------------------------------------------------
// Branch-aware fetch helpers (port of storage_helpers.py, production-branch
// path). The TS client surface has no branchId; createKeboolaClients exposes a
// production-only `default` branch alias, matching has_storage_branches=false.
// ---------------------------------------------------------------------------

const safeBucketDetail = async (
  raw: RawClient,
  bucketId: string,
  branchId: string,
): Promise<RawObj | null> => {
  try {
    return await raw.get<RawObj>(`branch/${branchId}/buckets/${bucketId}`);
  } catch (error) {
    if (error instanceof RawHttpError && error.status === 404) return null;
    throw error;
  }
};

// ---------------------------------------------------------------------------
// Links packing (port of GetBucketsOutput.pack_links / GetTablesOutput.pack_links).
// ---------------------------------------------------------------------------

const sortLinks = (links: Link[]): Link[] =>
  [...links].sort((a, b) =>
    a.type < b.type ? -1 : a.type > b.type ? 1 : a.title < b.title ? -1 : a.title > b.title ? 1 : 0,
  );

const dedupeLinks = (links: Link[]): Link[] => {
  const seen = new Set<string>();
  const out: Link[] = [];
  for (const link of links) {
    const key = `${link.type} ${link.title} ${link.url}`;
    if (!seen.has(key)) {
      seen.add(key);
      out.push(link);
    }
  }
  return out;
};

// ---------------------------------------------------------------------------
// Table listing / detail (ports of _list_tables and _get_table, production path).
// ---------------------------------------------------------------------------

const TABLE_LIST_INCLUDES = [
  'metadata',
  'columnMetadata',
  'sourceMetadata',
  'sourceColumnMetadata',
];

const listTables = async (
  raw: RawClient,
  branchId: string,
  bucketIds: string[],
  linksManager: ProjectLinksManager,
): Promise<Table[]> => {
  const tablesByProdId = new Map<string, Table>();

  for (const bucketId of bucketIds) {
    const prodRaw = await safeBucketDetail(raw, bucketId, branchId);
    if (!prodRaw) continue;
    const prodBucket = validateBucket(prodRaw);
    if (prodBucket.branch_id) continue; // production path

    const rawTables = await raw.get<RawObj[]>(
      `branch/${branchId}/buckets/${prodBucket.id}/tables`,
      {
        params: { include: TABLE_LIST_INCLUDES.join(',') },
      },
    );
    for (const rawTable of rawTables) {
      const tableName = String(rawTable.name ?? '');
      const summary: Table = {
        ...validateTableCommon(rawTable),
        isDetail: false,
        links: [linksManager.getTableDetailLink(prodBucket.id, tableName)],
      };
      tablesByProdId.set(summary.id, summary);
    }
  }

  return [...tablesByProdId.values()];
};

/**
 * Resolve a fully qualified name from the table's bucket backendPath, Snowflake-style.
 * Port of _SnowflakeWorkspace.get_table_info — Snowflake is the dominant backend.
 * NOTE: the actual SQL dialect requires a live workspace (WorkspaceManager, not yet
 * ported); BigQuery FQN/alias rules and quoting differ. See REPORT gaps.
 */
const tableFqn = (rawTable: RawObj): string | null => {
  const bucket = rawTable.bucket;
  const backendPath =
    bucket && typeof bucket === 'object' ? (bucket as RawObj).backendPath : undefined;
  if (!Array.isArray(backendPath) || backendPath.length < 2) return null;
  const name = String(rawTable.name ?? '');
  return [backendPath[0], backendPath[1], name].map((p) => `"${p}"`).join('.');
};

const getTableDetail = async (
  raw: RawClient,
  branchId: string,
  tableId: string,
  linksManager: ProjectLinksManager,
): Promise<Table | null> => {
  let rawTable: RawObj | null;
  try {
    rawTable = await raw.get<RawObj>(`branch/${branchId}/tables/${tableId}`);
  } catch (error) {
    if (error instanceof RawHttpError && error.status === 404) return null;
    throw error;
  }
  if (!rawTable) return null;

  // production path: a table carrying branch metadata is not a prod table.
  if (getMetadataProperty(rawTable.metadata, FAKE_DEVELOPMENT_BRANCH)) return null;

  const rawColumns = Array.isArray(rawTable.columns) ? (rawTable.columns as string[]) : [];
  const columnMetadata = (rawTable.columnMetadata as Record<string, unknown>) ?? {};
  const sourceColumnMetadata =
    (getNested(rawTable, 'sourceTable.columnMetadata') as Record<string, unknown>) ?? {};

  const columns: TableColumnInfo[] = rawColumns.map((colName) => {
    const colMeta = columnMetadata[colName] ?? [];
    const srcMeta = sourceColumnMetadata[colName] ?? [];

    const description =
      getMetadataProperty(colMeta, MetadataField.DESCRIPTION) ||
      getMetadataProperty(srcMeta, MetadataField.DESCRIPTION) ||
      null;
    const baseType =
      getMetadataProperty(colMeta, DATATYPE_BASETYPE, ['user']) ||
      getMetadataProperty(srcMeta, DATATYPE_BASETYPE, ['user']) ||
      null;
    let nativeType =
      getMetadataProperty(colMeta, DATATYPE_TYPE) || getMetadataProperty(srcMeta, DATATYPE_TYPE);
    const nullableStr =
      getMetadataProperty(colMeta, DATATYPE_NULLABLE) ||
      getMetadataProperty(srcMeta, DATATYPE_NULLABLE);

    if (nativeType === null) {
      nativeType = 'VARCHAR'; // Snowflake default (BigQuery would be STRING)
    }
    const nullable =
      nullableStr != null ? ['1', 'true'].includes(String(nullableStr).toLowerCase()) : false;

    return {
      name: colName,
      quotedName: `"${colName}"`,
      database_native_type: nativeType,
      nullable,
      keboola_base_type: baseType,
      description,
    };
  });

  const bucketInfo = (rawTable.bucket as RawObj) ?? {};
  const bucketId = String(bucketInfo.id ?? '');
  const tableName = String(rawTable.name ?? '');

  const common = validateTableCommon(rawTable);
  let table: Table = {
    ...common,
    isDetail: true,
    columns,
    fullyQualifiedName: tableFqn(rawTable),
    used_by: null,
    created_by: null,
    last_updated_by: null,
    links: [linksManager.getTableDetailLink(bucketId, tableName)],
  };
  table = withTableLineage(table, rawTable);
  // collapse dev id to prod id (no-op on production path)
  return { ...table, id: table.prod_id, branch_id: null };
};

type ItemType = 'bucket' | 'table' | 'column';
type ParsedItemId = {
  itemType: ItemType;
  bucketId?: string;
  tableId?: string;
  columnName?: string;
};
type UpdateItemResult = { item_id: string; success: boolean; error?: string; timestamp?: string };
type MetadataEntry = { key?: string; value?: string; timestamp?: string };

/** Parse "in.c-bucket[.table[.column]]" into its parts (port of _parse_item_id). */
const parseItemId = (itemId: string): ParsedItemId => {
  if (!itemId.startsWith('in.') && !itemId.startsWith('out.')) {
    throw new Error(`Invalid item_id format: ${itemId} - must start with in. or out.`);
  }
  const parts = itemId.split('.');
  if (parts.length === 2) {
    return { itemType: 'bucket', bucketId: itemId };
  }
  if (parts.length === 3) {
    return { itemType: 'table', bucketId: `${parts[0]}.${parts[1]}`, tableId: itemId };
  }
  if (parts.length === 4) {
    return {
      itemType: 'column',
      bucketId: `${parts[0]}.${parts[1]}`,
      tableId: `${parts[0]}.${parts[1]}.${parts[2]}`,
      columnName: parts[3],
    };
  }
  throw new Error(`Invalid item_id format: ${itemId}`);
};

const findDescriptionEntry = (entries: MetadataEntry[]): MetadataEntry | undefined =>
  entries.find((entry) => entry.key === MetadataField.DESCRIPTION);

const updateBucketDescription = async (
  raw: RawClient,
  bucketId: string,
  description: string,
): Promise<UpdateItemResult> => {
  try {
    const response = await raw.post<MetadataEntry[]>(`buckets/${bucketId}/metadata`, {
      body: {
        provider: 'user',
        metadata: [{ key: MetadataField.DESCRIPTION, value: description }],
      },
    });
    return {
      item_id: bucketId,
      success: true,
      timestamp: findDescriptionEntry(response)?.timestamp,
    };
  } catch (error) {
    return {
      item_id: bucketId,
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
};

const updateTableDescription = async (
  raw: RawClient,
  tableId: string,
  description: string,
): Promise<UpdateItemResult> => {
  try {
    const response = await raw.post<{ metadata?: MetadataEntry[] }>(`tables/${tableId}/metadata`, {
      body: {
        provider: 'user',
        metadata: [{ key: MetadataField.DESCRIPTION, value: description }],
      },
    });
    return {
      item_id: tableId,
      success: true,
      timestamp: findDescriptionEntry(response.metadata ?? [])?.timestamp,
    };
  } catch (error) {
    return {
      item_id: tableId,
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
};

const updateColumnDescriptions = async (
  raw: RawClient,
  tableId: string,
  columnUpdates: Record<string, string>,
): Promise<UpdateItemResult[]> => {
  try {
    const columnsMetadata: Record<string, MetadataEntry[]> = {};
    for (const [columnName, description] of Object.entries(columnUpdates)) {
      columnsMetadata[columnName] = [
        { key: MetadataField.DESCRIPTION, value: description, columnName } as MetadataEntry,
      ];
    }

    const response = await raw.post<{ columnsMetadata?: Record<string, MetadataEntry[]> }>(
      `tables/${tableId}/metadata`,
      { body: { provider: 'user', columnsMetadata } },
    );

    const returned = response.columnsMetadata ?? {};
    return Object.keys(columnUpdates).map((columnName) => {
      const entry = findDescriptionEntry(returned[columnName] ?? []);
      return entry
        ? { item_id: `${tableId}.${columnName}`, success: true, timestamp: entry.timestamp }
        : {
            item_id: `${tableId}.${columnName}`,
            success: false,
            error: 'No description metadata returned.',
          };
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return Object.keys(columnUpdates).map((columnName) => ({
      item_id: `${tableId}.${columnName}`,
      success: false,
      error: message,
    }));
  }
};

export const registerStorageTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'get_buckets',
    title: 'Get buckets',
    description: `Lists buckets or retrieves full details of specific buckets, including descriptions,
lineage references (created/updated by), and links.

WHEN NOT TO USE:
- Do NOT call with \`bucket_ids=[]\` just to find a bucket by name. Use \`search\` with
  item_types=["bucket"] instead.
- Only use \`bucket_ids=[]\` when you need a complete inventory of all buckets in the project.

EXAMPLES:
- \`bucket_ids=[]\` → summaries of all buckets in the project
- \`bucket_ids=["id1", ...]\` → full details of the buckets with the specified IDs`,
    annotations: { readOnlyHint: true },
    inputSchema: {
      bucket_ids: z.array(z.string()).default([]).describe('Filter by specific bucket IDs.'),
    },
    handler: async ({ bucket_ids }) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);
      const raw = clients.rawStorage;
      const branchId = clients.branchId; // 'default' on production

      const bucketDetailLink = (id: string, name: string): Link =>
        linksManager.getBucketDetailLink(id, name);

      const buckets: Bucket[] = [];
      const missingIds: string[] = [];
      let bucketCounts: {
        total_buckets: number;
        input_buckets: number;
        output_buckets: number;
      } | null = null;

      if (bucket_ids.length > 0) {
        const results = await Promise.all(
          bucket_ids.map(async (bucketId): Promise<Bucket | string> => {
            const prodRaw = await safeBucketDetail(raw, bucketId, branchId);
            if (!prodRaw) return bucketId;
            const bucket = withBucketLineage(validateBucket(prodRaw), prodRaw);
            // production path: only prod buckets (no branch metadata) are surfaced.
            if (bucket.branch_id) return bucketId;
            return {
              ...bucket,
              links: [bucketDetailLink(bucket.id, bucket.name || bucket.id)],
            };
          }),
        );
        for (const r of results) {
          if (typeof r === 'string') missingIds.push(r);
          else buckets.push(r);
        }
      } else {
        const rawList = await raw.get<RawObj[]>(`branch/${branchId}/buckets`, {
          params: { include: 'metadata,linkedBuckets' },
        });
        for (const item of rawList) {
          const bucket = validateBucket(item);
          if (bucket.branch_id) continue; // skip other-branch buckets (production path)
          buckets.push({
            ...bucket,
            links: [bucketDetailLink(bucket.id, bucket.name || bucket.id)],
          });
        }
        const total = buckets.length;
        const input = buckets.filter((b) => b.stage === 'in').length;
        bucketCounts = {
          total_buckets: total,
          input_buckets: input,
          output_buckets: total - input,
        };
      }

      // pack_links: hoist per-bucket links to the output level, deduped + sorted.
      const allLinks: Link[] = [linksManager.getBucketDashboardLink()];
      for (const b of buckets) {
        if (b.links) allLinks.push(...b.links);
      }
      const packedBuckets = buckets.map((b) => serializeBucket({ ...b, links: null }));

      return {
        buckets: packedBuckets,
        links: sortLinks(dedupeLinks(allLinks)),
        buckets_not_found: missingIds.length ? missingIds : null,
        bucket_counts: bucketCounts,
      };
    },
  });

  registerTool(server, {
    name: 'get_tables',
    title: 'Get tables',
    description: `Lists tables in buckets or retrieves full details of specific tables, including fully qualified database name,
column definitions, lineage references (created/updated by) and links.

WHEN NOT TO USE:
- Do NOT list tables across buckets just to find a table by name. Use \`search\` with
  item_types=["table"] instead — it also matches column names and descriptions.
- Only use \`bucket_ids\` listing when you need all tables in specific known buckets.

RETURNS:
- With \`bucket_ids\`: Summaries of tables (ID, name, description, primary key).
- With \`table_ids\`: Full details including columns, data types, and fully qualified database names.
- With \`table_ids\` and \`include_usage\`: Full details plus components / transformations that use the tables
  in their input / output mappings. Use only when explicitly needed or evident from context; usage calculation
  might be demanding in big projects.

COLUMN DATA TYPES:
- database_native_type: The actual type in the storage backend (Snowflake, BigQuery, etc.)
  with precision, scale, and other implementation details
- keboola_base_type: Standardized type indicating the semantic data type. May not always be
  available. When present, it reveals the actual type of data stored in the column - for example,
  a column with database_native_type VARCHAR might have keboola_base_type INTEGER, indicating
  it stores integer values despite being stored as text in the backend.

QUERYABILITY RULE:
- A table is directly queryable via query_data tool only if fullyQualifiedName is present and non-null
  in the response.
- If fullyQualifiedName is absent or null (e.g. for linked/alias tables from other projects),
  the table cannot be queried via SQL from this workspace.
- Do not attempt to construct or guess the FQN — it will not work. In that case,
  inform the user of the limitation immediately.

EXAMPLES:
- \`bucket_ids=["id1", ...]\` → summary info of the tables in the buckets with the specified IDs
- \`table_ids=["id1", ...]\` → detailed info of the tables specified by their IDs
- \`bucket_ids=[]\` and \`table_ids=[]\` → empty list; you have to specify at least one filter`,
    annotations: { readOnlyHint: true },
    inputSchema: {
      bucket_ids: z.array(z.string()).default([]).describe('Filter by specific bucket IDs.'),
      table_ids: z.array(z.string()).default([]).describe('Filter by specific table IDs.'),
      include_usage: z
        .boolean()
        .default(false)
        .describe('Show components / transformations where each table is used.'),
    },
    handler: async ({ bucket_ids, table_ids, include_usage }) => {
      const clients = createKeboolaClients(config);
      const linksManager = await createLinksManager(config, clients);
      const raw = clients.rawStorage;
      const branchId = clients.branchId;

      const tablesById = new Map<string, Table>();
      const missingIds: string[] = [];

      if (bucket_ids.length > 0) {
        for (const t of await listTables(raw, branchId, bucket_ids, linksManager)) {
          tablesById.set(t.id, t);
        }
      }

      if (table_ids.length > 0) {
        const results = await Promise.all(
          table_ids.map(async (tableId): Promise<Table | string> => {
            const t = await getTableDetail(raw, branchId, tableId, linksManager);
            return t ?? tableId;
          }),
        );
        for (const r of results) {
          if (typeof r === 'string') missingIds.push(r);
          else tablesById.set(r.id, r);
        }

        if (include_usage) {
          // find_id_usage depends on the search subsystem (tools/search.py), which is
          // not yet ported to TypeScript. Initialize empty used_by for parity of shape.
          for (const t of tablesById.values()) {
            if (t.isDetail) t.used_by = [];
          }
          logger.warn(
            'get_tables: include_usage requested but the search subsystem is not yet ported; returning empty usage.',
          );
        }
      }

      const tables = [...tablesById.values()];
      const allLinks: Link[] = [linksManager.getBucketDashboardLink()];
      for (const t of tables) {
        if (t.links) allLinks.push(...t.links);
      }
      const packed = tables.map((t) => serializeTable({ ...t, links: null }));

      return {
        tables: packed,
        links: sortLinks(dedupeLinks(allLinks)),
        tables_not_found: missingIds.length ? missingIds : null,
      };
    },
  });

  registerTool(server, {
    name: 'update_descriptions',
    title: 'Update descriptions',
    description: 'Updates the description for Keboola storage items (buckets, tables, or columns).',
    inputSchema: {
      updates: z
        .array(
          z.object({
            item_id: z
              .string()
              .describe(
                'Storage item: "bucket_id", "bucket_id.table_id", or "bucket_id.table_id.column_name".',
              ),
            description: z.string().describe('New description to set.'),
          }),
        )
        .describe('List of description updates to apply.'),
    },
    handler: async ({ updates }) => {
      const { rawStorage } = createKeboolaClients(config);
      const results: UpdateItemResult[] = [];

      // Group valid updates by type; record invalid item_ids up front.
      const bucketUpdates: Record<string, string> = {};
      const tableUpdates: Record<string, string> = {};
      const columnUpdatesByTable: Record<string, Record<string, string>> = {};

      for (const update of updates) {
        let parsed: ParsedItemId;
        try {
          parsed = parseItemId(update.item_id);
        } catch (error) {
          results.push({
            item_id: update.item_id,
            success: false,
            error: `Invalid item_id format: ${error instanceof Error ? error.message : String(error)}`,
          });
          continue;
        }
        if (parsed.itemType === 'bucket') {
          bucketUpdates[parsed.bucketId!] = update.description;
        } else if (parsed.itemType === 'table') {
          tableUpdates[parsed.tableId!] = update.description;
        } else {
          (columnUpdatesByTable[parsed.tableId!] ??= {})[parsed.columnName!] = update.description;
        }
      }

      for (const [bucketId, description] of Object.entries(bucketUpdates)) {
        results.push(await updateBucketDescription(rawStorage, bucketId, description));
      }
      for (const [tableId, description] of Object.entries(tableUpdates)) {
        results.push(await updateTableDescription(rawStorage, tableId, description));
      }
      for (const [tableId, columnUpdates] of Object.entries(columnUpdatesByTable)) {
        results.push(...(await updateColumnDescriptions(rawStorage, tableId, columnUpdates)));
      }

      const successful = results.filter((result) => result.success).length;
      return {
        results,
        total_processed: results.length,
        successful,
        failed: results.length - successful,
      };
    },
  });
};
