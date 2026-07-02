import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import { type RawClient, RawHttpError } from '@/clients/raw';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import type { Link, ProjectLinksManager } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import {
  type Bucket,
  buildTableColumns,
  type Dialect,
  FAKE_DEVELOPMENT_BRANCH_KEY,
  getMetadataProperty,
  type RawObj,
  serializeBucket,
  serializeTable,
  type Table,
  tableFqn,
  validateBucket,
  validateTableCommon,
  withBucketLineage,
  withTableLineage,
} from './model';

// Ported from tools/storage/tools.py (get_buckets, get_tables, update_descriptions),
// tools/storage_helpers.py and tools/storage/usage.py. The model/serialization layer lives in
// ./model, lineage references in ./usage.

// ---------------------------------------------------------------------------
// Project backend resolution. The SQL dialect is needed to build dialect-aware
// fully-qualified names; it is read from the verified token's owner.defaultBackend,
// which is far cheaper than provisioning a live workspace. Snowflake is assumed
// when the field is absent (the dominant backend / legacy projects).
// ---------------------------------------------------------------------------

const resolveDialect = async (raw: RawClient): Promise<Dialect> => {
  try {
    const tokenInfo = await raw.get<RawObj>('tokens/verify');
    const owner = (tokenInfo.owner ?? {}) as RawObj;
    return owner.defaultBackend === 'bigquery' ? 'bigquery' : 'snowflake';
  } catch (error) {
    logger.warn(
      `get_tables: failed to resolve project backend (${String(error)}); defaulting to snowflake.`,
    );
    return 'snowflake';
  }
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
    const key = `${link.type} ${link.title} ${link.url}`;
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

const getTableDetail = async (
  raw: RawClient,
  branchId: string,
  tableId: string,
  linksManager: ProjectLinksManager,
  dialect: Dialect,
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
  if (getMetadataProperty(rawTable.metadata, FAKE_DEVELOPMENT_BRANCH_KEY)) return null;

  const columns = buildTableColumns(rawTable, dialect);

  const bucketInfo = (rawTable.bucket as RawObj) ?? {};
  const bucketId = String(bucketInfo.id ?? '');
  const tableName = String(rawTable.name ?? '');

  const common = validateTableCommon(rawTable);
  let table: Table = {
    ...common,
    isDetail: true,
    columns,
    fullyQualifiedName: tableFqn(rawTable, dialect),
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
        // Resolve the SQL dialect once so detail FQNs / native-type defaults are
        // backend-correct (Snowflake double-quote 3-part vs BigQuery backtick 2-part).
        const dialect = await resolveDialect(raw);
        const results = await Promise.all(
          table_ids.map(async (tableId): Promise<Table | string> => {
            const t = await getTableDetail(raw, branchId, tableId, linksManager, dialect);
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
