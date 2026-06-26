import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients } from '@/clients/keboola';
import type { RawClient } from '@/clients/raw';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import { registerTool } from '@/mcp/tool';

// Ported from tools/storage.py (update_descriptions).

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
