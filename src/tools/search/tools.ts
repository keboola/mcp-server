import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager } from '@/clients/keboola';
import type { Config } from '@/config';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import { enumerationSearch, globalTextualSearch, isGlobalSearchEnabled } from './globalSearch';
import {
  DEFAULT_GLOBAL_SEARCH_LIMIT,
  MAX_GLOBAL_SEARCH_LIMIT,
  SEARCH_ITEM_TYPES,
  SEARCH_PATTERN_MODES,
  SEARCH_TYPES,
  type SearchOutput,
  SearchSpec,
} from './model';

// Ported from tools/search.py: the `find_component_id` and global `search` tools.

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

      const response = await clients.ai.suggestComponent({ prompt: query });

      return (response.components ?? []).map((component) => {
        const componentId = component.componentId ?? '';
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
