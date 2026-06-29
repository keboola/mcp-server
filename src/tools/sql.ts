import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

import { createKeboolaClients } from '@/clients/keboola';
import { createRawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import { type SqlSelectData, WorkspaceManager } from '@/workspace';

// Ported from tools/sql.py.

const MAX_ROWS = 1_000;
const MAX_CHARS = 50_000;

const QUERY_DATA_DESCRIPTION = `
    Executes an SQL SELECT query to get the data from the underlying database.

    BEFORE QUERYING:
    * Always verify the table has a non-null fullyQualifiedName from get_tables tool.
      If it does not, the table is not SQL-accessible from this workspace — do not attempt the query and inform user.

    CRITICAL SQL REQUIREMENTS:

    * ALWAYS check the SQL dialect before constructing queries.
    * Do not include any comments in the SQL code
    * Use delimited identifiers and FQN format for the current SQL dialect.

    TABLE AND COLUMN REFERENCES:
    * Always use fully qualified table names in the exact FQN format provided by table information tools
    * Follow the identifier structure exactly as shown by table info tools for the current SQL dialect
    * Always use delimited identifiers when referring to table columns

    CTE (WITH CLAUSE) RULES:
    * ALL column references in main query MUST match exact case used in the CTE
    * If you alias a column in a CTE, reference it under the aliased name in the subsequent queries
    * Define all column aliases explicitly in CTEs
    * Use delimited identifiers in both CTE definition and references to preserve case

    FUNCTION COMPATIBILITY:
    * Check data types before using date functions (DATE_TRUNC, EXTRACT require proper date/timestamp types)
    * Cast VARCHAR columns to appropriate types before using in date/numeric functions

    ERROR PREVENTION:
    * Never pass empty strings ('') where numeric or date values are expected
    * Use NULLIF or CASE statements to handle empty values
    * Always use TRY_CAST or similar safe casting functions when converting data types
    * Check for division by zero using NULLIF(denominator, 0)
    * Always use the LIMIT clause in your SELECT statements when fetching data. There are hard limits imposed
      by this tool on the maximum number of rows that can be fetched and the maximum number of characters.
      The tool will truncate the data if those limits are exceeded.

    DATA VALIDATION:
    * When querying columns with categorical values, use query_data tool to inspect distinct values beforehand
    * Ensure valid filtering by checking actual data values first
    `;

/**
 * Serializes rows to CSV, matching Python's `csv.DictWriter` defaults:
 * comma delimiter, `\r\n` line terminator, minimal quoting (quote a field only
 * when it contains the delimiter, a quote, CR, or LF; double embedded quotes).
 */
const toCsv = (data: SqlSelectData): string => {
  const needsQuote = (field: string): boolean =>
    field.includes(',') || field.includes('"') || field.includes('\n') || field.includes('\r');
  const formatField = (value: unknown): string => {
    const s = value === null || value === undefined ? '' : String(value);
    return needsQuote(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };
  const lines: string[] = [];
  lines.push(data.columns.map(formatField).join(','));
  for (const row of data.rows) {
    lines.push(data.columns.map((col) => formatField(row[col])).join(','));
  }
  return lines.map((line) => `${line}\r\n`).join('');
};

/**
 * Builds the WorkspaceManager from the resolved config. The query service /
 * workspace-discovery clients are built locally (the shared `createKeboolaClients`
 * does not expose a query service client), rooted at `query.<suffix>`.
 */
const createWorkspaceManager = async (config: Config): Promise<WorkspaceManager> => {
  const clients = createKeboolaClients(config);
  const urls = deriveServiceUrls(config.storageApiUrl ?? '');
  // Query Service host: no dedicated key in deriveServiceUrls — derive `query.<suffix>`
  // from the storage host, matching the Python `query.<hostname_suffix>` derivation.
  const suffix = new URL(urls.storage).hostname.slice('connection.'.length);
  const queryServiceUrl = `https://query.${suffix}`;

  // Query Service prefers the OAuth bearer token, falling back to the SAPI token.
  const queryServiceToken = config.bearerToken
    ? `Bearer ${config.bearerToken}`
    : (config.storageToken ?? '');

  // A production-branch raw storage client for the legacy / default-branch fallback path.
  const storageToken = config.bearerToken
    ? `Bearer ${config.bearerToken}`
    : (config.storageToken ?? '');
  const makeProdRawStorage = () =>
    createRawClient({ baseUrl: `${urls.storage}/v2/storage`, token: storageToken });

  return WorkspaceManager.create(config, {
    rawStorage: clients.rawStorage,
    makeProdRawStorage,
    queryServiceUrl,
    queryServiceToken,
  });
};

export const registerSqlTools = (server: McpServer, config: Config): void => {
  registerTool(server, {
    name: 'query_data',
    title: 'Query data',
    description: QUERY_DATA_DESCRIPTION,
    annotations: { readOnlyHint: true },
    inputSchema: {
      sql_query: z.string().describe('SQL SELECT query to run.'),
      query_name: z
        .string()
        .describe(
          'A concise, human-readable name for this query based on its purpose and what data it retrieves. ' +
            'Use normal words with spaces (e.g., "Customer Orders Last Month", "Top Selling Products", ' +
            '"User Activity Summary").',
        ),
    },
    handler: async (args) => {
      const workspaceManager = await createWorkspaceManager(config);

      const result = await workspaceManager.executeQuery(args.sql_query, {
        maxRows: MAX_ROWS,
        maxChars: MAX_CHARS,
      });

      if (result.status === 'ok') {
        logger.info(
          [`Query "${args.query_name}" executed successfully.`, result.message]
            .filter(Boolean)
            .join(' '),
        );
        const data: SqlSelectData = result.data
          ? result.data
          : // Non-SELECT query (should not happen for this SELECT-only tool).
            { columns: ['message'], rows: [{ message: result.message }] };

        return {
          query_name: args.query_name,
          csv_data: toCsv(data),
          message: result.message ?? null,
        };
      }

      // Surface cancellation cleanly without the generic "Failed to run SQL query" prefix.
      if (result.message === 'Query was cancelled') {
        logger.info(`Query "${args.query_name}" was cancelled.`);
        throw new Error('Query was cancelled');
      }
      logger.warn([`Query "${args.query_name}" failed.`, result.message].filter(Boolean).join(' '));
      throw new Error(`Failed to run SQL query, error: ${result.message}`);
    },
  });
};
