import { createQueryServiceClient } from '@keboola/api-client/queryService';

import type { RawClient } from '@/clients/raw';
import { RawHttpError } from '@/clients/raw';
import type { Config } from '@/config';
import { logger } from '@/logger';

/**
 * Workspace layer — port of `keboola_mcp_server.workspace`.
 *
 * Resolves (or creates) the read-only SQL workspace for the project/branch and runs
 * `SELECT` queries over HTTP only:
 *   - Snowflake  -> Query Service API (submit job -> poll -> paginate results)
 *   - BigQuery   -> same Query Service API; differs only in identifier quoting,
 *                   FQN construction, and error-message normalization.
 *
 * No DB drivers are used; all SQL flows through the Query Service.
 */

const STORAGE_BRANCHES_FEATURE = 'storage-branches';

const QUERY_TIMEOUT_MS = 300_000; // 5 minutes
const CANCELLATION_TIMEOUT_MS = 30_000; // 30 seconds
const PAGE_SIZE = 1_000;
const SELECTED_ROWS_MSG = (rows: number, total: number | null) =>
  `Returning ${rows} of ${total} selected rows.`;

const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ---------------------------------------------------------------------------
// Result types (port of the pydantic dataclasses).
// ---------------------------------------------------------------------------

export type SqlSelectDataRow = Record<string, unknown>;

export type SqlSelectData = {
  columns: string[];
  rows: SqlSelectDataRow[];
};

export type QueryResult = {
  status: 'ok' | 'error';
  data?: SqlSelectData | null;
  message?: string | null;
};

export type JobSubmittedInfo = {
  job_id: string;
  cancellation_url: string | null;
  backend: string;
};

export type JobSubmittedCallback = (info: JobSubmittedInfo) => Promise<void>;

const isOk = (result: QueryResult): boolean => result.status === 'ok';

// ---------------------------------------------------------------------------
// Query Service client (built locally via the api-client factory).
// ---------------------------------------------------------------------------

type QsClient = ReturnType<typeof createQueryServiceClient>;

/** Status values Query Service reports for a terminal job. Snowflake/BQ both go through QS. */
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'canceled', 'cancelled']);
const CANCELLED_STATUSES = new Set(['canceled', 'cancelled']);

// ---------------------------------------------------------------------------
// Workspace discovery (SAPI raw client) — port of `_WspInfo`.
// ---------------------------------------------------------------------------

type WspInfo = {
  id: number;
  schema: string;
  backend: string;
  credentials: string | null; // serialized JSON for BigQuery
  readonly: boolean | null;
};

const fromSapiInfo = (sapi: Record<string, unknown>): WspInfo => {
  const connection = (sapi.connection ?? {}) as Record<string, unknown>;
  return {
    id: sapi.id as number,
    schema: connection.schema as string,
    backend: connection.backend as string,
    credentials: (connection.user as string | undefined) ?? null,
    readonly: (sapi.readOnlyStorageAccess as boolean | undefined) ?? null,
  };
};

// ---------------------------------------------------------------------------
// Per-backend workspace behavior — port of `_SnowflakeWorkspace` / `_BigQueryWorkspace`.
// ---------------------------------------------------------------------------

abstract class Workspace {
  protected qsClient: QsClient | null = null;

  constructor(
    readonly id: number,
    protected readonly deps: WorkspaceDeps,
  ) {}

  abstract getSqlDialect(): string;
  abstract getQuotedName(name: string): string;
  protected formatErrorMessage(message: string | null): string | null {
    return message;
  }

  private async createQsClient(): Promise<{ client: QsClient; branchId: string }> {
    let realBranchId = this.deps.branchId;
    if (!realBranchId) {
      const branches = await this.deps.rawStorage.get<Record<string, unknown>[]>('dev-branches');
      for (const branch of branches) {
        if (branch.isDefault === true) {
          realBranchId = String(branch.id);
          break;
        }
      }
    }
    if (!realBranchId) {
      throw new Error('Cannot determine the default branch ID');
    }

    const client = createQueryServiceClient({
      baseUrl: this.deps.queryServiceUrl,
      token: this.deps.queryServiceToken,
      middlewares: [],
    });
    return { client, branchId: realBranchId };
  }

  private async ensureQsClient(): Promise<{ client: QsClient; branchId: string }> {
    if (this.qsClient && this.cachedBranchId) {
      return { client: this.qsClient, branchId: this.cachedBranchId };
    }
    const created = await this.createQsClient();
    this.qsClient = created.client;
    this.cachedBranchId = created.branchId;
    return created;
  }

  private cachedBranchId: string | null = null;

  async getBranchId(): Promise<string> {
    const { branchId } = await this.ensureQsClient();
    return branchId;
  }

  buildCancelUrl(jobId: string): string {
    return `${this.deps.queryServiceUrl}/api/v1/queries/${jobId}/cancel`;
  }

  /**
   * Cancel a query job and poll until cancellation is confirmed.
   * Returns [cancellationConfirmed, queryCompleted].
   */
  private async cancelJobWithTimeout(
    client: QsClient,
    jobId: string,
    reason: string,
  ): Promise<[boolean, boolean]> {
    try {
      await client.cancelQueryJob(jobId);
      logger.info(`Query cancellation requested: job_id=${jobId} reason=${reason}`);

      const cancelStart = Date.now();
      for (;;) {
        const jobStatus = await client.getQueryJob(jobId);
        const status = jobStatus.status as string | undefined;
        if (!status) {
          logger.warn(`Query status response missing "status" field: job_id=${jobId}`);
          return [false, false];
        }
        if (status === 'completed') {
          logger.info(`Query completed successfully during cancellation attempt: job_id=${jobId}`);
          return [true, true];
        }
        if (status === 'failed' || CANCELLED_STATUSES.has(status)) {
          logger.info(`Query job cancellation confirmed: job_id=${jobId}, status=${status}`);
          return [true, false];
        }
        if (Date.now() - cancelStart > CANCELLATION_TIMEOUT_MS) {
          logger.info(
            `Query cancellation polling timed out after ${CANCELLATION_TIMEOUT_MS / 1000}s: ` +
              `job_id=${jobId}, status=${status}`,
          );
          return [false, false];
        }
        await wait(500);
      }
    } catch (error) {
      logger.error({ err: error }, `Unexpected error during query cancellation: job_id=${jobId}`);
      return [false, false];
    }
  }

  async executeQuery(
    sqlQuery: string,
    opts: {
      maxRows?: number | null;
      maxChars?: number | null;
      onJobSubmitted?: JobSubmittedCallback | null;
    } = {},
  ): Promise<QueryResult> {
    const maxRows = opts.maxRows ?? null;
    const maxChars = opts.maxChars ?? null;
    if (maxRows !== null && maxRows <= 0) {
      throw new Error('The "max_rows" must be a positive integer or None.');
    }
    if (maxChars !== null && maxChars <= 0) {
      throw new Error('The "max_chars" must be a positive integer or None.');
    }

    const { client, branchId } = await this.ensureQsClient();

    const tsStart = Date.now();
    const submitResp = await client.createQueryJob(branchId, String(this.id), {
      statements: [sqlQuery],
    } as never);
    const jobId = submitResp.queryJobId;

    if (opts.onJobSubmitted) {
      const info: JobSubmittedInfo = {
        job_id: jobId,
        cancellation_url: this.buildCancelUrl(jobId),
        backend: this.getSqlDialect().toLowerCase(),
      };
      try {
        await opts.onJobSubmitted(info);
      } catch (exc) {
        // Best-effort: a failed progress notification must not kill the running query.
        logger.warn(
          `on_job_submitted callback raised for job_id=${jobId}: ${String(exc)} — continuing`,
        );
      }
    }

    let jobStatus = await client.getQueryJob(jobId);
    while (!TERMINAL_STATUSES.has(jobStatus.status as string)) {
      await wait(1000);
      const elapsed = Date.now() - tsStart;
      if (elapsed > QUERY_TIMEOUT_MS) {
        const [cancellationConfirmed, queryCompleted] = await this.cancelJobWithTimeout(
          client,
          jobId,
          `Query timeout exceeded after ${(elapsed / 1000).toFixed(2)} seconds`,
        );
        if (queryCompleted) {
          logger.info(
            `Query completed during cancellation polling, returning results: job_id=${jobId}`,
          );
          jobStatus = await client.getQueryJob(jobId);
          break;
        }
        if (cancellationConfirmed) {
          throw new Error(
            `Query execution timed out after ${(elapsed / 1000).toFixed(2)} seconds. ` +
              `The query has been cancelled: job_id=${jobId}`,
          );
        }
        throw new Error(
          `Query execution timed out after ${(elapsed / 1000).toFixed(2)} seconds. ` +
            `Cancellation was attempted but could not be confirmed. ` +
            `The query may still be running on the server: job_id=${jobId}`,
        );
      }
      jobStatus = await client.getQueryJob(jobId);
    }

    // Short-circuit when the job reached a terminal CANCELLED state out-of-band.
    const terminalStatus = jobStatus.status as string;
    if (CANCELLED_STATUSES.has(terminalStatus)) {
      logger.info(`Query was cancelled (terminal status=${terminalStatus}): job_id=${jobId}`);
      return { status: 'error', data: null, message: 'Query was cancelled' };
    }

    const statements = jobStatus.statements as { id: string }[];
    const statementId = statements[0]!.id;

    // Fetch results with pagination.
    const allRows: unknown[][] = [];
    let allRowsChars = 0;
    let columns: string[] = [];
    let offset = 0;
    let message: string | null = null;
    let totalQueryRows: number | null = null;

    for (;;) {
      let rowsToFetch: number;
      if (maxRows !== null) {
        const remaining = maxRows - allRows.length;
        if (remaining <= 0) break;
        rowsToFetch = Math.min(PAGE_SIZE, remaining);
      } else {
        rowsToFetch = PAGE_SIZE;
      }

      const results = await client.getQueryResults(jobId, statementId, {
        offset,
        pageSize: Math.max(rowsToFetch, 100), // QueryService requires 100 - 10_000
      } as never);

      if (offset === 0) {
        const status = results.status as string;
        message = (results.message as string | undefined) ?? null;
        totalQueryRows = (results.numberOfRows as number | undefined) ?? null;

        if (status === 'failed' || CANCELLED_STATUSES.has(status)) {
          return { status: 'error', data: null, message: this.formatErrorMessage(message) };
        }
        if (status !== 'completed') {
          throw new Error(`Unexpected query status: ${status}`);
        }

        columns = ((results.columns as { name: string }[] | undefined) ?? []).map((c) => c.name);
      }

      const pageDataAll = (results.data as unknown[][] | undefined) ?? [];
      if (pageDataAll.length === 0) break;

      const pageData = pageDataAll.slice(0, rowsToFetch);
      let charLimitReached = false;
      if (maxChars !== null) {
        for (const row of pageData) {
          const chars = row.reduce<number>(
            (sum, v) => (v !== null && v !== undefined ? sum + String(v).length : sum),
            0,
          );
          if (allRowsChars + chars <= maxChars) {
            allRows.push(row);
            allRowsChars += chars;
          } else {
            // First row that does not fit ends pagination, keeping a contiguous prefix.
            charLimitReached = true;
            break;
          }
        }
      } else {
        allRows.push(...pageData);
      }

      if (pageData.length < rowsToFetch) break;
      if (maxRows !== null && allRows.length >= maxRows) break;
      if (charLimitReached || (maxChars !== null && allRowsChars >= maxChars)) break;

      offset += pageData.length;
    }

    const rows: SqlSelectDataRow[] = allRows.map((row) => {
      const obj: SqlSelectDataRow = {};
      columns.forEach((colName, i) => {
        obj[colName] = row[i];
      });
      return obj;
    });

    if (columns.length > 0) {
      message = [message, SELECTED_ROWS_MSG(rows.length, totalQueryRows)].filter(Boolean).join(' ');
      return { status: 'ok', data: { columns, rows }, message };
    }
    return { status: 'ok', message };
  }
}

class SnowflakeWorkspace extends Workspace {
  getSqlDialect(): string {
    return 'Snowflake';
  }
  getQuotedName(name: string): string {
    return `"${name}"`;
  }
}

class BigQueryWorkspace extends Workspace {
  // Query Service surfaces BigQuery errors as a serialized object; extract the Message: "..." part.
  private static readonly BQ_ERROR_MESSAGE_RE = /Message:\s*"((?:[^"\\]|\\.)*)"/;

  getSqlDialect(): string {
    return 'BigQuery';
  }
  getQuotedName(name: string): string {
    return `\`${name}\``;
  }
  protected override formatErrorMessage(message: string | null): string | null {
    if (message) {
      const m = BigQueryWorkspace.BQ_ERROR_MESSAGE_RE.exec(message);
      if (m) return m[1]!.replaceAll('\\"', '"');
    }
    return message;
  }
}

// ---------------------------------------------------------------------------
// WorkspaceManager — port of `WorkspaceManager`.
// ---------------------------------------------------------------------------

/** Everything a Workspace needs to talk to Storage + Query Service, resolved from Config. */
type WorkspaceDeps = {
  rawStorage: RawClient;
  /** Effective branch id for SAPI branch-scoped endpoints ('default' on production). */
  storageBranchId: string;
  /** The real branch id (config.branchId), or null on production. */
  branchId: string | null;
  queryServiceUrl: string;
  queryServiceToken: string;
};

export class WorkspaceManager {
  static readonly MCP_META_KEY = 'KBC.McpServer.v2.workspaceId';
  static readonly MCP_WORKSPACE_COMPONENT_ID = 'keboola.mcp-server-tool';

  private workspace: Workspace | null = null;

  private constructor(
    private readonly deps: WorkspaceDeps,
    private readonly workspaceSchema: string | undefined,
  ) {}

  /**
   * Builds a WorkspaceManager for the given config + raw storage client.
   *
   * On projects with the `storage-branches` feature (and a dev branch), the manager
   * is bound to that branch's workspace. On legacy projects / the default branch it
   * falls back to the production-branch workspace shared by the whole project.
   */
  static async create(
    config: Config,
    deps: {
      rawStorage: RawClient;
      makeProdRawStorage: () => RawClient;
      queryServiceUrl: string;
      queryServiceToken: string;
    },
  ): Promise<WorkspaceManager> {
    const hasBranches = await WorkspaceManager.hasStorageBranches(config, deps.rawStorage);
    if (hasBranches) {
      return new WorkspaceManager(
        {
          rawStorage: deps.rawStorage,
          storageBranchId: config.branchId ?? 'default',
          branchId: config.branchId ?? null,
          queryServiceUrl: deps.queryServiceUrl,
          queryServiceToken: deps.queryServiceToken,
        },
        config.workspaceSchema,
      );
    }
    // Fall back to the production-branch client.
    return new WorkspaceManager(
      {
        rawStorage: deps.makeProdRawStorage(),
        storageBranchId: 'default',
        branchId: null,
        queryServiceUrl: deps.queryServiceUrl,
        queryServiceToken: deps.queryServiceToken,
      },
      config.workspaceSchema,
    );
  }

  private static async hasStorageBranches(config: Config, rawStorage: RawClient): Promise<boolean> {
    if (!config.branchId) return false;
    const tokenInfo = await rawStorage.get<Record<string, unknown>>('tokens/verify');
    const owner = (tokenInfo.owner ?? {}) as Record<string, unknown>;
    const features = Array.isArray(owner.features) ? (owner.features as string[]) : [];
    return features.includes(STORAGE_BRANCHES_FEATURE);
  }

  private async findWsBySchema(schema: string): Promise<WspInfo | null> {
    const list = await this.deps.rawStorage.get<Record<string, unknown>[]>(
      `branch/${this.deps.storageBranchId}/workspaces`,
    );
    for (const sapi of list) {
      const wi = fromSapiInfo(sapi);
      if (wi.id && wi.backend && wi.schema && wi.schema === schema) {
        return wi;
      }
    }
    return null;
  }

  private async findWsById(workspaceId: string | number): Promise<WspInfo | null> {
    try {
      const sapi = await this.deps.rawStorage.get<Record<string, unknown>>(
        `branch/${this.deps.storageBranchId}/workspaces/${workspaceId}`,
      );
      const wi = fromSapiInfo(sapi);
      if (wi.id && wi.backend && wi.schema) {
        return wi;
      }
      throw new Error(`Invalid workspace info: ${JSON.stringify(sapi)}`);
    } catch (error) {
      if (error instanceof RawHttpError && error.status === 404) {
        return null;
      }
      throw error;
    }
  }

  private async findWsInBranch(): Promise<WspInfo | null> {
    const metadata = await this.deps.rawStorage.get<Record<string, unknown>[]>(
      `branch/${this.deps.storageBranchId}/metadata`,
    );
    for (const m of metadata) {
      if (m.key === WorkspaceManager.MCP_META_KEY && m.value) {
        const info = await this.findWsById(m.value as string);
        if (info && info.readonly) {
          return info;
        }
      }
    }
    return null;
  }

  private async createWs(timeoutSec = 300.0): Promise<WspInfo | null> {
    const tokenInfo = await this.deps.rawStorage.get<Record<string, unknown>>('tokens/verify');
    const owner = (tokenInfo.owner ?? {}) as Record<string, unknown>;
    const defaultBackend = owner.defaultBackend as string | undefined;

    let loginType: string;
    if (defaultBackend === 'snowflake') {
      loginType = 'snowflake-person-sso';
    } else if (defaultBackend === 'bigquery') {
      loginType = 'default';
    } else {
      throw new Error(`Unexpected default backend: ${defaultBackend}`);
    }

    const componentId = WorkspaceManager.MCP_WORKSPACE_COMPONENT_ID;
    const configName = `mcp-workspace-${Math.random().toString(16).slice(2, 10)}`;
    const configResp = await this.deps.rawStorage.post<Record<string, unknown>>(
      `branch/${this.deps.storageBranchId}/components/${componentId}/configs`,
      {
        body: {
          name: configName,
          description: 'Auto-created by MCP server for workspace billing.',
          configuration: {},
        },
      },
    );
    const configId = String(configResp.id);

    let resp: Record<string, unknown>;
    try {
      resp = await this.deps.rawStorage.post<Record<string, unknown>>(
        `branch/${this.deps.storageBranchId}/components/${componentId}/configs/${configId}/workspaces`,
        {
          params: { async: true },
          body: { readOnlyStorageAccess: true, loginType, backend: defaultBackend },
        },
      );
    } catch (error) {
      try {
        await this.deps.rawStorage.delete(
          `branch/${this.deps.storageBranchId}/components/${componentId}/configs/${configId}`,
        );
      } catch (cleanupErr) {
        logger.warn(
          `Failed to clean up configuration ${componentId}/${configId} ` +
            `after workspace creation failure: ${String(cleanupErr)}`,
        );
      }
      throw error;
    }

    const jobId = resp.id as number;
    const startTs = Date.now();
    logger.info(
      `Requested new workspace: job_id=${jobId}, timeout=${timeoutSec.toFixed(2)} seconds`,
    );

    for (;;) {
      const jobInfo = await this.deps.rawStorage.get<Record<string, unknown>>(`jobs/${jobId}`);
      const jobStatusVal = jobInfo.status as string;
      const duration = (Date.now() - startTs) / 1000;
      logger.info(
        `Job info: job_id=${jobId}, status=${jobStatusVal}, ` +
          `duration=${duration.toFixed(2)} seconds, timeout=${timeoutSec.toFixed(2)} seconds`,
      );

      if (jobStatusVal === 'success') {
        const jobResults = jobInfo.results as Record<string, unknown>;
        const workspaceId = jobResults.id as number;
        logger.info(`Created workspace: ${workspaceId}`);
        return this.findWsById(workspaceId);
      }
      if (duration > timeoutSec) {
        logger.info(`Workspace creation timed out after ${duration.toFixed(2)} seconds.`);
        return null;
      }
      const remaining = Math.max(0.0, timeoutSec - duration);
      await wait(Math.min(5.0, remaining) * 1000);
    }
  }

  private initWorkspace(info: WspInfo): Workspace {
    if (info.backend === 'snowflake') {
      return new SnowflakeWorkspace(info.id, this.deps);
    }
    if (info.backend === 'bigquery') {
      const credentials = JSON.parse(info.credentials || '{}') as Record<string, unknown>;
      const projectId = credentials.project_id as string | undefined;
      if (projectId) {
        return new BigQueryWorkspace(info.id, this.deps);
      }
      throw new Error(`No credentials or no project ID in workspace: ${info.schema}`);
    }
    throw new Error(`Unexpected backend type "${info.backend}" in workspace: ${info.schema}`);
  }

  private async getWorkspace(): Promise<Workspace> {
    if (this.workspace) return this.workspace;

    if (this.workspaceSchema) {
      // Use the explicitly-requested workspace; never written to the default branch metadata.
      logger.info(`Looking up workspace by schema: ${this.workspaceSchema}`);
      const info = await this.findWsBySchema(this.workspaceSchema);
      if (info) {
        logger.info(`Found workspace: ${JSON.stringify(info)}`);
        this.workspace = this.initWorkspace(info);
        return this.workspace;
      }
      throw new Error(
        `No Keboola workspace found or the workspace has no read-only storage access: ` +
          `workspace_schema=${this.workspaceSchema}`,
      );
    }

    logger.info('Looking up workspace in the default branch.');
    const existing = await this.findWsInBranch();
    if (existing) {
      logger.info(`Found workspace: ${JSON.stringify(existing)}`);
      this.workspace = this.initWorkspace(existing);
      return this.workspace;
    }

    logger.info('Creating workspace in the default branch.');
    const created = await this.createWs();
    if (created) {
      // All tokens share the same read-only workspace; last-write-wins is acceptable.
      await this.deps.rawStorage.post(`branch/${this.deps.storageBranchId}/metadata`, {
        body: {
          metadata: [{ key: WorkspaceManager.MCP_META_KEY, value: created.id }],
        },
      });
      this.workspace = this.initWorkspace(created);
      return this.workspace;
    }
    throw new Error('Failed to initialize Keboola Workspace.');
  }

  async executeQuery(
    sqlQuery: string,
    opts: {
      maxRows?: number | null;
      maxChars?: number | null;
      onJobSubmitted?: JobSubmittedCallback | null;
    } = {},
  ): Promise<QueryResult> {
    const workspace = await this.getWorkspace();
    return workspace.executeQuery(sqlQuery, opts);
  }

  async getQuotedName(name: string): Promise<string> {
    const workspace = await this.getWorkspace();
    return workspace.getQuotedName(name);
  }

  async getSqlDialect(): Promise<string> {
    const workspace = await this.getWorkspace();
    return workspace.getSqlDialect();
  }

  async getWorkspaceId(): Promise<number> {
    const workspace = await this.getWorkspace();
    return workspace.id;
  }

  async getBranchId(): Promise<string> {
    const workspace = await this.getWorkspace();
    return workspace.getBranchId();
  }
}

export { isOk };
