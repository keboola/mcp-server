import type { KeboolaClients } from '@/clients/keboola';
import { createRawClient, RawHttpError } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { DATA_APP_COMPONENT_ID, MetadataField } from '@/constants';
import type { ProjectLinksManager } from '@/links';
import { logger } from '@/logger';
import {
  type AppGitRepoResponse,
  type AppRunInfo,
  type AppRunResponse,
  type CreatedGitCredentialResponse,
  type DataApp,
  type DataAppResponse,
  type DataAppSummary,
  parseAppGitRepoResponse,
  parseAppRunResponse,
  parseCredentialResponse,
  parseDataAppResponse,
} from './model';
import {
  APP_RUN_LOG_LINES,
  APP_RUN_MESSAGE_LIMIT,
  asRecord,
  dataAppFromApiResponses,
  isDraftConfig,
  type MetadataItem,
  summaryFromDataApp,
  usesBasicAuthentication,
} from './utils';

// ---------------------------------------------------------------------------
// Data Science access.
//
// Apps CRUD (list/get/create/patch/delete) and runs are migrated to the typed
// `clients.dataScience` client. Three endpoints stay on a raw client rooted at the
// data-science URL because the typed `createDataScienceClient` exposes no method for
// them (verified against node_modules/@keboola/api-client/dist/dataScience/index.d.ts):
//   - GET    apps/{id}/git-repo               (managed-repo URL read)
//   - POST   apps/{id}/git-repo/credentials   (mint one-time http_token)
//   - GET    apps/{id}/logs/tail              (typed getAppLogsTail re-parses the
//                                              text/plain body into structured
//                                              LogEntry[] + drops empty lines, which
//                                              would change tool output; we need the
//                                              raw lines verbatim)
// ---------------------------------------------------------------------------

export type DataScience = {
  getDataApp: (id: string) => Promise<DataAppResponse>;
  listDataApps: (limit: number, offset: number) => Promise<DataAppResponse[]>;
  createDataApp: (params: {
    name: string;
    description: string;
    config: Record<string, unknown>;
    branchId: string | null;
    appType: string;
    useManagedGitRepo: boolean;
  }) => Promise<DataAppResponse>;
  deployDataApp: (
    id: string,
    configVersion: string | null,
    mode: string | null,
  ) => Promise<DataAppResponse>;
  suspendDataApp: (id: string) => Promise<DataAppResponse>;
  deleteDataApp: (id: string) => Promise<void>;
  // kept-raw endpoints (no typed method)
  createAppGitCredential: (id: string) => Promise<CreatedGitCredentialResponse>;
  getAppGitRepo: (id: string) => Promise<AppGitRepoResponse>;
  listAppRuns: (id: string, limit: number) => Promise<AppRunResponse[]>;
  tailAppLogs: (id: string, lines: number) => Promise<string>;
};

export const createDataScience = (clients: KeboolaClients, config: Config): DataScience => {
  const ds = clients.dataScience;
  // Raw client for the three uncovered data-science endpoints (git-repo read,
  // credential create, raw logs tail). Same baseUrl/token as the typed client.
  const urls = deriveServiceUrls(config.storageApiUrl ?? '');
  const token = config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken;
  const raw = createRawClient({ baseUrl: urls.dataScience, token });

  return {
    getDataApp: async (id) =>
      parseDataAppResponse((await ds.getApp(id)) as unknown as Record<string, unknown>),
    listDataApps: async (limit, offset) => {
      const resp = await ds.getApps({ limit, offset });
      return resp.map((r) => parseDataAppResponse(r as unknown as Record<string, unknown>));
    },
    createDataApp: async ({
      name,
      description,
      config: cfg,
      branchId,
      appType,
      useManagedGitRepo,
    }) => {
      const body: Record<string, unknown> = {
        branchId,
        name,
        type: appType,
        description,
        config: cfg,
      };
      if (useManagedGitRepo) body.useManagedGitRepo = true;
      const resp = await ds.createApp(body as Parameters<typeof ds.createApp>[0]);
      return parseDataAppResponse(resp as unknown as Record<string, unknown>);
    },
    deployDataApp: async (id, configVersion, mode) => {
      const body: Record<string, unknown> = {
        desiredState: 'running',
        restartIfRunning: true,
        updateDependencies: false,
      };
      if (configVersion !== null) body.configVersion = configVersion;
      if (mode !== null) body.mode = mode;
      const resp = await ds.patchApp(id, body as Parameters<typeof ds.patchApp>[1]);
      return parseDataAppResponse(resp as unknown as Record<string, unknown>);
    },
    suspendDataApp: async (id) => {
      const resp = await ds.patchApp(id, { desiredState: 'stopped' });
      return parseDataAppResponse(resp as unknown as Record<string, unknown>);
    },
    deleteDataApp: async (id) => {
      await ds.deleteApp(id);
    },
    // KEEP RAW: no typed method for POST apps/{id}/git-repo/credentials.
    createAppGitCredential: async (id) =>
      parseCredentialResponse(
        await raw.post<Record<string, unknown>>(`apps/${id}/git-repo/credentials`, {
          body: { type: 'http_token', permissions: 'readWrite' },
        }),
      ),
    // KEEP RAW: no typed method for GET apps/{id}/git-repo.
    getAppGitRepo: async (id) =>
      parseAppGitRepoResponse(await raw.get<Record<string, unknown>>(`apps/${id}/git-repo`)),
    listAppRuns: async (id, limit) => {
      const resp = await ds.getAppRuns(id, { limit, offset: 0 });
      return resp.map((r) => parseAppRunResponse(r as unknown as Record<string, unknown>));
    },
    // KEEP RAW: the typed getAppLogsTail parses the text/plain body into structured
    // LogEntry[] (and drops empty lines); we need the raw text split into lines verbatim.
    tailAppLogs: async (id, lines) =>
      raw.getText(`apps/${id}/logs/tail`, { params: { lines: Math.max(lines, 1) } }),
  };
};

// ---------------------------------------------------------------------------
// Storage helpers (KEEP RAW: storage-config writes/metadata via rawStorage, matching
// the exact Python SAPI calls; the typed storage client diverges from these shapes).
// ---------------------------------------------------------------------------
export const storageHelpers = (clients: KeboolaClients) => {
  const branch = clients.branchId;
  const cfgBase = (configurationId: string): string =>
    `branch/${branch}/components/${DATA_APP_COMPONENT_ID}/configs/${configurationId}`;

  return {
    configurationDetail: (configurationId: string) =>
      clients.rawStorage.get<Record<string, unknown>>(cfgBase(configurationId)),
    configurationList: () =>
      clients.rawStorage.get<Record<string, unknown>[]>(
        `branch/${branch}/components/${DATA_APP_COMPONENT_ID}/configs`,
      ),
    configurationUpdate: (params: {
      configurationId: string;
      configuration: Record<string, unknown>;
      changeDescription: string;
      updatedName?: string;
      updatedDescription?: string | null;
    }) => {
      const body: Record<string, unknown> = {
        configuration: params.configuration,
        changeDescription: params.changeDescription,
      };
      if (params.updatedName) body.name = params.updatedName;
      if (params.updatedDescription) body.description = params.updatedDescription;
      return clients.rawStorage.put<Record<string, unknown>>(cfgBase(params.configurationId), {
        body,
      });
    },
    configurationMetadataGet: (configurationId: string) =>
      clients.rawStorage.get<MetadataItem[]>(`${cfgBase(configurationId)}/metadata`),
    configurationMetadataUpdate: (configurationId: string, metadata: Record<string, string>) =>
      clients.rawStorage.post<MetadataItem[]>(`${cfgBase(configurationId)}/metadata`, {
        body: {
          metadata: Object.entries(metadata).map(([key, value]) => ({ key, value })),
        },
      }),
    configurationMetadataDelete: (configurationId: string, metadataId: string) =>
      clients.rawStorage.delete(`${cfgBase(configurationId)}/metadata/${metadataId}`),
    configurationVersionLatest: async (configurationId: string): Promise<number> => {
      const versions = await clients.rawStorage.get<{ version?: number }[]>(
        `${cfgBase(configurationId)}/versions`,
      );
      let latest = 0;
      for (const v of versions) {
        if (typeof v.version === 'number' && v.version > latest) latest = v.version;
      }
      return latest;
    },
    branchesList: () => clients.rawStorage.get<Record<string, unknown>[]>('dev-branches'),
    workspaceList: () =>
      clients.rawStorage.get<Record<string, unknown>[]>(`branch/${branch}/workspaces`),
  };
};

export type StorageHelpers = ReturnType<typeof storageHelpers>;

// ---------------------------------------------------------------------------
// Project feature check (port of KeboolaClient.has_feature): tokens/verify.owner.features
// ---------------------------------------------------------------------------
export const hasFeature = async (clients: KeboolaClients, feature: string): Promise<boolean> => {
  const token = (await clients.storage.tokens.verify()) as { owner?: { features?: string[] } };
  return (token.owner?.features ?? []).includes(feature);
};

// --- minimal workspace resolution (Streamlit path only) ---------------------
// Resolves workspace_id / sql_dialect / branch_id from the configured workspaceSchema,
// matching the standard MCP path in workspace.py (_find_ws_by_schema + backend dialect).
export const resolveWorkspace = async (
  config: Config,
  helpers: StorageHelpers,
): Promise<{ workspaceId: string; sqlDialect: string; branchId: string }> => {
  if (!config.workspaceSchema) {
    throw new Error(
      'No Keboola workspace schema configured; required to create or update a Streamlit data app.',
    );
  }
  const workspaces = await helpers.workspaceList();
  const match = workspaces.find((ws) => {
    const connection = (ws.connection as { schema?: string; backend?: string } | undefined) ?? {};
    return connection.schema === config.workspaceSchema;
  });
  if (!match) {
    throw new Error(
      `No Keboola workspace found or the workspace has no read-only storage access: ` +
        `workspace_schema=${config.workspaceSchema}`,
    );
  }
  const connection = (match.connection as { backend?: string }) ?? {};
  const backend = String(connection.backend ?? '');
  const workspaceId = String(match.id ?? '');

  let branchId = config.branchId ?? '';
  if (!branchId) {
    const branches = await helpers.branchesList();
    const defaultBranch = branches.find((b) => b.isDefault === true);
    if (!defaultBranch?.id) throw new Error('Cannot determine the default branch ID');
    branchId = String(defaultBranch.id);
  }
  return { workspaceId, sqlDialect: backend, branchId };
};

// --- data app fetch / build (ports of _fetch_data_app etc.) -----------------

const buildDataAppWithRepo = async (
  ds: DataScience,
  dataAppScience: DataAppResponse,
  rawConfig: Record<string, unknown>,
): Promise<DataApp> => {
  const dataApp = dataAppFromApiResponses(
    dataAppScience,
    rawConfig,
    MetadataField.CONFIGURATION_FOLDER_NAME,
  );
  if (dataAppScience.type === 'python-js') {
    try {
      const repo = await ds.getAppGitRepo(dataAppScience.id);
      dataApp.repo_url = repo.https_url;
    } catch (error) {
      logger.warn(`Could not fetch git repo URL for python-js app ${dataAppScience.id}: ${error}`);
    }
  }
  return dataApp;
};

export const fetchDataApp = async (
  ds: DataScience,
  helpers: StorageHelpers,
  configurationId: string,
): Promise<DataApp> => {
  const rawConfig = await helpers.configurationDetail(configurationId);
  const config = asRecord(rawConfig.configuration);
  const dataAppId = String(asRecord(config.parameters).id ?? '');
  const dataAppScience = await ds.getDataApp(dataAppId);
  if (dataAppScience.component_id !== DATA_APP_COMPONENT_ID) {
    throw new Error(
      `Data app tools only support ${DATA_APP_COMPONENT_ID} component, but the data app ` +
        `"${dataAppId}" has component_id "${dataAppScience.component_id}".`,
    );
  }
  return buildDataAppWithRepo(ds, dataAppScience, rawConfig);
};

const appRunInfoFromResponse = (run: AppRunResponse): AppRunInfo => {
  const lines = (run.startup_logs ?? '').trim().split('\n');
  const startupLogs = lines.slice(Math.max(0, lines.length - APP_RUN_LOG_LINES)).filter(Boolean);
  let failureMessage = run.failure_reason?.message ?? null;
  if (failureMessage && failureMessage.length > APP_RUN_MESSAGE_LIMIT) {
    failureMessage = '…' + failureMessage.slice(-(APP_RUN_MESSAGE_LIMIT - 1));
  }
  return {
    state: run.state,
    created_at: run.created_at,
    stopped_at: run.stopped_at,
    failure_reason: run.failure_reason?.reason ?? null,
    failure_message: failureMessage,
    startup_logs: startupLogs,
  };
};

export const fetchLogs = async (ds: DataScience, dataAppId: string): Promise<string[]> => {
  try {
    const text = await ds.tailAppLogs(dataAppId, 20);
    return text.split('\n');
  } catch (error) {
    if (error instanceof RawHttpError) return [];
    throw error;
  }
};

export const fetchLatestRun = async (
  ds: DataScience,
  dataAppId: string,
): Promise<AppRunInfo | null> => {
  try {
    const runs = await ds.listAppRuns(dataAppId, 1);
    if (runs.length === 0) return null;
    return appRunInfoFromResponse(runs[0]!);
  } catch (error) {
    logger.error({ err: error }, `Failed to fetch app runs for data app: ${dataAppId}`);
    return null;
  }
};

export const withDeploymentInfo = (
  dataApp: DataApp,
  logs: string[],
  lastRun: AppRunInfo | null,
): DataApp => {
  dataApp.deployment_info = {
    version: dataApp.config_version,
    state: dataApp.state,
    url: dataApp.deployment_url ?? 'deployment link not available yet',
    last_request_timestamp: null,
    last_start_timestamp: null,
    logs,
    last_run: lastRun,
  };
  return dataApp;
};

const fetchProdDrafts = async (
  ds: DataScience,
  helpers: StorageHelpers,
  prodConfigurationId: string,
): Promise<{ drafts: DataAppSummary[]; unavailable: number }> => {
  const configs = await helpers.configurationList();
  const draftCfgIds: string[] = [];
  for (const cfg of configs) {
    const body = asRecord(cfg.configuration);
    if (!isDraftConfig(body)) continue;
    const dataAppBlock = asRecord(asRecord(body.parameters).dataApp);
    if (dataAppBlock.parentConfigurationId === prodConfigurationId) {
      if (typeof cfg.id === 'string') draftCfgIds.push(cfg.id);
    }
  }
  if (draftCfgIds.length === 0) return { drafts: [], unavailable: 0 };

  const results = await Promise.all(
    draftCfgIds.map(async (cfgId): Promise<DataAppSummary | null> => {
      try {
        const draft = await fetchDataApp(ds, helpers, cfgId);
        const summary = summaryFromDataApp(draft);
        summary.repo_url = draft.repo_url;
        return summary;
      } catch (error) {
        logger.error(
          { err: error },
          `Failed to fetch draft data app by configuration ID: ${cfgId}`,
        );
        return null;
      }
    }),
  );
  const drafts = results.filter((s): s is DataAppSummary => s !== null);
  return { drafts, unavailable: draftCfgIds.length - drafts.length };
};

export const fetchDataAppDetailsTask = async (
  ds: DataScience,
  helpers: StorageHelpers,
  linksManager: ProjectLinksManager,
  configurationId: string,
): Promise<DataApp | string> => {
  try {
    let dataApp = await fetchDataApp(ds, helpers, configurationId);
    dataApp.links = linksManager.getDataAppLinks(
      dataApp.configuration_id,
      dataApp.name,
      dataApp.deployment_url ?? undefined,
      usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
    );
    const logs = await fetchLogs(ds, dataApp.data_app_id);
    const lastRun = await fetchLatestRun(ds, dataApp.data_app_id);
    dataApp = withDeploymentInfo(dataApp, logs, lastRun);
    if (dataApp.type === 'python-js' && !isDraftConfig(dataApp.configuration)) {
      const { drafts, unavailable } = await fetchProdDrafts(ds, helpers, dataApp.configuration_id);
      dataApp.drafts = drafts;
      dataApp.drafts_unavailable = unavailable;
    }
    return dataApp;
  } catch (error) {
    logger.error(
      { err: error },
      `Failed to fetch data app by configuration ID: ${configurationId}`,
    );
    return configurationId;
  }
};
