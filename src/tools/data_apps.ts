import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { readFileSync } from 'node:fs';
import { z } from 'zod';

import { createKeboolaClients, createLinksManager, type KeboolaClients } from '@/clients/keboola';
import { createRawClient, type RawClient, RawHttpError } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { DATA_APP_COMPONENT_ID, MetadataField } from '@/constants';
import type { Link, ProjectLinksManager } from '@/links';
import { logger } from '@/logger';
import { registerTool } from '@/mcp/tool';
import { resourcePath } from '@/resource-path';
import { toonSerializeCompact } from '@/serialize';
import {
  actionSchema,
  type AppGitRepoResponse,
  type AppRunInfo,
  type AppRunResponse,
  type AuthenticationType,
  authenticationTypeSchema,
  type CreatedGitCredentialResponse,
  type DataApp,
  type DataAppResponse,
  type DataAppSummary,
  modeSchema,
  parseAppGitRepoResponse,
  parseAppRunResponse,
  parseCredentialResponse,
  parseDataAppResponse,
} from './data_apps.model';

// Ported from tools/data_apps.py. Data App tools are blocked outside the main branch
// centrally via tool filtering; this module registers them normally.

// MCP-only metadata keys (port of config.py MetadataField; not yet in constants.ts).
const CREATED_BY_MCP = 'KBC.MCP.createdBy';
const UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.';

// --- resource code templates -------------------------------------------------
// Copied from src/keboola_mcp_server/resources/data_app/* into src/resources/data_app/*.
// Loaded once at module init via fs, resolving relative to this module's URL so it works
// when running from source (vitest / tsx). Mirrors the Python `importlib.resources` read.
const readResource = (name: string): string =>
  readFileSync(resourcePath('data_app', name), { encoding: 'utf-8' });

const QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE = readResource('qsapi_query_data_code.py');
const STORAGE_QUERY_DATA_FUNCTION_CODE = readResource('sapi_query_data_code.py');

const DEFAULT_STREAMLIT_THEME =
  '[theme]\nfont = "sans serif"\ntextColor = "#222529"\nbackgroundColor = "#FFFFFF"\n' +
  'secondaryBackgroundColor = "#E6F2FF"\nprimaryColor = "#1F8FFF"';
const DEFAULT_PACKAGES = ['pandas', 'httpx'];

const MANAGED_GIT_REPO_USERNAME = 'kai';
const DEFAULT_DRAFT_BRANCH = 'init';

const APP_RUN_LOG_LINES = 30;
const APP_RUN_MESSAGE_LIMIT = 3000;

const INJECTED_BLOCK_RE =
  /(?<before>[\s\S]*?)#\s###\sINJECTED_CODE\s####[\s\S]*?#\s###\sEND_OF_INJECTED_CODE\s####(?<after>[\s\S]*)/;

const SECRET_WORKSPACE_ID = 'WORKSPACE_ID';
const SECRET_BRANCH_ID = 'BRANCH_ID';

const DATA_APPS_STORAGE_WORKSPACE_FEATURE = 'data-apps-storage-workspace';

const MAX_DNS_LABEL_LENGTH = 63;

// ---------------------------------------------------------------------------
// data-science raw client. The published @keboola/api-client dataScience subpath
// does not expose the managed-git-repo, credential, or runs/logs-tail endpoints the
// Python DataScienceClient uses, so a raw client (rooted at the data-science URL)
// reproduces the exact calls 1:1.
// ---------------------------------------------------------------------------
type DataScienceClient = {
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
  createAppGitCredential: (id: string) => Promise<CreatedGitCredentialResponse>;
  getAppGitRepo: (id: string) => Promise<AppGitRepoResponse>;
  listAppRuns: (id: string, limit: number) => Promise<AppRunResponse[]>;
  tailAppLogs: (id: string, lines: number) => Promise<string>;
};

const createDataScienceClient = (raw: RawClient): DataScienceClient => {
  return {
    getDataApp: async (id) =>
      parseDataAppResponse(await raw.get<Record<string, unknown>>(`apps/${id}`)),
    listDataApps: async (limit, offset) => {
      const resp = await raw.get<Record<string, unknown>[]>('apps', {
        params: { limit, offset },
      });
      return resp.map(parseDataAppResponse);
    },
    createDataApp: async ({
      name,
      description,
      config: cfg,
      branchId: bid,
      appType,
      useManagedGitRepo,
    }) => {
      const body: Record<string, unknown> = {
        branchId: bid,
        name,
        type: appType,
        description,
        config: cfg,
      };
      if (useManagedGitRepo) body.useManagedGitRepo = true;
      return parseDataAppResponse(await raw.post<Record<string, unknown>>('apps', { body }));
    },
    deployDataApp: async (id, configVersion, mode) => {
      const body: Record<string, unknown> = {
        desiredState: 'running',
        restartIfRunning: true,
        updateDependencies: false,
      };
      if (configVersion !== null) body.configVersion = configVersion;
      if (mode !== null) body.mode = mode;
      return parseDataAppResponse(await raw.patch<Record<string, unknown>>(`apps/${id}`, { body }));
    },
    suspendDataApp: async (id) =>
      parseDataAppResponse(
        await raw.patch<Record<string, unknown>>(`apps/${id}`, {
          body: { desiredState: 'stopped' },
        }),
      ),
    deleteDataApp: async (id) => {
      // DSAPI returns 204 with an empty body; the shared raw client always parses JSON,
      // so tolerate the resulting "empty JSON" parse error after a successful call.
      try {
        await raw.delete(`apps/${id}`);
      } catch (error) {
        if (error instanceof RawHttpError) throw error;
        if (error instanceof SyntaxError) return; // empty body on 2xx
        throw error;
      }
    },
    createAppGitCredential: async (id) =>
      parseCredentialResponse(
        await raw.post<Record<string, unknown>>(`apps/${id}/git-repo/credentials`, {
          body: { type: 'http_token', permissions: 'readWrite' },
        }),
      ),
    getAppGitRepo: async (id) =>
      parseAppGitRepoResponse(await raw.get<Record<string, unknown>>(`apps/${id}/git-repo`)),
    listAppRuns: async (id, limit) => {
      const resp = await raw.get<Record<string, unknown>[]>(`apps/${id}/runs`, {
        params: { limit, offset: 0 },
      });
      return resp.map(parseAppRunResponse);
    },
    tailAppLogs: async (id, lines) =>
      raw.getText(`apps/${id}/logs/tail`, { params: { lines: Math.max(lines, 1) } }),
  };
};

// ---------------------------------------------------------------------------
// Storage helpers (raw, rooted at <storage>/v2/storage), matching Python endpoints.
// ---------------------------------------------------------------------------
type MetadataItem = { id?: string; key?: string; value?: string };

const storageHelpers = (clients: KeboolaClients) => {
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

// ---------------------------------------------------------------------------
// Project feature check (port of KeboolaClient.has_feature): tokens/verify.owner.features
// ---------------------------------------------------------------------------
const hasFeature = async (clients: KeboolaClients, feature: string): Promise<boolean> => {
  const token = (await clients.storage.tokens.verify()) as { owner?: { features?: string[] } };
  return (token.owner?.features ?? []).includes(feature);
};

// ---------------------------------------------------------------------------
// Encryption client (port of clients/encryption.py): POST encrypt with id params.
// ---------------------------------------------------------------------------
const encryptConfig = async (
  config: Config,
  body: Record<string, unknown>,
  params: { projectId: string; componentId: string },
): Promise<Record<string, unknown>> => {
  const urls = deriveServiceUrls(config.storageApiUrl ?? '');
  const token = config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken;
  const enc = createRawClient({ baseUrl: urls.encryption, token });
  return enc.post<Record<string, unknown>>('encrypt', {
    body,
    params: { componentId: params.componentId, projectId: params.projectId },
  });
};

// --- minimal workspace resolution (Streamlit path only) ---------------------
// Resolves workspace_id / sql_dialect / branch_id from the configured workspaceSchema,
// matching the standard MCP path in workspace.py (_find_ws_by_schema + backend dialect).
const resolveWorkspace = async (
  config: Config,
  helpers: ReturnType<typeof storageHelpers>,
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

// --- pure helpers (ports of the module-level functions in data_apps.py) -----

const getAuthorization = (authWithPassword: boolean): Record<string, unknown> => {
  if (authWithPassword) {
    return {
      app_proxy: {
        auth_providers: [{ id: 'simpleAuth', type: 'password' }],
        auth_rules: [{ type: 'pathPrefix', value: '/', auth_required: true, auth: ['simpleAuth'] }],
      },
    };
  }
  return {
    app_proxy: {
      auth_providers: [],
      auth_rules: [{ type: 'pathPrefix', value: '/', auth_required: false }],
    },
  };
};

const usesBasicAuthentication = (authorization: Record<string, unknown>): boolean => {
  try {
    const rules = ((authorization.app_proxy as { auth_rules?: Record<string, unknown>[] })
      .auth_rules ?? []) as Record<string, unknown>[];
    return rules.some(
      (rule) =>
        rule.auth_required === true &&
        Array.isArray(rule.auth) &&
        (rule.auth as unknown[]).includes('simpleAuth'),
    );
  } catch {
    return false;
  }
};

class DataAppSlugTooLongError extends Error {}

const getDataAppSlug = (name: string): string => {
  const slug = name
    .trim()
    .toLowerCase()
    .replaceAll(' ', '-')
    .replace(/[^a-z0-9-]/g, '');
  if (slug.length > MAX_DNS_LABEL_LENGTH) {
    throw new DataAppSlugTooLongError(
      `Data app name "${name}" generates a URL slug that is ${slug.length} characters long, ` +
        `which exceeds the maximum DNS label length of ${MAX_DNS_LABEL_LENGTH} characters. ` +
        `Please use a shorter name (the slug "${slug.slice(0, 20)}..." is too long). ` +
        `The name should generate a slug of at most ${MAX_DNS_LABEL_LENGTH} characters after ` +
        `converting to lowercase, replacing spaces with hyphens, and removing special characters.`,
    );
  }
  return slug;
};

const getQueryFunctionCode = (sqlDialect: string): string => {
  const dialect = sqlDialect.toLowerCase();
  if (dialect === 'snowflake') return QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE;
  if (dialect === 'bigquery') return STORAGE_QUERY_DATA_FUNCTION_CODE;
  throw new Error(`Unsupported SQL dialect: ${sqlDialect}`);
};

const stripInjectedQueryCode = (sourceCode: string): string => {
  let out = sourceCode;
  for (const snippet of [
    QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE,
    STORAGE_QUERY_DATA_FUNCTION_CODE,
  ]) {
    out = out.split(snippet).join('');
  }
  return out;
};

const injectQueryToSourceCode = (sourceCode: string, sqlDialect: string): string => {
  if (!sourceCode) return '';
  const queryFunctionCode = getQueryFunctionCode(sqlDialect);
  if (sourceCode.includes(queryFunctionCode)) return sourceCode;

  let stripped = stripInjectedQueryCode(sourceCode);
  if (stripped.includes('{QUERY_DATA_FUNCTION}')) {
    return stripped.replaceAll('{QUERY_DATA_FUNCTION}', queryFunctionCode);
  }
  const match = INJECTED_BLOCK_RE.exec(stripped);
  if (match?.groups) {
    const before = (match.groups.before ?? '').replace(/\s+$/, '');
    const after = (match.groups.after ?? '').replace(/^\s+/, '');
    return `${before}\n\n${queryFunctionCode}\n\n${after}`;
  }
  stripped = stripped.replace(/^\s+/, '');
  return `${queryFunctionCode}\n\n${stripped}`;
};

const getSecrets = (workspaceId: string, branchId: string): Record<string, unknown> => ({
  [SECRET_WORKSPACE_ID]: workspaceId,
  [SECRET_BRANCH_ID]: branchId,
});

const sortedUnique = (items: string[]): string[] =>
  Array.from(new Set(items)).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

const buildDataAppConfig = (
  name: string,
  sourceCode: string,
  packages: string[],
  authenticationType: AuthenticationType,
  secrets: Record<string, unknown>,
  sqlDialect: string,
): Record<string, unknown> => {
  const allPackages = sortedUnique([...packages, ...DEFAULT_PACKAGES]);
  const slug = getDataAppSlug(name) || 'Data-App';
  const parameters: Record<string, unknown> = {
    size: 'tiny',
    autoSuspendAfterSeconds: 900,
    dataApp: {
      slug,
      streamlit: { 'config.toml': DEFAULT_STREAMLIT_THEME },
      secrets,
    },
    script: [injectQueryToSourceCode(sourceCode, sqlDialect)],
    packages: allPackages,
  };
  const authorization = getAuthorization(
    authenticationType === 'basic-auth' || authenticationType === 'default',
  );
  return { parameters, authorization };
};

const deepClone = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

const asRecord = (value: unknown): Record<string, unknown> =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

const updateExistingDataAppConfig = (
  existingConfig: Record<string, unknown>,
  name: string,
  sourceCode: string,
  packages: string[],
  authenticationType: AuthenticationType,
  secrets: Record<string, unknown>,
  sqlDialect: string,
): Record<string, unknown> => {
  const newConfig = deepClone(existingConfig);
  const params = asRecord(newConfig.parameters);
  newConfig.parameters = params;
  const dataApp = asRecord(params.dataApp);
  params.dataApp = dataApp;

  dataApp.slug = getDataAppSlug(name) || dataApp.slug;
  if (sourceCode) params.script = [injectQueryToSourceCode(sourceCode, sqlDialect)];
  params.packages =
    packages.length > 0
      ? sortedUnique([...packages, ...DEFAULT_PACKAGES])
      : sortedUnique([...((params.packages as string[]) ?? []), ...DEFAULT_PACKAGES]);

  const updatedSecrets = { ...asRecord(dataApp.secrets) };
  for (const [key, value] of Object.entries(secrets)) {
    if (!(key in updatedSecrets)) updatedSecrets[key] = value;
  }
  dataApp.secrets = updatedSecrets;

  if (authenticationType !== 'default') {
    newConfig.authorization = getAuthorization(authenticationType === 'basic-auth');
  }
  normalizeConfigStorage(newConfig);
  return newConfig;
};

const pruneEmptyStorageObjects = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(pruneEmptyStorageObjects);
  if (value && typeof value === 'object') {
    const pruned: Record<string, unknown> = {};
    for (const [key, sub] of Object.entries(value)) {
      const prunedSub = pruneEmptyStorageObjects(sub);
      if (prunedSub && typeof prunedSub === 'object' && !Array.isArray(prunedSub)) {
        if (Object.keys(prunedSub).length === 0) continue;
      }
      pruned[key] = prunedSub;
    }
    return pruned;
  }
  return value;
};

const normalizeConfigStorage = (config: Record<string, unknown>): void => {
  if (!('storage' in config)) return;
  const storage = config.storage;
  const pruned =
    storage && typeof storage === 'object' && !Array.isArray(storage)
      ? (pruneEmptyStorageObjects(storage) as Record<string, unknown>)
      : null;
  if (pruned && Object.keys(pruned).length > 0) {
    config.storage = pruned;
  } else {
    delete config.storage;
  }
};

const validateDataAppStorage = (
  storage: Record<string, unknown> | null | undefined,
): Record<string, unknown> | null => {
  if (storage == null) return null;
  // Accept both raw `storage` dict and pre-wrapped {'storage': storage}.
  const storageCfg = Object.keys(storage).length > 0 ? asRecord(storage.storage ?? storage) : {};
  // NOTE: the JSON-schema validation (validate_storage_configuration_against_schema)
  // is owned by a not-yet-ported validation module; structural unwrap + prune are
  // preserved here for parity. See REPORT gap note.
  return pruneEmptyStorageObjects(storageCfg) as Record<string, unknown>;
};

const updateExistingCodeDataAppConfig = (
  existingConfig: Record<string, unknown>,
  autoSuspendAfterSeconds: number,
  authenticationType: AuthenticationType,
  secrets: Record<string, unknown> | null,
  storage: Record<string, unknown> | null,
): Record<string, unknown> => {
  const newConfig = deepClone(existingConfig);
  const params = asRecord(newConfig.parameters);
  newConfig.parameters = params;
  params.autoSuspendAfterSeconds = autoSuspendAfterSeconds;
  if (authenticationType !== 'default') {
    newConfig.authorization = getAuthorization(authenticationType === 'basic-auth');
  }
  if (secrets && Object.keys(secrets).length > 0) {
    const dataApp = asRecord(params.dataApp);
    params.dataApp = dataApp;
    const updatedSecrets = { ...asRecord(dataApp.secrets) };
    for (const [key, value] of Object.entries(secrets)) {
      if (!(key in updatedSecrets)) updatedSecrets[key] = value;
    }
    dataApp.secrets = updatedSecrets;
  }
  if (storage !== null) newConfig.storage = storage;
  normalizeConfigStorage(newConfig);
  return newConfig;
};

const isDraftConfig = (configuration: Record<string, unknown>): boolean => {
  const parameters = configuration.parameters;
  if (!parameters || typeof parameters !== 'object' || Array.isArray(parameters)) return false;
  const dataApp = (parameters as Record<string, unknown>).dataApp;
  if (!dataApp || typeof dataApp !== 'object' || Array.isArray(dataApp)) return false;
  return (dataApp as Record<string, unknown>).isDraft === true;
};

const buildAuthenticatedCloneUrl = (httpsUrl: string, secret: string): string => {
  let parts: URL;
  try {
    parts = new URL(httpsUrl);
  } catch {
    throw new Error(`Could not parse HTTPS clone URL: '${httpsUrl}'`);
  }
  if (!parts.protocol || !parts.host) {
    throw new Error(`Could not parse HTTPS clone URL: '${httpsUrl}'`);
  }
  const host = parts.host; // includes port if present
  const scheme = parts.protocol.replace(':', '');
  const netloc = `${MANAGED_GIT_REPO_USERNAME}:${encodeURIComponent(secret)}@${host}`;
  return `${scheme}://${netloc}${parts.pathname}${parts.search}${parts.hash}`;
};

// --- metadata helpers (ports of components/utils.py) ------------------------

const setCfgCreationMetadata = async (
  helpers: ReturnType<typeof storageHelpers>,
  configurationId: string,
): Promise<void> => {
  try {
    await helpers.configurationMetadataUpdate(configurationId, { [CREATED_BY_MCP]: 'true' });
  } catch (error) {
    logger.error(
      { err: error },
      `Failed to set "${CREATED_BY_MCP}" metadata for ${configurationId}`,
    );
  }
};

const setCfgUpdateMetadata = async (
  helpers: ReturnType<typeof storageHelpers>,
  configurationId: string,
  configurationVersion: number,
): Promise<void> => {
  const key = `${UPDATED_BY_MCP_PREFIX}${configurationVersion}`;
  try {
    await helpers.configurationMetadataUpdate(configurationId, { [key]: 'true' });
  } catch (error) {
    logger.error({ err: error }, `Failed to set "${key}" metadata for ${configurationId}`);
  }
};

const folderFieldDescription = (singular: string, plural: string): string =>
  `Folder name to organize this ${singular} in the Keboola UI. ` +
  `Pass an empty string to remove an existing folder assignment. ` +
  `Existing folder names are returned in the response change_summary when no folder is provided ` +
  `and there are 20 or more ${plural} in the project. ` +
  `If there are 20 or more ${plural}, you should assign one of the existing folders or ` +
  `create a new one that clearly reflects the ${singular} purpose.`;

const buildFolderHint = (
  total: number,
  existingFolders: string[],
  configLabel: string,
  updateTool: string,
  lowerBound: boolean,
): string | null => {
  if (total < 20) return null;
  const countStr = lowerBound ? `at least ${total}` : String(total);
  let hint = `Note: This project already has ${countStr} ${configLabel}. Consider organizing them with folders. `;
  if (existingFolders.length > 0) {
    hint +=
      `Existing folders: ${existingFolders.join(', ')}. ` +
      `Call ${updateTool} with a folder= parameter to assign this to one.`;
  } else {
    hint += `No folders have been created yet. Call ${updateTool} with a folder= parameter to start organizing.`;
  }
  return hint;
};

const getConfigFolders = async (
  helpers: ReturnType<typeof storageHelpers>,
): Promise<{ total: number; folders: string[]; lowerBound: boolean }> => {
  const allConfigs = await helpers.configurationList();
  const seen = new Set<string>();
  const folders: string[] = [];
  let folderBearing = 0;
  for (const cfg of allConfigs) {
    const metadata = (cfg.metadata as MetadataItem[]) ?? [];
    let hasFolder = false;
    for (const meta of metadata) {
      if (meta.key === MetadataField.CONFIGURATION_FOLDER_NAME) {
        hasFolder = true;
        const folderName = (meta.value ?? '').trim();
        if (folderName && !seen.has(folderName)) {
          seen.add(folderName);
          folders.push(folderName);
        }
      }
    }
    if (hasFolder) folderBearing += 1;
  }
  // configuration_list does not embed metadata server-side the way the search endpoint does,
  // so we derive the total from the same list (faithful to the resulting hint behavior).
  const total = allConfigs.length;
  if (folderBearing >= 20) return { total: folderBearing, folders, lowerBound: true };
  if (total < 20) return { total, folders: [], lowerBound: false };
  return { total, folders, lowerBound: false };
};

const applyFolderMetadata = async (
  helpers: ReturnType<typeof storageHelpers>,
  configurationId: string,
  folder: string | null | undefined,
  plural: string,
  toolName: string,
  isNew = false,
): Promise<string | null> => {
  if (folder == null) {
    try {
      const { total, folders, lowerBound } = await getConfigFolders(helpers);
      return buildFolderHint(total, folders, plural, toolName, lowerBound);
    } catch {
      logger.warn(`Unable to fetch ${plural} folders for configuration "${configurationId}".`);
      return null;
    }
  }
  const normalized = folder.trim();
  if (normalized) {
    try {
      await helpers.configurationMetadataUpdate(configurationId, {
        [MetadataField.CONFIGURATION_FOLDER_NAME]: normalized,
      });
    } catch {
      logger.warn(`Unable to set folder metadata for configuration "${configurationId}".`);
    }
  } else if (!isNew) {
    try {
      const metadata = await helpers.configurationMetadataGet(configurationId);
      for (const entry of metadata) {
        if (entry.key === MetadataField.CONFIGURATION_FOLDER_NAME && entry.id) {
          await helpers.configurationMetadataDelete(configurationId, entry.id);
        }
      }
    } catch {
      logger.warn(`Unable to clear folder metadata for configuration "${configurationId}".`);
    }
  }
  return null;
};

// --- data app fetch / build (ports of _fetch_data_app etc.) -----------------

const getMetadataProperty = (metadata: MetadataItem[], key: string): string | undefined =>
  metadata.find((m) => m.key === key)?.value;

const dataAppFromApiResponses = (
  apiResponse: DataAppResponse,
  rawConfig: Record<string, unknown>,
): DataApp => {
  const metadata = (rawConfig.metadata as MetadataItem[]) ?? [];
  return {
    component_id: DATA_APP_COMPONENT_ID,
    configuration_id: String(rawConfig.id ?? ''),
    data_app_id: apiResponse.id,
    project_id: apiResponse.project_id,
    branch_id: apiResponse.branch_id ?? '',
    config_version: String(rawConfig.version ?? ''),
    state: apiResponse.state,
    type: apiResponse.type,
    deployment_url: apiResponse.url ?? null,
    auto_suspend_after_seconds: apiResponse.auto_suspend_after_seconds ?? null,
    name: String(rawConfig.name ?? ''),
    description: (rawConfig.description as string | null) ?? null,
    folder: getMetadataProperty(metadata, MetadataField.CONFIGURATION_FOLDER_NAME) ?? '',
    configuration: asRecord(rawConfig.configuration),
    repo_url: null,
    deployment_info: null,
    drafts: [],
    drafts_unavailable: 0,
    links: [],
  };
};

const summaryFromDataApp = (dataApp: DataApp): DataAppSummary => ({
  component_id: dataApp.component_id,
  configuration_id: dataApp.configuration_id,
  data_app_id: dataApp.data_app_id,
  project_id: dataApp.project_id,
  branch_id: dataApp.branch_id,
  config_version: dataApp.config_version,
  state: dataApp.state,
  type: dataApp.type,
  deployment_url: dataApp.deployment_url,
  auto_suspend_after_seconds: dataApp.auto_suspend_after_seconds,
  repo_url: dataApp.repo_url,
});

const summaryFromApiResponse = (api: DataAppResponse): DataAppSummary => ({
  component_id: api.component_id,
  configuration_id: api.config_id,
  data_app_id: api.id,
  project_id: api.project_id,
  branch_id: api.branch_id ?? '',
  config_version: api.config_version,
  state: api.state,
  type: api.type,
  deployment_url: api.url ?? null,
  auto_suspend_after_seconds: api.auto_suspend_after_seconds ?? null,
  repo_url: null,
});

const buildDataAppWithRepo = async (
  ds: DataScienceClient,
  dataAppScience: DataAppResponse,
  rawConfig: Record<string, unknown>,
): Promise<DataApp> => {
  const dataApp = dataAppFromApiResponses(dataAppScience, rawConfig);
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

const fetchDataApp = async (
  ds: DataScienceClient,
  helpers: ReturnType<typeof storageHelpers>,
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

const fetchLogs = async (ds: DataScienceClient, dataAppId: string): Promise<string[]> => {
  try {
    const text = await ds.tailAppLogs(dataAppId, 20);
    return text.split('\n');
  } catch (error) {
    if (error instanceof RawHttpError) return [];
    throw error;
  }
};

const fetchLatestRun = async (
  ds: DataScienceClient,
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

const withDeploymentInfo = (
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
  ds: DataScienceClient,
  helpers: ReturnType<typeof storageHelpers>,
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

const fetchDataAppDetailsTask = async (
  ds: DataScienceClient,
  helpers: ReturnType<typeof storageHelpers>,
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

const responseForState = (state: string): string =>
  state === 'running' || state === 'starting'
    ? 'updated (redeploy required to apply changes in the running app)'
    : 'updated';

// ===========================================================================
// Tool registration
// ===========================================================================

export const registerDataAppTools = (server: McpServer, config: Config): void => {
  const makeContext = () => {
    const clients = createKeboolaClients(config);
    const urls = deriveServiceUrls(config.storageApiUrl ?? '');
    const token = config.bearerToken ? `Bearer ${config.bearerToken}` : config.storageToken;
    const dsRaw = createRawClient({ baseUrl: urls.dataScience, token });
    const ds = createDataScienceClient(dsRaw);
    const helpers = storageHelpers(clients);
    return { clients, ds, helpers };
  };

  registerTool(server, {
    name: 'modify_streamlit_data_app',
    title: 'Modify Streamlit data app',
    description: `Creates or updates a Streamlit data app.

Considerations:
- The \`source_code\` parameter must be a complete and runnable Streamlit app. It must include a placeholder \`{QUERY_DATA_FUNCTION}\` where a \`query_data\` function will be injected. This function queries the workspace to get data, it accepts a string of SQL query following current sql dialect and returns a pandas DataFrame with the results from the workspace.
- Write SQL queries so they are compatible with the current workspace backend, you can ensure this by using the \`query_data\` tool to inspect the data in the workspace before using it in the data app.
- If you're updating an existing data app, provide the \`configuration_id\` parameter and the \`change_description\` parameter. To keep existing data app values during an update, leave them as empty strings, lists, or None appropriately based on the parameter type.
- After creating or updating a data app with this tool, ALWAYS call \`deploy_data_app(action="deploy", configuration_id=...)\` to start a new app or restart an existing app so changes take effect. Without this step, a newly created app will not start, and an existing app will keep running the previous deployment without the latest changes.
- New apps use the HTTP basic authentication by default for security unless explicitly specified otherwise; when updating, set \`authentication_type\` to \`default\` to keep the existing authentication type configuration (including OIDC setups) unless explicitly specified otherwise.

SQL & DATA TYPE RULES:
- Use delimited identifiers for the current SQL dialect for all column names and aliases in SQL. Match the exact identifier case used in SQL when referencing columns in Python code.
- \`query_data\` RETURNS ALL COLUMNS AS STRINGS regardless of SQL CAST. Always convert types in Python after loading: \`df["col"] = pd.to_numeric(df["col"], errors="coerce").fillna(0)\` and \`df["date"] = pd.to_datetime(df["date"], errors="coerce")\`.`,
    annotations: { destructiveHint: true },
    inputSchema: {
      name: z.string().describe('Name of the data app (max ~50 chars to fit DNS label limit).'),
      description: z.string().describe('Description of the data app.'),
      source_code: z.string().describe('Complete Python/Streamlit source code for the data app.'),
      packages: z
        .array(z.string())
        .describe(
          'Python packages used in the source code that will be installed by `pip install` ' +
            'into the environment before the code runs. For example: ["pandas", "requests~=2.32"].',
        ),
      authentication_type: authenticationTypeSchema.describe(
        'Authentication type, "no-auth" removes authentication completely, "basic-auth" sets the data ' +
          'app to be secured using the HTTP basic authentication, and "default" keeps the existing ' +
          'authentication type when updating.',
      ),
      configuration_id: z
        .string()
        .default('')
        .describe(
          'The ID of existing data app configuration when updating, otherwise empty string.',
        ),
      change_description: z
        .string()
        .default('')
        .describe(
          'The description of the change when updating (e.g. "Update Code"), otherwise empty string.',
        ),
      folder: z.string().nullish().describe(folderFieldDescription('data app', 'data apps')),
    },
    serializer: toonSerializeCompact,
    handler: async (args) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);
      const projectId = String(
        ((await clients.storage.tokens.verify()) as { owner: { id: string | number } }).owner.id,
      );
      const ws = await resolveWorkspace(config, helpers);
      const secrets = getSecrets(ws.workspaceId, ws.branchId);

      if (args.configuration_id) {
        const dataAppPre = await fetchDataApp(ds, helpers, args.configuration_id);
        let updatedConfig = updateExistingDataAppConfig(
          dataAppPre.configuration,
          args.name,
          args.source_code,
          args.packages,
          args.authentication_type,
          secrets,
          ws.sqlDialect,
        );
        updatedConfig = await encryptConfig(config, updatedConfig, {
          projectId,
          componentId: DATA_APP_COMPONENT_ID,
        });
        const updateResp = await helpers.configurationUpdate({
          configurationId: args.configuration_id,
          configuration: updatedConfig,
          changeDescription: args.change_description || 'Change Data App',
          updatedName: args.name || dataAppPre.name,
          updatedDescription: args.description || dataAppPre.description || undefined,
        });
        // --- write committed past this point; response building is best-effort ---
        const newVersion = String(updateResp.version ?? '');
        try {
          if (/^\d+$/.test(newVersion)) {
            await setCfgUpdateMetadata(helpers, args.configuration_id, Number(newVersion));
          }
          const folderHint = await applyFolderMetadata(
            helpers,
            args.configuration_id,
            args.folder,
            'data apps',
            'modify_streamlit_data_app',
          );
          const dataApp = await fetchDataApp(ds, helpers, args.configuration_id);
          const links = linksManager.getDataAppLinks(
            dataApp.configuration_id,
            args.name,
            dataApp.deployment_url ?? undefined,
            usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
          );
          return {
            response: responseForState(dataApp.state),
            change_summary: folderHint,
            data_app: summaryFromDataApp(dataApp),
            links,
          };
        } catch (error) {
          logger.error(
            { err: error },
            `Data app configuration ${args.configuration_id} was updated (version ${newVersion || '?'}) ` +
              `but building the response failed; returning a partial success.`,
          );
          const summary = summaryFromDataApp(dataAppPre);
          summary.config_version = newVersion || summary.config_version;
          let links: Link[] = [];
          try {
            links = linksManager.getDataAppLinks(
              args.configuration_id,
              args.name || dataAppPre.name,
              dataAppPre.deployment_url ?? undefined,
              usesBasicAuthentication(asRecord(dataAppPre.configuration.authorization)),
            );
          } catch {
            links = [];
          }
          return {
            response: responseForState(dataAppPre.state),
            change_summary:
              `The configuration WAS updated (version ${newVersion || 'unknown'}), but loading the full app ` +
              `details failed, so this response is partial. Do NOT retry the update -- the change is already ` +
              `applied. Call deploy_data_app to apply it to the running app.`,
            data_app: summary,
            links,
          };
        }
      }

      // Create new data app.
      let createCfg = buildDataAppConfig(
        args.name,
        args.source_code,
        args.packages,
        args.authentication_type,
        secrets,
        ws.sqlDialect,
      );
      createCfg = await encryptConfig(config, createCfg, {
        projectId,
        componentId: DATA_APP_COMPONENT_ID,
      });
      const dataAppResp = await ds.createDataApp({
        name: args.name,
        description: args.description,
        config: createCfg,
        branchId: config.branchId ?? null,
        appType: 'streamlit',
        useManagedGitRepo: false,
      });
      try {
        await setCfgCreationMetadata(helpers, dataAppResp.config_id);
        const folderHint = await applyFolderMetadata(
          helpers,
          dataAppResp.config_id,
          args.folder,
          'data apps',
          'modify_streamlit_data_app',
          true,
        );
        const links = linksManager.getDataAppLinks(
          dataAppResp.config_id,
          args.name,
          dataAppResp.url ?? undefined,
          usesBasicAuthentication(asRecord(createCfg.authorization)),
        );
        return {
          response: 'created',
          change_summary: folderHint,
          data_app: summaryFromApiResponse(dataAppResp),
          links,
        };
      } catch (error) {
        logger.error(
          { err: error },
          `Data app ${dataAppResp.id} was created (configuration ${dataAppResp.config_id}) but building ` +
            `the response failed; returning a partial success.`,
        );
        let links: Link[] = [];
        try {
          links = linksManager.getDataAppLinks(
            dataAppResp.config_id,
            args.name,
            dataAppResp.url ?? undefined,
            usesBasicAuthentication(asRecord(createCfg.authorization)),
          );
        } catch {
          links = [];
        }
        return {
          response: 'created',
          change_summary:
            `The data app WAS created (configuration ${dataAppResp.config_id}), but building the full response ` +
            `failed, so this response is partial. Do NOT retry creation -- it would create a duplicate. ` +
            `Call deploy_data_app to start the app.`,
          data_app: summaryFromApiResponse(dataAppResp),
          links,
        };
      }
    },
  });

  registerTool(server, {
    name: 'modify_python_js_data_app',
    title: 'Modify python-js data app',
    description: `Creates or updates a python-js data app.

Two-app project model. Every python-js project has a persistent **prod app** that owns the only managed git repository for the project, and zero or more **drafts** parented to that prod app. A draft is a Storage configuration with \`parameters.dataApp.isDraft=true\` and \`parameters.dataApp.parentConfigurationId=<prod cfg id>\`; it's an *external-git* app that clones the parent prod's repo at a pinned branch on every deploy. Drafts are surfaced in the Keboola UI under their parent prod app. Use \`deploy_data_app(mode='dev')\` to deploy a draft as a dev version of the data app (hot reload + auto-auth for iframe preview); use \`delete_python_js_data_app_draft\` to tear a draft down after its branch has been promoted.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. MCP gives you authenticated clone URLs and manages configs/deploys; it never invokes git.

**The draft flow is mandatory — never edit prod source directly.** Every source-code change goes through a draft branch that the user previews and explicitly approves first. NEVER push directly to \`main\`: \`main\` only ever advances by merging an approved draft branch, and only after the user has approved that draft's preview.

## Argument rules
- \`parent_configuration_id\` is **create-only**. Rejected on update.
- \`branch\` is **create-only** and only valid when \`parent_configuration_id\` is set. Defaults to \`'init'\`. Must not be \`'main'\`. Rejected on prod create and on update.
- \`slug\` is required on create and immutable after.
- The **update path** (passing \`configuration_id\`) is for changing \`name\`, \`description\`, \`authentication_type\`, \`auto_suspend_after_seconds\`, \`storage\` on either a prod app or a draft. Source code changes go through the git flow above, not this tool.

## Authentication
New apps default to HTTP basic authentication for safety. Pass \`authentication_type='no-auth'\` to expose publicly. On update, \`authentication_type='default'\` preserves the existing \`authorization\` block (including OIDC setups configured outside the MCP); \`'basic-auth'\` / \`'no-auth'\` overwrite it.

## Slug constraint
Must be DNS-label-safe (lowercase letters, digits, hyphens, ≤63 chars). For drafts, append a short suffix (e.g. \`-draft-abc123\`) to keep slugs unique across the prod and its drafts.`,
    annotations: { destructiveHint: true },
    inputSchema: {
      name: z.string().describe('Name of the data app (max ~50 chars to fit DNS label limit).'),
      description: z.string().describe('Description of the data app.'),
      configuration_id: z
        .string()
        .default('')
        .describe(
          'The ID of existing data app configuration when updating, otherwise empty string.',
        ),
      change_description: z
        .string()
        .default('')
        .describe(
          'The description of the change when updating (e.g. "Bump image"), otherwise empty string.',
        ),
      slug: z
        .string()
        .nullish()
        .describe(
          'URL-safe slug for the data app (used as a subdomain). Required when creating; immutable after.',
        ),
      parent_configuration_id: z
        .string()
        .nullish()
        .describe(
          'Storage configuration ID of the prod python-js data app this draft will iterate against. ' +
            'When set on create, the new app is created as a **draft**: no managed repo is provisioned ' +
            "for it; instead its `parameters.dataApp.git` block is populated to point at the prod app's " +
            'managed repo, with a freshly-minted prod-app HTTPS token and the chosen draft branch. ' +
            'Leave None on create to make a **prod app** (which gets its own managed repo). Rejected on update.',
        ),
      branch: z
        .string()
        .nullish()
        .describe(
          'Draft branch to pin the new draft to. Only valid on the draft create path ' +
            '(when `parent_configuration_id` is set). Defaults to `init` when unset. Must not be `main` ' +
            '(reserved for the prod app). Rejected on prod create and on update.',
        ),
      authentication_type: authenticationTypeSchema
        .default('default')
        .describe(
          'Authentication type. "no-auth" removes authentication completely, "basic-auth" secures the ' +
            'data app via HTTP basic authentication, and "default" means: on create, apply basic auth ' +
            '(safe default for new apps); on update, keep the existing authentication configuration ' +
            '(including OIDC setups configured outside the MCP).',
        ),
      auto_suspend_after_seconds: z
        .number()
        .int()
        .default(900)
        .describe('Number of seconds after which the running data app is automatically suspended.'),
      storage: z
        .record(z.string(), z.any())
        .nullish()
        .describe(
          'Complete storage configuration for the data app (input/output table mappings). ' +
            'Replaces the ENTIRE storage block when updating an existing app. Leave unset (None) to ' +
            'preserve the existing storage configuration; pass an empty dict to explicitly clear it.',
        ),
      folder: z.string().nullish().describe(folderFieldDescription('data app', 'data apps')),
    },
    serializer: toonSerializeCompact,
    handler: async (args) => {
      if (args.configuration_id) {
        if (args.slug) throw new Error('slug cannot be changed after the data app is created.');
        if (args.parent_configuration_id) {
          throw new Error(
            'parent_configuration_id is only valid when creating a draft (no configuration_id).',
          );
        }
        if (args.branch) {
          throw new Error('branch is only valid when creating a draft (no configuration_id).');
        }
      } else {
        if (!args.slug) {
          throw new Error('slug is required when creating a python-js data app.');
        }
        if (args.branch != null && !args.parent_configuration_id) {
          throw new Error(
            'branch is only valid on the draft create path (pair it with parent_configuration_id).',
          );
        }
      }

      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      const validatedStorage = validateDataAppStorage(args.storage);

      const hasStorageWorkspace = await hasFeature(clients, DATA_APPS_STORAGE_WORKSPACE_FEATURE);
      let legacySecrets: Record<string, unknown> | null = null;
      if (!hasStorageWorkspace) {
        const ws = await resolveWorkspace(config, helpers);
        legacySecrets = { [SECRET_WORKSPACE_ID]: ws.workspaceId };
      }

      if (args.configuration_id) {
        let dataApp = await fetchDataApp(ds, helpers, args.configuration_id);
        const updatedConfig = updateExistingCodeDataAppConfig(
          dataApp.configuration,
          args.auto_suspend_after_seconds,
          args.authentication_type,
          legacySecrets,
          validatedStorage,
        );
        await helpers.configurationUpdate({
          configurationId: args.configuration_id,
          configuration: updatedConfig,
          changeDescription: args.change_description || 'Update python-js data app',
          updatedName: args.name || dataApp.name,
          updatedDescription: args.description || dataApp.description || undefined,
        });
        dataApp = await fetchDataApp(ds, helpers, args.configuration_id);
        await setCfgUpdateMetadata(helpers, args.configuration_id, Number(dataApp.config_version));
        const folderHint = await applyFolderMetadata(
          helpers,
          args.configuration_id,
          args.folder,
          'data apps',
          'modify_python_js_data_app',
        );
        const repoUrl = dataApp.repo_url;
        const links = linksManager.getDataAppLinks(
          dataApp.configuration_id,
          args.name || dataApp.name,
          dataApp.deployment_url ?? undefined,
          usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
        );
        const summary = summaryFromDataApp(dataApp);
        summary.repo_url = repoUrl;
        return {
          response: responseForState(dataApp.state),
          change_summary: folderHint,
          data_app: summary,
          repo_url: repoUrl,
          links,
        };
      }

      // Create new python-js data app (prod or draft).
      const slug = args.slug!;
      const usesBasicAuth =
        args.authentication_type === 'basic-auth' || args.authentication_type === 'default';
      const authorizationModel = getAuthorization(usesBasicAuth);

      let gitCloneUrl: string | null = null;
      let draftBranch: string | null = null;
      let gitBlock: Record<string, unknown> | null = null;

      if (args.parent_configuration_id) {
        const parent = await fetchDataApp(ds, helpers, args.parent_configuration_id);
        if (parent.type !== 'python-js') {
          throw new Error(
            `parent_configuration_id "${args.parent_configuration_id}" is type "${parent.type}", but only ` +
              `python-js prod apps can parent a draft.`,
          );
        }
        if (isDraftConfig(parent.configuration)) {
          throw new Error(
            `parent_configuration_id "${args.parent_configuration_id}" is itself a python-js **draft**, ` +
              "not a prod app. Drafts iterate against the prod app's repo and cannot parent another " +
              "draft — pass the prod app's configuration_id (a draft's parentConfigurationId points to it).",
          );
        }
        if (!parent.repo_url) {
          throw new Error(
            `Parent python-js data app "${args.parent_configuration_id}" has no managed git repo URL. ` +
              'This indicates a platform-side bug — retry or contact support.',
          );
        }
        draftBranch = (args.branch || DEFAULT_DRAFT_BRANCH).trim();
        if (!draftBranch || /\s/.test(draftBranch)) {
          throw new Error(`branch "${args.branch}" is not a valid git branch name.`);
        }
        if (draftBranch === 'main') {
          throw new Error(
            'branch "main" is reserved for the prod app — pick a different draft branch.',
          );
        }
        const cred = await ds.createAppGitCredential(parent.data_app_id);
        if (!cred.secret) {
          throw new Error(
            `Parent data app ${parent.data_app_id} credentials endpoint returned no \`secret\` for an ` +
              `http_token credential. This indicates a platform-side bug — retry or contact support.`,
          );
        }
        gitBlock = {
          repository: parent.repo_url,
          username: MANAGED_GIT_REPO_USERNAME,
          '#password': cred.secret,
          branch: draftBranch,
        };
        gitCloneUrl = buildAuthenticatedCloneUrl(parent.repo_url, cred.secret);
      }

      const dataAppBlock: Record<string, unknown> = { slug };
      if (legacySecrets) dataAppBlock.secrets = legacySecrets;
      if (gitBlock) dataAppBlock.git = gitBlock;
      if (args.parent_configuration_id != null) {
        dataAppBlock.isDraft = true;
        dataAppBlock.parentConfigurationId = args.parent_configuration_id;
      }
      let configPayload: Record<string, unknown> = {
        parameters: {
          autoSuspendAfterSeconds: args.auto_suspend_after_seconds,
          dataApp: dataAppBlock,
        },
        authorization: authorizationModel,
      };
      if (hasStorageWorkspace) {
        configPayload.runtime = { workspace: { enabled: true } };
      }
      if (validatedStorage && Object.keys(validatedStorage).length > 0) {
        configPayload.storage = validatedStorage;
      }

      if (gitBlock !== null) {
        const projectId = String(
          ((await clients.storage.tokens.verify()) as { owner: { id: string | number } }).owner.id,
        );
        configPayload = await encryptConfig(config, configPayload, {
          projectId,
          componentId: DATA_APP_COMPONENT_ID,
        });
      }

      const dataAppResp = await ds.createDataApp({
        name: args.name,
        description: args.description,
        config: configPayload,
        branchId: config.branchId ?? null,
        appType: 'python-js',
        useManagedGitRepo: args.parent_configuration_id == null,
      });

      let repoUrl: string;
      if (args.parent_configuration_id) {
        repoUrl = gitBlock!.repository as string;
      } else {
        const repoResp = await ds.getAppGitRepo(dataAppResp.id);
        if (repoResp.https_url == null) {
          throw new Error(
            `Data app ${dataAppResp.id} reports no HTTPS clone URL despite having a managed git repo. ` +
              'This indicates a platform-side bug — retry or contact support.',
          );
        }
        repoUrl = repoResp.https_url;
      }
      await setCfgCreationMetadata(helpers, dataAppResp.config_id);
      const folderHint = await applyFolderMetadata(
        helpers,
        dataAppResp.config_id,
        args.folder,
        'data apps',
        'modify_python_js_data_app',
        true,
      );
      const links = linksManager.getDataAppLinks(
        dataAppResp.config_id,
        args.name,
        dataAppResp.url ?? undefined,
        usesBasicAuth,
      );
      const summary = summaryFromApiResponse(dataAppResp);
      summary.repo_url = repoUrl;
      return {
        response: 'created',
        change_summary: folderHint,
        data_app: summary,
        repo_url: repoUrl,
        git_clone_url: gitCloneUrl,
        branch: draftBranch,
        links,
      };
    },
  });

  registerTool(server, {
    name: 'create_python_js_data_app_git_credential',
    title: 'Create python-js data app git credential',
    description: `Mints a one-time HTTPS token on a python-js **prod** data app so the caller can clone, pull, and push to the app's managed git repo over HTTPS.

**Always call against the prod app's configuration_id** — drafts have no managed repo of their own, so calling this on a draft fails. The prod app is the canonical repo owner; drafts iterate against branches of that same repo.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. This tool only mints credentials.

Returns a ready-to-use \`git_clone_url\` of the form \`https://kai:<secret>@<host>/<path>.git\` plus the raw \`secret\`. The token is returned **only** at creation — the platform cannot return it again on any subsequent read. Stash the URL (or the secret) somewhere the LLM can reuse for the rest of the session.

## Constraints
- Only python-js prod data apps have a managed git repo. Streamlit apps reject the call with a clear error.
- Permissions are always \`readWrite\`.`,
    annotations: { destructiveHint: false },
    inputSchema: {
      configuration_id: z.string().describe('Storage configuration ID of the python-js data app.'),
    },
    serializer: toonSerializeCompact,
    handler: async ({ configuration_id }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      const dataApp = await fetchDataApp(ds, helpers, configuration_id);
      if (dataApp.type !== 'python-js') {
        throw new Error(
          `create_python_js_data_app_git_credential only supports python-js data apps, but configuration ` +
            `"${configuration_id}" is type "${dataApp.type}".`,
        );
      }
      if (isDraftConfig(dataApp.configuration)) {
        const dataAppBlock = asRecord(asRecord(dataApp.configuration.parameters).dataApp);
        const parentCfgId = dataAppBlock.parentConfigurationId;
        const hint =
          typeof parentCfgId === 'string' ? ` (parentConfigurationId="${parentCfgId}")` : '';
        throw new Error(
          `Configuration "${configuration_id}" is a python-js **draft**, which has no managed git repo ` +
            `of its own. Mint credentials against the parent prod app instead${hint}.`,
        );
      }

      const repoResp = await ds.getAppGitRepo(dataApp.data_app_id);
      if (repoResp.https_url == null) {
        throw new Error(
          `Data app ${dataApp.data_app_id} reports no HTTPS clone URL despite being a python-js managed-repo ` +
            `app. This indicates a platform-side bug — retry or contact support.`,
        );
      }

      const credentialResp = await ds.createAppGitCredential(dataApp.data_app_id);
      if (!credentialResp.secret) {
        throw new Error(
          `Data app ${dataApp.data_app_id} credentials endpoint returned no \`secret\` for an http_token ` +
            `credential. This indicates a platform-side bug — retry or contact support.`,
        );
      }

      const gitCloneUrl = buildAuthenticatedCloneUrl(repoResp.https_url, credentialResp.secret);
      const links = linksManager.getDataAppLinks(
        dataApp.configuration_id,
        dataApp.name,
        dataApp.deployment_url ?? undefined,
        false,
      );
      return {
        response: 'created',
        configuration_id: dataApp.configuration_id,
        data_app_id: dataApp.data_app_id,
        credential_id: credentialResp.id,
        git_clone_url: gitCloneUrl,
        secret: credentialResp.secret,
        permissions: credentialResp.permissions,
        links,
      };
    },
  });

  registerTool(server, {
    name: 'get_data_apps',
    title: 'Get data apps',
    description: `Lists summaries of data apps in the project given the limit and offset or gets details of a data apps by providing their configuration IDs.

WHEN NOT TO USE:
- Do NOT list all data apps just to find one by name. Use \`search\` with item_types=["data-app"] instead.
- Only list all data apps when you need a complete inventory.

Considerations:
- If configuration_ids are provided, the tool will return details of the data apps by their configuration IDs.
- If no configuration_ids are provided, the tool will list all data apps in the project given the limit and offset.
- Data App detail contains configuration, metadata, source code, links, and deployment info along with the latest data app logs to investigate in-app errors. The logs may be updated after opening the data app URL.
- \`deployment_info.last_run\` carries the outcome of the most recent deployment attempt. For an app that fails to start, check its \`failure_reason\`/\`failure_message\` FIRST — they cover setup-phase failures (e.g. invalid secrets, git clone errors, failing setup scripts) that happen before the container starts and therefore never appear in the regular logs.
- \`repo_url\` (managed git repo URL for python-js apps) is ONLY populated on the detail path (when \`configuration_ids\` is provided). The inventory list always returns \`repo_url=None\`, even for python-js apps with a managed repo — to retrieve the URL, call this tool again with the target \`configuration_ids\`.
- When called with \`configuration_ids=[<prod-cfg>]\` for a python-js **prod** app, the response includes a \`drafts: [...]\` array of every draft (configs with \`isDraft=true\` and \`parentConfigurationId == <prod-cfg>\`) currently in the project. Drafts in trash are not included. The array is empty for drafts themselves and for Streamlit apps.`,
    annotations: { readOnlyHint: true },
    inputSchema: {
      configuration_ids: z
        .array(z.string())
        .default([])
        .describe('The IDs of the data app configurations.'),
      limit: z.number().int().default(100).describe('The limit of the data apps to fetch.'),
      offset: z.number().int().default(0).describe('The offset of the data apps to fetch.'),
    },
    serializer: toonSerializeCompact,
    handler: async ({ configuration_ids, limit, offset }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      if (configuration_ids.length > 0) {
        const details = await Promise.all(
          configuration_ids.map((id) => fetchDataAppDetailsTask(ds, helpers, linksManager, id)),
        );
        const found = details.filter((d): d is DataApp => typeof d !== 'string');
        const notFound = details.filter((d): d is string => typeof d === 'string');
        if (notFound.length > 0) {
          logger.error(`Could not find Data Apps Configurations for IDs: ${notFound.join(', ')}`);
        }
        return { data_apps: found };
      }

      let dataApps = await ds.listDataApps(limit, offset);
      dataApps = dataApps.filter((app) => app.component_id === DATA_APP_COMPONENT_ID);
      return {
        data_apps: dataApps.map(summaryFromApiResponse),
        links: [linksManager.getDataAppDashboardLink()],
      };
    },
  });

  registerTool(server, {
    name: 'deploy_data_app',
    title: 'Deploy data app',
    description: `Deploys/redeploys a data app or stops a running data app in the Keboola environment asynchronously, given the action and the configuration ID.

**MCP never runs git on your behalf.** All git work — clone, branch, commit, push, merge, branch-delete — is yours. This tool only triggers deploys against existing git state.

## Mode (python-js apps)
- \`mode='dev'\` deploys the target as a **dev version of the data app** — the runtime uses a development \`setup.sh\` (hot reload) and the data-app proxy enables an auto-auth path so an iframe preview can render without a manual login. Only meaningful on **draft** configs (python-js apps with \`isDraft=true\`).
- For prod redeploys (including after merging a draft's branch into \`main\`), use no \`mode\` — the prod app picks up the current \`main\`.
- The branch a draft deploys from is pinned in \`parameters.dataApp.git.branch\` at create time; there is no deploy-time override.
- python-js apps do NOT fetch a Storage \`configVersion\` for deployment (their source lives in git, not in the Storage configuration); this is handled automatically.

## Streamlit apps
Streamlit apps have no managed git repo, so \`mode\` has no effect on the deployed app. \`mode=None\` is the expected call shape.

## General considerations
- Redeploying a data app takes some time, and the app may temporarily report status "stopped" during the restart.
- After deployment, the deployment info includes the app URL and the latest logs to help diagnose in-app errors.`,
    annotations: { destructiveHint: false },
    inputSchema: {
      action: actionSchema.describe('The action to perform.'),
      configuration_id: z.string().describe('The ID of the data app configuration.'),
      mode: modeSchema
        .nullish()
        .describe(
          'Deployment mode. Set to "dev" to deploy a python-js draft as a **dev version of the data ' +
            'app** — the runtime uses a development `setup.sh` (hot reload), and the data-app proxy ' +
            'enables an auto-auth path so an iframe preview can render without a manual login. ' +
            'Only meaningful on **draft** configs (python-js apps with `isDraft=true`). Leave None ' +
            '(default) for prod redeploys and for Streamlit apps.',
        ),
    },
    serializer: toonSerializeCompact,
    handler: async ({ action, configuration_id, mode }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      if (action === 'deploy') {
        let dataApp = await fetchDataApp(ds, helpers, configuration_id);
        if (dataApp.state === 'stopping') {
          throw new Error('Data app is currently "stopping", could not be started at the moment.');
        }
        let configVersionArg: string | null = null;
        if (dataApp.type !== 'python-js') {
          const version = await helpers.configurationVersionLatest(dataApp.configuration_id);
          configVersionArg = String(version);
        }
        await ds.deployDataApp(dataApp.data_app_id, configVersionArg, mode ?? null);
        dataApp = await fetchDataApp(ds, helpers, configuration_id);
        dataApp = withDeploymentInfo(
          dataApp,
          await fetchLogs(ds, dataApp.data_app_id),
          await fetchLatestRun(ds, dataApp.data_app_id),
        );
        const links = linksManager.getDataAppLinks(
          dataApp.configuration_id,
          dataApp.name,
          dataApp.deployment_url ?? undefined,
          usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
        );
        return { state: dataApp.state, deployment_info: dataApp.deployment_info, links };
      }

      // action === 'stop'
      let dataApp = await fetchDataApp(ds, helpers, configuration_id);
      if (dataApp.state === 'starting' || dataApp.state === 'restarting') {
        throw new Error('Data app is currently "starting", could not be stopped at the moment.');
      }
      await ds.suspendDataApp(dataApp.data_app_id);
      dataApp = await fetchDataApp(ds, helpers, configuration_id);
      const links = linksManager.getDataAppLinks(
        dataApp.configuration_id,
        dataApp.name,
        undefined,
        usesBasicAuthentication(asRecord(dataApp.configuration.authorization)),
      );
      return { state: dataApp.state, deployment_info: null, links };
    },
  });

  registerTool(server, {
    name: 'delete_python_js_data_app_draft',
    title: 'Delete python-js data app draft',
    description: `Deletes a python-js DRAFT data app — both the data-app instance (DSAPI) and its Storage configuration.

**MCP never runs git on your behalf.** Deleting the feature branch on the remote is your job; this tool only tears down the draft config and its data-app instance.

WHEN TO CALL: at the end of a promote-to-prod sequence, after you have merged the draft's branch into \`main\`, pushed, deleted the feature branch from the remote, and redeployed the prod app. The Keboola UI lists drafts under their parent prod app; once you call this tool, the draft disappears from that list.

WHAT THIS TOOL REFUSES:
  - prod apps (no \`isDraft\` flag) — protects against accidental prod deletion;
  - Streamlit apps — they have no draft concept.

WHAT THIS TOOL DOES NOT DO:
  - Run git. Deleting the feature branch on the remote is your job.
  - Revoke the prod-side git credential minted when the draft was created.

After a successful call, pivot back to the parent prod app (its configuration_id is returned in the response) or to \`get_data_apps\` for further work.`,
    annotations: { destructiveHint: true },
    inputSchema: {
      configuration_id: z
        .string()
        .describe('Storage configuration ID of the python-js draft data app to delete.'),
    },
    serializer: toonSerializeCompact,
    handler: async ({ configuration_id }) => {
      const { clients, ds, helpers } = makeContext();
      const linksManager = await createLinksManager(config, clients);

      const dataApp = await fetchDataApp(ds, helpers, configuration_id);
      if (dataApp.type !== 'python-js') {
        throw new Error(
          `delete_python_js_data_app_draft only supports python-js data apps, but configuration ` +
            `"${configuration_id}" is type "${dataApp.type}".`,
        );
      }
      if (!isDraftConfig(dataApp.configuration)) {
        throw new Error(
          `Configuration "${configuration_id}" is a python-js **prod** app, not a draft ` +
            '(parameters.dataApp.isDraft is not true). This tool only deletes drafts — ' +
            'prod apps must be deleted from the Keboola UI.',
        );
      }

      const dataAppBlock = asRecord(asRecord(dataApp.configuration.parameters).dataApp);
      const parentCfgId = dataAppBlock.parentConfigurationId;
      const parentConfigurationId = typeof parentCfgId === 'string' ? parentCfgId : null;

      await ds.deleteDataApp(dataApp.data_app_id);

      const links = linksManager.getDataAppLinks(
        parentConfigurationId ?? configuration_id,
        parentConfigurationId ? 'parent prod app' : dataApp.name,
        undefined,
        false,
      );
      return {
        response: 'deleted',
        configuration_id,
        data_app_id: dataApp.data_app_id,
        parent_configuration_id: parentConfigurationId,
        links,
      };
    },
  });

  logger.info('Data app tools initialized.');
};
