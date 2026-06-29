import { readFileSync } from 'node:fs';

import { createRawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { DATA_APP_COMPONENT_ID } from '@/constants';
import { resourcePath } from '@/resource-path';
import type { AuthenticationType, DataApp, DataAppResponse, DataAppSummary } from './model';

// Pure helpers + config builders ported from tools/data_apps.py. No I/O except the
// resource-code loader and the encryption client (a one-off raw POST that the typed
// api-client does not cover).

// MCP-only metadata keys (port of config.py MetadataField; not yet in constants.ts).
export const CREATED_BY_MCP = 'KBC.MCP.createdBy';
export const UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.';

// --- resource code templates -------------------------------------------------
// Copied from src/keboola_mcp_server/resources/data_app/* into src/resources/data_app/*.
// Loaded once at module init via fs, resolving relative to this module's URL so it works
// when running from source (vitest / tsx). Mirrors the Python `importlib.resources` read.
const readResource = (name: string): string =>
  readFileSync(resourcePath('data_app', name), { encoding: 'utf-8' });

export const QUERY_SERVICE_QUERY_DATA_FUNCTION_CODE = readResource('qsapi_query_data_code.py');
export const STORAGE_QUERY_DATA_FUNCTION_CODE = readResource('sapi_query_data_code.py');

const DEFAULT_STREAMLIT_THEME =
  '[theme]\nfont = "sans serif"\ntextColor = "#222529"\nbackgroundColor = "#FFFFFF"\n' +
  'secondaryBackgroundColor = "#E6F2FF"\nprimaryColor = "#1F8FFF"';
const DEFAULT_PACKAGES = ['pandas', 'httpx'];

export const MANAGED_GIT_REPO_USERNAME = 'kai';
export const DEFAULT_DRAFT_BRANCH = 'init';

export const APP_RUN_LOG_LINES = 30;
export const APP_RUN_MESSAGE_LIMIT = 3000;

const INJECTED_BLOCK_RE =
  /(?<before>[\s\S]*?)#\s###\sINJECTED_CODE\s####[\s\S]*?#\s###\sEND_OF_INJECTED_CODE\s####(?<after>[\s\S]*)/;

export const SECRET_WORKSPACE_ID = 'WORKSPACE_ID';
export const SECRET_BRANCH_ID = 'BRANCH_ID';

export const DATA_APPS_STORAGE_WORKSPACE_FEATURE = 'data-apps-storage-workspace';

const MAX_DNS_LABEL_LENGTH = 63;

// ---------------------------------------------------------------------------
// Encryption client (port of clients/encryption.py): POST encrypt with id params.
// KEEP RAW: the typed api-client does not expose the encryption service.
// ---------------------------------------------------------------------------
export const encryptConfig = async (
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

// --- pure helpers (ports of the module-level functions in data_apps.py) -----

export const getAuthorization = (authWithPassword: boolean): Record<string, unknown> => {
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

export const usesBasicAuthentication = (authorization: Record<string, unknown>): boolean => {
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

export class DataAppSlugTooLongError extends Error {}

export const getDataAppSlug = (name: string): string => {
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

export const getSecrets = (workspaceId: string, branchId: string): Record<string, unknown> => ({
  [SECRET_WORKSPACE_ID]: workspaceId,
  [SECRET_BRANCH_ID]: branchId,
});

const sortedUnique = (items: string[]): string[] =>
  Array.from(new Set(items)).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));

export const buildDataAppConfig = (
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

export const asRecord = (value: unknown): Record<string, unknown> =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};

export const updateExistingDataAppConfig = (
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

export const validateDataAppStorage = (
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

export const updateExistingCodeDataAppConfig = (
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

export const isDraftConfig = (configuration: Record<string, unknown>): boolean => {
  const parameters = configuration.parameters;
  if (!parameters || typeof parameters !== 'object' || Array.isArray(parameters)) return false;
  const dataApp = (parameters as Record<string, unknown>).dataApp;
  if (!dataApp || typeof dataApp !== 'object' || Array.isArray(dataApp)) return false;
  return (dataApp as Record<string, unknown>).isDraft === true;
};

export const buildAuthenticatedCloneUrl = (httpsUrl: string, secret: string): string => {
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

export const folderFieldDescription = (singular: string, plural: string): string =>
  `Folder name to organize this ${singular} in the Keboola UI. ` +
  `Pass an empty string to remove an existing folder assignment. ` +
  `Existing folder names are returned in the response change_summary when no folder is provided ` +
  `and there are 20 or more ${plural} in the project. ` +
  `If there are 20 or more ${plural}, you should assign one of the existing folders or ` +
  `create a new one that clearly reflects the ${singular} purpose.`;

export const responseForState = (state: string): string =>
  state === 'running' || state === 'starting'
    ? 'updated (redeploy required to apply changes in the running app)'
    : 'updated';

// --- summary projections (pure) ---------------------------------------------

export const summaryFromDataApp = (dataApp: DataApp): DataAppSummary => ({
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

export const summaryFromApiResponse = (api: DataAppResponse): DataAppSummary => ({
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

export const dataAppFromApiResponses = (
  apiResponse: DataAppResponse,
  rawConfig: Record<string, unknown>,
  metadataFolderField: string,
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
    folder: getMetadataProperty(metadata, metadataFolderField) ?? '',
    configuration: asRecord(rawConfig.configuration),
    repo_url: null,
    deployment_info: null,
    drafts: [],
    drafts_unavailable: 0,
    links: [],
  };
};

export type MetadataItem = { id?: string; key?: string; value?: string };

export const getMetadataProperty = (metadata: MetadataItem[], key: string): string | undefined =>
  metadata.find((m) => m.key === key)?.value;
