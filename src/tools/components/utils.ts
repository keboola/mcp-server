import { redactSecrets } from '@/clients/encryption';
import { type KeboolaClients } from '@/clients/keboola';
import { createRawClient, type RawClient, RawHttpError } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { MetadataField } from '@/constants';
import type { Link } from '@/links';
import { logger } from '@/logger';
import { type ComponentForValidation, type JsonDict } from '@/tools/validation';
import { type VariableDefinition, VARIABLES_COMPONENT_ID } from './model';

// Ported from tools/components/{utils,api_models}.py.

export type RawConfig = Record<string, unknown>;
export type RawComponent = Record<string, unknown>;
export type MetadataItem = { key?: string; value?: string };

const metadataProperty = (metadata: MetadataItem[] | undefined, key: string): string | undefined =>
  (metadata ?? []).find((item) => item.key === key)?.value;

export const pick = <T>(raw: RawComponent, ...keys: string[]): T | undefined => {
  for (const key of keys) {
    if (raw[key] !== undefined && raw[key] !== null) return raw[key] as T;
  }
  return undefined;
};

/** Configuration root/row summary (list mode) — port of ConfigSummary.from_api_response. */
export const toConfigSummary = (raw: RawConfig, componentId: string, links: Link[]) => {
  const metadata = (raw.metadata as MetadataItem[]) ?? [];
  const rows = (raw.rows as RawConfig[]) ?? null;
  return {
    configuration_root: {
      component_id: componentId,
      configuration_id: String(raw.id ?? ''),
      name: raw.name ?? '',
      description: raw.description ?? null,
      is_disabled: raw.isDisabled ?? false,
      is_deleted: raw.isDeleted ?? false,
      folder: metadataProperty(metadata, MetadataField.CONFIGURATION_FOLDER_NAME) ?? '',
    },
    configuration_rows: rows
      ? rows.map((row) => ({
          component_id: componentId,
          configuration_id: String(raw.id ?? ''),
          row_configuration_id: String(row.id ?? ''),
          name: row.name ?? '',
          description: row.description ?? null,
          is_disabled: row.isDisabled ?? false,
          is_deleted: row.isDeleted ?? false,
        }))
      : null,
    links,
  };
};

/**
 * Full configuration root/rows (detail mode) — port of Configuration.from_api_response.
 * NOTE: transformation parameter *simplification* (Snowflake/BigQuery) is deferred until
 * create_sql_transformation lands (needs the inverse); transformations return raw params.
 */
export const toConfiguration = (
  raw: RawConfig,
  componentId: string,
  component: unknown,
  links: Link[],
) => {
  const metadata = (raw.metadata as MetadataItem[]) ?? [];
  const cfg = (raw.configuration as Record<string, unknown>) ?? {};
  const rows = (raw.rows as RawConfig[]) ?? null;
  return {
    configuration_root: {
      component_id: componentId,
      configuration_id: String(raw.id ?? ''),
      name: raw.name ?? '',
      description: raw.description ?? null,
      version: raw.version ?? 0,
      is_disabled: raw.isDisabled ?? false,
      is_deleted: raw.isDeleted ?? false,
      folder: metadataProperty(metadata, MetadataField.CONFIGURATION_FOLDER_NAME) ?? '',
      parameters: redactSecrets(cfg.parameters ?? {}),
      storage: cfg.storage ?? null,
      processors: redactSecrets(cfg.processors ?? null),
      variables_id: cfg.variables_id ?? null,
      variables_values_id: cfg.variables_values_id ?? null,
      variables: cfg.variables ?? null,
      configuration_metadata: metadata,
    },
    configuration_rows: rows
      ? rows.map((row) => {
          const rowCfg = (row.configuration as Record<string, unknown>) ?? {};
          return {
            component_id: componentId,
            configuration_id: String(raw.id ?? ''),
            configuration_row_id: String(row.id ?? ''),
            name: row.name ?? '',
            description: row.description ?? null,
            version: row.version ?? 0,
            is_disabled: row.isDisabled ?? false,
            is_deleted: row.isDeleted ?? false,
            parameters: redactSecrets(rowCfg.parameters ?? {}),
            storage: rowCfg.storage ?? null,
            processors: redactSecrets(rowCfg.processors ?? null),
            values: rowCfg.values ?? null,
            configuration_metadata: (rowCfg.metadata as unknown) ?? [],
          };
        })
      : null,
    component,
    links,
  };
};

export const toComponentSummary = (raw: RawComponent) => {
  const flags = (pick<string[]>(raw, 'flags', 'componentFlags') ?? []) as string[];
  return {
    component_id: pick<string>(raw, 'id', 'componentId', 'component_id') ?? '',
    component_name: pick<string>(raw, 'name', 'componentName', 'component_name') ?? '',
    component_type: pick<string>(raw, 'type', 'componentType', 'component_type') ?? '',
    capabilities: capabilitiesFromFlags(flags),
    links: [] as Link[],
  };
};

export const jsonBlock = (label: string, index: number, example: unknown): string =>
  `${index}. ${label}:\n\`\`\`json\n${JSON.stringify(example, null, 2)}\n\`\`\`\n\n`;

/** Capabilities derived from developer-portal flags (port of ComponentCapabilities.from_flags). */
const capabilitiesFromFlags = (flags: string[]) => ({
  is_row_based: flags.includes('genericDockerUI-rows'),
  has_table_input:
    flags.includes('genericDockerUI-tableInput') ||
    flags.includes('genericDockerUI-simpleTableInput'),
  has_table_output: flags.includes('genericDockerUI-tableOutput'),
  has_file_input: flags.includes('genericDockerUI-fileInput'),
  has_file_output: flags.includes('genericDockerUI-fileOutput'),
  requires_oauth: flags.includes('genericDockerUI-authorization'),
});

/** Maps a raw component (AI catalog or Storage API) to the Component output shape. */
export const toComponent = (raw: RawComponent) => {
  const flags = (pick<string[]>(raw, 'flags', 'componentFlags') ?? []) as string[];
  const data = (pick<Record<string, unknown>>(raw, 'data') ?? {}) as Record<string, unknown>;
  return {
    component_id: pick<string>(raw, 'id', 'componentId', 'component_id') ?? '',
    component_name: pick<string>(raw, 'name', 'componentName', 'component_name') ?? '',
    component_type: pick<string>(raw, 'type', 'componentType', 'component_type') ?? '',
    component_categories: pick<string[]>(raw, 'categories', 'componentCategories') ?? [],
    capabilities: capabilitiesFromFlags(flags),
    documentation_url: pick<string>(raw, 'documentationUrl', 'documentation_url') ?? null,
    documentation: pick<string>(raw, 'documentation') ?? null,
    configuration_schema: pick<unknown>(raw, 'configurationSchema', 'configuration_schema') ?? null,
    configuration_row_schema:
      pick<unknown>(raw, 'configurationRowSchema', 'configuration_row_schema') ?? null,
    sync_actions: (data.synchronous_actions as string[] | undefined) ?? null,
    links: [] as unknown[],
  };
};

/**
 * Fetches a component, preferring the AI catalog (docs + schemas) and merging the
 * Storage API `data` (sync actions); falls back to Storage API on 404. Port of
 * components/utils.py `fetch_component`.
 *
 * KEPT RAW: the AI catalog `docs/components/{id}` endpoint has no typed api-client
 * equivalent, and the result merges the AI doc shape with the Storage component
 * `data` field — a bespoke merge the typed `getComponent` does not reproduce.
 */
export const fetchComponent = async (
  clients: KeboolaClients,
  componentId: string,
): Promise<RawComponent> => {
  try {
    const fromAi = await clients.rawAi.get<RawComponent>(`docs/components/${componentId}`);
    const fromStorage = await clients.rawStorage.get<RawComponent>(
      `branch/${clients.branchId}/components/${componentId}`,
    );
    fromAi.data = fromStorage.data ?? {};
    return fromAi;
  } catch (error) {
    if (error instanceof RawHttpError && error.status === 404) {
      return clients.rawStorage.get<RawComponent>(
        `branch/${clients.branchId}/components/${componentId}`,
      );
    }
    throw error;
  }
};

// ============================================================================
// WRITE-TOOL HELPERS (ported from components/tools.py + utils.py).
// ============================================================================

const CREATED_BY_MCP = 'KBC.MCP.createdBy';
const UPDATED_BY_MCP_PREFIX = 'KBC.MCP.updatedBy.version.';

/** Maps a fetched raw component into the minimal shape the validators need. */
export const toComponentForValidation = (raw: RawComponent): ComponentForValidation => {
  const flags = (pick<string[]>(raw, 'flags', 'componentFlags') ?? []) as string[];
  return {
    component_id: pick<string>(raw, 'id', 'componentId', 'component_id') ?? '',
    component_type: pick<string>(raw, 'type', 'componentType', 'component_type') ?? '',
    capabilities: { is_row_based: flags.includes('genericDockerUI-rows') },
    configuration_schema:
      (pick<JsonDict>(raw, 'configurationSchema', 'configuration_schema') as JsonDict | null) ??
      null,
    configuration_row_schema:
      (pick<JsonDict>(
        raw,
        'configurationRowSchema',
        'configuration_row_schema',
      ) as JsonDict | null) ?? null,
  };
};

/** Builds a raw Encryption-service client rooted at the project's encryption URL. */
const encryptionClient = (config: Config): RawClient => {
  const urls = deriveServiceUrls(config.storageApiUrl ?? '');
  return createRawClient({ baseUrl: urls.encryption, token: config.storageToken });
};

const isEncryptedValue = (value: unknown): boolean =>
  typeof value === 'string' && value.startsWith('KBC::');

const REDACTED_SECRET_VALUE = '[REDACTED]';

/** Yields [key, value] for every '#'-prefixed key, recursively. */
const iterSecretItems = (value: unknown, out: [string, unknown][] = []): [string, unknown][] => {
  if (Array.isArray(value)) {
    for (const item of value) iterSecretItems(item, out);
  } else if (value !== null && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (key.startsWith('#')) out.push([key, item]);
      else iterSecretItems(item, out);
    }
  }
  return out;
};

/**
 * Encrypts plaintext '#'-prefixed secrets before a config is written to Storage.
 * Fail-closed: refuses to store redacted placeholders; otherwise calls the encryption
 * service. Port of StorageClient._encrypt_secrets.
 *
 * KEPT RAW: the Encryption service (`encrypt`) is a separate service not covered by
 * the typed Storage api-client.
 */
const encryptSecrets = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  configuration: JsonDict,
): Promise<JsonDict> => {
  const items = iterSecretItems(configuration);
  const plaintextKeys = items.filter(([, v]) => !isEncryptedValue(v)).map(([k]) => k);
  if (plaintextKeys.length === 0) return configuration;

  const redactedKeys = items.filter(([, v]) => v === REDACTED_SECRET_VALUE).map(([k]) => k);
  if (redactedKeys.length > 0) {
    throw new Error(
      `The configuration contains redacted secret values for keys: ${[...new Set(redactedKeys)].sort().join(', ')}. ` +
        'These are placeholders returned on configuration reads, not the actual secret values. ' +
        'Either leave the existing secret values untouched or ask the user to provide new ones.',
    );
  }

  const token = await clients.storage.tokens.verify();
  const projectId = String((token.owner as { id: string | number }).id);
  return encryptionClient(config).post<JsonDict>('encrypt', {
    params: { componentId, projectId },
    body: configuration,
  });
};

// ============================================================================
// Configuration CRUD — typed where the api-client matches the SAPI shape exactly,
// raw where the typed method diverges (see per-method notes).
// ============================================================================

/** Branch-scoped Storage config base path (used by the kept-raw operations). */
const cfgBase = (clients: KeboolaClients, componentId: string): string =>
  `branch/${clients.branchId}/components/${componentId}/configs`;

/** MIGRATED → storage.componentsAndConfigurations.createConfiguration (POST configs). */
export const configurationCreate = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  name: string,
  description: string,
  configuration: JsonDict,
): Promise<JsonDict> =>
  clients.storage.componentsAndConfigurations.createConfiguration({
    branchId: clients.branchId,
    componentId,
    name,
    description,
    configuration: await encryptSecrets(config, clients, componentId, configuration),
  }) as Promise<JsonDict>;

/**
 * KEPT RAW: the typed `updateConfiguration` JSON-encodes the `configuration` field
 * to a string before sending; the SAPI call this server makes sends it as a nested
 * JSON object, so the typed method's request shape diverges.
 */
export const configurationUpdate = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  configuration: JsonDict,
  changeDescription: string,
  updatedName?: string,
  updatedDescription?: string,
): Promise<JsonDict> => {
  const body: JsonDict = {
    configuration: await encryptSecrets(config, clients, componentId, configuration),
    changeDescription,
  };
  if (updatedName) body.name = updatedName;
  if (updatedDescription) body.description = updatedDescription;
  return clients.rawStorage.put<JsonDict>(`${cfgBase(clients, componentId)}/${configurationId}`, {
    body,
  });
};

/** MIGRATED → storage.componentsAndConfigurations.createConfigurationRow (POST rows). */
export const configurationRowCreate = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  configId: string,
  name: string,
  description: string,
  configuration: JsonDict,
): Promise<JsonDict> =>
  clients.storage.componentsAndConfigurations.createConfigurationRow({
    branchId: clients.branchId,
    componentId,
    configId,
    name,
    description,
    configuration: await encryptSecrets(config, clients, componentId, configuration),
  }) as Promise<JsonDict>;

/**
 * KEPT RAW: the typed client has no `updateConfigurationRow` method, and the only
 * row writer (`createConfigurationRow`) cannot carry `isDisabled`/`changeDescription`
 * onto an existing row update.
 */
export const configurationRowUpdate = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  configId: string,
  rowId: string,
  configuration: JsonDict,
  changeDescription: string,
  updatedName?: string,
  updatedDescription?: string,
  isDisabled?: boolean | null,
): Promise<JsonDict> => {
  const body: JsonDict = {
    configuration: await encryptSecrets(config, clients, componentId, configuration),
    changeDescription,
  };
  if (updatedName) body.name = updatedName;
  if (updatedDescription) body.description = updatedDescription;
  if (isDisabled !== undefined && isDisabled !== null) body.isDisabled = isDisabled;
  return clients.rawStorage.put<JsonDict>(
    `${cfgBase(clients, componentId)}/${configId}/rows/${rowId}`,
    { body },
  );
};

/**
 * KEPT RAW: callers (variables resolution, update_sql_transformation) branch on
 * `RawHttpError.status === 404` for control flow; the typed `getConfiguration`
 * throws an `ApiError` of a different shape, so its error contract diverges.
 */
export const configurationDetail = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<JsonDict> =>
  clients.rawStorage.get<JsonDict>(`${cfgBase(clients, componentId)}/${configurationId}`);

/**
 * KEPT RAW: the typed client has no single-row GET; only the parent config GET
 * (which embeds rows) is exposed.
 */
export const configurationRowDetail = (
  clients: KeboolaClients,
  componentId: string,
  configId: string,
  rowId: string,
): Promise<JsonDict> =>
  clients.rawStorage.get<JsonDict>(`${cfgBase(clients, componentId)}/${configId}/rows/${rowId}`);

/**
 * KEPT RAW: paired with the kept-raw `configurationDetail` for a consistent error
 * contract (callers fall back from a 404 detail to a `RawHttpError`-typed list scan).
 */
export const configurationList = (
  clients: KeboolaClients,
  componentId: string,
): Promise<JsonDict[]> => clients.rawStorage.get<JsonDict[]>(cfgBase(clients, componentId));

// ============================================================================
// Configuration metadata helpers.
//
// KEPT RAW: configuration-metadata endpoints (`.../configs/{id}/metadata`) are not
// exposed by the typed api-client.
// ============================================================================

const metadataUpdate = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  metadata: Record<string, string>,
): Promise<JsonDict[]> =>
  clients.rawStorage.post<JsonDict[]>(
    `${cfgBase(clients, componentId)}/${configurationId}/metadata`,
    {
      body: { metadata: Object.entries(metadata).map(([key, value]) => ({ key, value })) },
    },
  );

const metadataGet = (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<MetadataItem[]> =>
  clients.rawStorage.get<MetadataItem[]>(
    `${cfgBase(clients, componentId)}/${configurationId}/metadata`,
  );

export const setCfgCreationMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<void> => {
  try {
    await metadataUpdate(clients, componentId, configurationId, { [CREATED_BY_MCP]: 'true' });
  } catch (error) {
    logger.error(
      { err: error },
      `Failed to set "${CREATED_BY_MCP}" metadata for ${configurationId}.`,
    );
  }
};

export const setCfgUpdateMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  version: number | string,
): Promise<void> => {
  const key = `${UPDATED_BY_MCP_PREFIX}${version}`;
  try {
    await metadataUpdate(clients, componentId, configurationId, { [key]: 'true' });
  } catch (error) {
    logger.error({ err: error }, `Failed to set "${key}" metadata for ${configurationId}.`);
  }
};

const setFolderMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
  folder: string,
): Promise<void> => {
  const normalized = folder.trim();
  if (!normalized) return;
  await metadataUpdate(clients, componentId, configurationId, {
    [MetadataField.CONFIGURATION_FOLDER_NAME]: normalized,
  });
};

const clearFolderMetadata = async (
  clients: KeboolaClients,
  componentId: string,
  configurationId: string,
): Promise<void> => {
  try {
    const metadata = await metadataGet(clients, componentId, configurationId);
    for (const entry of metadata) {
      if ((entry as MetadataItem).key === MetadataField.CONFIGURATION_FOLDER_NAME) {
        const id = (entry as { id?: string }).id;
        if (id === undefined) continue;
        await clients.rawStorage.delete(
          `${cfgBase(clients, componentId)}/${configurationId}/metadata/${id}`,
        );
      }
    }
  } catch {
    logger.warn(`Unable to clear folder metadata for "${componentId}"/"${configurationId}".`);
  }
};

export { setFolderMetadata, clearFolderMetadata };

/** Counts configs + distinct folders for the folder hint (port of get_config_folders). */
export const getConfigFolders = async (
  clients: KeboolaClients,
  componentId: string,
): Promise<[number, string[], boolean]> => {
  // KEPT RAW: the search endpoint's `metadataKeys[N]` bracketed-index query param is
  // not expressible through the typed `searchComponentConfigurations` query shape.
  const folderConfigs = await clients.rawStorage.get<JsonDict[]>(
    `branch/${clients.branchId}/search/component-configurations`,
    { params: { componentId, 'metadataKeys[0]': MetadataField.CONFIGURATION_FOLDER_NAME } },
  );
  const seen = new Set<string>();
  const folders: string[] = [];
  for (const cfg of folderConfigs) {
    for (const meta of (cfg.metadata as MetadataItem[]) ?? []) {
      if (meta.key === MetadataField.CONFIGURATION_FOLDER_NAME) {
        const name = (meta.value ?? '').trim();
        if (name && !seen.has(name)) {
          seen.add(name);
          folders.push(name);
        }
      }
    }
  }
  if (folderConfigs.length >= 20) return [folderConfigs.length, folders, true];
  const allConfigs = await configurationList(clients, componentId);
  const total = allConfigs.length;
  if (total < 20) return [total, [], false];
  return [total, folders, false];
};

/**
 * Resolves the workspace SQL dialect ('snowflake' | 'bigquery'). The TS port has no
 * WorkspaceManager yet, so this mirrors the essential resolution: prefer an existing
 * workspace's backend, else the token owner's defaultBackend.
 *
 * KEPT RAW: the branch `workspaces` listing is not exposed by the typed api-client.
 */
export const resolveSqlDialect = async (clients: KeboolaClients): Promise<string> => {
  try {
    const workspaces = await clients.rawStorage.get<JsonDict[]>(
      `branch/${clients.branchId}/workspaces`,
    );
    for (const ws of workspaces) {
      const backend = (ws.connection as JsonDict | undefined)?.backend as string | undefined;
      if (backend === 'snowflake' || backend === 'bigquery') return backend;
    }
  } catch {
    // fall through to token-based default
  }
  const token = await clients.storage.tokens.verify();
  const defaultBackend = (token.owner as { defaultBackend?: string } | undefined)?.defaultBackend;
  if (defaultBackend === 'snowflake' || defaultBackend === 'bigquery') return defaultBackend;
  throw new Error(`Unexpected default backend: ${defaultBackend}`);
};

// ============================================================================
// Variables management (port of _apply_vars_to_parent_cfg / apply_configuration_variables).
// ============================================================================

/**
 * Creates/updates/clears the keboola.variables config linked to a parent. Mirrors
 * _apply_vars_to_parent_cfg: mutates parentCfg with link fields and returns the id of
 * any variables config the caller must delete AFTER writing the parent.
 */
export const applyVarsToParentCfg = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  configId: string,
  variables: VariableDefinition[],
  parentCfg: JsonDict,
): Promise<{ changed: boolean; varsConfigIdToDelete: string | null }> => {
  const varsName = `Variables definition for ${componentId}/${configId}`;

  const findVarsConfig = async (): Promise<JsonDict | null> => {
    const existingId = parentCfg.variables_id as string | undefined;
    if (existingId) {
      try {
        return await configurationDetail(clients, VARIABLES_COMPONENT_ID, existingId);
      } catch (error) {
        if (!(error instanceof RawHttpError) || error.status !== 404) throw error;
      }
    }
    const all = await configurationList(clients, VARIABLES_COMPONENT_ID);
    const found = all.find((c) => c.name === varsName);
    if (!found) return null;
    return configurationDetail(clients, VARIABLES_COMPONENT_ID, String(found.id));
  };

  const existing = await findVarsConfig();

  if (variables.length === 0) {
    const varsConfigIdToDelete = existing ? String(existing.id) : null;
    let changed = false;
    for (const key of ['variables_id', 'variables_values_id']) {
      if (key in parentCfg) {
        delete parentCfg[key];
        changed = true;
      }
    }
    return { changed, varsConfigIdToDelete };
  }

  const varDefs = variables.map((v) => ({ name: v.name, type: v.type }));
  const varsConfiguration = { variables: varDefs };
  let varsConfigId: string;
  if (existing === null) {
    const created = await configurationCreate(
      config,
      clients,
      VARIABLES_COMPONENT_ID,
      varsName,
      '',
      varsConfiguration,
    );
    varsConfigId = String(created.id);
  } else {
    varsConfigId = String(existing.id);
    await configurationUpdate(
      config,
      clients,
      VARIABLES_COMPONENT_ID,
      varsConfigId,
      varsConfiguration,
      'Update variable definitions',
    );
  }

  const defaults = variables
    .filter((v) => v.default_value !== null && v.default_value !== undefined)
    .map((v) => ({ name: v.name, value: v.default_value }));
  const existingRows = ((existing ?? {}).rows as JsonDict[] | undefined) ?? [];
  const defaultRow = existingRows.find((r) => r.name === 'Default Values');
  let defaultValuesRowId: string | null = null;
  if (defaults.length > 0) {
    const rowCfg = { values: defaults };
    if (!defaultRow) {
      const createdRow = await configurationRowCreate(
        config,
        clients,
        VARIABLES_COMPONENT_ID,
        varsConfigId,
        'Default Values',
        '',
        rowCfg,
      );
      defaultValuesRowId = String(createdRow.id);
    } else {
      defaultValuesRowId = String(defaultRow.id);
      await configurationRowUpdate(
        config,
        clients,
        VARIABLES_COMPONENT_ID,
        varsConfigId,
        defaultValuesRowId,
        rowCfg,
        'Update default variable values',
      );
    }
  } else if (defaultRow) {
    await configurationRowUpdate(
      config,
      clients,
      VARIABLES_COMPONENT_ID,
      varsConfigId,
      String(defaultRow.id),
      { values: [] },
      'Clear default variable values',
    );
  }

  parentCfg.variables_id = varsConfigId;
  if (defaultValuesRowId !== null) parentCfg.variables_values_id = defaultValuesRowId;
  else delete parentCfg.variables_values_id;
  return { changed: true, varsConfigIdToDelete: null };
};

/** Full create-or-clear variables flow used by create_config / create_sql_transformation. */
export const applyConfigurationVariables = async (
  config: Config,
  clients: KeboolaClients,
  componentId: string,
  configId: string,
  variables: VariableDefinition[],
): Promise<JsonDict | null> => {
  const parent = await configurationDetail(clients, componentId, configId);
  const parentCfg = structuredClone((parent.configuration as JsonDict) ?? {});
  const { changed, varsConfigIdToDelete } = await applyVarsToParentCfg(
    config,
    clients,
    componentId,
    configId,
    variables,
    parentCfg,
  );
  if (!changed && !varsConfigIdToDelete) return null;
  const changeDescription = variables.length ? 'Link variables' : 'Unlink variables';
  let result: JsonDict | null = null;
  if (changed) {
    result = await configurationUpdate(
      config,
      clients,
      componentId,
      configId,
      parentCfg,
      changeDescription,
    );
  }
  if (varsConfigIdToDelete) {
    await deleteVariablesConfig(clients, varsConfigIdToDelete);
  }
  return result;
};

/**
 * Deletes a keboola.variables config with skip_trash semantics (two deletes).
 *
 * KEPT RAW: skip-trash requires issuing the DELETE twice; the typed
 * `deleteConfiguration` performs a single delete.
 */
export const deleteVariablesConfig = async (
  clients: KeboolaClients,
  varsConfigId: string,
): Promise<void> => {
  const path = `${cfgBase(clients, VARIABLES_COMPONENT_ID)}/${varsConfigId}`;
  await clients.rawStorage.delete(path);
  await clients.rawStorage.delete(path); // skip_trash = two deletes
};

export const nowIso = (): string => new Date().toISOString();
