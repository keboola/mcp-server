/**
 * `/preview/configuration` custom HTTP endpoint — a faithful port of the Python
 * `preview.py` (`preview_config_diff`).
 *
 * It simulates a config-changing MCP tool ("update_config", "update_config_row", …)
 * WITHOUT writing anything, returning the original vs. updated configuration so a UI
 * can show a diff before the user commits. The route lives outside the MCP tool-call
 * path, so it re-applies the exact same authorization the real tool call would get:
 *
 * 1. Header authorization (`X-Allowed-Tools` / `X-Disallowed-Tools` / `X-Read-Only-Mode`)
 *    — port of `ToolAuthorizationMiddleware`.
 * 2. Project-feature / token-role / branch gating via {@link authorizeToolCall} — the
 *    AI-3438 hardening: without it a restricted caller could drive a write tool's
 *    preview (e.g. a data-app tool on a non-main branch, or any write tool with a
 *    read-only token).
 *
 * Everything runs against a READ-ONLY Keboola client: the raw clients are built with
 * `readonly: true`, so any accidental non-GET request throws instead of mutating.
 */

import type { ErrorObject } from 'ajv';
// zod emits draft 2020-12 JSON Schema, so use the matching Ajv build.
import Ajv2020 from 'ajv/dist/2020.js';
import { z } from 'zod';

import { createKeboolaClients, type KeboolaClients } from '@/clients/keboola';
import { createRawClient } from '@/clients/raw';
import { deriveServiceUrls } from '@/clients/urls';
import type { Config } from '@/config';
import { logger } from '@/logger';
import {
  hasAuthorizationFilters,
  isToolNameAuthorized,
  parseAuthorizationConfig,
} from '@/mcp/authorization';
import {
  authorizeToolCall,
  getProjectFeatures,
  getTokenRole,
  isSemanticToolName,
  type TokenInfo,
} from '@/mcp/filtering';
import { createServer } from '@/server';
import { configPreviewInternals } from '@/tools/components';
import type { ConfigParamUpdate } from '@/tools/components.model';

type JsonDict = Record<string, unknown>;

/** Error that carries an HTTP status code, mirroring the Python JSONResponse status codes. */
export class PreviewHttpError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = 'PreviewHttpError';
  }
}

// ---------------------------------------------------------------------------
// Request / response shapes (port of the Pydantic models, including alias handling).
// ---------------------------------------------------------------------------

/** Reads the first present alias from a record, mirroring Pydantic `AliasChoices`. */
const pickAlias = (body: JsonDict, aliases: string[]): unknown => {
  for (const alias of aliases) {
    if (alias in body) return body[alias];
  }
  return undefined;
};

export type PreviewConfigDiffRq = {
  toolName: string;
  toolParams: JsonDict;
};

/** Parses the POST body into the request model (port of `PreviewConfigDiffRq.model_validate`). */
export const parsePreviewRequest = (raw: unknown): PreviewConfigDiffRq => {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new PreviewHttpError('Request body must be a JSON object.', 400);
  }
  const body = raw as JsonDict;
  const toolName = pickAlias(body, ['toolName', 'tool_name', 'tool-name', 'ToolName']);
  const toolParams = pickAlias(body, ['toolParams', 'tool_params', 'tool-params', 'ToolParams']);
  if (typeof toolName !== 'string' || toolName.length === 0) {
    throw new PreviewHttpError('Field "toolName" is required and must be a string.', 400);
  }
  if (!toolParams || typeof toolParams !== 'object' || Array.isArray(toolParams)) {
    throw new PreviewHttpError('Field "toolParams" is required and must be an object.', 400);
  }
  return { toolName, toolParams: toolParams as JsonDict };
};

export type ConfigCoordinates = {
  componentId?: string | null;
  configurationId?: string | null;
  configurationRowId?: string | null;
};

export type PreviewConfigDiffResp = {
  coordinates: ConfigCoordinates;
  originalConfig: JsonDict | null;
  updatedConfig: JsonDict | null;
  isValid: boolean;
  validationErrors?: string[] | null;
};

const toStringOrNull = (value: unknown): string | null =>
  value === undefined || value === null ? null : String(value);

/**
 * Serializes the response with camelCase aliases and `exclude_none` semantics
 * (drops null/undefined entries, like Python's `model_dump(by_alias=True, exclude_none=True)`).
 */
const serializeResponse = (resp: PreviewConfigDiffResp): JsonDict => {
  const coordinates: JsonDict = {};
  if (resp.coordinates.componentId != null) coordinates.componentId = resp.coordinates.componentId;
  if (resp.coordinates.configurationId != null) {
    coordinates.configurationId = resp.coordinates.configurationId;
  }
  if (resp.coordinates.configurationRowId != null) {
    coordinates.configurationRowId = resp.coordinates.configurationRowId;
  }

  const out: JsonDict = { coordinates, isValid: resp.isValid };
  if (resp.originalConfig != null) out.originalConfig = resp.originalConfig;
  if (resp.updatedConfig != null) out.updatedConfig = resp.updatedConfig;
  if (resp.validationErrors != null) out.validationErrors = resp.validationErrors;
  return out;
};

// ---------------------------------------------------------------------------
// Tool metadata lookup (input schema + read-only hint), parity with the Python
// `app.state.mcp_tools_input_schema` / `mcp_read_only_tools` built from list_tools().
// ---------------------------------------------------------------------------

type RegisteredTool = {
  inputSchema?: z.ZodType;
  annotations?: { readOnlyHint?: boolean };
};

type ToolMetadata = {
  /** JSON Schema (draft) for the tool input, or undefined when the tool has no schema. */
  inputSchema?: JsonDict;
  isReadOnly: boolean;
};

/**
 * Enumerates the registered tools (unfiltered, like Python's `list_tools(run_middleware=False)`)
 * and returns the JSON Schema + read-only hint for the named tool. Building a server is the
 * single source of truth for tool schemas; no per-tool schema is duplicated here.
 */
const lookupToolMetadata = (config: Config, toolName: string): ToolMetadata | undefined => {
  const server = createServer(config) as unknown as {
    _registeredTools: Record<string, RegisteredTool>;
  };
  const tool = server._registeredTools[toolName];
  if (!tool) return undefined;
  // `io: 'input'` so fields with defaults stay optional (matching what the tool call
  // actually accepts, and the schema the MCP SDK advertises in tools/list).
  const inputSchema = tool.inputSchema
    ? (z.toJSONSchema(tool.inputSchema, { io: 'input' }) as JsonDict)
    : undefined;
  return { inputSchema, isReadOnly: tool.annotations?.readOnlyHint === true };
};

// ---------------------------------------------------------------------------
// Read-only client.
// ---------------------------------------------------------------------------

/**
 * Builds a Keboola client whose raw HTTP clients reject any non-GET request. The
 * preview path only reads (config detail, component fetch) + validates in memory, so a
 * read-only client is both sufficient and a safety net against accidental writes —
 * mirroring the Python `create_session_state(..., readonly=True)`.
 */
const createReadOnlyClients = (config: Config): KeboolaClients => {
  const clients = createKeboolaClients(config);
  if (!config.storageApiUrl) {
    throw new Error('Storage API URL is not configured.');
  }
  const urls = deriveServiceUrls(config.storageApiUrl);
  const token = config.storageToken!;
  const storageToken = config.bearerToken ? `Bearer ${config.bearerToken}` : token;
  return {
    ...clients,
    rawStorage: createRawClient({
      baseUrl: `${urls.storage}/v2/storage`,
      token: storageToken,
      readonly: true,
    }),
    rawQueue: createRawClient({ baseUrl: urls.queue, token, readonly: true }),
    rawAi: createRawClient({ baseUrl: urls.ai, token, readonly: true }),
    rawSyncActions: createRawClient({ baseUrl: urls.syncActions, token, readonly: true }),
  };
};

// ---------------------------------------------------------------------------
// Schema validation (port of `_validate_tool_params`, using ajv in place of jsonschema).
// ---------------------------------------------------------------------------

const formatValidationError = (toolName: string, error: ErrorObject): string => {
  // `instancePath` like "/parameter_updates/1" -> "parameter_updates.1" (parity with
  // Python's '.'.join(e.path)).
  const field = error.instancePath.replace(/^\//, '').replaceAll('/', '.');
  const message = error.message ?? 'is invalid';
  const detail = field ? `${field}: ${message}` : message;
  return `Found 1 validation error for ${toolName}:\n${detail}`;
};

/**
 * Validates raw tool params against the tool's JSON Schema. Returns the error message
 * (already formatted) when invalid, or null when valid. Mirrors `_validate_tool_params`.
 */
const validateToolParams = (
  toolName: string,
  toolParams: JsonDict,
  schema: JsonDict,
): string | null => {
  try {
    const ajv = new Ajv2020({ allErrors: false, strict: false });
    const validate = ajv.compile(schema);
    if (validate(toolParams)) return null;
    const first = validate.errors?.[0];
    if (!first) return `Found 1 validation error for ${toolName}:\nunknown validation error`;
    return formatValidationError(toolName, first);
  } catch (error) {
    logger.error({ err: error, tool: toolName }, '[preview] Invalid tool schema');
    return 'Internal error: Invalid tool schema';
  }
};

// ---------------------------------------------------------------------------
// Coordinate extraction (port of `_extract_coordinates`).
// ---------------------------------------------------------------------------

const UPDATE_FLOW_TOOL_NAME = 'update_flow';
const MODIFY_FLOW_TOOL_NAME = 'modify_flow';
const DATA_APP_COMPONENT_ID = 'keboola.data-apps';

const extractCoordinates = (toolName: string, toolParams: JsonDict): ConfigCoordinates => {
  switch (toolName) {
    case 'update_config':
      return {
        componentId: toStringOrNull(toolParams.component_id),
        configurationId: toStringOrNull(toolParams.configuration_id),
      };
    case 'update_config_row':
      return {
        componentId: toStringOrNull(toolParams.component_id),
        configurationId: toStringOrNull(toolParams.configuration_id),
        configurationRowId: toStringOrNull(toolParams.configuration_row_id),
      };
    case UPDATE_FLOW_TOOL_NAME:
    case MODIFY_FLOW_TOOL_NAME:
      return {
        componentId: toStringOrNull(toolParams.flow_type),
        configurationId: toStringOrNull(toolParams.configuration_id),
      };
    case 'modify_streamlit_data_app':
      return {
        componentId: DATA_APP_COMPONENT_ID,
        configurationId: toStringOrNull(toolParams.configuration_id),
      };
    case 'update_sql_transformation':
      // The Python endpoint resolves the component id from the workspace SQL dialect.
      // The diff for this tool is not implemented in the TS port (see DIFF_IMPLEMENTED_TOOLS),
      // so only the configuration id is surfaced here.
      return { configurationId: toStringOrNull(toolParams.configuration_id) };
    default:
      throw new PreviewHttpError(`Invalid tool name: "${toolName}"`, 400);
  }
};

// ---------------------------------------------------------------------------
// Mutator dispatch (port of `_prepare_mutator` + the diff assembly).
// ---------------------------------------------------------------------------

/** The set of config-changing tools the preview endpoint can diff. */
const SUPPORTED_TOOLS = new Set<string>([
  'update_config',
  'update_config_row',
  'update_sql_transformation',
  UPDATE_FLOW_TOOL_NAME,
  MODIFY_FLOW_TOOL_NAME,
  'modify_streamlit_data_app',
]);

/** Tools whose original/updated config diff is implemented in the TypeScript port. */
const DIFF_IMPLEMENTED_TOOLS = new Set<string>(['update_config', 'update_config_row']);

/**
 * Computes (originalConfig, newConfiguration) for the config tools whose mutation
 * internals are factored out and safe to run read-only. Returns the original config
 * record and the rebuilt `configuration` object.
 */
const computeConfigDiff = async (
  config: Config,
  clients: KeboolaClients,
  rq: PreviewConfigDiffRq,
): Promise<{ original: JsonDict; newConfiguration: JsonDict }> => {
  const params = rq.toolParams;
  const componentId = String(params.component_id ?? '');
  const configurationId = String(params.configuration_id ?? '');
  const parameterUpdates =
    (params.parameter_updates as ConfigParamUpdate[] | null | undefined) ?? null;
  const storage = (params.storage as JsonDict | null | undefined) ?? null;
  const processorsBefore = (params.processors_before as JsonDict[] | null | undefined) ?? null;
  const processorsAfter = (params.processors_after as JsonDict[] | null | undefined) ?? null;

  if (rq.toolName === 'update_config') {
    const original = await configPreviewInternals.configurationDetail(
      clients,
      componentId,
      configurationId,
    );
    const newConfiguration = await configPreviewInternals.buildUpdatedConfigPayload({
      config,
      clients,
      componentId,
      configurationId,
      parameterUpdates,
      storage,
      processorsBefore,
      processorsAfter,
      isRow: false,
    });
    return { original, newConfiguration };
  }

  // update_config_row
  const configurationRowId = String(params.configuration_row_id ?? '');
  const original = await configPreviewInternals.configurationRowDetail(
    clients,
    componentId,
    configurationId,
    configurationRowId,
  );
  const newConfiguration = await configPreviewInternals.buildUpdatedConfigPayload({
    config,
    clients,
    componentId,
    configurationId,
    configurationRowId,
    parameterUpdates,
    storage,
    processorsBefore,
    processorsAfter,
    isRow: true,
  });
  return { original, newConfiguration };
};

/**
 * Assembles the updated config from the original + new `configuration` and the
 * top-level field overrides (name/description/changeDescription/isDisabled), mirroring
 * the diff assembly block in `preview_config_diff`.
 */
const assembleUpdatedConfig = (
  original: JsonDict,
  newConfiguration: JsonDict,
  toolParams: JsonDict,
): JsonDict => {
  const updated = structuredClone(original);
  updated.configuration = newConfiguration;
  if (toolParams.name) updated.name = toolParams.name;
  if (toolParams.description) updated.description = toolParams.description;
  if (toolParams.is_disabled !== undefined && toolParams.is_disabled !== null) {
    updated.isDisabled = toolParams.is_disabled;
  }
  if (toolParams.change_description) updated.changeDescription = toolParams.change_description;
  return updated;
};

// ---------------------------------------------------------------------------
// Main handler.
// ---------------------------------------------------------------------------

/**
 * Verifies the Storage token to read project features + admin role. Throwing here would
 * surface as a 500; on failure we degrade to no-feature / no-role defaults so gating is
 * still applied (and a misconfigured token simply fails the project gate).
 */
const verifyToken = async (clients: KeboolaClients): Promise<TokenInfo> => {
  try {
    return (await clients.storage.tokens.verify()) as TokenInfo;
  } catch {
    return {};
  }
};

/**
 * Runs the full preview flow for an already-parsed request and returns the response
 * payload, throwing {@link PreviewHttpError} for the 4xx cases. The Hono route is a thin
 * wrapper around this.
 */
export const runPreviewConfigDiff = async (
  config: Config,
  rq: PreviewConfigDiffRq,
): Promise<JsonDict> => {
  const meta = lookupToolMetadata(config, rq.toolName);
  const isReadOnly = meta?.isReadOnly ?? false;

  // 1) Header authorization (port of ToolAuthorizationMiddleware). This route runs
  // outside the MCP middleware chain, so enforce the same headers explicitly.
  const auth = parseAuthorizationConfig({
    allowedTools: config.allowedTools,
    disallowedTools: config.disallowedTools,
    readOnlyMode: config.readOnlyMode,
  });
  if (hasAuthorizationFilters(auth) && !isToolNameAuthorized(rq.toolName, isReadOnly, auth)) {
    logger.info(`[preview] Tool authorization denied (headers): ${rq.toolName}`);
    throw new PreviewHttpError(`The tool "${rq.toolName}" is not authorized for this client.`, 403);
  }

  // Log only non-sensitive metadata; toolParams can carry user-supplied secrets.
  logger.info(
    `[preview] toolName=${rq.toolName} paramKeys=${Object.keys(rq.toolParams).sort().join(',')}`,
  );

  const clients = createReadOnlyClients(config);

  // 2) Project-feature / token-role / branch gating (the AI-3438 hardening) via the
  // exact same authorizeToolCall decision a real MCP tool call uses.
  const tokenInfo = await verifyToken(clients);
  const denial = authorizeToolCall({
    toolName: rq.toolName,
    isReadOnly,
    isSemantic: isSemanticToolName(rq.toolName),
    tokenRole: getTokenRole(tokenInfo),
    features: getProjectFeatures(tokenInfo),
    isOauth: Boolean(config.bearerToken),
    isMainBranch: config.branchId === undefined,
  });
  if (denial) {
    logger.info(`[preview] Tool authorization denied (project/role/branch): ${rq.toolName}`);
    throw new PreviewHttpError(denial, 403);
  }

  const coordinates = extractCoordinates(rq.toolName, rq.toolParams);

  // 3) Validate the params against the tool's input schema. A schema failure returns a
  // 200 with isValid=false (empty configs) — the KAI backend relies on that shape.
  if (meta?.inputSchema) {
    const validationError = validateToolParams(rq.toolName, rq.toolParams, meta.inputSchema);
    if (validationError) {
      return serializeResponse({
        coordinates,
        originalConfig: {},
        updatedConfig: {},
        isValid: false,
        validationErrors: [validationError],
      });
    }
  } else {
    logger.warn(`[preview] No input schema found for tool "${rq.toolName}"`);
  }

  if (!SUPPORTED_TOOLS.has(rq.toolName)) {
    throw new PreviewHttpError(`Invalid tool name: "${rq.toolName}"`, 400);
  }

  // The mutation internals for update_sql_transformation / update_flow / modify_flow /
  // modify_streamlit_data_app are not factored out as reusable, write-free functions in
  // the TypeScript port (they live inline in the tool handlers and perform the write),
  // so their read-only diff is not yet available. Authorization, validation and
  // coordinate extraction above still run for them.
  if (!DIFF_IMPLEMENTED_TOOLS.has(rq.toolName)) {
    throw new PreviewHttpError(
      `Configuration diff preview for tool "${rq.toolName}" is not supported by this server.`,
      400,
    );
  }

  // 4) Run the (read-only) mutator and assemble the diff. A value/validation error
  // returns a 200 with isValid=false (parity with the Python except block).
  try {
    const { original, newConfiguration } = await computeConfigDiff(config, clients, rq);
    const updatedConfig = assembleUpdatedConfig(original, newConfiguration, rq.toolParams);
    return serializeResponse({
      coordinates,
      originalConfig: original,
      updatedConfig,
      isValid: true,
      validationErrors: null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.info(`[preview] ${message}`);
    return serializeResponse({
      coordinates,
      originalConfig: {},
      updatedConfig: {},
      isValid: false,
      validationErrors: [message],
    });
  }
};
