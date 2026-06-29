import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { ErrorCode, McpError } from '@modelcontextprotocol/sdk/types.js';

import { createKeboolaClients } from '@/clients/keboola';
import type { Config } from '@/config';
import {
  type AuthorizationConfig,
  hasAuthorizationFilters,
  isToolNameAuthorized,
  parseAuthorizationConfig,
} from '@/mcp/authorization';
import {
  authorizeToolCall,
  filterToolsList,
  type GatedTool,
  type GatingContext,
  getProjectFeatures,
  getTokenRole,
  isSemanticToolName,
  type TokenInfo,
} from '@/mcp/filtering';
import { registerTool } from '@/mcp/tool';
import { registerPrompts } from '@/prompts';
import { registerComponentTools } from '@/tools/components';
import { registerDataAppTools } from '@/tools/data_apps';
import { registerDocTools } from '@/tools/doc';
import { registerFlowTools } from '@/tools/flow';
import { registerJobTools } from '@/tools/jobs';
import { registerOAuthTools } from '@/tools/oauth';
import { registerProjectTools } from '@/tools/project';
import { registerSearchTools } from '@/tools/search';
import { registerSemanticTools } from '@/tools/semantic';
import { registerSqlTools } from '@/tools/sql';
import { registerStorageTools } from '@/tools/storage';

// Reading package.json at build time would need JSON import assertions; keep a
// constant and bump alongside package.json until the build wiring lands.
export const SERVER_NAME = 'keboola';
export const SERVER_VERSION = '2.0.0-alpha.0';

/**
 * Builds the MCP server and registers all tools/prompts/resources.
 *
 * A single scaffold tool proves the registration → schema → TOON-serialize →
 * transport path end to end. Real tool modules land in later phases (see
 * feature_spec/mcp-typescript-rewrite/PLAN.md §4).
 */
/** Options for {@link createServer}. */
export type CreateServerOptions = {
  /**
   * Skip the project/role/branch + header authorization gating wrappers. Used by the
   * TOOLS.md generator so `tools/list` returns every registered tool regardless of the
   * (dummy) token's features/role. Never set this on a serving instance.
   */
  skipGating?: boolean;
};

export const createServer = (config: Config, options: CreateServerOptions = {}): McpServer => {
  const server = new McpServer({ name: SERVER_NAME, version: SERVER_VERSION });

  // ponytail: scaffold tool, replaced when the real tool modules are ported.
  registerTool(server, {
    name: 'get_server_info',
    title: 'Get server info',
    description: 'Returns basic information about the running Keboola MCP server.',
    annotations: { readOnlyHint: true },
    handler: () => ({
      name: SERVER_NAME,
      version: SERVER_VERSION,
      branchId: config.branchId ?? null,
      hasStorageToken: Boolean(config.storageToken),
    }),
  });

  registerProjectTools(server, config);
  registerJobTools(server, config);
  registerStorageTools(server, config);
  registerOAuthTools(server, config);
  registerComponentTools(server, config);
  registerDocTools(server, config);
  registerSearchTools(server, config);
  registerSqlTools(server, config);
  registerFlowTools(server, config);
  registerSemanticTools(server, config);
  registerDataAppTools(server, config);

  registerPrompts(server);

  if (!options.skipGating) {
    wrapToolGating(server, config);
  }

  return server;
};

type RawHandler = (request: unknown, extra: unknown) => Promise<unknown> | unknown;

/** Read-only hint map keyed by tool name, derived from registered tool annotations. */
type ReadOnlyMap = Map<string, boolean>;

/**
 * Verifies the Storage token to obtain project features + admin role. Returns an
 * empty record when verification is not possible (e.g. no credentials configured) so
 * that gating degrades to its no-feature / no-role defaults instead of failing the
 * whole `tools/list` or `tools/call` request.
 */
const verifyToken = async (config: Config): Promise<TokenInfo> => {
  try {
    const clients = createKeboolaClients(config);
    return (await clients.storage.tokens.verify()) as TokenInfo;
  } catch {
    return {};
  }
};

/**
 * Wraps the low-level `tools/list` and `tools/call` request handlers (registered by
 * the McpServer when the first tool was added) with both gating layers:
 *
 * 1. Project/role/branch gating — port of `ToolsFilteringMiddleware`.
 * 2. Header authorization — port of `ToolAuthorizationMiddleware`.
 *
 * The SDK exposes no middleware hook, so we replace the handlers in the low-level
 * `server.server` request-handler map, delegating to the originals after filtering.
 */
const wrapToolGating = (server: McpServer, config: Config): void => {
  // The low-level Server keeps its request handlers in a private `_requestHandlers`
  // Map keyed by JSON-RPC method. The SDK exposes no middleware hook, so we reach in
  // to wrap the tool handlers that McpServer registered when the tools were added.
  const handlers = (server.server as unknown as { _requestHandlers: Map<string, RawHandler> })
    ._requestHandlers;
  const originalList = handlers.get('tools/list');
  const originalCall = handlers.get('tools/call');
  if (!originalList || !originalCall) {
    throw new Error('Tool request handlers are not initialized; register tools before gating.');
  }

  const authConfig = (): AuthorizationConfig =>
    parseAuthorizationConfig({
      allowedTools: config.allowedTools,
      disallowedTools: config.disallowedTools,
      readOnlyMode: config.readOnlyMode,
    });

  const toolReadOnly = (tool: { annotations?: { readOnlyHint?: boolean } }): boolean =>
    tool.annotations?.readOnlyHint === true;

  handlers.set('tools/list', async (request, extra) => {
    const result = (await originalList(request, extra)) as {
      tools: { name: string; annotations?: { readOnlyHint?: boolean } }[];
    };

    const tokenInfo = await verifyToken(config);
    const ctx: GatingContext = {
      tokenRole: getTokenRole(tokenInfo),
      features: getProjectFeatures(tokenInfo),
      isOauth: Boolean(config.bearerToken),
      // Discovery always treats the branch as main/production (Python forces
      // branch_id=None for list requests).
      isMainBranch: true,
    };

    const gated: GatedTool[] = result.tools.map((t) => ({
      name: t.name,
      readOnly: toolReadOnly(t),
    }));
    const allowedByProject = new Set(filterToolsList(gated, ctx).map((t) => t.name));

    const auth = authConfig();
    const filtered = result.tools.filter((t) => {
      if (!allowedByProject.has(t.name)) return false;
      if (hasAuthorizationFilters(auth) && !isToolNameAuthorized(t.name, toolReadOnly(t), auth)) {
        return false;
      }
      return true;
    });

    return { ...result, tools: filtered };
  });

  // Per-call gating needs each tool's read-only hint by name; derive it from the
  // registered tools via the (unwrapped) list handler once, lazily.
  let readOnlyMap: ReadOnlyMap | undefined;
  const getReadOnlyMap = async (extra: unknown): Promise<ReadOnlyMap> => {
    if (readOnlyMap) return readOnlyMap;
    const listed = (await originalList({ method: 'tools/list', params: {} }, extra)) as {
      tools: { name: string; annotations?: { readOnlyHint?: boolean } }[];
    };
    readOnlyMap = new Map(listed.tools.map((t) => [t.name, toolReadOnly(t)]));
    return readOnlyMap;
  };

  handlers.set('tools/call', async (request, extra) => {
    const params = (request as { params?: { name?: string } }).params ?? {};
    const toolName = params.name ?? '';

    const roMap = await getReadOnlyMap(extra);
    // Unknown tools fall through to the original handler, which reports them as
    // "not found" (parity: the gating layers only decide on known tools).
    const isReadOnly = roMap.get(toolName) ?? false;
    const isKnown = roMap.has(toolName);

    if (isKnown) {
      const tokenInfo = await verifyToken(config);

      // Header authorization first (port of ToolAuthorizationMiddleware.on_call_tool).
      const auth = authConfig();
      if (hasAuthorizationFilters(auth) && !isToolNameAuthorized(toolName, isReadOnly, auth)) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          `Access denied: The tool "${toolName}" is not authorized for this client. ` +
            `Contact your administrator to request access.`,
        );
      }

      // Project/role/branch gating (port of ToolsFilteringMiddleware.on_call_tool).
      const denial = authorizeToolCall({
        toolName,
        isReadOnly,
        isSemantic: isSemanticToolName(toolName),
        tokenRole: getTokenRole(tokenInfo),
        features: getProjectFeatures(tokenInfo),
        isOauth: Boolean(config.bearerToken),
        isMainBranch: config.branchId === undefined,
      });
      if (denial) {
        throw new McpError(ErrorCode.InvalidRequest, denial);
      }
    }

    return originalCall(request, extra);
  });
};
