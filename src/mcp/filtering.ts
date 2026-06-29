/**
 * Project / role / branch tool gating, ported 1:1 from the Python
 * `ToolsFilteringMiddleware` (`mcp.py`).
 *
 * The MCP TypeScript SDK has no FastMCP-style middleware, so the gating is applied
 * by wrapping the low-level `tools/list` and `tools/call` request handlers after the
 * tools are registered (see `wrapToolGating` in `server.ts`). This module is the
 * single source of truth for the project-feature / token-role / branch rules — the
 * same `authorizeToolCall` decision is used for both discovery (list) and execution
 * (call).
 */

export const SEMANTIC_TOOLING_FEATURE = 'mcp-semantic-tooling';

export const SEMANTIC_TOOL_NAMES = new Set<string>([
  'search_semantic_context',
  'get_semantic_context',
  'get_semantic_schema',
  'validate_semantic_query',
]);

/**
 * Data app tools are supported only in the main/production branch. This single set is
 * the source of truth for both the list filter and the call guard — keeping them in
 * sync is what prevents a new (possibly destructive) data app tool from leaking onto
 * non-main branches.
 */
export const DATA_APP_BRANCH_GATED_TOOLS = new Set<string>([
  'modify_streamlit_data_app',
  'modify_python_js_data_app',
  'create_python_js_data_app_git_credential',
  'get_data_apps',
  'deploy_data_app',
  'delete_python_js_data_app_draft',
]);

export const MODIFY_FLOW_TOOL_NAME = 'modify_flow';
export const UPDATE_FLOW_TOOL_NAME = 'update_flow';

/** Token info as returned by the Storage API `tokens/verify` endpoint (loosely typed). */
export type TokenInfo = Record<string, unknown>;

/** Minimal tool shape the gating needs (name + read-only hint). */
export type GatedTool = {
  name: string;
  readOnly: boolean;
};

/** Whether the tool belongs to semantic tooling (name-based; TS tools carry no tags). */
export const isSemanticToolName = (name: string): boolean => SEMANTIC_TOOL_NAMES.has(name);

const asRecord = (value: unknown): Record<string, unknown> | undefined =>
  value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

export const getProjectFeatures = (tokenInfo: TokenInfo): Set<string> => {
  const owner = asRecord(tokenInfo.owner);
  const features = owner?.features;
  if (!Array.isArray(features)) return new Set();
  return new Set(features.filter((f): f is string => typeof f === 'string' && f.length > 0));
};

export const getTokenRole = (tokenInfo: TokenInfo): string => {
  const admin = asRecord(tokenInfo.admin);
  const role = admin?.role;
  return typeof role === 'string' ? role : '';
};

/**
 * Context for a gating decision. `isMainBranch` is `branchId === undefined`;
 * `isOauth` is whether a bearer token is present.
 */
export type GatingContext = {
  tokenRole: string;
  features: Set<string>;
  isOauth: boolean;
  isMainBranch: boolean;
};

/**
 * Filters a tool list for `tools/list`. Ported from `on_list_tools`. Branch is
 * always treated as main during discovery (the caller passes `isMainBranch=true`),
 * matching the Python behavior of forcing `branch_id=None` for list requests.
 */
export const filterToolsList = (tools: GatedTool[], ctx: GatingContext): GatedTool[] => {
  const { features, isOauth, isMainBranch } = ctx;
  const tokenRole = ctx.tokenRole.toLowerCase();
  let result = tools;

  if (features.has('hide-conditional-flows')) {
    result = result.filter((t) => t.name !== 'create_conditional_flow');
  } else {
    result = result.filter((t) => t.name !== 'create_flow');
  }

  // Show modify_flow to admin/share or OAuth users; update_flow to everyone else.
  if (tokenRole === 'admin' || tokenRole === 'share' || isOauth) {
    result = result.filter((t) => t.name !== UPDATE_FLOW_TOOL_NAME);
  } else {
    result = result.filter((t) => t.name !== MODIFY_FLOW_TOOL_NAME);
  }

  if (!isMainBranch) {
    result = result.filter((t) => !DATA_APP_BRANCH_GATED_TOOLS.has(t.name));
  }

  if (tokenRole === 'readonly') {
    result = result.filter((t) => t.readOnly);
  }

  if (!features.has(SEMANTIC_TOOLING_FEATURE)) {
    result = result.filter((t) => !isSemanticToolName(t.name));
  }

  return result;
};

/**
 * Decides whether a call to `toolName` is allowed. Ported 1:1 from
 * `authorize_tool_call`. Returns a denial message, or `null` if allowed.
 */
export const authorizeToolCall = (params: {
  toolName: string;
  isReadOnly: boolean;
  isSemantic: boolean;
  tokenRole: string;
  features: Set<string>;
  isOauth: boolean;
  isMainBranch: boolean;
}): string | null => {
  const { toolName, isReadOnly, isSemantic, features, isOauth, isMainBranch } = params;
  const tokenRole = params.tokenRole.toLowerCase();

  if (tokenRole === 'readonly' && !isReadOnly) {
    return (
      `Access denied: The tool "${toolName}" requires write permissions. ` +
      `Your current role (${tokenRole}) only allows read-only operations. ` +
      `Contact your administrator to request write access.`
    );
  }

  if (!features.has(SEMANTIC_TOOLING_FEATURE) && isSemantic) {
    return (
      `The tool "${toolName}" is not available in this project. ` +
      'Please ask Keboola support to enable "Semantic Layer Tooling" feature.'
    );
  }

  if (features.has('hide-conditional-flows')) {
    if (toolName === 'create_conditional_flow') {
      return (
        'The "create_conditional_flow" tool is not available in this project. ' +
        'Please ask Keboola support to enable "Conditional Flows" feature ' +
        'or use "create_flow" tool instead.'
      );
    }
  } else {
    if (toolName === 'create_flow') {
      return (
        'The "create_flow" tool is not available in this project. ' +
        'This project uses "Conditional Flows", ' +
        'please use "create_conditional_flow" tool instead.'
      );
    }
  }

  if (tokenRole === 'admin' || tokenRole === 'share' || isOauth) {
    if (toolName === UPDATE_FLOW_TOOL_NAME) {
      return (
        'The "update_flow" tool is not available for admin/OAuth tokens. ' +
        `Use "${MODIFY_FLOW_TOOL_NAME}" to manage schedules instead.`
      );
    }
  } else {
    if (toolName === MODIFY_FLOW_TOOL_NAME) {
      return (
        `The "${MODIFY_FLOW_TOOL_NAME}" tool is not available for this token. ` +
        `Use "${UPDATE_FLOW_TOOL_NAME}" to update flow configuration instead.`
      );
    }
  }

  if (DATA_APP_BRANCH_GATED_TOOLS.has(toolName) && !isMainBranch) {
    return 'Data apps are supported only in the main production branch.';
  }

  return null;
};
