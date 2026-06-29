/**
 * Header-based tool authorization, ported 1:1 from the Python
 * `ToolAuthorizationMiddleware` (`authorization.py`).
 *
 * Authorization is configured via HTTP headers, surfaced onto the per-request Config
 * (see `config.ts`):
 * - `X-Allowed-Tools`: comma-separated allow list of tool names
 * - `X-Disallowed-Tools`: comma-separated deny list (removed from the allowed set)
 * - `X-Read-Only-Mode`: "true"/"1"/"yes" restricts to tools with `readOnlyHint=true`
 *
 * These headers are intended to be injected by infrastructure/proxy layers rather than
 * set directly by end clients.
 */

/** Parsed header-authorization configuration for a request. */
export type AuthorizationConfig = {
  /** Allow list, or `null` when no `X-Allowed-Tools` restriction is present. */
  allowedTools: Set<string> | null;
  /** Deny list, or `null` when no `X-Disallowed-Tools` restriction is present. */
  disallowedTools: Set<string> | null;
  /** Whether `X-Read-Only-Mode` is enabled. */
  readOnlyMode: boolean;
};

const READ_ONLY_TRUTHY = new Set(['true', '1', 'yes']);

const parseToolSet = (raw: string | undefined): Set<string> | null => {
  if (!raw) return null;
  const parsed = new Set(
    raw
      .split(',')
      .map((t) => t.trim())
      .filter((t) => t.length > 0),
  );
  return parsed.size > 0 ? parsed : null;
};

/** Builds the authorization config from the raw header values. */
export const parseAuthorizationConfig = (raw: {
  allowedTools?: string;
  disallowedTools?: string;
  readOnlyMode?: string;
}): AuthorizationConfig => ({
  allowedTools: parseToolSet(raw.allowedTools),
  disallowedTools: parseToolSet(raw.disallowedTools),
  readOnlyMode: READ_ONLY_TRUTHY.has((raw.readOnlyMode ?? '').toLowerCase()),
});

/** Whether any header-authorization filter is active. */
export const hasAuthorizationFilters = (config: AuthorizationConfig): boolean =>
  config.allowedTools !== null || config.disallowedTools !== null || config.readOnlyMode;

/**
 * Header-based authorization decision for a single tool. Ported 1:1 from
 * `_is_tool_name_authorized`: disallow list first, then read-only mode, then allow list.
 */
export const isToolNameAuthorized = (
  toolName: string,
  isReadOnly: boolean,
  config: AuthorizationConfig,
): boolean => {
  const { allowedTools, disallowedTools, readOnlyMode } = config;
  if (disallowedTools && disallowedTools.has(toolName)) return false;
  if (readOnlyMode && !isReadOnly) return false;
  if (allowedTools !== null && !allowedTools.has(toolName)) return false;
  return true;
};
