/**
 * Validated, process-level deployment environment — segregated from the
 * per-request runtime Config.
 *
 * Two distinct concepts, deliberately kept apart:
 *
 * - **This module (`env`)**: process/infra variables resolved once at boot —
 *   listen host/port, log level, app env/version, dd-trace, OAuth client
 *   credentials, and `HOSTNAME_SUFFIX` (the deploy convention that derives the
 *   Storage/OAuth/MCP URLs). These are set at `docker run` / k8s.
 * - **`Config` (config.ts)**: the per-request tenant context (Storage token,
 *   branch, workspace) that in HTTP mode arrives via `X-*` headers / bearer
 *   token on each request. It is NOT validated here, because the server must be
 *   able to boot in multi-tenant HTTP mode with no Storage token present.
 *
 * Build vs. run: validation is skipped during the image build (`npm run build`
 * runs with `SKIP_ENV_VALIDATION=1`), so a build never needs runtime secrets.
 * Mirrors the kai-agent `env.ts` (createEnv) pattern, using zod directly since
 * it is already a dependency.
 */
import { z } from 'zod';

import type { Config } from '@/config';

const boolFromString = z.enum(['true', 'false']).transform((v) => v === 'true');

/** Schema for process-level deployment env. Everything optional/defaulted so the
 * server can boot in HTTP multi-tenant mode without per-request secrets. */
const envSchema = z.object({
  // Listener (HTTP transport).
  HOST: z.string().default('localhost'),
  PORT: z.coerce.number().int().positive().default(8000),

  // Logging + app identity (port of ServerRuntimeInfo app_env/app_version).
  LOG_LEVEL: z.string().default('INFO'),
  APP_ENV: z.string().default('local'),
  APP_VERSION: z.string().default('DEV'),

  /**
   * Deploy convention: when the explicit Storage/OAuth/MCP URLs are absent, they
   * are derived from this suffix (e.g. `keboola.com` →
   * `https://connection.keboola.com`). Port of server.py's HOSTNAME_SUFFIX use.
   */
  HOSTNAME_SUFFIX: z.string().optional(),

  // OAuth provider (process-level; tenant tokens are per-request). Presence of
  // both client id + secret enables the provider (HTTP only).
  KBC_OAUTH_CLIENT_ID: z.string().optional(),
  KBC_OAUTH_CLIENT_SECRET: z.string().optional(),
  KBC_OAUTH_SERVER_URL: z.string().optional(),
  KBC_OAUTH_SCOPE: z.string().optional(),
  KBC_MCP_SERVER_URL: z.string().optional(),
  KBC_JWT_SECRET: z.string().optional(),

  // Docs-search index (pgvector). All optional: when DATABASE_URL (or the embedder
  // credentials) is absent, the docs_query / find_component_id tools gate off and the
  // rest of the server is unaffected. The index is read-only from the MCP's side —
  // it is built out-of-band by a cron job. See feature_spec/docs-search-pgvector/.
  DATABASE_URL: z.string().optional(),
  // DOCS_EMBEDDER_MODEL selects the embedder: 'stub' (offline CI), 'local' (in-process
  // HuggingFace/ONNX — no service/key), or a remote model name (needs ENDPOINT+API_KEY).
  // DOCS_EMBEDDER_DIM must match the model output AND the pgvector column dim (defaults:
  // 3072 for stub/remote, 384 for local). DOCS_EMBEDDER_LOCAL_MODEL overrides the local HF id.
  DOCS_EMBEDDER_ENDPOINT: z.string().optional(),
  DOCS_EMBEDDER_API_KEY: z.string().optional(),
  DOCS_EMBEDDER_MODEL: z.string().optional(),
  DOCS_EMBEDDER_LOCAL_MODEL: z.string().optional(),
  DOCS_EMBEDDER_DIM: z.coerce.number().int().positive().optional(),
  // LLM for answerQuestion synthesis. Optional: without it, docs_query falls back to
  // returning the retrieved documentation snippets rather than a synthesized answer.
  DOCS_LLM_ENDPOINT: z.string().optional(),
  DOCS_LLM_API_KEY: z.string().optional(),
  DOCS_LLM_MODEL: z.string().optional(),

  // Datadog APM (consumed by dd-trace via NODE_OPTIONS in the image; listed so
  // the contract is explicit and validated).
  DD_SERVICE: z.string().optional(),
  DD_ENV: z.string().optional(),
  DD_VERSION: z.string().optional(),
  DD_AGENT_HOST: z.string().optional(),
  DD_LOGS_INJECTION: boolFromString.optional(),
});

export type Env = z.infer<typeof envSchema>;

const shouldSkip = (raw: NodeJS.ProcessEnv): boolean =>
  raw.SKIP_ENV_VALIDATION === '1' || raw.SKIP_ENV_VALIDATION === 'true';

/** Parses + validates process env (treating empty strings as unset). On a build
 * (`SKIP_ENV_VALIDATION`), returns parsed-with-defaults without throwing. */
export const parseEnv = (raw: NodeJS.ProcessEnv = process.env): Env => {
  const cleaned: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(raw)) {
    cleaned[key] = value === '' ? undefined : value;
  }
  if (shouldSkip(raw)) {
    // Build phase: never fail on runtime vars. Use valid values if they parse,
    // otherwise fall back to all-defaults — the build doesn't consume them.
    const skipped = envSchema.safeParse(cleaned);
    return skipped.success ? skipped.data : envSchema.parse({});
  }
  const result = envSchema.safeParse(cleaned);
  if (!result.success) {
    const issues = result.error.issues.map((i) => `  ${i.path.join('.')}: ${i.message}`).join('\n');
    throw new Error(`Invalid deployment environment:\n${issues}`);
  }
  return result.data;
};

/**
 * Applies the HOSTNAME_SUFFIX-based deployment defaults to a base Config — a
 * faithful port of server.py's create_server() derivation:
 *   - no storageApiUrl + suffix          → https://connection.<suffix>
 *   - oauth configured, no oauthServerUrl → https://connection.<suffix>
 *   - oauth configured, no mcpServerUrl   → https://mcp.<suffix>
 *   - oauth configured, no oauthScope     → "email"
 */
export const applyDeploymentDefaults = (config: Config, env: Env): Config => {
  const patch: Record<string, string | undefined> = {};
  const suffix = env.HOSTNAME_SUFFIX;

  if (!config.storageApiUrl && suffix) {
    patch.storageApiUrl = `https://connection.${suffix}`;
  }

  const oauthConfigured = Boolean(config.oauthClientId && config.oauthClientSecret);
  if (oauthConfigured) {
    if (!config.oauthServerUrl && suffix) patch.oauthServerUrl = `https://connection.${suffix}`;
    if (!config.mcpServerUrl && suffix) patch.mcpServerUrl = `https://mcp.${suffix}`;
    if (!config.oauthScope) patch.oauthScope = 'email';
  }

  return Object.keys(patch).length > 0 ? config.replaceBy(patch) : config;
};
