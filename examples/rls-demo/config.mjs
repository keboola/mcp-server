// Configuration for the RLS pilot demo.
//
// Everything that depends on the machine comes from environment variables — there are no hardcoded
// paths, project ids or credentials in this directory. A local `.env` file (never committed, see
// .gitignore) is read first, so `npm start` works without exporting anything by hand.
//
// SECURITY: `keboola.token` holds the Storage API token in memory only. It is handed to the spawned
// MCP server process as an environment variable and is never logged, never written to disk and never
// returned in an HTTP response.

import { readFileSync, existsSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const APP_DIR = path.dirname(fileURLToPath(import.meta.url));

/** Environment variables that must be present; the process exits 1 when any is missing. */
const REQUIRED_ENV = ['KBC_STORAGE_API_URL', 'KBC_STORAGE_TOKEN'];

/** Defaults for the optional variables. Required ones deliberately have none. */
const DEFAULTS = {
  RLS_RULES_PATH: './rls.yaml',
  RLS_DEMO_PORT: '8787',
  RLS_DEMO_HOST: '127.0.0.1',
  KEBOOLA_MCP_COMMAND: 'keboola_mcp_server',
  RLS_DEMO_PYTHON: 'python3',
};

/**
 * Minimal `.env` reader: `KEY=VALUE` lines, `#` comments, optional surrounding quotes.
 * Deliberately dependency-free. Values already present in the real environment win.
 * @param {string} filePath
 */
function loadDotEnv(filePath) {
  if (!existsSync(filePath)) return;
  for (const rawLine of readFileSync(filePath, 'utf8').split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) continue;
    const key = line.slice(0, eq).trim();
    let value = line.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"') && value.length >= 2) ||
      (value.startsWith("'") && value.endsWith("'") && value.length >= 2)
    ) {
      value = value.slice(1, -1);
    }
    if (!(key in process.env)) process.env[key] = value;
  }
}

loadDotEnv(path.join(APP_DIR, '.env'));

const missing = REQUIRED_ENV.filter((name) => !String(process.env[name] ?? '').trim());
if (missing.length) {
  // Fail fast with a clear message — no invented defaults for required configuration.
  console.error(
    `[rls-demo] FATAL: missing required environment variable(s): ${missing.join(', ')}\n` +
      '[rls-demo] Set them in the environment or in a local .env file in this directory.\n' +
      '[rls-demo] See README.md for the full configuration table.'
  );
  process.exit(1);
}

/** @param {string} name */
function env(name) {
  const value = process.env[name];
  const resolved = value === undefined || value === '' ? DEFAULTS[name] : value;
  if (resolved === undefined) throw new Error(`No value and no default for ${name}`);
  return resolved;
}

const port = Number(env('RLS_DEMO_PORT'));
if (!Number.isInteger(port) || port <= 0 || port > 65535) {
  console.error(`[rls-demo] FATAL: RLS_DEMO_PORT must be a TCP port number, got "${env('RLS_DEMO_PORT')}"`);
  process.exit(1);
}

const rulesPath = path.resolve(APP_DIR, env('RLS_RULES_PATH'));

export const config = {
  appDir: APP_DIR,
  http: {
    // Local-use demo: there is no authentication, so it never binds to a public interface.
    host: env('RLS_DEMO_HOST'),
    port,
  },
  keboola: {
    stackUrl: process.env.KBC_STORAGE_API_URL,
    token: process.env.KBC_STORAGE_TOKEN,
    // Optional: when unset, the MCP server manages its own workspace.
    workspaceId: String(process.env.KBC_WORKSPACE_ID ?? '').trim() || null,
  },
  mcp: {
    // The Keboola MCP Server executable. Defaults to the CLI on PATH; point it at a checkout with
    // KEBOOLA_MCP_COMMAND=/path/to/mcp-server/.venv/bin/keboola_mcp_server.
    command: env('KEBOOLA_MCP_COMMAND'),
    // Python interpreter used for the rewrite preview and the rules validation. It must be an
    // environment where `keboola_mcp_server` is importable.
    pythonPath: env('RLS_DEMO_PYTHON'),
    rulesPath,
    // Shipped template, copied to rulesPath on first start when the rules file does not exist.
    exampleRulesPath: path.join(APP_DIR, 'rls.example.yaml'),
    // Single backup of the previous rules file, rewritten on every successful save.
    rulesBackupPath: `${rulesPath}.bak`,
    clientName: 'rls-demo',
    clientVersion: '1.0.0',
    // The rewrite preview and every tool call go through the same single stdio session.
    dialect: 'snowflake',
    expectedTool: 'query_data_rls',
    forbiddenTool: 'query_data',
    startupTimeoutMs: 60000,
    callTimeoutMs: 180000,
    rewriteTimeoutMs: 30000,
    validateTimeoutMs: 30000,
  },
  ui: {
    defaultQueryName: 'RLS demo query',
    // Debounce for the live "Rewritten SQL" panel in the browser.
    rewriteDebounceMs: 400,
  },
};
