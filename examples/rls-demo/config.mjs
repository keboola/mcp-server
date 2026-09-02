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
  RLS_DEMO_CONFIG_PATH: './demo.json',
  RLS_DEMO_PORT: '8787',
  RLS_DEMO_HOST: '127.0.0.1',
  // The MCP server is started as a local streamable-HTTP server so this app can assert the signed-in
  // principal in a per-request header. It listens on loopback only — see `mcp.host` below.
  RLS_DEMO_MCP_PORT: '8788',
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

/** @param {string} name */
function portFromEnv(name) {
  const value = Number(env(name));
  if (!Number.isInteger(value) || value <= 0 || value > 65535) {
    console.error(`[rls-demo] FATAL: ${name} must be a TCP port number, got "${env(name)}"`);
    process.exit(1);
  }
  return value;
}

const port = portFromEnv('RLS_DEMO_PORT');
const mcpPort = portFromEnv('RLS_DEMO_MCP_PORT');
if (mcpPort === port) {
  console.error('[rls-demo] FATAL: RLS_DEMO_PORT and RLS_DEMO_MCP_PORT must differ');
  process.exit(1);
}

const rulesPath = path.resolve(APP_DIR, env('RLS_RULES_PATH'));
const demoConfigPath = path.resolve(APP_DIR, env('RLS_DEMO_CONFIG_PATH'));

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
  demo: {
    // Personas + example queries shown in the UI. When this file does not exist, `loadDemoConfig()`
    // falls back to `BUILTIN_DEMO_CONFIG` below (after `ensureDemoConfigFile()` in server.mjs has had
    // a chance to seed it from `exampleConfigPath`, the same way the rules file is seeded).
    configPath: demoConfigPath,
    exampleConfigPath: path.join(APP_DIR, 'demo.example.json'),
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
    // The MCP server is spawned as a local streamable-HTTP server, bound to loopback: it trusts the
    // X-RLS-Principal header this app sends, so nothing but this app may be able to reach it.
    host: '127.0.0.1',
    port: mcpPort,
    url: `http://127.0.0.1:${mcpPort}/mcp`,
    healthUrl: `http://127.0.0.1:${mcpPort}/health-check`,
    // The MCP server takes the principal from this header (KBC_RLS_PRINCIPAL_SOURCE=header), never
    // from a tool argument. Both are set in one place so the demo cannot drift from the server.
    principalHeader: 'X-RLS-Principal',
    principalSource: 'header',
    dialect: 'snowflake',
    expectedTool: 'query_data_rls',
    forbiddenTool: 'query_data',
    startupTimeoutMs: 60000,
    // How long to wait for the spawned server's /health-check to answer, and how often to retry.
    readyTimeoutMs: 60000,
    readyPollIntervalMs: 250,
    // How long to wait for the spawned server to exit on SIGTERM before sending SIGKILL.
    shutdownTimeoutMs: 10000,
    callTimeoutMs: 180000,
    rewriteTimeoutMs: 30000,
    validateTimeoutMs: 30000,
  },
  session: {
    // Mock sign-in cookie. This stands in for whatever a real wrapper application uses (an AD /
    // Google / Okta session, its own JWT); the demo keeps the mapping in memory only.
    cookieName: 'rls_demo_session',
  },
  ui: {
    defaultQueryName: 'RLS demo query',
    // Debounce for the live "Rewritten SQL" panel in the browser.
    rewriteDebounceMs: 400,
    idpLabel: 'mock IdP',
  },
};

// --- demo.json (personas + example queries) ---------------------------------------------------
//
// Deployment-specific content that used to be hardcoded in public/index.html: the one-line persona
// descriptions shown next to each sign-in button, and the example queries in the "Examples"
// dropdown. Kept here (not in public/index.html) so a deployment can swap them without touching
// code — see demo.example.json and README.md.

/**
 * Generic content used only when `config.demo.configPath` does not exist AND could not be seeded
 * from `config.demo.exampleConfigPath` (see `ensureDemoConfigFile()` in server.mjs). Mirrors
 * demo.example.json exactly.
 */
export const BUILTIN_DEMO_CONFIG = {
  personas: {
    alice: 'Czech market only',
    bob: 'Poland + Slovakia',
    carol: 'DACH + Benelux (DE, AT, NL, BE)',
    dave: 'Southern markets (ES, IT, FR), no in.c-sales.deals',
    admin: 'Sees everything',
  },
  examples: [
    {
      label: 'Rows per country — invoices',
      sql: 'SELECT country, COUNT(*) AS n\nFROM "in.c-crm"."invoices"\nGROUP BY 1\nORDER BY 2 DESC\nLIMIT 20',
    },
    {
      label: 'Top 10 invoices by amount',
      sql: 'SELECT id, country, amount\nFROM "in.c-crm"."invoices"\nORDER BY amount DESC\nLIMIT 10',
    },
    {
      label: 'Orders by country and status',
      sql:
        'SELECT country, status, COUNT(*) AS n\nFROM "in.c-crm"."orders"\nWHERE status IS NOT NULL\n' +
        'GROUP BY 1, 2\nORDER BY 3 DESC\nLIMIT 25',
    },
    {
      label: 'Join invoices and orders per country',
      sql:
        'SELECT i.country, COUNT(*) AS matched_rows, COUNT(DISTINCT o.id) AS orders\n' +
        'FROM "in.c-crm"."invoices" AS i\nJOIN "in.c-crm"."orders" AS o ON o.customer_id = i.customer_id\n' +
        'GROUP BY 1\nORDER BY 2 DESC\nLIMIT 20',
    },
    {
      label: 'Deals by stage — in.c-sales.deals (refused for dave)',
      sql: 'SELECT country, stage, COUNT(*) AS n\nFROM "in.c-sales"."deals"\nGROUP BY 1, 2\nORDER BY 3 DESC\nLIMIT 25',
    },
    {
      label: 'Refused — RESULT_SCAN escape attempt',
      sql: 'SELECT *\nFROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))\nLIMIT 10',
    },
    {
      label: 'Refused — non-SELECT statement',
      sql: "DELETE FROM \"in.c-crm\".\"invoices\"\nWHERE country = 'CZ'",
    },
  ],
};

/**
 * Validate the shape of a candidate demo config object. Throws with a clear, specific message on
 * the first problem found — never silently coerces or drops bad data.
 * @param {unknown} data
 * @returns {{personas: Record<string, string>, examples: {label: string, sql: string}[]}}
 */
export function validateDemoConfig(data) {
  if (data === null || typeof data !== 'object' || Array.isArray(data)) {
    throw new Error('demo config must be a JSON object with "personas" and "examples" fields');
  }
  const { personas, examples } = /** @type {Record<string, unknown>} */ (data);

  if (personas === null || typeof personas !== 'object' || Array.isArray(personas)) {
    throw new Error('demo config "personas" must be an object mapping user name -> description string');
  }
  for (const [user, desc] of Object.entries(personas)) {
    if (typeof desc !== 'string') {
      throw new Error(`demo config "personas.${user}" must be a string, got ${typeof desc}`);
    }
  }

  if (!Array.isArray(examples)) {
    throw new Error('demo config "examples" must be an array of {label, sql} objects');
  }
  examples.forEach((entry, i) => {
    if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
      throw new Error(`demo config "examples[${i}]" must be an object with "label" and "sql" strings`);
    }
    const { label, sql } = /** @type {Record<string, unknown>} */ (entry);
    if (typeof label !== 'string' || !label.trim()) {
      throw new Error(`demo config "examples[${i}].label" must be a non-empty string`);
    }
    if (typeof sql !== 'string' || !sql.trim()) {
      throw new Error(`demo config "examples[${i}].sql" must be a non-empty string`);
    }
  });

  return {
    personas: /** @type {Record<string, string>} */ (personas),
    examples: /** @type {{label: string, sql: string}[]} */ (examples),
  };
}

/**
 * Load and validate the demo config from `filePath`. When `filePath` does not exist, resolves to
 * `BUILTIN_DEMO_CONFIG` with `source: 'built-in'`. An existing-but-invalid file is a fatal error —
 * the caller is expected to print it and exit(1), never to fall back silently.
 * @param {string} filePath
 * @returns {Promise<{source: string, personas: Record<string, string>, examples: {label: string, sql: string}[]}>}
 */
export async function loadDemoConfig(filePath) {
  if (!existsSync(filePath)) {
    return { source: 'built-in', personas: BUILTIN_DEMO_CONFIG.personas, examples: BUILTIN_DEMO_CONFIG.examples };
  }
  let raw;
  try {
    raw = readFileSync(filePath, 'utf8');
  } catch (e) {
    throw new Error(`cannot read demo config ${filePath}: ${e?.message ?? e}`);
  }
  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    throw new Error(`${filePath} is not valid JSON: ${e?.message ?? e}`);
  }
  const validated = validateDemoConfig(parsed);
  return { source: filePath, ...validated };
}
