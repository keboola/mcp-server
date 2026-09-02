// RLS demo server.
//
// Boots a single stdio MCP session against the Keboola MCP Server running in RLS mode and exposes a
// tiny HTTP API for the browser UI:
//   GET  /              -> public/index.html
//   GET  /api/config    -> UI-only constants (no secrets, no paths)
//   GET  /api/rules     -> {users, tables, yaml} parsed from the rules file
//   PUT  /api/rules     -> validates + writes the rules file, then restarts the MCP process
//   GET  /api/tools     -> tool names reported by the MCP server
//   POST /api/query     -> runs query_data_rls for a user
//   POST /api/rewrite   -> shows the rewritten SQL without executing it
//
// SECURITY: the Keboola Storage API token comes from the environment (see config.mjs) and is passed
// only as an environment variable to the spawned MCP server process. It is never logged, never
// returned in an HTTP response and never written to disk by this app.

import http from 'node:http';
import { readFile, writeFile, copyFile, mkdtemp, rm, access } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

import { config } from './config.mjs';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.join(HERE, 'public');

// --- rules -----------------------------------------------------------------------------------

/**
 * Copy the shipped `rls.example.yaml` to the configured rules path when that file does not exist,
 * so a fresh checkout starts with a working (generic) set of rules.
 */
async function ensureRulesFile() {
  try {
    await access(config.mcp.rulesPath);
    return;
  } catch {
    /* not there yet — fall through and seed it */
  }
  await copyFile(config.mcp.exampleRulesPath, config.mcp.rulesPath);
  console.log(
    `[rls-demo] ${config.mcp.rulesPath} did not exist — seeded it from ${config.mcp.exampleRulesPath}`
  );
}

/**
 * Parse rules YAML text into `{users, tables, yaml}`.
 * `yaml` is the verbatim source text so the UI can show the file with its comments.
 * @param {string} raw
 * @returns {{users: string[], tables: Record<string, Record<string, string>>, yaml: string}}
 */
function parseRules(raw) {
  const doc = YAML.parse(raw);
  const tables = doc?.tables;
  if (!tables || typeof tables !== 'object') {
    throw new Error(`${config.mcp.rulesPath} has no top-level "tables" mapping`);
  }
  const users = [];
  for (const perUser of Object.values(tables)) {
    for (const user of Object.keys(perUser ?? {})) {
      if (!users.includes(user)) users.push(user);
    }
  }
  return { users, tables, yaml: raw };
}

/**
 * Load the rules file from disk.
 * @returns {Promise<{users: string[], tables: Record<string, Record<string, string>>, yaml: string}>}
 */
async function loadRules() {
  return parseRules(await readFile(config.mcp.rulesPath, 'utf8'));
}

/** Current rules, replaced in place by a successful `PUT /api/rules`. */
let rules = { users: [], tables: {}, yaml: '' };

// Validation runs in the MCP server's Python environment against a TEMP copy of the candidate YAML.
// The path is passed as argv[1] — never interpolated into the script — so no file name can become
// Python code.
const VALIDATE_SCRIPT = `
import json, sys
from keboola_mcp_server.rls import RlsRules, RlsError

try:
    rules = RlsRules.load(sys.argv[1])
    print(json.dumps({"ok": True, "tables": len(rules.tables)}))
except RlsError as e:
    print(json.dumps({"ok": False, "error": str(e)}))
except Exception as e:
    print(json.dumps({"ok": False, "error": f"{type(e).__name__}: {e}"}))
`;

/**
 * Run `RlsRules.load()` from the MCP server's own Python environment against `filePath`.
 * @param {string} filePath
 * @returns {Promise<{ok: true} | {ok: false, error: string}>}
 */
function validateRulesFile(filePath) {
  return new Promise((resolve) => {
    const child = spawn(config.mcp.pythonPath, ['-c', VALIDATE_SCRIPT, filePath], {
      // Validation is a pure file parse — it needs no Keboola credentials.
      env: { ...process.env, KBC_STORAGE_TOKEN: '', PYTHONWARNINGS: 'ignore' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), config.mcp.validateTimeoutMs);
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.stderr.on('data', (d) => {
      err += d;
    });
    child.on('error', (e) => {
      clearTimeout(timer);
      resolve({ ok: false, error: `Cannot run the rules validator: ${e.message}` });
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      const line = out.trim().split('\n').filter(Boolean).pop();
      if (!line) {
        resolve({ ok: false, error: `Rules validator exited with code ${code}: ${err.trim().slice(0, 500)}` });
        return;
      }
      try {
        const parsed = JSON.parse(line);
        resolve(parsed.ok ? { ok: true } : { ok: false, error: parsed.error });
      } catch {
        resolve({ ok: false, error: `Rules validator produced unparseable output: ${line.slice(0, 500)}` });
      }
    });
  });
}

/**
 * Validate, persist and activate a new rules file.
 *
 * Nothing is written unless `RlsRules.load()` accepts the candidate YAML. The MCP server reads its
 * rules once at startup (there is deliberately no hot reload), so the stdio process is restarted —
 * inside the call queue, so no query ever runs against a half-restarted server.
 *
 * @param {string} yamlText
 * @returns {Promise<{ok: true, users: string[], tables: object, yaml: string, tools: string[]} | {ok: false, error: string}>}
 */
async function saveRules(yamlText) {
  // 1) Shape check in Node first — a cheap, clearer error than the Python one for an empty body.
  let candidate;
  try {
    candidate = parseRules(yamlText);
  } catch (e) {
    return { ok: false, error: String(e?.message ?? e) };
  }

  // 2) Validate with the real thing: the MCP server's own RlsRules.load(), against a temp file.
  const dir = await mkdtemp(path.join(os.tmpdir(), 'rls-demo-rules-'));
  const tempPath = path.join(dir, 'rls.yaml');
  let verdict;
  try {
    await writeFile(tempPath, yamlText, 'utf8');
    verdict = await validateRulesFile(tempPath);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
  if (!verdict.ok) return verdict;

  // 3) Write, keeping a single backup of the previous version, then restart the MCP session.
  return serialise(async () => {
    await copyFile(config.mcp.rulesPath, config.mcp.rulesBackupPath);
    await writeFile(config.mcp.rulesPath, yamlText, 'utf8');
    rules = candidate;
    const tools = await restartMcp();
    console.log(`[rls-demo] rules saved and MCP restarted: users = ${rules.users.join(', ')}`);
    return { ok: true, users: rules.users, tables: rules.tables, yaml: rules.yaml, tools };
  });
}

// --- MCP session -----------------------------------------------------------------------------

/** @type {Client | null} */
let mcpClient = null;
/** @type {string[]} */
let toolNames = [];

// The stdio client is a single session: serialise every tool call behind one promise chain.
let callChain = Promise.resolve();

/**
 * Queue `fn` so only one MCP call is in flight at a time.
 * @template T
 * @param {() => Promise<T>} fn
 * @returns {Promise<T>}
 */
function serialise(fn) {
  const result = callChain.then(fn, fn);
  // Keep the chain alive even when a call rejects.
  callChain = result.then(
    () => undefined,
    () => undefined
  );
  return result;
}

/** Environment handed to the spawned MCP server process. The token lives only here. */
function mcpEnv() {
  const env = {
    ...process.env,
    KBC_STORAGE_API_URL: config.keboola.stackUrl,
    KBC_STORAGE_TOKEN: config.keboola.token,
  };
  if (config.keboola.workspaceId) env.KBC_WORKSPACE_ID = config.keboola.workspaceId;
  else delete env.KBC_WORKSPACE_ID;
  return env;
}

async function startMcp() {
  const transport = new StdioClientTransport({
    command: config.mcp.command,
    args: ['--transport', 'stdio', '--rls-rules-path', config.mcp.rulesPath],
    env: mcpEnv(),
    stderr: 'inherit',
  });
  const client = new Client(
    { name: config.mcp.clientName, version: config.mcp.clientVersion },
    { capabilities: {} }
  );
  await client.connect(transport, { timeout: config.mcp.startupTimeoutMs });
  const listed = await client.listTools();
  mcpClient = client;
  toolNames = (listed.tools ?? []).map((t) => t.name).sort();
  return toolNames;
}

/**
 * Tear the stdio session down and start a fresh one, so the MCP server re-reads the rules file.
 * Call it from inside `serialise()` — a query must never see a half-restarted session.
 * @returns {Promise<string[]>}
 */
async function restartMcp() {
  const old = mcpClient;
  mcpClient = null;
  toolNames = [];
  if (old) {
    try {
      // Closing the client closes the transport, which kills the spawned server process.
      await old.close();
    } catch (e) {
      console.error(`[rls-demo] closing the old MCP session failed: ${e?.message ?? e}`);
    }
  }
  return startMcp();
}

/**
 * Call `query_data_rls` for one user.
 * @param {{user: string, sql: string, queryName: string}} args
 */
async function callQueryDataRls({ user, sql, queryName }) {
  return serialise(async () => {
    if (!mcpClient) {
      return { ok: false, error: 'The MCP session is not running — a rules reload may have failed.' };
    }
    const result = await mcpClient.callTool(
      {
        name: config.mcp.expectedTool,
        arguments: { sql_query: sql, query_name: queryName, user },
      },
      undefined,
      { timeout: config.mcp.callTimeoutMs }
    );
    const text = (result.content ?? [])
      .filter((c) => c.type === 'text')
      .map((c) => c.text)
      .join('\n');
    if (result.isError) {
      return { ok: false, error: text || 'The MCP tool returned an error without a message.' };
    }
    let payload = result.structuredContent;
    if (!payload || typeof payload !== 'object' || !('csv_data' in payload)) {
      // Fall back to the text content, which carries the JSON-serialised output model.
      try {
        payload = JSON.parse(text);
      } catch {
        return { ok: false, error: `Unexpected tool result: ${text.slice(0, 500)}` };
      }
    }
    return {
      ok: true,
      applied_rules: payload.applied_rules ?? [],
      csv_data: payload.csv_data ?? '',
      message: payload.message ?? null,
    };
  });
}

// --- rewrite preview -------------------------------------------------------------------------

// Runs in the MCP server's Python environment; input arrives as JSON on stdin so nothing is
// shell-interpolated.
const REWRITE_SCRIPT = `
import json, sys
from keboola_mcp_server.rls import RlsRules, rewrite_query, RlsError

payload = json.load(sys.stdin)
try:
    rules = RlsRules.load(payload["rules_path"])
    rewritten = rewrite_query(
        payload["sql"], user=payload["user"], dialect=payload["dialect"], rules=rules
    )
    print(json.dumps({"sql": rewritten.sql, "applied_rules": list(rewritten.applied_rules)}))
except RlsError as e:
    print(json.dumps({"error": str(e)}))
except Exception as e:
    print(json.dumps({"error": f"{type(e).__name__}: {e}"}))
`;

/**
 * Rewrite the SQL without executing it.
 * @param {{user: string, sql: string}} args
 */
function rewritePreview({ user, sql }) {
  return new Promise((resolve) => {
    const child = spawn(config.mcp.pythonPath, ['-c', REWRITE_SCRIPT], {
      // The rewrite needs no Keboola credentials at all — it is a pure SQL transformation.
      env: { ...process.env, KBC_STORAGE_TOKEN: '', PYTHONWARNINGS: 'ignore' },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let out = '';
    let err = '';
    const timer = setTimeout(() => child.kill('SIGKILL'), config.mcp.rewriteTimeoutMs);
    child.stdout.on('data', (d) => {
      out += d;
    });
    child.stderr.on('data', (d) => {
      err += d;
    });
    child.on('error', (e) => {
      clearTimeout(timer);
      resolve({ ok: false, error: `Cannot run the rewrite helper: ${e.message}` });
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      const line = out.trim().split('\n').filter(Boolean).pop();
      if (!line) {
        resolve({ ok: false, error: `Rewrite helper exited with code ${code}: ${err.trim().slice(0, 500)}` });
        return;
      }
      try {
        const parsed = JSON.parse(line);
        if (parsed.error) resolve({ ok: false, error: parsed.error });
        else resolve({ ok: true, sql: parsed.sql, applied_rules: parsed.applied_rules ?? [] });
      } catch {
        resolve({ ok: false, error: `Rewrite helper produced unparseable output: ${line.slice(0, 500)}` });
      }
    });
    child.stdin.end(
      JSON.stringify({ rules_path: config.mcp.rulesPath, sql, user, dialect: config.mcp.dialect })
    );
  });
}

// --- HTTP ------------------------------------------------------------------------------------

function sendJson(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(payload);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 1_000_000) {
        reject(new Error('Request body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

async function readJsonBody(req) {
  const raw = await readBody(req);
  if (!raw) return {};
  return JSON.parse(raw);
}

async function handle(req, res) {
  const url = new URL(req.url, `http://${config.http.host}:${config.http.port}`);

  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/index.html')) {
    const html = await readFile(path.join(PUBLIC_DIR, 'index.html'), 'utf8');
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/config') {
    // UI-only constants (no secrets, no paths) so index.html hardcodes nothing.
    sendJson(res, 200, { ui: config.ui });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/rules') {
    sendJson(res, 200, rules);
    return;
  }

  if (req.method === 'PUT' && url.pathname === '/api/rules') {
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, 400, { ok: false, error: `Invalid JSON body: ${e.message}` });
      return;
    }
    const yamlText = typeof body.yaml === 'string' ? body.yaml : '';
    if (!yamlText.trim()) {
      sendJson(res, 400, { ok: false, error: 'A non-empty "yaml" field is required.' });
      return;
    }
    try {
      // An invalid rules file is a normal outcome here, so it is 200 with ok:false.
      sendJson(res, 200, await saveRules(yamlText));
    } catch (e) {
      sendJson(res, 200, { ok: false, error: String(e?.message ?? e) });
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/tools') {
    sendJson(res, 200, { tools: toolNames });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/query') {
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, 400, { ok: false, error: `Invalid JSON body: ${e.message}` });
      return;
    }
    const user = typeof body.user === 'string' ? body.user.trim() : '';
    const sql = typeof body.sql === 'string' ? body.sql.trim() : '';
    if (!user || !sql) {
      sendJson(res, 400, { ok: false, error: 'Both "user" and "sql" are required.' });
      return;
    }
    const queryName =
      typeof body.query_name === 'string' && body.query_name.trim()
        ? body.query_name.trim()
        : config.ui.defaultQueryName;
    try {
      // A refusal is a normal outcome of this demo, so it comes back as 200 with ok:false.
      const result = await callQueryDataRls({ user, sql, queryName });
      sendJson(res, 200, result);
    } catch (e) {
      sendJson(res, 200, { ok: false, error: String(e?.message ?? e) });
    }
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/rewrite') {
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, 400, { ok: false, error: `Invalid JSON body: ${e.message}` });
      return;
    }
    const user = typeof body.user === 'string' ? body.user.trim() : '';
    const sql = typeof body.sql === 'string' ? body.sql.trim() : '';
    if (!user || !sql) {
      sendJson(res, 400, { ok: false, error: 'Both "user" and "sql" are required.' });
      return;
    }
    sendJson(res, 200, await rewritePreview({ user, sql }));
    return;
  }

  sendJson(res, 404, { ok: false, error: 'Not found' });
}

// --- startup ---------------------------------------------------------------------------------

async function main() {
  await ensureRulesFile();
  rules = await loadRules();
  console.log(`[rls-demo] rules loaded from ${config.mcp.rulesPath}: users = ${rules.users.join(', ')}`);
  console.log(`[rls-demo] starting the Keboola MCP Server (stdio, RLS mode) via "${config.mcp.command}"...`);

  let tools;
  try {
    tools = await startMcp();
  } catch (e) {
    console.error(`[rls-demo] FATAL: the MCP server failed to start: ${e?.message ?? e}`);
    process.exit(1);
  }
  console.log(`[rls-demo] MCP tools: ${tools.join(', ')}`);
  if (!tools.includes(config.mcp.expectedTool)) {
    console.error(`[rls-demo] FATAL: ${config.mcp.expectedTool} is not registered — is --rls-rules-path in effect?`);
    process.exit(1);
  }
  if (tools.includes(config.mcp.forbiddenTool)) {
    console.error(`[rls-demo] FATAL: ${config.mcp.forbiddenTool} is registered — RLS mode is not active.`);
    process.exit(1);
  }
  console.log(
    `[rls-demo] smoke test OK: ${config.mcp.expectedTool} present, ${config.mcp.forbiddenTool} absent.`
  );

  const server = http.createServer((req, res) => {
    handle(req, res).catch((e) => {
      console.error(`[rls-demo] request failed: ${e?.message ?? e}`);
      if (!res.headersSent) sendJson(res, 500, { ok: false, error: 'Internal server error' });
      else res.end();
    });
  });
  server.listen(config.http.port, config.http.host, () => {
    console.log(`[rls-demo] listening on http://${config.http.host}:${config.http.port}/`);
  });

  const shutdown = async () => {
    server.close();
    try {
      await mcpClient?.close();
    } catch {
      /* ignore */
    }
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((e) => {
  console.error(`[rls-demo] FATAL: ${e?.message ?? e}`);
  process.exit(1);
});
