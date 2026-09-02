// RLS demo server — an *authenticating wrapper* around the Keboola MCP Server.
//
// This app plays the role the row-level-security design expects of a real application: it
// authenticates its own users (here: a mock sign-in standing in for AD / Google / Okta / an app
// session), and it is the only client of the MCP server. The MCP server is started in RLS mode with
// KBC_RLS_PRINCIPAL_SOURCE=header and listens on loopback; this app asserts the signed-in user on
// every call in the `X-RLS-Principal` header. The identity is therefore never a tool argument, and
// neither the browser nor a model can choose it.
//
//   GET  /              -> public/index.html
//   GET  /api/config    -> UI-only constants (no secrets, no paths)
//   GET  /api/session   -> {principal} of the current sign-in, or {principal: null}
//   POST /api/login     -> mock IdP: sign in as one of the principals in the rules file
//   POST /api/logout    -> sign out
//   GET  /api/rules     -> {users, tables, yaml} parsed from the rules file
//   PUT  /api/rules     -> validates + writes the rules file, then restarts the MCP process
//   GET  /api/tools     -> tool names reported by the MCP server
//   POST /api/query     -> runs query_data_rls as the signed-in principal
//   POST /api/query-all -> admin comparison: runs the same query as every principal, server-side
//   POST /api/rewrite   -> shows the rewritten SQL without executing it
//
// SECURITY: the Keboola Storage API token comes from the environment (see config.mjs) and is passed
// only as an environment variable to the spawned MCP server process. It is never logged, never
// returned in an HTTP response and never written to disk by this app.

import http from 'node:http';
import { readFile, writeFile, copyFile, mkdtemp, rm, access } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js';

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
 * rules once at startup (there is deliberately no hot reload), so the server process is restarted —
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

/** The spawned Keboola MCP Server process (streamable-HTTP, RLS header mode). */
let mcpProcess = null;
/** One client (and thus one transport, carrying one `X-RLS-Principal` value) per principal. */
const mcpClients = new Map();
/** @type {string[]} */
let toolNames = [];

// Rules reloads restart the MCP process, so every call is serialised behind one promise chain and a
// restart runs inside it: no query can hit a half-restarted server.
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
    // The whole point of this demo: the server takes the principal from the header this app sets,
    // and refuses a `principal` tool argument outright.
    KBC_RLS_PRINCIPAL_SOURCE: config.mcp.principalSource,
  };
  if (config.keboola.workspaceId) env.KBC_WORKSPACE_ID = config.keboola.workspaceId;
  else delete env.KBC_WORKSPACE_ID;
  return env;
}

/** Resolve once the spawned server answers its health check, or reject with why it did not. */
async function waitForMcpReady() {
  const deadline = Date.now() + config.mcp.readyTimeoutMs;
  for (;;) {
    if (mcpProcess === null || mcpProcess.exitCode !== null || mcpProcess.signalCode !== null) {
      throw new Error('the MCP server process exited during startup (see its log output above)');
    }
    try {
      const res = await fetch(config.mcp.healthUrl);
      if (res.ok) return;
    } catch {
      /* not listening yet */
    }
    if (Date.now() > deadline) {
      throw new Error(`the MCP server did not become ready within ${config.mcp.readyTimeoutMs} ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, config.mcp.readyPollIntervalMs));
  }
}

/**
 * The MCP client for one principal, created on first use.
 *
 * One client per principal, because the principal is a *transport* property here: the header is set
 * on the transport's `requestInit` and therefore travels with every request that client makes. The
 * server runs stateless (its default), so per-request headers are honoured on every call.
 * @param {string} principal
 * @returns {Promise<Client>}
 */
async function clientFor(principal) {
  const existing = mcpClients.get(principal);
  if (existing) return existing;
  const transport = new StreamableHTTPClientTransport(new URL(config.mcp.url), {
    requestInit: { headers: { [config.mcp.principalHeader]: principal } },
  });
  const client = new Client(
    { name: config.mcp.clientName, version: config.mcp.clientVersion },
    { capabilities: {} }
  );
  await client.connect(transport, { timeout: config.mcp.startupTimeoutMs });
  mcpClients.set(principal, client);
  return client;
}

async function startMcp() {
  mcpProcess = spawn(
    config.mcp.command,
    [
      '--transport',
      'streamable-http',
      '--host',
      config.mcp.host,
      '--port',
      String(config.mcp.port),
      '--rls-rules-path',
      config.mcp.rulesPath,
    ],
    { env: mcpEnv(), stdio: ['ignore', 'inherit', 'inherit'] }
  );
  mcpProcess.on('exit', (code, signal) => {
    console.log(`[rls-demo] the MCP server process exited (code=${code}, signal=${signal})`);
  });
  await waitForMcpReady();
  // Tool discovery needs no principal: `tools/list` is not an RLS-guarded call.
  const listed = await (await clientFor('')).listTools();
  toolNames = (listed.tools ?? []).map((t) => t.name).sort();
  return toolNames;
}

/** Close every per-principal client and stop the spawned MCP server process. */
async function stopMcp() {
  for (const [principal, client] of mcpClients) {
    try {
      await client.close();
    } catch (e) {
      console.error(`[rls-demo] closing the MCP client for "${principal}" failed: ${e?.message ?? e}`);
    }
  }
  mcpClients.clear();
  toolNames = [];
  const child = mcpProcess;
  mcpProcess = null;
  if (!child || child.exitCode !== null) return;
  await new Promise((resolve) => {
    const timer = setTimeout(() => child.kill('SIGKILL'), config.mcp.shutdownTimeoutMs);
    child.once('exit', () => {
      clearTimeout(timer);
      resolve();
    });
    child.kill('SIGTERM');
  });
}

/**
 * Stop the MCP server and start a fresh one, so it re-reads the rules file.
 * Call it from inside `serialise()` — a query must never see a half-restarted server.
 * @returns {Promise<string[]>}
 */
async function restartMcp() {
  await stopMcp();
  return startMcp();
}

/**
 * Call `query_data_rls` on behalf of one principal.
 *
 * The principal is NOT passed as a tool argument — in header mode the server refuses that. It is
 * asserted in the `X-RLS-Principal` header of the client we picked for it.
 * @param {{principal: string, sql: string, queryName: string}} args
 */
async function callQueryDataRls({ principal, sql, queryName }) {
  return serialise(async () => {
    if (!mcpProcess) {
      return { ok: false, error: 'The MCP server is not running — a rules reload may have failed.' };
    }
    const client = await clientFor(principal);
    const result = await client.callTool(
      {
        name: config.mcp.expectedTool,
        arguments: { sql_query: sql, query_name: queryName },
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
 * Rewrite the SQL without executing it, for the signed-in principal.
 *
 * This calls the server's own rewrite function directly (no MCP round-trip), so the principal is an
 * argument here — it comes from the demo session, never from the browser's request body.
 * @param {{principal: string, sql: string}} args
 */
function rewritePreview({ principal, sql }) {
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
      JSON.stringify({ rules_path: config.mcp.rulesPath, sql, user: principal, dialect: config.mcp.dialect })
    );
  });
}

// --- mock sign-in ------------------------------------------------------------------------------
//
// Stands in for whatever a real wrapper application already has: an Active Directory / Google /
// Okta login, or its own session cookie. Nothing here is a security mechanism — the point is only
// that the *application* decides who the user is, and the browser never names the principal for a
// query. Sessions live in memory and die with the process.

/** @type {Map<string, string>} sessionId -> principal */
const sessions = new Map();

/** @param {import('node:http').IncomingMessage} req */
function sessionIdFromRequest(req) {
  for (const part of String(req.headers.cookie ?? '').split(';')) {
    const eq = part.indexOf('=');
    if (eq <= 0) continue;
    if (part.slice(0, eq).trim() === config.session.cookieName) return part.slice(eq + 1).trim();
  }
  return null;
}

/**
 * The principal of the current sign-in, or null when the caller is not signed in.
 * @param {import('node:http').IncomingMessage} req
 * @returns {string | null}
 */
function principalFromRequest(req) {
  const sid = sessionIdFromRequest(req);
  return sid ? (sessions.get(sid) ?? null) : null;
}

// --- HTTP ------------------------------------------------------------------------------------

function sendJson(res, status, body, headers) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', ...(headers ?? {}) });
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

  if (req.method === 'GET' && url.pathname === '/api/session') {
    sendJson(res, 200, { principal: principalFromRequest(req) });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/login') {
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, 400, { ok: false, error: `Invalid JSON body: ${e.message}` });
      return;
    }
    const principal = typeof body.principal === 'string' ? body.principal.trim() : '';
    // The mock IdP only knows the principals the rules file mentions. A real wrapper would accept
    // whatever its identity provider returns and let the MCP server refuse an unknown principal.
    if (!principal || !rules.users.includes(principal)) {
      sendJson(res, 400, { ok: false, error: 'Unknown user — pick one of the principals in the rules file.' });
      return;
    }
    const sid = randomUUID();
    sessions.set(sid, principal);
    console.log(`[rls-demo] mock sign-in: ${principal}`);
    sendJson(res, 200, { ok: true, principal }, {
      // Local demo over plain HTTP, so no `Secure`; `HttpOnly` keeps the page's own scripts (and
      // anything injected into them) from reading the session id.
      'set-cookie': `${config.session.cookieName}=${sid}; HttpOnly; SameSite=Strict; Path=/`,
    });
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/logout') {
    const sid = sessionIdFromRequest(req);
    if (sid) sessions.delete(sid);
    sendJson(res, 200, { ok: true, principal: null }, {
      'set-cookie': `${config.session.cookieName}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0`,
    });
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

  if (req.method === 'POST' && (url.pathname === '/api/query' || url.pathname === '/api/query-all')) {
    // The principal comes from the demo's own session, never from the request body: this app
    // authenticated the user, so this app — not the page, and not the model — says who they are.
    const principal = principalFromRequest(req);
    if (!principal) {
      sendJson(res, 401, { ok: false, error: 'Not signed in — sign in first, queries always run as someone.' });
      return;
    }
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, 400, { ok: false, error: `Invalid JSON body: ${e.message}` });
      return;
    }
    const sql = typeof body.sql === 'string' ? body.sql.trim() : '';
    if (!sql) {
      sendJson(res, 400, { ok: false, error: '"sql" is required.' });
      return;
    }
    const queryName =
      typeof body.query_name === 'string' && body.query_name.trim()
        ? body.query_name.trim()
        : config.ui.defaultQueryName;
    // "Run for all users" is an admin-style comparison: the sign-ins are iterated here, server-side,
    // so the browser still never names a principal.
    const principals = url.pathname === '/api/query-all' ? rules.users : [principal];
    try {
      // A refusal is a normal outcome of this demo, so it comes back as 200 with ok:false.
      const results = [];
      for (const p of principals) {
        results.push({ principal: p, result: await callQueryDataRls({ principal: p, sql, queryName }) });
      }
      sendJson(res, 200, url.pathname === '/api/query-all' ? { ok: true, results } : results[0].result);
    } catch (e) {
      sendJson(res, 200, { ok: false, error: String(e?.message ?? e) });
    }
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/rewrite') {
    const principal = principalFromRequest(req);
    if (!principal) {
      sendJson(res, 401, { ok: false, error: 'Not signed in — the rewrite is always for someone.' });
      return;
    }
    let body;
    try {
      body = await readJsonBody(req);
    } catch (e) {
      sendJson(res, 400, { ok: false, error: `Invalid JSON body: ${e.message}` });
      return;
    }
    const sql = typeof body.sql === 'string' ? body.sql.trim() : '';
    if (!sql) {
      sendJson(res, 400, { ok: false, error: '"sql" is required.' });
      return;
    }
    sendJson(res, 200, await rewritePreview({ principal, sql }));
    return;
  }

  sendJson(res, 404, { ok: false, error: 'Not found' });
}

// --- startup ---------------------------------------------------------------------------------

async function main() {
  await ensureRulesFile();
  rules = await loadRules();
  console.log(`[rls-demo] rules loaded from ${config.mcp.rulesPath}: users = ${rules.users.join(', ')}`);
  console.log(
    `[rls-demo] starting the Keboola MCP Server (streamable-http on ${config.mcp.host}:${config.mcp.port}, ` +
      `RLS mode, principal from the ${config.mcp.principalHeader} header) via "${config.mcp.command}"...`
  );

  let tools;
  try {
    tools = await startMcp();
  } catch (e) {
    console.error(`[rls-demo] FATAL: the MCP server failed to start: ${e?.message ?? e}`);
    await stopMcp();
    process.exit(1);
  }
  console.log(`[rls-demo] MCP tools: ${tools.join(', ')}`);
  if (!tools.includes(config.mcp.expectedTool)) {
    console.error(`[rls-demo] FATAL: ${config.mcp.expectedTool} is not registered — is --rls-rules-path in effect?`);
    await stopMcp();
    process.exit(1);
  }
  if (tools.includes(config.mcp.forbiddenTool)) {
    console.error(`[rls-demo] FATAL: ${config.mcp.forbiddenTool} is registered — RLS mode is not active.`);
    await stopMcp();
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
      await stopMcp();
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
