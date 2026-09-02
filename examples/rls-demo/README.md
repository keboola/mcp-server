# RLS demo — Keboola MCP Server

A small Node.js app for trying out the row-level-security pilot by hand: sign in as a user, type
SQL, and see exactly what the server's `query_data_rls` tool returns for that user against a real
Keboola project.

This is an **example / manual test harness**. It is not part of the MCP server, it is not shipped
with it, and nothing in `src/` depends on it.

## The wrapper role

The demo plays the part the RLS design expects of a real application:

1. **It authenticates its own users.** Here that is a mock sign-in — clicking a persona *is* the
   login. In a real deployment this would be Active Directory, Google, Okta, or the app's own
   session; the MCP server has no opinion about which.
2. **It is the only client of the MCP server.** The MCP server is started by this app as a local
   streamable-HTTP server bound to `127.0.0.1` (`RLS_DEMO_MCP_PORT`, default 8788), with
   `KBC_RLS_PRINCIPAL_SOURCE=header`.
3. **It asserts the signed-in user on every call** in the `X-RLS-Principal` request header. The
   server does not verify that header — it trusts this app — which is exactly why the MCP port must
   not be reachable by anyone else.

The identity is therefore never a tool argument: the browser sends only SQL, and `/api/query`
refuses outright when nobody is signed in. A `principal` tool argument would be refused by the
server anyway in header mode.

The MCP server is started in RLS mode (`--rls-rules-path <file>`), so it registers `query_data_rls`
and does **not** register the unrestricted `query_data` — and the whole server is read-only, so no
write tool is registered either. Every query is rewritten server-side: each referenced table becomes
`(SELECT * FROM <table> WHERE <predicate>)`. It is fail-closed — a table with no rule for the
principal is refused, and so is anything that is not a single plain `SELECT`.

## Prerequisites

- Node.js 20 or newer.
- A Python environment where `keboola_mcp_server` is installed and importable — the same environment
  that runs the server. A checkout works: `uv sync` in the repository root gives you
  `.venv/bin/keboola_mcp_server` and `.venv/bin/python`.
- A Keboola project with a Storage API token, and tables to point the rules at.

## Configuration

Everything is configured through environment variables. Put them in the real environment or in a
local `.env` file in this directory (`KEY=VALUE` lines) — `.env` is git-ignored and must never be
committed. Missing required variables abort the start with a message naming them; there are no
invented defaults.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `KBC_STORAGE_API_URL` | yes | — | Keboola stack URL, e.g. `https://connection.north-europe.azure.keboola.com` |
| `KBC_STORAGE_TOKEN` | yes | — | Storage API token for that project |
| `KBC_WORKSPACE_ID` | no | — | Existing workspace to query in; when unset, the MCP server manages its own |
| `RLS_RULES_PATH` | no | `./rls.yaml` | Rules file, relative to this directory |
| `RLS_DEMO_PORT` | no | `8787` | HTTP port of this app |
| `RLS_DEMO_HOST` | no | `127.0.0.1` | Listen address of this app — leave it local, the sign-in is a mock |
| `RLS_DEMO_MCP_PORT` | no | `8788` | Port the spawned MCP server listens on (always bound to `127.0.0.1`) |
| `KEBOOLA_MCP_COMMAND` | no | `keboola_mcp_server` | The server executable. Point it at a checkout with `/path/to/mcp-server/.venv/bin/keboola_mcp_server` |
| `RLS_DEMO_PYTHON` | no | `python3` | Interpreter for the rewrite preview and the rules validation. Must be a Python where `keboola_mcp_server` is importable, e.g. `/path/to/mcp-server/.venv/bin/python` |

## Rules

`rls.example.yaml` is a template with generic tables (`in.c-crm.invoices`, `in.c-crm.orders`,
`in.c-sales.deals`) and users (`alice`, `bob`, `carol`, `dave`, `admin`). On first start the app
copies it to the configured rules file (`rls.yaml` by default, git-ignored) and logs that it did.
Edit that copy — in a text editor or in the app's Rules panel — and replace the table keys with
tables that exist in your own project. The example queries in `public/index.html` reference the same
placeholder tables, so change them together.

## Run

```
npm install
npm start
```

Then open <http://127.0.0.1:8787/>.

On startup the app spawns the MCP server on `RLS_DEMO_MCP_PORT`, waits for its `/health-check`,
lists the tools and refuses to start if `query_data_rls` is missing or `query_data` is present.

## The UI

- **Sign in** — a strip of persona chips: clicking one signs you in (mock IdP) and the header line
  says *Signed in as `<name>` (mock IdP)*, with a **Sign out** button. Nothing can be run while
  signed out.
- **Query** — two columns. Left: the SQL as the user writes it. Right: **Rewritten SQL for `<user>`**,
  a read-only view that refreshes automatically (debounced) whenever the SQL or the sign-in
  changes. It shows the applied-rule chips and the rewritten statement, pretty-printed in the browser
  by a small keyword-based formatter. A refusal is shown in red in the same panel. Below 900 px the
  two columns stack.
- **Result** — tabbed. *Run as `<user>`* produces a single tab; **Run for all users** — an
  admin-style comparison the app runs server-side, one sign-in at a time — produces a
  **Summary** tab (the comparison table, active by default) plus one tab per user with that user's
  full result — applied-rule chips, message and data table, or the red refusal callout. Tabs are
  `role="tab"` buttons with `aria-selected`, reachable by Tab and navigable with the arrow keys.
- **Rules** — full width, editable, with a **Grid** / **YAML** segmented control.
  - *Grid*: rows are tables (bucket prefix in grey, table name in bold), columns are users, and each
    cell is an input holding the predicate. An empty cell means *no rule* and is shown in red.
    **Add user** and **Add table** prompt for a name; the small `×` removes a row or a column.
  - *YAML*: the raw file, comments included. Saving from the grid regenerates the YAML with a short
    header comment explaining the format.
  - **Save and reload rules** validates the YAML, rewrites the rules file (keeping the previous
    version next to it as `<rules file>.bak`) and restarts the MCP server process — the product has
    no hot reload, so the server process is stopped and started again. **Discard changes** reverts
    to the saved file.

## Files

| File | Purpose |
| --- | --- |
| `config.mjs` | Environment-driven configuration, `.env` reader, fail-fast validation. No secrets in the file. |
| `server.mjs` | HTTP server + mock sign-in + per-principal MCP HTTP clients + rewrite-preview helper. |
| `public/index.html` | Single-file UI (inline CSS/JS). |
| `rls.example.yaml` | Rules template, copied to the rules file on first start. |

## API

| Endpoint | Result |
| --- | --- |
| `GET /api/config` | `{ui}` — UI-only constants, no secrets |
| `GET /api/session` | `{principal}` — the current sign-in, or `null` |
| `POST /api/login` `{principal}` | `{ok: true, principal}` + session cookie, or `{ok: false, error}` (400) |
| `POST /api/logout` | `{ok: true, principal: null}` and the cookie is cleared |
| `GET /api/rules` | `{users, tables, yaml}` — parsed rules plus the raw file text |
| `PUT /api/rules` `{yaml}` | `{ok: true, users, tables, yaml, tools}` or `{ok: false, error}` |
| `GET /api/tools` | tool names reported by the MCP server |
| `POST /api/query` `{sql, query_name?}` | `{ok: true, applied_rules, csv_data, message}` or `{ok: false, error}` |
| `POST /api/query-all` `{sql, query_name?}` | `{ok: true, results: [{principal, result}]}` — the same query as every principal |
| `POST /api/rewrite` `{sql}` | `{ok: true, sql, applied_rules}` — the rewritten SQL, not executed |

**No endpoint takes a principal for a query.** `/api/query`, `/api/query-all` and `/api/rewrite`
read it from the mock sign-in session (an `HttpOnly` cookie) and answer **401** when there is none.
`/api/login` is the only place a name is accepted, and it only accepts principals that the rules
file mentions.

A refusal by the MCP server is a normal outcome of this demo, so it comes back as HTTP 200 with
`ok: false` and the verbatim `RLS: ...` message.

### `PUT /api/rules`

1. The candidate YAML is written to a **temp file** and validated by the MCP server's own
   `RlsRules.load()`, run through `RLS_DEMO_PYTHON` with the temp path passed as `argv[1]` (never
   interpolated into the script). A bad predicate — say `country = = 1` — comes back as
   `{ok: false, error}` with the verbatim `RlsError` message, and **nothing is written**.
2. Only then is the rules file replaced, after copying the previous version to `<rules file>.bak`.
3. Every per-principal MCP client is closed and the spawned server process is stopped (SIGTERM, then
   SIGKILL after a timeout), a new one is spawned with the same environment and `listTools` is run
   again, so the new rules take effect.

Steps 2 and 3 run inside the same queue as the tool calls, so a query never hits a half-restarted
server.

## Security

- **Local use only.** The sign-in is a *mock*: anyone who can reach this app's port can sign in as
  anybody and rewrite the rules file. Both this app and the MCP server it spawns bind to
  `127.0.0.1`. Do not expose either.
- The Storage API token is read from the environment, kept in memory and handed only to the spawned
  MCP server process as an environment variable. It is never logged, never written to disk by this
  app and never returned in an HTTP response.
- **Trust boundary.** The MCP server does not verify `X-RLS-Principal` — it trusts whatever this app
  asserts. That is the whole architecture: the wrapper authenticates, the MCP server enforces. It
  only holds as long as the MCP port is reachable from the wrapper alone, which is why it is bound to
  loopback here and must be equally fenced off in any real deployment.

## Status

Pilot demo. Not production code: the sign-in is a mock, sessions live in memory, and rules are
edited by anyone who can open the page.
