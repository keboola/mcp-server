# RLS demo — Keboola MCP Server

A small Node.js app for trying out the row-level-security pilot by hand: pick a user, type SQL, and
see exactly what the server's `query_data_rls` tool returns for that user against a real Keboola
project.

This is an **example / manual test harness**. It is not part of the MCP server, it is not shipped
with it, and nothing in `src/` depends on it.

The MCP server is started in RLS mode (`--rls-rules-path <file>`), so it registers `query_data_rls`
and does **not** register the unrestricted `query_data`. Every query is rewritten server-side: each
referenced table becomes `(SELECT * FROM <table> WHERE <predicate>)`. It is fail-closed — a table
with no rule for the user is refused, and so is anything that is not a single plain `SELECT`.

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
| `RLS_DEMO_PORT` | no | `8787` | HTTP port |
| `RLS_DEMO_HOST` | no | `127.0.0.1` | Listen address — leave it local, there is no authentication |
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

On startup the app spawns one stdio MCP session, lists the tools and refuses to start if
`query_data_rls` is missing or `query_data` is present.

## The UI

- **Users** — a strip of persona chips at the top; the selected one is highlighted.
- **Query** — two columns. Left: the SQL as the user writes it. Right: **Rewritten SQL for `<user>`**,
  a read-only view that refreshes automatically (debounced) whenever the SQL or the selected user
  changes. It shows the applied-rule chips and the rewritten statement, pretty-printed in the browser
  by a small keyword-based formatter. A refusal is shown in red in the same panel. Below 900 px the
  two columns stack.
- **Result** — tabbed. *Run as `<user>`* produces a single tab; **Run for all users** produces a
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
    no hot reload, so the stdio session is torn down and started again. **Discard changes** reverts
    to the saved file.

## Files

| File | Purpose |
| --- | --- |
| `config.mjs` | Environment-driven configuration, `.env` reader, fail-fast validation. No secrets in the file. |
| `server.mjs` | HTTP server + MCP stdio client + rewrite-preview helper. |
| `public/index.html` | Single-file UI (inline CSS/JS). |
| `rls.example.yaml` | Rules template, copied to the rules file on first start. |

## API

| Endpoint | Result |
| --- | --- |
| `GET /api/config` | `{ui}` — UI-only constants, no secrets |
| `GET /api/rules` | `{users, tables, yaml}` — parsed rules plus the raw file text |
| `PUT /api/rules` `{yaml}` | `{ok: true, users, tables, yaml, tools}` or `{ok: false, error}` |
| `GET /api/tools` | tool names reported by the MCP server |
| `POST /api/query` `{user, sql, query_name?}` | `{ok: true, applied_rules, csv_data, message}` or `{ok: false, error}` |
| `POST /api/rewrite` `{user, sql}` | `{ok: true, sql, applied_rules}` — the rewritten SQL, not executed |

A refusal is a normal outcome of this demo, so it comes back as HTTP 200 with `ok: false` and the
verbatim `RLS: ...` message.

### `PUT /api/rules`

1. The candidate YAML is written to a **temp file** and validated by the MCP server's own
   `RlsRules.load()`, run through `RLS_DEMO_PYTHON` with the temp path passed as `argv[1]` (never
   interpolated into the script). A bad predicate — say `country = = 1` — comes back as
   `{ok: false, error}` with the verbatim `RlsError` message, and **nothing is written**.
2. Only then is the rules file replaced, after copying the previous version to `<rules file>.bak`.
3. The stdio MCP session is closed (which kills the spawned server process), a new one is spawned
   with the same environment and `listTools` is run again, so the new rules take effect.

Steps 2 and 3 run inside the same queue as the tool calls, so a query never hits a half-restarted
server.

## Security

- **Local use only.** The app binds to `127.0.0.1` by default and has no authentication: anything
  that can reach the port can run queries as any user and rewrite the rules file. Do not expose it.
- The Storage API token is read from the environment, kept in memory and handed only to the spawned
  MCP server process as an environment variable. It is never logged, never written to disk by this
  app and never returned in an HTTP response.
- **Trust boundary.** The `user` argument is supplied by the caller — here, the browser — and is not
  verified by the server. That is the documented pilot limitation of `query_data_rls`, and this demo
  exists precisely to make it visible.

## Status

Pilot demo. Not production code: single shared MCP session, no authentication, no user identity.
