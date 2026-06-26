# Plan: Rewrite Keboola MCP Server (Python → TypeScript)

**Linear:** PSGO-268 · branch `martinvasko-psgo-268-rewrite-keboola-mcp-server-from-python-to-typescript-11`

**Goal:** Replace the Python (FastMCP, v1.72.8) implementation **in this same standalone
public repo** with a TypeScript implementation at **1:1 functional parity**. Ship as a
Docker container AND publish to npm so it installs via `npx @keboola/mcp-server` /
`npm i -g`. Migrate unit tests to vitest and iterate until green. Integtests last.
Delivered as a **draft branch + draft PR**.

> Parity source of truth: the current Python tree (before replacement), `TOOLS.md`
> (39 tools), and the unit test suite (`tests/`, 33 files).

---

## 0. Decisions (confirmed)

- **Standalone repo.** Rewrite in place; do NOT fold into the `ui` monorepo. Later,
  consider extracting reusable packages _out of_ MCP for `ui` to consume.
- **Reuse `@keboola/api-client`** as a **published npm dependency** (v4) for the client
  layer: `storage`, `queue` (jobs), `queryService`, `oauth`, `encryption`, `metastore`,
  `syncActions`, `dataScience`, `management`.
- **MCP protocol**: `@modelcontextprotocol/sdk` (replaces FastMCP) — stdio +
  streamable-HTTP transports, zod tool schemas.
- **HTTP server**: Hono + `@hono/node-server` (model: `ui/apps/kai-agent`).
- **API gaps** (scheduler, AI service: `docs_query` / semantic / global `search`):
  **add to `@keboola/api-client`** in the `ui` repo (coordinated cross-repo workstream),
  publish, then consume here. Track as a dependency of the relevant tool phases.
- **MCP server only** (In Platform Agent variant deferred), but keep this repo's release
  tag scheme (`v*`, `agent-v*`, `canary-orion-*`, `dev-*`) and CI workflows, adapted to TS.
- **Models**: pydantic → **zod v4**.
- **SQL**: runs via the Query Service **HTTP API** — no DB drivers.

---

## 1. What we reuse vs. rewrite (laziness budget)

| Python component                                                                                  | TS strategy                                                                |
| ------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| `fastmcp` / `mcp`, stdio + streamable-http                                                        | `@modelcontextprotocol/sdk`. No reimplementation.                          |
| `clients/` (storage, jobs_queue, query, oauth, encryption, metastore, sync_actions, data_science) | `@keboola/api-client` subpaths. Thin adapters only.                        |
| `clients/scheduler.py`, `clients/ai_service.py`, semantic + global search + docs                  | Gap → add to `@keboola/api-client`, then consume.                          |
| `clients/base.py` (httpx + retries)                                                               | api-client `fetchClient`/middlewares.                                      |
| pydantic models                                                                                   | zod v4.                                                                    |
| sqlglot (identifier quoting / dialect)                                                            | port the small quoting helpers; `node-sql-parser` only if strictly needed. |
| Starlette/uvicorn                                                                                 | Hono + `@hono/node-server`.                                                |
| `cli.py` argparse                                                                                 | small arg+env parser; `bin` for npx.                                       |
| Dockerfile (uv/python)                                                                            | Node 24 multi-stage.                                                       |
| pytest (parametrize, fixtures)                                                                    | vitest 4 (`test.each`) + msw.                                              |
| `generate_tool_docs.py`                                                                           | `gen:tools-docs` script; keep `check-tools-docs` CI gate.                  |

**Net:** client layer (~3.4k LOC) and MCP protocol layer become dependencies. The real
work is **tools (15.5k LOC) + models + tests**.

---

## 2. Target repo layout (replacing Python in place)

```
package.json            # name @keboola/mcp-server, bin, type module, exports, tsup, vitest
tsconfig.json
tsup.config.ts          # ESM+CJS + bin; model ui/apps/kai-agent/tsup.config.ts
oxlint.config.ts        # match monorepo lint (oxlint/oxfmt) OR keep eslint — see §9
vitest.config.ts
Dockerfile              # node:24 multi-stage; replaces the Python one
.github/workflows/      # ci.yml, release.yml, kaibench.yml — adapted to Node/TS
src/
  index.ts              # bin entry: parse args/env -> Config -> start transport
  config.ts             # Config (env KBC_*, X-* headers, CLI) — port config.py
  server.ts             # build McpServer, register tools/prompts/resources, lifespan
  transports/{stdio.ts,http.ts}   # http.ts = Hono app + routes
  mcp/{toolFiltering.ts,authorization.ts,errors.ts}
  oauth.ts              # SimpleOAuthProvider (oauth.py)
  preview.ts            # /preview/configuration (preview.py)
  workspace.ts          # Workspace + SQL dialect quoting (workspace.py)
  clients/              # thin adapters over @keboola/api-client (+ gap clients until api-client ships)
  tools/                # one module per Python tools/ submodule (see §4)
  models/               # zod schemas
  prompts/  resources/
__tests__/              # ported unit tests, mirror Python tests/
README.md
TOOLS.md                # regenerated
feature_spec/mcp-typescript-rewrite/  # this PLAN + SECRETS doc
```

HTTP routes on the Hono app (parity with cli.py/server.py): `/mcp` (streamable-http),
`/` (info), `/health-check`, `/preview/configuration` (POST), `/oauth/callback` (GET).
Default transport `stdio`; `--transport streamable-http` (alias `http-compat`) for server.

---

## 3. Core infra (Phase 1)

1. **Config** — port `config.py` field set and resolution (CLI → `KBC_*` env → `X-*`
   header), alias/normalize logic, URL amendment, branch_id "production/default/none"→null,
   secret redaction.
2. **Transports** via SDK: `StdioServerTransport`; stateless streamable-HTTP at `/mcp`.
   Reject OAuth config on stdio (cli.py parity).
3. **Per-request config**: Hono middleware reads `X-*` + `Authorization: Bearer` per
   request, layered over base Config via AsyncLocalStorage context.
4. **Logging**: pino (JSON), `--log-level`. **Errors**: 400 for validation/value/JSON,
   500 otherwise; debug includes stack. **dd-trace** for the container.

**Async stance:** native async.

- Await-to-completion: `query_data` (submit query job → poll Query Service), reads,
  `run_sync_action`. Parallelize independent fetches with `Promise.all`.
- Fire-and-return-progress: `run_job`, `deploy_data_app` — return job/task id + status.

---

## 4. Tools (Phase 4) — all 39

Each = zod input schema + handler + `server.registerTool` (preserve names, descriptions,
read-only annotations exactly — they drive TOOLS.md and tool filtering).

| Module     | Tools                                                                                                                                                                                                       |
| ---------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| components | get_components, find_component_id, get_configs, get_config_examples, create_config, add_config_row, update_config, update_config_row, create_sql_transformation, update_sql_transformation, run_sync_action |
| flow       | get_flows, get_flow_schema, get_flow_examples, create_flow, create_conditional_flow, update_flow, modify_flow                                                                                               |
| storage    | get_buckets, get_tables, update_descriptions                                                                                                                                                                |
| jobs       | get_jobs, run_job                                                                                                                                                                                           |
| sql        | query_data                                                                                                                                                                                                  |
| project    | get_project_info, update_project_description                                                                                                                                                                |
| search     | search                                                                                                                                                                                                      |
| semantic   | get_semantic_context, get_semantic_schema, search_semantic_context, validate_semantic_query                                                                                                                 |
| oauth      | create_oauth_url                                                                                                                                                                                            |
| data_apps  | get_data_apps, deploy_data_app, modify_streamlit_data_app, modify_python_js_data_app, create_python_js_data_app_git_credential, delete_python_js_data_app_draft                                             |
| doc        | docs_query                                                                                                                                                                                                  |

Cross-cutting (Phase 5): tool filtering (`is_read_only_tool`/`is_semantic_tool`,
`X-Read-Only-Mode`, semantic gating), authorization + `/preview/configuration`
(AI-3438 hardening — port checks exactly), OAuth provider, prompts, resources,
TOOLS.md generator + `check-tools-docs` gate.

---

## 5. Models (Phase 3)

Port pydantic → zod across `tools/*/model.py`, `*/api_models.py`, `search_models.py`,
`flow/scheduler_model.py`, `semantic/model.py`, `components/model.py`. Reuse api-client
`*/types` where API shapes already match. Confirm whether `toon-format` output encoding
must be byte-preserved (§9 Q1).

---

## 6. Tests (Phase 6)

Port `tests/` (33 files) → `__tests__/` mirroring structure. parametrize → `test.each`;
`conftest.py` → vitest setup + factories. **HTTP mocking via msw** (don't over-mock
client internals). Iterate module-by-module to green. `integtests/` ported **last**.

---

## 7. CI/CD (Phase 7) — adapt existing workflows in place

- **`ci.yml`**: replace Python matrix with Node 24; run `vitest`, `tsc`, lint, `build`,
  and the TOOLS.md check. Keep Codecov.
- **`release.yml`**: keep the tag→image mapping verbatim (`v*`/`agent-v*` →
  `production-<sha>`+`latest`; `canary-orion-*`; `dev-*`) so kbc-stacks routing is
  unchanged. Build the Node Dockerfile. Secrets: `DOCKERHUB_PUSH_USER`,
  `DOCKERHUB_PUSH_TOKEN`.
- **npm publish**: add a publish step (npm token) — publish `@keboola/mcp-server` on
  `v*` tags (or via release). `bin` + `files:[dist]` + `publishConfig.access:public`.
- **`kaibench.yml`**: keep the service wiring (MCP server from branch + kai-assistant +
  postgres + redis); point it at the Node build.

---

## 8. Phasing (draft PR, iterate to green)

0. Branch off `main`; scaffold TS skeleton alongside Python (keep Python running until
   parity), open **draft PR** early.
1. Core infra; smoke: `tools/list` over stdio returns 39 tools with correct schemas.
2. Client adapters (+ track api-client gap additions).
3. Models for first module → 4. Tools module-by-module, porting each test file with it
   (storage → jobs → sql → components → flow → project → search → semantic → oauth →
   data_apps → doc).
4. Cross-cutting (filtering, auth, preview, oauth, prompts, resources, TOOLS.md gen).
5. Unit-test parity green; TOOLS.md regenerated + gated; **remove Python sources**.
6. CI/CD adapted; Docker + npm + KaiBench green.
7. Integtests ported. 9. README + SECRETS doc finalized.

Commits start with `PSGO-268:`; PR references the issue.

---

## 9. Open questions

1. **Output encoding**: Python uses `toon-format` for some tool outputs. Preserve
   byte-for-byte or is JSON acceptable for parity?
2. **Lint stack**: match the `ui` monorepo (oxlint/oxfmt) or keep an eslint/prettier setup
   standalone? (Affects CI + dev ergonomics.)
3. **api-client gap timing**: can we land scheduler/AI-service endpoints in
   `@keboola/api-client` in time, or ship temporary local clients and swap later?

---

## 10. Out of scope / deferred

- In Platform Agent variant (follow-up; tags kept reserved).
- Integtests ported last.
- DB drivers (SQL is HTTP via Query Service).
- No reimplementation of MCP protocol or HTTP client.
