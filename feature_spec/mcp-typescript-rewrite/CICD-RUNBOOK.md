# Keboola MCP Server (TS) — Config, Distribution & CI/CD Runbook

Operational runbook for running, publishing, and CI-testing the rewritten
`@keboola/mcp-server`. **No credential values here** — only what to set and where.
Repo stays standalone (`keboola/keboola-mcp-server`); CI workflows are adapted in place.

---

## 1. Runtime config (the server itself)

Resolution precedence (highest first): **CLI arg → `KBC_*` env var → `X-*` HTTP header**
(same model as the Python `Config`).

| Purpose                                                     | Env var                                           | CLI flag             | HTTP header                     | Required          |
| ----------------------------------------------------------- | ------------------------------------------------- | -------------------- | ------------------------------- | ----------------- |
| Storage API URL (`https://connection.<region>.keboola.com`) | `KBC_STORAGE_API_URL`                             | `--api-url`          | `X-StorageApiUrl`               | yes               |
| Storage API token                                           | `KBC_STORAGE_TOKEN`                               | `--storage-token`    | `X-StorageApiToken`             | yes (token mode)  |
| Branch id (`null`/`default`/`production` ⇒ prod)            | `KBC_BRANCH_ID`                                   | —                    | `X-Branch-Id`                   | no                |
| Workspace schema (SQL)                                      | `KBC_WORKSPACE_SCHEMA`                            | `--workspace-schema` | `X-Workspace-Schema`            | for SQL tools     |
| OAuth bearer (HTTP transports)                              | —                                                 | —                    | `Authorization: Bearer <token>` | OAuth mode        |
| OAuth client id / secret                                    | `KBC_OAUTH_CLIENT_ID` / `KBC_OAUTH_CLIENT_SECRET` | —                    | —                               | OAuth server mode |
| OAuth server URL / scope                                    | `KBC_OAUTH_SERVER_URL` / `KBC_OAUTH_SCOPE`        | —                    | —                               | OAuth server mode |
| MCP server public URL                                       | `KBC_MCP_SERVER_URL`                              | —                    | —                               | OAuth mode        |
| JWT signing key                                             | `KBC_JWT_SECRET`                                  | —                    | —                               | OAuth mode        |
| Read-only mode                                              | —                                                 | —                    | `X-Read-Only-Mode`              | no                |
| Conversation id                                             | —                                                 | —                    | `X-Conversation-Id`             | no                |
| Log level                                                   | —                                                 | `--log-level`        | —                               | no                |
| App env / version (telemetry)                               | `APP_ENV` / `APP_VERSION`                         | —                    | —                               | no                |
| Datadog tracing                                             | `DD_*` (dd-trace)                                 | —                    | —                               | container only    |

Rules (port from Python): stdio transport **rejects** OAuth config (HTTP only); secret
fields redacted in logs; never commit credentials (`.env` gitignored); local dev via
`--env-file=.env`.

---

## 2. npm distribution (`npx @keboola/mcp-server`)

Published from this repo (standalone). `package.json` must have: `"type":"module"`,
`bin` entry, `files:["dist"]`, `"publishConfig":{"access":"public"}`, ESM+CJS exports
(model `@keboola/api-client`). Build with tsup before publish.

Required CI repo secret:

- **`NPM_TOKEN`** — npm automation token with publish rights to the `@keboola` scope.

Publish trigger: on `v*` release tags (wire into `release.yml` or a dedicated
`npm-publish.yml`). Verify after first publish: `npx @keboola/mcp-server` (stdio),
`npm i -g`, `npm i @keboola/mcp-server` as a lib.

---

## 3. Docker image distribution

Image `keboola/mcp-server` on Docker Hub. Tag→image mapping kept **identical** to the
current Python `release.yml` so `kbc-stacks` routing is unchanged:

| Git tag                     | Image tag                       | Stack        |
| --------------------------- | ------------------------------- | ------------ |
| `vX.Y.Z`                    | `production-<sha>` + `latest`   | production   |
| `agent-vX.Y.Z`              | `production-<sha>` (agent helm) | production   |
| `canary-orion-vX.Y.Z-dev.N` | `canary-orion-<sha>`            | canary-orion |
| `dev-vX.Y.Z-dev.N`          | `dev-<sha>`                     | testing      |

Required repo secrets (already present for the Python build):

- **`DOCKERHUB_PUSH_USER`** + **`DOCKERHUB_PUSH_TOKEN`**.

Dockerfile: Node 24 multi-stage, non-root user, `dd-trace` preloaded,
`ENTRYPOINT ["node","dist/index.js","--transport","streamable-http"]`.

---

## 4. CI/CD secret summary (set in `keboola/keboola-mcp-server`)

| Repo secret / var                                            | Used by      | Purpose                       | Status               |
| ------------------------------------------------------------ | ------------ | ----------------------------- | -------------------- |
| `DOCKERHUB_PUSH_USER` / `DOCKERHUB_PUSH_TOKEN`               | release.yml  | Docker Hub push               | already set (Python) |
| `NPM_TOKEN`                                                  | npm publish  | publish `@keboola/mcp-server` | **add**              |
| `CODECOV_TOKEN`                                              | ci.yml       | coverage upload (optional)    | already set          |
| KaiBench: model/credential secrets, kai-assistant image pull | kaibench.yml | eval run                      | already set (Python) |

KaiBench: the workflow spins up MCP server (from branch) + kai-assistant (prebuilt image)

- Postgres + Redis and runs the eval suite on production `v*` tags. Reuse the existing
  `kaibench.yml`; only the MCP server build step changes (Node instead of uv/Python).

---

## 5. Coordinated `@keboola/api-client` work (cross-repo, in `keboola/ui`)

Endpoints the MCP server needs that api-client v4 does not yet expose — add as new
subpaths in `ui/packages/api-client`, publish, then bump the dependency here:

- **scheduler** (flow activation/scheduling)
- **AI service**: `docs_query`, semantic context/schema/search, global `search`,
  config examples, component finder, description suggestions

Until published, ship thin local clients in `src/clients/` and swap to api-client later.

---

## 6. First-run checklist

- [ ] `NPM_TOKEN` added to repo secrets; `@keboola` scope publish dry-run OK.
- [ ] `DOCKERHUB_PUSH_*` confirmed (carried over from Python build).
- [ ] KaiBench services/credentials confirmed in `kaibench.yml`.
- [ ] Local `.env` (gitignored) with `KBC_STORAGE_API_URL` + `KBC_STORAGE_TOKEN`.
- [ ] Smoke: `npx @keboola/mcp-server` lists 39 tools over stdio.
- [ ] api-client gap endpoints tracked (scheduler, AI service) — local clients vs. published.
