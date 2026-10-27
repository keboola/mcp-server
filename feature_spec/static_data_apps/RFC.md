# RFC: `static_app_*` tools — deploy, confirm, promote, refresh and inspect static data apps

Linear: [AJDA-3676](https://linear.app/keboola/issue/AJDA-3676/mcp-server-static-app-tools-on-the-v2-static-api) (parent [AJDA-3662](https://linear.app/keboola/issue/AJDA-3662))

Design: "Static Data Apps — a runtime-free app type with scheduled snapshots" (v2.1). Interfaces:
[contracts 01 and 06](https://github.com/keboola/static-apps-prototype/tree/main/contracts) (app format;
sandboxes-service management API + OpenAPI).

## Problem

Static data apps are a new data-app type: a folder of HTML/JS/CSS plus `queries.yml`. The platform runs the
queries on a schedule and serves the page with JSON snapshots, with no container. Kai builds such apps, but the MCP
server has no way to publish one. The existing data-app tools deploy Storage configurations to containers
(`deploy_data_app`) and do not fit: a static app is uploaded as a bundle, validated asynchronously, and its
**exposure** (which query results every viewer can download) must be confirmed by a human before it can go live.

## Required Behavior

Five new tools, tag `data-apps`, main branch only (added to `DATA_APP_BRANCH_GATED_TOOLS`), not available to
read-only roles except `static_app_status`. All call sandboxes-service `/v2/apps/{appId}/static/…` through
`DataScienceClient` (the `/v2` client of AJDA-3631).

| Tool | Parameters | Backend calls | Output |
|---|---|---|---|
| `static_app_deploy` | `app_id`, `files` (see below), `project_id` | `POST …/static/deployments` (multipart `bundle`), then `GET …/static/deployments/{id}` until `status ≠ validating` (≤ 60 s) | `deployment_id`, `status`, `exposure`, `exposure_hash`, `exposure_changed`, `validation_errors`, `next_step` |
| `static_app_confirm` | `app_id`, `deployment_id`, `exposure_hash`, `project_id` | `POST …/deployments/{id}/confirm` `{exposureHash}` | `deployment_id`, `status` |
| `static_app_promote` | `app_id`, `deployment_id`, `project_id` | `POST …/deployments/{id}/promote` | `promoted`, `refresh_id`, `status`, `url` |
| `static_app_refresh` | `app_id`, `project_id` | `POST …/static/refresh`, then `GET …/static/refreshes/{id}` until done (≤ 60 s) | `refresh_id`, `coalesced`, `status`, per-query rows/bytes, `error` |
| `static_app_status` | `app_id`, optional `deployment_id`, `project_id` | `GET …/static/status` (+ `GET …/deployments/{id}` when given) | status fields of contract 06; the deployment when asked |

Annotations: `static_app_deploy`, `static_app_confirm`, `static_app_promote` and `static_app_refresh` are not
read-only and not idempotent; `static_app_promote` is destructive (it replaces what viewers see);
`static_app_status` is read-only.

### Bundle input of `static_app_deploy`

The MCP server usually runs remotely and cannot read the agent's disk, so the agent passes the files:

```
files: list[{path: str, content: str, encoding: 'utf-8' | 'base64' = 'utf-8'}]
```

- Must contain `index.html` and `queries.yml` at the root; ≤ 500 entries, ≤ 10 MB decoded (contract 01).
- Paths follow contract 01 (relative, ASCII, no `..`/`//`/dot segments, served extensions only); the tool rejects
  a bad path before uploading, with the same messages as the server.
- The tool packs a `tar.gz` (regular files only) and uploads it. All other checks (SQL guard, dry-run, inline
  scripts, external URLs, secrets) are the server's; their `validation_errors` are returned as they come.
- Binary assets (images, fonts) use `encoding: 'base64'`.

### Exposure confirmation (the main rule)

Contract 06: *"The MCP tool must show `exposure` to the user before calling confirm."*

- `static_app_deploy` returns `exposure` as a readable table (per query: name, row cap, columns and types) plus
  `exposure_hash`, and `next_step` says to show it to the user and ask.
- `static_app_confirm`'s description says: show the exposure from `static_app_deploy` (or `static_app_status`
  with `deployment_id`) to the user, explain that everyone who can open the app can download these columns, and
  call this tool **only after the user explicitly approved this exposure in the conversation**. Never confirm on the
  agent's own judgement, never as part of a "deploy and publish" request.
- `exposure_hash` is required and must equal the deployment's; the server answers `409 exposure_mismatch`
  otherwise. So the agent can only confirm the exposure it was shown.
- **When the client supports form elicitation**, the tool asks the user itself (`ctx.elicit`) with the exposure
  table and a yes/no, and confirms only on "yes". Declined or cancelled → tool error "not confirmed". Clients
  without elicitation rely on the description (the canary limitation of contract 06: the API cannot tell a human
  confirm from an agent confirm).
- An unchanged exposure (`status: ready` after deploy) needs no confirmation.

### Errors

| Response | Tool error tells the agent |
|---|---|
| 400 `invalid_archive` with `context.errors[]` | the listed paths and reasons; fix the files and deploy again |
| deployment `invalid` | the `validation_errors` (`schema`, `sql_guard`, `dry_run_failed`, `unsupported_type`, `inline_script`, `external_url`, `secret_like`, `reserved_path`, `too_large`) and what each means |
| 409 on confirm (`exposure_mismatch`) | the exposure changed or the hash is wrong; show the current exposure again |
| 409 on promote | the deployment is not `ready` (still validating, invalid, or waiting for confirmation) |
| promotion `promotionFailed` | the refresh error (`query_failed`, `exposure_drift`, …); production is unchanged |
| 429 on refresh | refreshed < 5 min ago; `Retry-After` seconds |
| 404 `No route found` | static apps are not available on this stack yet |

## Resolution Strategy

- `clients/data_science.py`: response models `StaticDeploymentResponse`, `StaticRefreshResponse`,
  `StaticStatusResponse` (aliases as in contract 06) and methods `upload_static_deployment`,
  `get_static_deployment`, `confirm_static_deployment`, `promote_static_deployment`, `refresh_static_app`,
  `get_static_refresh`, `get_static_status`.
- `clients/base.py`: a multipart `post_files` (the JSON `post` sets `Content-Type: application/json`) and
  `204`-tolerant handling for confirm.
- `tools/static_apps.py`: the five tools, the packer (`tarfile` + `gzip`, deterministic headers) and the path check.
  A separate module keeps `data_apps.py` (3 k lines) from growing.
- `TOOLS.md` regenerated; version bump.

## Scope

In scope: the five tools, client methods, packer, tests, docs.

Out of scope: creating the static app and its settings (`PATCH …/static/settings`, the refresh interval);
`unpublish`; parquet; UI.

## Open questions

1. **Identifier.** The other data-app tools take the Storage `configuration_id`. Does a static app have a Storage
   configuration (contract 06 creates it with `POST /v2/apps` `type: static`), or should these tools take the
   `/v2` `app_id`? This RFC assumes `app_id`, matching contract 06.
2. **Creation.** The issue says "the existing create tool with `type: static`", but the existing create paths
   (`modify_streamlit_data_app`, `modify_python_js_data_app`) build container configurations. Proposal: a sixth tool
   `static_app_create(name, description)` → `POST /v2/apps {type: static, name, description}`. Needs the create
   body for `static` from AJDA-3674.
3. **Elicitation in Kai.** Does Kai's MCP client support form elicitation? If not, the description is the only
   control there until GA (contract 06 known limitation).

## Testing / Verification

- Unit tests per tool against an in-memory implementation of contract 06; every response it sends is validated
  against `06-management-api.openapi.yaml` (copied into `tests/resources/`), so the tests cannot drift from the
  contract.
- Parametrized cases: unchanged exposure; changed exposure with confirm via elicitation yes / no / unsupported;
  wrong hash → 409; invalid deployment; 400 `invalid_archive`; promote with snapshot / waiting / failed; refresh
  coalesced / 429 / failed; local path rejection; binary file round-trip through the packer.
- Tool description test: `static_app_confirm` mentions showing the exposure and explicit user approval.
- Integration test on canary-orion after AJDA-3674 is deployed: create → deploy → confirm → promote → status.
