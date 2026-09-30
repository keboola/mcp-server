# RFC: `get_data_app_preview_link` — open a dev-mode data app in the agent's browser

Linear: [PAT-2167](https://linear.app/keboola/issue/PAT-2167/)

Cross-repo design: "Agent Access to Dev-Mode Data Apps" (approved 2026-09-23, updated 2026-09-27).

## Problem

An agent that develops a python-js data app deploys its draft with `deploy_data_app(mode='dev')`,
but it cannot see the running app. The dev app sits behind the app's normal authentication
(basic auth by default). Today the only way past it is the kai-preview iframe handshake in kbc-ui,
which needs the user's own Storage token inside kbc-ui. Kai has no kbc-ui session and never holds a
Storage token, and a local agent (Claude Code) has no iframe. The visible symptom: the agent can
deploy and read logs, but cannot load the app in a browser to check what it built.

The platform side is being added: sandboxes-service mints a 60-second signed preview link, and
apps-proxy redeems it in the browser for a session cookie on the app host (4 h idle, 12 h hard cap,
valid only while the app is in dev mode). The MCP server needs a tool that mints the link.

## Required Behavior

New tool `get_data_app_preview_link`.

| Item | Value |
| --- | --- |
| Parameters | `configuration_id: str` (Storage configuration ID of the data app, as in every other data-app tool); `project_id: ProjectIdArg = None` (the single-target rule for non-read-only tools) |
| Backend call | `POST apps/{data_app_id}/preview-link` on the data-science API, with no request body |
| Output | `url` (the link, `https://<app host>/_proxy/preview#t=<token>`), `link_expires_at` (ISO 8601, about 60 s after minting) |
| Annotations | not read-only, not destructive (`ToolAnnotations(destructiveHint=False)`); tag `data-apps` |
| Branch | main branch only (in `DATA_APP_BRANCH_GATED_TOOLS`, like every data-app tool) |
| Read-only role / read-only scope | not available (the tool is not read-only) |

The tool description tells the agent:

- open `url` in its browser tool before `link_expires_at`, and never fetch it with an HTTP client;
- the browser session ends without notice (about 4 h without requests, 12 h after the link was
  opened at the latest, or at once when the app leaves dev mode);
- after one successful open the browser session keeps working and slides while in use, so reloads
  and navigation in the app need no new link;
- it calls the tool again only when the session has ended, which shows as the app's login page
  ("This app is password protected") or an app that looks broken until reloaded, or when a link
  was not opened before its `link_expires_at`;
- it never types a password into the app's login page and never asks the user for one;
- it does not share `url` (it grants access to the app until it expires).

Refusals from sandboxes-service are mapped to actionable tool errors:

| Response | Tool error tells the agent |
| --- | --- |
| 400 `App "<id>" is not in dev mode.` on a python-js draft | deploy it with `deploy_data_app(action="deploy", mode="dev", configuration_id=...)`, then call again |
| 400 not-dev on a python-js prod app | do not switch the prod app to dev mode; find or create a draft (`get_data_apps`, `modify_python_js_data_app(parent_configuration_id=...)`), deploy it in dev mode, preview the draft |
| 400 not-dev on a Streamlit app | Streamlit apps have no preview link (no dev mode) |
| 400 `App "<id>" has no URL yet.` | deploy the app if needed, wait until it runs, call again |
| 400 or 403 `Token is not authorized to manage app …` (permission checker; 400 today, 403 after its fix), any other 403 | the token cannot manage this app (other project); use `project_id` or a token of that project |
| 404 `No route found for …` (sandboxes-service without the endpoint yet) | preview links are not available on this stack yet; tell the user |
| other 404 | the data app was not found by the data-science service (probably just deleted) |
| 503 `App preview links are not configured.` | preview links are unavailable on this stack; tell the user; do not try the password login |
| anything else | the original HTTP error, unchanged |

The link and its token are never logged: the client response model and the tool output leave
`url` out of `repr()`, and `tool_errors` records only arguments in its Storage event.

## Resolution Strategy

- `clients/data_science.py`: `AppPreviewLinkResponse` (`url` with `repr=False`, `link_expires_at`
  aliased from `linkExpiresAt`) and `DataScienceClient.create_app_preview_link(data_app_id)`.
- `tools/data_apps.py`: `DataAppPreviewLinkOutput` and the tool. The tool resolves the data-app ID
  with the existing `_fetch_data_app(client, configuration_id=..., data_app_id=None)`, which also
  validates the component and gives the app type and draft flag used for the not-dev message. Only
  the mint call is wrapped in the error mapping. The 400 cases are told apart by the `error` field
  of the sandboxes-service JSON body (`keboola/api-error-control` format:
  `{"error", "code", "exceptionId", "status", "context"}`), not by status code alone.
- `mcp.py`: add the tool to `DATA_APP_BRANCH_GATED_TOOLS`.
- `deploy_data_app` docstring: one line in its "Mode" section pointing at the new tool.
- No client-side dev-mode pre-check: `DataAppResponse` has no `mode`, and the server decides.

Trade-offs:

- `configuration_id` rather than the data-app ID: every data-app tool is keyed by configuration
  ID and agents hold configuration IDs. The price is one Storage call plus one data-science call
  before minting.
- The url is returned in the tool output, so it reaches the transcript and LLM provider logs. The
  design accepts this because of the 60 s lifetime; the redeemed 12 h session cookie never passes
  through the MCP server.

## Scope

In scope: the tool, the client method, branch gating, TOOLS.md, `docs/python-js-data-apps.md`,
unit and integration tests.

Out of scope:

- sandboxes-service mint endpoint and JWKS, apps-proxy redeem and session (other repos).
- v2 draft preview links (`POST /v2/apps/{appId}/drafts/{draftId}/preview-link`); MCP drafts are
  legacy apps and use the legacy endpoint.
- Kai's tool approval class, prompt-injection limits and browser setup (ui repo,
  `packages/constants/mcp-tools.ts`, owned by the Kai developers).
- Rewording the existing kai-preview descriptions in `deploy_data_app` and
  `modify_python_js_data_app` (removed together with kai-preview).

## Testing / Verification

- Unit (`tests/clients/test_data_science.py`): endpoint, no body, response parsing, `url` hidden
  from `repr`.
- Unit (`tests/tools/test_data_apps.py`): happy path; one parametrized test for every refusal
  row above, with exceptions built by the real `RawKeboolaClient._raise_for_status`; passthrough of
  unmapped errors (other 400, HTML 503, 500); no mint when the configuration lookup fails; no token
  in logs at DEBUG, in reprs or in the Storage event.
- Unit (`tests/test_server.py`, `tests/test_mcp.py`): registration, annotations and tags,
  branch gating.
- Integration (`integtests/tools/test_data_apps.py`): in the python-js prod/draft lifecycle,
  after the draft's dev deploy the tool returns a `/_proxy/preview#t=` url with a future
  `link_expires_at`; after the prod redeploy the tool refuses the prod app with the prod-specific
  message. Needs sandboxes-service with preview keys on the integtest stack.
- Manual on a dev stack: mint through the MCP tool, open the url in headless Chrome, the app loads.
