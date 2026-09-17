# RFC: Agent Provisioning Support

Linear: [DMD-1939](https://linear.app/keboola/issue/DMD-1939/agent-provisioning-support-in-keboolamcp-server)
Depends on: [DMD-1806](https://linear.app/keboola/issue/DMD-1806/variant-c-agent-created-project-with-immediate-user-confirmation)
(`POST /manage/programmatic-projects`, keboola/connection#8081, #8122, #8162)

## Problem

The MCP server can only ever *use* a Keboola project someone else already created: it needs both an
existing project and a credential for it before a single tool call works. A first-time user must
leave the agent, sign up in a browser, create a project, mint a token and paste it back.

Connection now has a provisioning endpoint that removes the whole detour — it creates a project and
hands back a working, project-pinned session with no prior Keboola identity — but nothing in the MCP
server can reach it, and the server refuses to start a session without a token at all
(`create_session_state` raises `ValueError('Storage API token is not provided.')`), so there is no
state from which the tool could even be called.

## Required Behavior

| # | Behavior |
| --- | --- |
| 1 | The server starts and serves `tools/list` with **only** a stack URL configured — no token, no stored login. |
| 2 | A new `create_project` tool provisions a project via `POST /manage/programmatic-projects` and returns the project id, name, backend and `confirm_url`. |
| 3 | The access + refresh tokens are **never** part of the tool result (they would land in the model context and the transcript). They are written to the local credential store only. |
| 4 | Tool calls after `create_project` work against the new project, in the same session and in later ones, with no further user action. |
| 5 | The 1 h access token is refreshed from the 30 d refresh token before it expires, at the existing `POST /v1/auth/token/refresh`. |
| 6 | `backend` (`snowflake` / `bigquery`) is an optional argument; omitted keeps the agent maintainer's default. |
| 7 | The request carries a `clientId` identifying the calling MCP client, so `auditLog.agentProvisioning.projectProvisioned` has real attribution. |
| 8 | A stack without the `agent-provisioning` feature (endpoint 404) is reported as "not available on this stack" — a sentence, not a stack trace. |
| 9 | After the human confirms the claim, the agent session is revoked. The next tool call reports that the session ended and how to re-authenticate, instead of a bare 401. |

## Resolution Strategy

### Bootstrap mode (`mcp.py`)

`SessionStateMiddleware.create_session_state()` currently raises when `config.storage_token` is
empty. It now returns a **bootstrap state** instead: no `KeboolaClient`, no `WorkspaceManager`, just
`CONVERSATION_ID`. The raise is kept for the case that is still an operator error — a token present
but no `storage_api_url`.

Everything that reads the client off the session state has to tolerate its absence:

* `KeboolaClient.from_state()` raises `ValueError` with the actionable message (call `create_project`
  or run `keboola-mcp-server login`) instead of a bare `KeyError`. One guard in the shared accessor
  rather than a check in each of its ~40 call sites.
* `ToolsFilteringMiddleware.on_list_tools()` skips feature/role filtering when there is no client and
  advertises the superset — the same thing it already does for programmatic sessions.
* `create_project` joins `BOOTSTRAP_TOOLS` (`tools/constants.py`), which already exempts a tool from
  `ToolsFilteringMiddleware.on_call_tool`'s `verify_token()` round-trip and from
  `MultiProjectMiddleware`'s ask-first scope gate.
* `tool_errors()`'s `_trigger_event` skips the Storage event when there is no client: a bootstrap
  session has no project to write one to. The provisioning is still attributed, by the `clientId`
  the tool sends to Connection.

### Provisioning call (`auth_login.py`)

`provision_agent_project()` sits next to `exchange_code` / `refresh_tokens`: the same unauthenticated
Connection auth surface, the same injected-`httpx.AsyncTransport` testing seam, and the same
credential store. It POSTs `{clientId, projectName?, backend?}` and maps the response to
`ProvisionedProject`, whose `tokens` field is a plain `TokenSet` — so the provisioned session is
stored, read and refreshed by the code that already does all three for a `login` session.
`accessTokenExpiresIn` is the endpoint's spelling of `expiresIn`; `parse_token_response` accepts
either.

A 404 becomes `AgentProvisioningUnavailableError`; a 429 keeps its `Retry-After` in the message.

### Storing the session

`save_tokens(storage_api_url, TokenSet(..., project_ids=[project_id]))` — the credential store keyed
by stack host + interface profile, exactly as `login` writes it. The consequences are all reuse:

* `SessionStateMiddleware._maybe_use_stored_session()` picks the token up on the next request, so
  requirement 4 needs no new code.
* `get_access_token()` refreshes it near expiry and persists the rotation (requirement 5).
* `project_ids=[id]` makes `_read_persisted_login_scope()` return a **confirmed** single-project
  scope, so data tools work immediately instead of being held at the ask-first gate.

Because the store holds one session per (stack, profile), `create_project` refuses when one is
already there rather than overwriting a login the user still needs. Provisioning for an
already-authenticated session is a separate feature (a project created *inside* an owned
organization) and is out of scope here.

### Revoked session (requirement 9)

Confirmation revokes the agent session, so the access token 401s while still unexpired — the refresh
path that self-heals an *expired* session never runs. `SessionStateMiddleware.on_request` therefore
catches a 401 raised under a locally-stored programmatic session and drops the credential, so the
next request starts clean instead of 401ing for the rest of the hour.

Deleting a credential is destructive, so all three of these must hold before anything is removed:

| Guard | Why |
| --- | --- |
| The request used the *stored* token | A token supplied per request (an HTTP header) is not this session's stored credential, and its 401 says nothing about the stored one. |
| `introspect_token` re-check fails with 401/403 | A 401 alone is not proof the credential is dead — an unconfirmed or stale project scope produces one too (`RawKeboolaClient._raise_for_status` already explains those). A timeout, connection error or 5xx proves even less: the credential is kept. |
| The stored entry is *still* that token | `forget_rejected_access_token` compares under the same credential-store locks `get_access_token` refreshes with, so a concurrent refresh or a fresh `login` is never thrown away. |

The original error is re-raised either way.

## Scope

In scope: the `create_project` tool, bootstrap session state, the provisioning client call, storing
and refreshing the provisioned session, and the revoked-session recovery.

Out of scope:

* Confirming the claim from the agent — that is a human action in a browser, by design.
* `syncBackendInit`. The endpoint defaults to async init and the MCP client is a chat session that
  should not block for minutes on backend provisioning; the tool says the project may take a moment
  to become usable instead.
* Provisioning from an already-authenticated session (see above), and provisioning on the deployed
  server, which authenticates with OAuth and has no local credential store — there `create_project`
  reports that it is a local-session tool.
* Any change to `POST /token/refresh` — it already exists and is already used.

## Testing / Verification

Unit (`tests/test_auth_login.py`, `tests/tools/test_project.py`, `tests/test_mcp.py`), all with a
mocked `httpx` transport:

* `provision_agent_project` maps the response, including `accessTokenExpiresIn`; 404 raises
  `AgentProvisioningUnavailableError`; 429 keeps `Retry-After`.
* `create_project` returns project id + confirm URL and **no** token material (asserted against the
  serialized result), stores the session with `project_ids=[id]`, forwards `backend` and a sanitized
  `clientId`, and refuses when a session is already stored.
* `create_session_state` with no token returns a state without `KeboolaClient`; `from_state` on it
  raises the actionable message; `on_list_tools` returns the full list in that state.

E2E (manual, canary-orion — the stack with `agent-provisioning` enabled): start the server with only
`KBC_STORAGE_API_URL`, call `create_project`, confirm the returned URL in a browser, verify a data
tool works before confirmation and reports an ended session after it.
