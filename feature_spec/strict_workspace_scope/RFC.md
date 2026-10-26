# RFC: Strict workspace scope for Kai in Data Apps

Linear: [AJDA-3453](https://linear.app/keboola/issue/AJDA-3453/investigate-restricting-kais-token-in-data-app-to-the-dgim-workspace)

## Problem

A Data App can be shared with people who are not members of the project. With direct-grant input
mapping (DGIM) the app's workspace can read only the tables the app maps, and Kai embedded in the app
pins its queries to that workspace with `X-Workspace-Id`. The pin covers `query_data` only. The same
session can still reach data around the workspace:

- `_get_managed_workspace()` ignores the pin and returns the shared MCP-managed workspace, which has
  read-only access to the whole Storage. The data app tools write its id into a new app's
  `WORKSPACE_ID` secret.
- The multi-project fan-out builds a `WorkspaceManager` without the pin, so `query_data` runs on (and
  may create) the MCP-managed workspace.
- Every other tool stays available: `create_config` (incl. a new `keboola.sandboxes` workspace),
  `run_job`, flows, data apps, `create_oauth_url`, `link_shared_bucket` and so on.
- The authorization headers (`X-Allowed-Tools`, `X-Disallowed-Tools`, `X-Read-Only-Mode`) are set by
  the caller. Kai's sandbox reaches this server through a relay that forwards them unchanged, so the
  agent can leave them out.

## Required Behavior

A new per-request field `workspace_scope` (`X-Workspace-Scope` header, `KBC_WORKSPACE_SCOPE` env var)
with a single valid value, `strict`. kai-agent's broker sends it, next to `X-Workspace-Id`, for a Data
App whose pinned workspace uses DGIM.

| | `strict` session |
| --- | --- |
| Pin | Required; a request without `X-Workspace-Id`, or with more than one header setting it (another spelling such as `KBC-Workspace-Id`, or a repeated header), is refused. |
| Tools | Only `query_data`, `get_buckets`, `get_tables`, `get_project_info`, `docs_query`, independent of the authorization headers. The headers can narrow the list further. |
| Pinned workspace | Must not have read-only access to the whole Storage (`readOnlyStorageAccess` must be `false`). |
| MCP-managed workspace | Never looked up or created. |
| Multi-project fan-out | Off; every call stays on the pinned workspace. |
| `/preview/configuration` | Refused (403). |
| `query_data` | Unchanged: read-only statements only (DMD-2473). |

Any other value of `workspace_scope` is invalid. An empty header means "not provided", so a request
can tighten a session but never loosen a server configured with `strict`. Sessions without the field
behave exactly as before.

`search` is not in the list because it also returns configurations, flows and data apps. Metadata is
limited by the Storage token: in a Data App, Kai's token has read permissions on the mapped buckets only.

## Resolution Strategy

- `config.py`: `Config.workspace_scope` (`require_prefix`, `empty_means_absent`, header-eligible),
  validated in `__post_init__`; `Config.is_strict_workspace_scope`.
- `mcp.py`: `apply_request_config` raises the caller-facing `WorkspaceScopeError` (defined in
  `config.py`, also for an invalid `workspace_scope` value) for a strict request without a pin, and
  for one where more than one header sets the pin (`Config.count_header_values`): `KBC_` beats `X-` and
  a repeated header's last value wins, so a caller behind the broker could otherwise replace the
  broker's pin. `create_session_state` checks the pin again (stdio has no request) and stores the scope in the session state
  (`WORKSPACE_SCOPE`, also for a bootstrap session without a token) and passes `strict` to
  `WorkspaceManager.create`. `is_strict_workspace_scope(state)` reads it back.
- `workspace.py`: `WorkspaceManager(strict=True)` refuses a pinned workspace whose `readonly` is not
  `False`, and `_get_managed_workspace()` (and so the `get_data_app_*` helpers) raises.
- `authorization.py`: `ToolAuthorizationMiddleware` intersects the allowed tools with
  `STRICT_WORKSPACE_SCOPE_TOOLS` (`tools/constants.py`) in both `on_list_tools` and `on_call_tool`.
  The scope is read from the session state, not from the authorization headers.
- `multiproject.py`: `MultiProjectMiddleware` passes a strict session straight through.
- `preview.py`: refuses a strict request before building a session.

## Scope

In scope: the strict scope described above.

Out of scope:
- SQL writes into direct-grant output tables (DGOM) through Kai; `query_data` stays read-only.
- Filtering tables inside a mapped bucket; needs Connection to expose a workspace's direct-grant set.
- Forwarding `X-Kbc-App-User-Token` to Query Service (Data App RLS, AJDA-3569).
- kai-agent, sandboxes-service and keboola-operator changes (Kai's own Storage token, the
  `runtime.kai` flag in the Data App configuration).

## Testing / Verification

Unit tests:
- `tests/test_config.py`: parsing, normalization, the prefix requirement, empty header, invalid value.
- `tests/test_mcp.py`: a strict request without a pin is refused; the scope is kept in the bootstrap
  session state.
- `tests/test_workspace.py`: a strict manager refuses a read-only-all workspace and never touches the
  MCP-managed workspace.
- `tests/test_authorization.py`: strict allowlist for listing and calls, with and without headers.
- `tests/test_multiproject.py`: no fan-out, no ask-first gate, no project filter.
- `tests/test_preview.py`: previews are refused.

Manual (canary stack): a Data App with DGIM and Kai; through the relay, a `tools/call create_config`
without authorization headers is refused, `query_data` on a mapped table works, and a query on an
unmapped table is refused by the backend.
