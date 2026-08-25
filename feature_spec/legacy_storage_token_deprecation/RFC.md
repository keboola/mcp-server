# RFC: Stop accepting legacy (non-programmatic) Storage API tokens as a client credential

Linear: [AI-3774](https://linear.app/keboola/issue/AI-3774/deprecate-legacy-non-programmatic-storage-api-token-as-an-accepted-mcp)

> Revision 2. Revision 1 proposed only a log-only deprecation warning and listed rejection as out of scope.
> The login flows have since become the supported way to authenticate, so this revision makes rejection the
> goal, with the rollout conditions below.

## Problem

PSGO-261 added three richer credential modes on top of the original design: programmatic multi-project tokens
(`kbc_at_`/`kbc_pat_`, `feature_spec/programmatic_token_project_scope`), OAuth login exchanged into a programmatic
session (`feature_spec/oauth_session_exchange`, `feature_spec/oauth_session_persistence`), and PAT exchange
(`feature_spec/pat_token_support`). Each is revocable, introspectable and carries the user's identity.

The original mode is still accepted unconditionally: a bare per-project Storage API token supplied via
`--storage-token` / `KBC_STORAGE_TOKEN`, or per request as `X-StorageAPI-Token` / `Authorization`. It has no
revocation model, no user identity, no multi-project support, and the multi-project tools
(`get_accessible_projects`, `set_project_scope`) already hard-reject it (`_parent_subject_token`,
`tools/project.py`). Keeping it means every code path that handles credentials carries a second, weaker branch
(`bearer_token is None`, "legacy header", `is_programmatic_token(...)` checks), and the integration tests and CI
keep long-lived per-project tokens in repository secrets.

**Not in scope, and must not be lost:** the legacy Storage token *format* does not go away.
`StorageTokenResolver` (`clients/auth_bridge.py`) exchanges a programmatic token *into* a per-project Storage
token for downstream services (jobs queue, AI service, sync actions) that only understand that format, and
`KeboolaClient.__init__` still takes it as `legacy_storage_token`. The change is about what an **external caller**
may present to this server.

## Required Behavior

| Scenario | Required behavior |
| --- | --- |
| A session is built from a token that is not `kbc_at_`/`kbc_pat_` (any path: CLI arg, env var, `X-StorageAPI-Token`, `Authorization`) | Refused with an error naming the supported alternatives. The message never contains token material. |
| A session is built from a programmatic token, an OAuth-exchanged session, a local `login` session, or a provisioned session | Unchanged. |
| Multi-project fan-out (`client_for_project`) | Unchanged; it already builds clients from scoped programmatic tokens through the same factory. |
| Integration tests and the release smoke test | Authenticate as a service user through the login flow, with no legacy token ever handed to the server under test. |
| `README.md` | States that a bare per-project token is no longer accepted and how to get a programmatic one, including headless. |

## Rollout precondition (blocking merge)

Production callers still send legacy tokens to the In Platform Agent deployment (`mcp-server-agent`, same image):

- The Kai assistant backend and the Kai agent forward the per-project session token as `X-Storage-Token` on every
  turn where the UI supplies one. Only their Bearer (`kbc_at_`) path survives a rejection.
- The change that adds the programmatic-token path to Kai (AI-3391) explicitly leaves rejection of the legacy
  header to a later change, and it has not been merged.
- A desktop assistant that spawns this server as a stdio subprocess with a vaulted per-project token pins an old
  server version, so it breaks only when that pin is raised.

Therefore: **this change must not be released until the Kai UI/agent send only programmatic tokens** (or the
deployment-level opt-out below is accepted). The implementation PR stays a draft until that is confirmed. This is
also why the version bump is major.

## Resolution Strategy

1. **One enforcement point: `KeboolaClient.create` (`clients/client.py`).** Both session builders
   (`SessionStateMiddleware.create_session_state`, `MultiProjectMiddleware.client_for_project`) go through it. It
   raises `ValueError(LEGACY_TOKEN_REJECTED_MESSAGE)` when `is_programmatic_token(storage_token)` is false. The
   `bearer_token` parameter of `create` is deleted: it was only meaningful next to a legacy token and was
   overwritten for a programmatic one. A `Bearer ` prefix is still tolerated, a leading space is not.
2. **`mcp.py`:** the `project_id if is_programmatic_token(...) else None` conditional in `create_session_state` is
   gone (always programmatic past the factory).
3. **Tests that used fake legacy tokens** (`test-token`, `SAPI_1234`, ...) switch to `kbc_at_...` values. Tests of the
   legacy branch become rejection tests: parametrized over bare, `Bearer`-prefixed, numeric-format and
   wrong-prefix tokens, asserting the message does not echo the token and the resolver is never called.
4. **Service-user login for CI (`integtests/service_login.py`).** Password + TOTP, no browser:
   `POST /v1/auth/login` -> (`mfaRequired`) -> `POST /v1/auth/mfa` returns the token pair directly (the same calls the
   Keboola CLI's `auth login-password` makes), then `POST /v1/auth/pat/exchange` mints a child token scoped to the one
   locked pool project. The integtest fixtures `mcp_token` / `service_session_token` replace the pool token wherever
   a token reaches the server under test; the old `INTEGTEST_STORAGE_PAT` secret and the `sapi` auth mode of
   `test_pat_multiproject.py` are deleted. The release smoke test (`release.yml` + `tests/docker/ci.sh`) logs in
   the same way and hands the container the token through the environment, never as an argument.
5. **Docs:** `README.md` and `integtests/README.md`.

### What stays on legacy tokens (deliberately, this PR)

The integration-test **fixtures' own direct Storage API calls** (project lock, cleanup, test data via `kbcstorage`,
and tests that build `KeboolaClient(legacy_storage_token=...)` directly) keep the per-project pool tokens. They are
not an MCP credential and never reach `create`. Moving them to login-minted tokens depends on whether Storage
accepts a programmatic token on those calls and is to be verified on the canary stack first (follow-up).

### Inventory: what each legacy-token reference is

| Bucket | Examples | Action |
| --- | --- | --- |
| Transport field | `Config.storage_token`, `--storage-token`, `KBC_STORAGE_TOKEN`, `X-StorageAPI-Token` header: they also carry `kbc_at_`/`kbc_pat_` | Keep |
| Legacy-acceptance branch / test | `create()` legacy pass-through and "separate OAuth bearer" cases, `test_local_legacy_token_keeps_legacy_header`, the `sapi` mode in `test_pat_multiproject.py`, `INTEGTEST_STORAGE_PAT`, the `bearer_token` argument of `create` | Deleted |
| Internal legacy format | `StorageTokenResolver`, `KeboolaClient.legacy_storage_token`, the legacy header to jobs queue / AI service / sync actions | Keep |
| Data App runtime | `resources/data_app/*_query_data_code.py` (they call Storage/Query, not this server) | Keep |
| Dead once only programmatic tokens are accepted | `bearer_token` is now a required `KeboolaClient` argument (the "no bearer" branches in `KeboolaClient.__init__` and `workspace.py` are gone); the `is_programmatic_token(client.bearer_token)` guards in `mcp.py` / `tools/project.py`; `_is_oauth_authenticated` and the `is_oauth` argument of `authorize_tool_call`; the role-dependent `update_flow`/`modify_flow` choice and the admin/share-only merge-request writes (the Storage API enforces the role itself); the legacy-token sentence in the server instructions | Deleted, in a separate commit |

### Consequence worth a decision

Every session now carries a bearer token, so the role/credential-dependent tool choices collapsed to the behaviour
programmatic sessions already had: `modify_flow` is the flow-update tool for everyone, merge-request writes are no
longer hidden by role, and `update_flow` stays unlisted and refused. The `update_flow` tool itself, its entry in the
`/preview/configuration` route and the prompts that mention it are a product surface shared with the UI and Kai, so
removing it is a separate change.

## Security considerations

- **No token material in messages or logs.** The rejection message is a constant; tests assert it. The existing
  `Failed to initialize Keboola client: <e>` log line therefore cannot leak the token.
- **CI credential.** A password + TOTP seed in repository secrets is a stronger credential than one per-project
  token: it reaches every project the service user belongs to, and the seed never expires. Mitigations: a dedicated
  least-privilege service user that is a member of the test projects only; secrets scoped to this repository's
  push/release jobs (never `pull_request` from forks, which already skip them) and stored as secrets of a GitHub
  Environment (`integtests`) whose deployment refs are limited to `main` and the release tags, so a pushed feature
  branch cannot read them; login output masked with
  `::add-mask::`; the helper never prints response bodies or tracebacks; the child token is project-scoped with a
  three-hour lifetime. The service user must not have SSO enforced or WebAuthn-only MFA.
- **TOTP single use.** Two jobs logging in inside one 30 s window collide; `login` waits for the next window and
  retries once. Failed attempts count against per-email and per-IP rate limits and a repeated failure triggers a
  captcha the script cannot solve, so the helper does not loop.
- **Downgrade.** Rejecting at the factory means no new code path can quietly reintroduce a legacy session: the
  constructor is only reachable from the factory in production code.
- **Opt-out.** None in this revision. If the Kai migration slips, a deployment-level environment flag for the agent
  chart (never a request header) is the smallest escape hatch and can be added without touching the factory contract.

## Scope

In scope: factory rejection, `bearer_token` parameter removal, test conversion and deletion, the service-user login
helper, CI/release workflow changes, README/integtests README, major version bump + `uv lock`.

Out of scope: removing `StorageTokenResolver` or `legacy_storage_token`; removing the `update_flow` tool and its
preview-route entry (a follow-up that needs the UI and Kai); renaming the inbound `storage_token` names, which stay
as the transport for programmatic tokens; migrating the integtest fixtures' direct Storage calls to
login-minted tokens; the Kai UI/agent and desktop-assistant migrations (owned by their repos); KaiBench's static
token, which goes through Kai and is covered by the Kai migration.

## Testing / Verification

1. Unit: `tests/clients/test_client.py` (rejection parametrization, resolver not called, no token echoed),
   `tests/test_mcp.py` (`create_session_state` rejects and does not echo), `tests/test_service_login.py` (RFC 6238
   vectors, login with and without MFA, error body not surfaced, one retry on a reused code).
2. `tox`: pytest, ruff, `check-tools-docs` (no tool signature changes, `TOOLS.md` untouched).
3. **Canary stack, before un-drafting:** run `uv run python -m integtests.service_login` against the pool stack with
   the real service user (confirms `/v1/auth/login` + `/v1/auth/mfa` work for it); run `tox -e integtests`; run the
   release smoke test locally with the produced token (`tests/docker/ci.sh`), which also confirms a project-scoped
   token plus `KBC_PROJECT_ID` is enough for a container session. None of these has been run yet.
4. Manual: `KBC_STORAGE_TOKEN=<bare token>` is refused with the documented message; `kbc_at_` / `kbc_pat_` and a
   stored `login` session still work.
