# RFC: Accept programmatic bearer tokens, scoped by `X-KBC-ProjectId`

Linear: [AI-3755](https://linear.app/keboola/issue/AI-3755/accept-programmatic-bearer-tokens-for-toolslist-scoped-by-x-kbc) —
filed off the "Plan D aftermath" `tools/list` incident writeup (kai-agent).
Superset design: [`feature_spec/pat_token_support/RFC.md`](../pat_token_support/RFC.md) (PSGO-261,
PR #605, unmerged) — this RFC extracts and ships only that RFC's "Part A" in isolation; see Scope.

## Problem

A one-line cache-key fix in Kai's tool-inventory cache (adding `principal.projectId` to the key)
stopped a shared cache entry from silently answering one project's `tools/list` with another
project's data. Fixing that correctness bug surfaced a second, previously-masked one: some Kai
sessions carry a Keboola **bearer session token** (`kbc_at_...`) instead of a legacy **Storage API
token**. Before the cache fix, the first successful (SAPI) caller's response was cached under a
project-less key and served to every other project — so a bearer session's request never actually
reached this MCP server, and its broken credential was invisible. After the fix, every project asks
with its own credential, and bearer sessions now make the call they were always supposed to make —
which fails, because **this server accepts only legacy Storage API tokens**. It has no way to turn
a bearer token into something Keboola's Storage API will accept.

**Symptom:** `tools/list` (and any other call) 401s for a session whose credential is a bearer
token, with no path to make it work — kbc-ui's bearer-token rollout is not something Kai (or this
server) can opt out of, and there is no client-side exchange path (`getSapiTokenString()`
deliberately returns empty under a bearer session).

## Required Behavior

- A request carrying `Authorization: Bearer kbc_at_...` (or `kbc_pat_...`) **and** an
  `X-KBC-ProjectId` header must be accepted and resolved to that project's legacy Storage token,
  exactly like a `X-StorageAPI-Token` request is today.
- Legacy `X-StorageAPI-Token` traffic is completely unaffected.
- `X-KBC-ProjectId` **pins** the session to exactly one project — there is no project enumeration,
  no scope confirmation, and no "ask which project(s)" gate. The caller (kbc-ui/Kai) already knows
  which project the request is for; this server does not need to ask again.
- `get_jobs`/`run_job` and the other AI-service/sync-actions-backed tools must keep working for a
  bearer session once `tools/list` succeeds — not just the Storage-API-backed tools.
- `tools/list` (and `resources/list`, `prompts/list`) must stay fast regardless of credential type.

## Resolution Strategy

This is "Part A" of the already-written and reviewed PSGO-261 RFC, cherry-picked out of the
(much larger, still-unmerged) PR #605 branch as its own standalone change, plus one fix from
later in that branch that Part A needs to be useful end-to-end, plus one new robustness fix this
narrower scope specifically calls for:

1. **`clients/auth_bridge.py`** (new file): `is_programmatic_token()` detects a `kbc_at_`/`kbc_pat_`
   bearer token; `StorageTokenResolver.resolve()` exchanges it at Connection's
   `POST /manage/internal/auth-bridge/resolve-storage-token`, authenticating with the server's own
   projected ServiceAccount JWT (`X-Kubernetes-Authorization`, read from
   `KBC_KUBERNETES_TOKEN_PATH` per request) plus the caller's token as `X-Subject-Token` and the
   project id (from `X-KBC-ProjectId`) in the body. Resolver `400/401/403` pass through unchanged;
   `5xx`/timeout/network map to `502`. No token material is ever logged.
2. **`config.py`**: new `project_id` field, mapping `X-KBC-ProjectId` (via alias) and
   `KBC_PROJECT_ID`.
3. **`mcp.py`**: `SessionStateMiddleware.create_session_state` detects a programmatic token and
   exchanges it before building `KeboolaClient`, using `config.project_id` to pin the exchange to
   one project. No scope object, no multi-project fan-out, no ask-first gate — a session either has
   a legacy token (used as today) or a bearer token + project id (exchanged once, used as today
   from that point on).
4. **`clients/client.py`**: `jobs_queue`, `ai_service`, and `sync_actions` are switched onto the
   already-existing `bearer_or_sapi_token` (storage/scheduler/data-science/metastore already use
   it) instead of the raw `self._token`. Without this, a resolved token still 401s on
   `get_jobs`/`run_job` because those three clients were wired to send the pre-exchange raw token.
   Cherry-picked from later in the PSGO-261 branch, where this exact gap was found and fixed.
5. **New for this narrower scope — skip the exchange on `/list`.** The resolver call has up to a
   ~35s timeout (`connect=5s, read=30s`); a client's initial `tools/list` fetch must be fast, and
   this server already has a precedent for this exact trade-off (branch-id validation is likewise
   skipped for `/list`). `create_session_state` gains a `skip_token_exchange` flag, set from
   `on_request` whenever `context.method.endswith('/list')`. On `/list`, the raw (unexchanged)
   token is used to build `KeboolaClient` as-is: any Storage/metastore call made with it fails fast
   (an ordinary 401/403, not a hang) and is already handled as a soft failure —
   `project_has_semantic_models` fails closed on any exception. This directly forecloses the
   `tools/list`-hangs-under-a-bad-credential failure mode this incident is about, for the new
   bearer-token path specifically.

## Scope

**In scope:** accepting a bearer token + `X-KBC-ProjectId`, resolving it to a legacy Storage token,
and making every downstream service (Storage, AI Service, Jobs Queue, sync-actions, metastore,
data-science, scheduler) work with the resolved token for that one pinned project.

**Explicitly out of scope** (all still live only on the unmerged PSGO-261 branch, PR #605):

- Local browser PKCE login (Part B).
- Multi-project scope: `get_accessible_projects`/`set_project_scope`, token introspection,
  scoped-token exchange, per-project fan-out, the ask-first gate, per-call `project_ids` filtering.
  `X-KBC-ProjectId` here pins one project outright; there is no session-scope object to manage and
  nothing for an ask-first gate to guard.
- OAuth session exchange (Part D).
- Everything under "Security hardening" (increment 7) in the PSGO-261 RFC — credential-race
  fixes, `scope_token` encryption/binding, login-time scoping, etc. None of that machinery exists
  in this narrower change, so none of those findings apply to it.

## Testing / Verification

- `tests/clients/test_auth_bridge.py`: `is_programmatic_token` detection, resolver success/error
  mapping (`400/401/403` passthrough, `5xx`/timeout/network → `502`), no-token-material-logged.
- `tests/test_mcp.py` (`TestProgrammaticTokenExchange`): missing `KBC_KUBERNETES_TOKEN_PATH`,
  missing/invalid `project_id`, happy-path resolver call; `test_on_request_branch_handling`
  extended to assert `skip_token_exchange` is `True` for every `/list` method and `False`
  otherwise; `test_create_session_state_skips_exchange_for_list_requests` asserts the resolver is
  never constructed when `skip_token_exchange=True` and the raw token is used as-is.
- `tests/clients/test_client.py`: parametrized token-selection test across all six
  bearer-capable clients (storage, scheduler, data-science, metastore, jobs-queue, ai-service,
  sync-actions), confirming a bearer token takes precedence over the raw token everywhere.
- `tox` (pytest, ruff, check-tools-docs) green.
- Manual/integration verification against a dev stack (bearer token + `X-KBC-ProjectId` against a
  real `tools/list` and a real `get_jobs`) is still pending — no dev-stack access from this
  environment; flagged as a known limitation until verified.
