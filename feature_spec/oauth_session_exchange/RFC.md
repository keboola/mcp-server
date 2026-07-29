# RFC: OAuth login exchanges for a programmatic session, replacing the project-bound SAPI mint

Linear: [PSGO-261](https://linear.app/keboola/issue/PSGO-261/support-pat-tokens-in-mcp-server-mcp-server)
Parent: PSGO-261 (multi-project PAT support) — this closes the "OAuth→PAT exchange is a separate PR" carve-out that RFC explicitly deferred.
Related: [keboola/connection#7836](https://github.com/keboola/connection/pull/7836) — the new Connection-side internal endpoint this RFC integrates with.

---

## Problem

The MCP server's public/remote OAuth login (`SimpleOAuthProvider`, `oauth.py`) currently:

1. Redirects to `{server_url}/oauth/authorize` (`oauth.py:160,226-237`) — no `scope` sent on purpose (`# send no scopes ... let it use its own default scope`).
2. Exchanges the resulting code at `{server_url}/oauth/token` for a league OAuth access/refresh token pair (`oauth.py:264-297`).
3. Mints a **project-bound legacy Storage API token** from that OAuth access token via `POST {storage_api_url}/v2/storage/tokens` (`_create_sapi_token`, `oauth.py:626-658`), because "AI Service and Jobs Queue... do not support bearer tokens yet" (`ProxyAccessToken.sapi_token` docstring, `oauth.py:114-118`).
4. Stores that legacy token as `config.storage_token` (`mcp.py` `apply_request_config`) — a session pinned to whichever single project was implicit at authorize time, entirely outside the PSGO-261 multi-project architecture (`get_accessible_projects`/`set_project_scope`/fan-out never apply to OAuth sessions today).

**This is changing, unconditionally and immediately** (per keboola/connection#7836):
- `/oauth/authorize` is being **removed outright** — no back-compat, no old-client fallback, no deprecation window, no rollout coordination needed on our side (verify locally, ship when ready).
- The front-channel authorize step moves to a new endpoint, `/oauth/consent`, requesting scope `claudai projectless` (see Decisions §1).
- After the standard code→token exchange (still against Connection, still yielding a league OAuth access token — now `claudai`+`projectless`-scoped), the MCP server must call a **new internal auth-bridge endpoint** to turn that OAuth token into a real Keboola session:

  ```
  POST {manage-host}/internal/auth-bridge/exchange-oauth-token
  Headers:
    X-Kubernetes-Authorization: Bearer <MCP server's projected SA JWT>   # same mechanism as resolve-storage-token
    X-KBC-ManageApiToken:       <see Decisions §3 — verify empirically whether required>
    X-Subject-Token:            Bearer <league OAuth access token, claudai+projectless scope>
  Auth: caller's own Manage token must be TYPE_SUPER or carry scope
        SCOPE_INTERNAL_AUTH_BRIDGE_EXCHANGE_OAUTH_TOKEN; caller must be
        Kubernetes-authenticated (validated KubernetesClaims)

  Response 200 (CliTokenResponse):
    { accessToken, refreshToken, tokenType: "Bearer", expiresIn, sessionId, user: {email, ...} }
  401: subject token missing/invalid, or not bound to an active admin
       (AuthBridgeAuthenticationException | OAuthExchangeUnauthorizedException)
  403: caller not Kubernetes-authenticated / Manage-access-denied, or subject
       token missing the claudai scope (ManageAccessDeniedException | MissingClaudaiScopeException)
  ```

- **Confirmed by keboola/connection's own E2E test** (`AuthBridgeOAuthExchangeTest.php`):
  - A normal `claudai`-scoped subject token exchanges to a **project-pinned** session (`testExchangeIssuesPinnedManagelessSession`).
  - A subject token additionally carrying the **`projectless`** scope (league `user_identifier` = `admin:{id}`, not project-bound) exchanges to an **unrestricted, whole-stack** session — the test explicitly asserts it can reach a project via live membership alone, with no pin (`testExchangeProjectLess...`).
  - The exchanged session has **no Manage API access** (`testExchangedSessionHasNoManageApiAccess` — 401 on `/manage/projects`): it's a pure Storage-scoped programmatic session, not a Manage token.
  - **Exchange-only enforcement:** a `projectless` league token cannot be used directly as a Storage bearer (401) nor via `resolve-storage-token` (401) — this new endpoint is the *only* redemption path for it.
- The `CliTokenResponse` shape is **identical** to a PKCE `login` session (`auth_login.py`'s `TokenSet`: `accessToken`/`refreshToken`/`expiresIn`/`sessionId`). This is the same `kbc_at_*`-style programmatic token the rest of PSGO-261 already knows how to handle.
- **The original league OAuth access/refresh token pair is used exactly once** (for this exchange call) **and then permanently discarded** — never stored, never sent to any other Keboola service, and — per `TokenRefreshProcessor.php` — never touched again even on refresh (see Decisions §4).

**Symptom if unaddressed:** the moment Connection removes `/oauth/authorize`, every public/remote MCP OAuth login (Claude.ai, Cursor, any HTTP client using this server's OAuth flow) breaks outright, with no fallback.

## Required Behavior

### Token contract

| Token | Source | Sent as | Lifetime | Fate |
|---|---|---|---|---|
| League OAuth access token (`claudai`+`projectless` scope) | Connection `/oauth/token` (front-channel via `/oauth/consent`) | `X-Subject-Token: Bearer <token>` — **only** to the new internal exchange | existing league OAuth TTL | Used once, then discarded permanently |
| Exchanged session (`accessToken`/`refreshToken`) | `POST manage/internal/auth-bridge/exchange-oauth-token` | `kbc_at_*` — same shape as a PKCE `login` `TokenSet`, whole-stack (projectless) | per `CliTokenResponse.expiresIn` | **Becomes the session's only credential; refreshed independently forever after (§4)** |

### Flow

1. `authorize()` redirects to `{server_url}/oauth/consent` (was `/oauth/authorize`), requesting scope `claudai projectless`.
2. Code→token exchange is **unchanged**: still `POST {server_url}/oauth/token`, still returns a league OAuth access/refresh pair (now carrying both scopes).
3. **New step, replacing `_create_sapi_token`:** exchange the league OAuth access token for a Keboola programmatic session via `manage/internal/auth-bridge/exchange-oauth-token`, reusing the exact SA-JWT + `X-Subject-Token` mechanism already implemented for `resolve-storage-token` (`clients/auth_bridge.py`).
4. Parse the `CliTokenResponse` into the same shape `auth_login.py._parse_token_response` already builds from a PKCE response.
5. From here on this session is **indistinguishable, downstream, from a directly-supplied `kbc_at_*` token**: `is_programmatic_token()` detects it, `create_session_state` forwards it as `Authorization: Bearer` narrowed by `X-KBC-ProjectId` once a project is known (see Decision §6 — no legacy-token resolver exchange is performed), and the full PSGO-261 multi-project machinery (`get_accessible_projects`, `set_project_scope`, read fan-out) becomes available to every OAuth client for the first time, starting whole-stack/unconfirmed exactly like a fresh PKCE login.
6. The league OAuth token pair from step 2 is discarded after step 3 completes — never persisted, never refreshed.
7. `ProxyAccessToken.sapi_token` (currently required, justified only by "Jobs Queue/AI Service don't support bearer tokens yet") is **obsolete**: those clients now speak bearer via `bearer_or_sapi_token` (PSGO-261, commit `5b8c65ed`). Remove the field; repurpose `ProxyAccessToken` to carry the new `kbc_at_` token and its own `refresh_token`/`session_id` instead.
8. **Refresh is fully decoupled from the league OAuth session** (confirmed, §4): `exchange_refresh_token()` calls `POST /v1/auth/token/refresh` (already implemented, `auth_login.py.refresh_tokens`) directly against the previously-exchanged refresh token. The league OAuth `/oauth/token` refresh grant is never invoked again after step 2.

## Resolution Strategy

- **`oauth.py`:**
  - `_oauth_server_auth_url` → `/oauth/consent`.
  - Add `'scope': 'claudai projectless'` to `authorize()`'s `url_params` (`oauth.py:226-232`).
  - Replace `_create_sapi_token()` with a new method (e.g. `_exchange_oauth_for_session`) that POSTs to the new internal endpoint. Build it as a sibling to `StorageTokenResolver` in `clients/auth_bridge.py` — **reuse**, don't reimplement, `read_service_account_jwt`/`normalize_storage_api_url` (`clients/base.py`) and the existing error-mapping convention, adapted to this endpoint's exception set (401 for `AuthBridgeAuthenticationException`/`OAuthExchangeUnauthorizedException`, 403 for `ManageAccessDeniedException`/`MissingClaudaiScopeException`). Reuse `auth_login.py._parse_token_response` to build the resulting `TokenSet`.
  - `ProxyAccessToken`: drop `sapi_token: str`; the `delegate` (league OAuth) token is kept only long enough to complete the exchange, then not referenced again — no ongoing refresh dependency on it (§4).
  - `exchange_authorization_code()`: call the new exchange method instead of `_create_sapi_token`; store the exchanged `refreshToken`/`sessionId` needed for the *independent* refresh path.
  - `exchange_refresh_token()`: **simplify** — drop the `POST {server_url}/oauth/token` (`grant_type=refresh_token`) call to Connection's league OAuth server entirely; call `refresh_tokens()` (`auth_login.py`) directly against the previously-exchanged `kbc_at_` refresh token.
- **`mcp.py`, `apply_request_config`:** set `config.storage_token` to the new `kbc_at_` token; the separate `bearer_token=user.access_token.delegate.token` assignment goes away (the league OAuth delegate token is discarded per step 6, never used downstream). `is_programmatic_token()` then does the rest unchanged.
- **No changes** to the PSGO-261 scoping tools, the local PKCE `login` CLI flow, or the local-stdio auth-bridge path — this RFC only touches the remote/HTTP OAuth flow.

## Scope

**In scope:** `oauth.py` flow change (consent endpoint + scope, new exchange call + client, `ProxyAccessToken` shape change, simplified refresh), `mcp.py` `apply_request_config` change, unit + integration tests for the new bridge.

**Out of scope:** the multi-project scoping tools themselves (reused unchanged); the CLI PKCE `login` flow (unaffected); the local-stdio auth-bridge/deployed-resolver path (unaffected — this only changes how the *OAuth* front door feeds a token into the *same* downstream pipe).

## Testing / Verification

**Unit** — mock the new internal endpoint: `authorize()` builds the `/oauth/consent` URL with `scope=claudai projectless`; `exchange_authorization_code` calls the new exchange instead of `_create_sapi_token`; error mapping matches the PHP action's declared exceptions (401/403); `apply_request_config` sets `storage_token` to the new `kbc_at_` token and it round-trips through `is_programmatic_token()` as `True`; `exchange_refresh_token` calls `refresh_tokens()` and never calls Connection's league OAuth refresh grant.

**Integration** — full `authorize→consent→callback→token→internal-exchange` cycle against a real (dev) stack; confirm `get_accessible_projects`/`set_project_scope` work immediately after OAuth login with no project pre-selected; confirm a refresh cycle works purely via `/v1/auth/token/refresh` with no call back to Connection's OAuth server.

**Manual** — connect a real OAuth MCP client (Claude.ai, Cursor) to a server running this change; confirm login completes and the scoping tools appear/work as expected. This is the practical local test @martin.vasko is running before finalizing implementation.

## Decisions

1. **Scope requested at `/oauth/consent` is `claudai projectless`** (space-separated, standard OAuth2 multi-scope) — `claudai` satisfies the exchange endpoint's `MissingClaudaiScopeException` guard; `projectless` is what makes the league token's `user_identifier` claim `admin:{id}` (not project-bound), which is what makes the *exchanged* session whole-stack. **Verify the exact literal string via the local test** — inferred from Connection's E2E test fixture comments, not from an explicit request example.
2. **Projectless = whole-stack, confirmed.** Both you and Connection's own E2E test agree: a `projectless`-scoped exchange yields an unrestricted session equivalent to a PKCE `login` lease — starts unconfirmed/whole-stack, `get_accessible_projects`/`set_project_scope` apply exactly as they do for a directly-supplied PAT today.
3. **`X-Subject-Token` confirmed** as the header name (shared constant with `resolve-storage-token`, both defined as `SUBJECT_TOKEN_HEADER = 'X-Subject-Token'` in Connection's source). **`X-KBC-ManageApiToken` confirmed NOT sent, verified against a real stack.** It's a separate, mutually-exclusive authenticator (`ManageTokenAuthenticator`, a real Manage-token lookup) from `X-Kubernetes-Authorization` (`KubernetesAuthenticator`, synthetic-token path) — sending both caused a live 401, because `AuthBridgeOAuthExchangeProcessor` explicitly rejects a non-synthetic (i.e. not Kubernetes-authenticated) token even if it also happens to carry the right Manage scope. Only `X-Kubernetes-Authorization` is sent, exactly like the sibling `resolve-storage-token` endpoint.
4. **No dual refresh — confirmed, not assumed.** `TokenRefreshProcessor.php` (Connection) operates on `ProgrammaticSession`/`ProgrammaticSessionRepository` — the same entity and `/v1/auth/token/refresh` mechanism already used by PAT/PKCE-login sessions, fully independent of the league OAuth session. The exchanged session refreshes on its own, forever, via the existing `refresh_tokens()`; the league OAuth token/refresh-token pair is used exactly once (at initial exchange) and never touched again, including on refresh. This **simplifies** `exchange_refresh_token()` relative to today's implementation (which currently re-negotiates with Connection's OAuth server on every refresh) rather than adding a second refresh call.
5. **No rollout coordination.** `/oauth/authorize` removal is immediate with no old-client fallback and no deploy-order dependency communicated from Connection's side. Verify locally against a real stack before shipping; no special deploy sequencing planned.
6. **As-built deviation: `resolve-storage-token`/`StorageTokenResolver` removed entirely, not reused.** §64/§66 originally assumed `create_session_state`'s deployed-path branch would keep converting a programmatic token into a legacy per-project Storage token via the auth-bridge resolver once a project id is known. Live testing against a real dev stack surfaced a 403 on that resolver call (a separate, independently-provisioned Manage scope, `SCOPE_INTERNAL_AUTH_BRIDGE_RESOLVE_STORAGE_TOKEN`, from `exchange-oauth-token`'s) — and confirmed `KeboolaClient` already forwards `bearer_or_sapi_token` (`Authorization: Bearer`) to every service it wraps (Storage, Queue, AI, Data Science, Scheduler, Sync Actions, Metastore) whenever a bearer token is set. So a programmatic token — OAuth-exchanged or a directly-supplied `kbc_pat_*` — is now **always** forwarded as a Bearer, narrowed to a project via `X-KBC-ProjectId` once known, on both local and deployed sessions. No further exchange into a legacy Storage token is performed anywhere in this flow; that resolver/endpoint is no longer called by this codebase. The old `X-StorageAPI-Token` legacy-token header path is unaffected and untouched — it only applies to a genuinely old, non-programmatic token supplied directly, and is expected to be deprecated separately in the future.
