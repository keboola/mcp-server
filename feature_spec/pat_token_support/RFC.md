# RFC: PAT / Access Token support and PKCE login for MCP Server

Linear: [PSGO-261](https://linear.app/keboola/issue/PSGO-261/support-pat-tokens-in-mcp-server-mcp-server)
Parent: [PAT-1838](https://linear.app/keboola/issue/PAT-1838) · Milestone: PAT token support across services
Related: [PSGO-263](https://linear.app/keboola/issue/PSGO-263/support-pat-tokens-in-stream-keboola-as-code) (stream / keboola-as-code)

Reference implementations:
- Go SDK exchange resolver: [keboola/keboola-sdk-go#90](https://github.com/keboola/keboola-sdk-go/pull/90)
- Go services consumption (Query/Metastore): [keboola/go-monorepo#540](https://github.com/keboola/go-monorepo/pull/540)
- UI PKCE login + `/v1/auth/*` client: [keboola/ui#6061](https://github.com/keboola/ui/pull/6061)
- PHP reference (decentralized exchange): [keboola/platform-libraries#507](https://github.com/keboola/platform-libraries/pull/507)
- Auth-bridge-proxy RFC: `go-monorepo/docs/rfcs/2026-05-18-auth-bridge-proxy.md`

---

> **As-built status.** This is the original design RFC. During implementation the scope grew
> beyond Parts A/B: the shipped server adds **multi-project session scope** with per-project
> fan-out (`get_accessible_projects` / `set_project_scope`), a **scoped-token exchange**
> (`POST /v1/auth/pat/exchange`), and **PAT/MFA leasing** (`/v1/auth/sudo`, `/v1/auth/pat`, exposed
> via `login --pat/--totp/--recovery`), plus `logout` and `login --force/--show-token`. The
> paragraphs below marked _(as-built)_ have been reconciled with the code; the authoritative,
> fully expanded description lives in the as-built RFC carried by the implementation PR.

---

## Problem

The MCP server authenticates to Keboola exclusively with **legacy Storage API tokens** (`X-StorageAPI-Token`), supplied either as `KBC_STORAGE_TOKEN` (stdio) or minted from an OAuth session (HTTP). Connection is rolling out **programmatic bearer tokens**:

- **Access token** `kbc_at_<uuid>_<random>` — short-lived (`expiresIn`, ~3600s), issued by a login/session flow.
- **Personal access token** `kbc_pat_<uuid>_<random>` — long-lived, user-created.

These are sent as `Authorization: Bearer <token>` and are **not** Storage tokens — they must be exchanged for a legacy Storage token at Connection before any current Storage-token API will accept them. Today the MCP server has no way to:

1. Accept a `kbc_at_*` / `kbc_pat_*` bearer token and exchange it for a Storage token (PSGO-261 core).
2. Acquire such a token in the first place for local/stdio use, where there is no browser-driven OAuth — the user must hand-paste a Storage token. We want a **browser PKCE login** that leases an access token + refresh token, stores them, and refreshes the access token until it expires, so `KBC_STORAGE_TOKEN` is no longer the only way in.

**Symptom:** A user presenting a `kbc_pat_*` token gets `401` from every tool. Local users have no login flow and must manually create and paste a Storage token.

## Required Behavior

### Token contract (authoritative, from the three PRs)

| Token | Format | Sent as | Lifetime |
| --- | --- | --- | --- |
| Access token | `kbc_at_<uuid>_<random>` | `Authorization: Bearer …` | short (`expiresIn` ≈ 3600s) |
| Personal access token | `kbc_pat_<uuid>_<random>` | `Authorization: Bearer …` | long |
| Refresh token | opaque (`kbc_rt_*` per task; **prefix unconfirmed in PRs**) | request body only | long |
| Legacy Storage token | unchanged | `X-StorageAPI-Token: …` | unchanged |

A token is "programmatic" iff it starts with `kbc_at_` or `kbc_pat_` (case-insensitive `Bearer ` prefix tolerated). This is the **only** trigger for exchange — legacy `X-StorageAPI-Token` traffic is untouched.

### Part A — Accept programmatic tokens and exchange for a Storage token (PSGO-261)

The MCP server is the "service" in the decentralized exchange model. On every request whose inbound credential is a programmatic token, it must:

1. Detect the `kbc_at_` / `kbc_pat_` prefix on the inbound bearer token.
2. Call the Connection resolver and use the returned legacy Storage token for the rest of the request, exactly as today.

```
POST {connection}/manage/internal/auth-bridge/resolve-storage-token
Headers:
  X-Kubernetes-Authorization: Bearer <MCP server's projected SA JWT, aud=keboola-connection>
  X-Subject-Token:            Bearer <kbc_at_* | kbc_pat_*>     # the user's token
  Content-Type: application/json
Body: { "projectId": <int> }                                   # from X-KBC-ProjectId

Response 200 (AuthBridgeStorageTokenResolveResponse):
  { "storageToken": "<legacy>", "projectId": <int>, "tokenId": "...",
    "userId": "...", "expiresAt": "<ISO8601>|null", "tokenDetail": { …tokens/verify payload… } }
```

Requirements (from acceptance criteria):

- A programmatic token — `Authorization: Bearer kbc_pat_*` **or** `kbc_at_*` — **together with** `X-KBC-ProjectId` (both token kinds need it; the resolver body always requires `projectId`) is accepted wherever a legacy Storage token works today, and resolves to that project's Storage token.
- Legacy `X-StorageAPI-Token` traffic is unaffected.
- The server reads its **own** projected SA JWT from the file path **per request** (kubelet rotation); the SA subject is mapped to `internal:auth-bridge:resolve-storage-token` in kbc-stacks (Part 2, separate repo).
- Resolver error mapping: `400→400`, `401→401`, `403→403`, `5xx/timeout/network→502`.
- **No token material** (subject token, resolved Storage token, SA JWT) is ever logged or put in exception messages.
- **No caching of the exchange result in v1.**

### Part B — PKCE browser login with token storage + auto-refresh (new login)

For local/stdio use we add a `login` flow that obtains and persists programmatic tokens so the server can run without a hand-pasted Storage token.

```
keboola-mcp-server login --api-url https://connection.<stack>.keboola.com
```

1. Generate PKCE verifier/challenge (S256) + `state`; start a loopback listener on `127.0.0.1:<port>/callback`.
2. Open the browser to:
   `GET {url}/admin/auth/pkce/authorize?responseType=code&clientId=<client>&redirectUri=http://127.0.0.1:<port>/callback&codeChallenge=<c>&codeChallengeMethod=S256&state=<s>`
3. On callback, verify `state` (constant-time), then exchange:
   `POST {url}/v1/auth/pkce/token` body `{clientId, code, state, redirectUri, codeVerifier}`
   → `{accessToken, refreshToken, tokenType:"Bearer", expiresIn, sessionId, user}`.
4. Persist `{accessToken, refreshToken, expiresAt, sessionId, storageApiUrl}` to a mode-`600` file (default `~/.keboola/mcp/credentials.json`).

Whenever the server holds the credential pair (login flow), it **refreshes during usage**: before each use, if the access token is within a refresh skew (e.g. 60s) of `expiresAt`, refresh first:

```
POST {url}/v1/auth/token/refresh   body { "refreshToken": "<rt>" }
→ { accessToken, refreshToken, expiresIn, sessionId, user }   # rotating refresh — persist both
```

The resulting `accessToken` is then the session's programmatic token: on the **deployed** server it is the subject token for the Part A resolver exchange; on the **local** server (no projected SA token) it is forwarded downstream as a Bearer and the Keboola services exchange it — see _Architecture: hybrid (Option C)_. **When the token is dead** (refresh token expired/revoked → refresh returns `invalid_grant`/`401`), the server clears the stored credentials and **enforces re-login** (surfaces a clear "run `login` again" error rather than silently failing).

### Mode matrix

| Mode | Inbound credential | How the token is used |
| --- | --- | --- |
| stdio (legacy) | `KBC_STORAGE_TOKEN` | no exchange — used directly (unchanged) |
| stdio (new, local) | stored PKCE creds (Part B) | load → refresh-if-needed → forward the bearer downstream + `X-KBC-ProjectId`; services exchange (local has no SA token — see hybrid) |
| HTTP / remote (deployed) | `Authorization: Bearer kbc_*` per request | exchange per request via the Part A resolver; refresh is the client's job, not ours |
| HTTP OAuth (existing) | `SimpleOAuthProvider` session | unchanged for now (see Scope) |

### Connection endpoints used _(as-built)_

Beyond the resolver and the two PKCE endpoints above, the shipped code also calls:

| Method + path | Purpose |
| --- | --- |
| `GET /v1/auth/token/introspect` | enumerate the projects a token can reach (drives default multi-project scope) |
| `POST /v1/auth/pat/exchange` | mint a session-scoped child token narrowed to selected project(s) |
| `POST /v1/auth/sudo` | MFA elevation (TOTP / recovery code) ahead of PAT creation |
| `POST /v1/auth/pat` | create a personal access token (`login --pat`) |

## Resolution Strategy

> Code `file:line` references below are relative to `src/keboola_mcp_server/` (e.g.
> `clients/client.py` is `src/keboola_mcp_server/clients/client.py`).

### Detection + exchange client (Part A)

- **New** `clients/auth_bridge.py`: `StorageTokenResolver` with `async def resolve(subject_token: str, project_id: int) -> str`. Builds the resolver URL from the stack host suffix (same derivation as `KeboolaClient.__init__`, `clients/client.py:154-166`). Reads the SA JWT from a path env var **per call** (no caching). Redacts all token material from logs/exceptions; maps status codes per the table.
- **Reuse the existing projected-SA-token mechanism** already added for workspace step-up (commit `b971146f`, workspace provisioning header). Same projected file, same per-request read — factor the file-read into one helper so both call sites share it.
- **Helper** `is_programmatic_token(token: str) -> bool` (prefix check, `Bearer ` tolerant). _(as-built: lives in `clients/auth_bridge.py`, not `config.py`/`auth.py`.)_
- **Wire-in point:** `create_session_state` in `mcp.py`. _(as-built: the programmatic token arrives as `config.storage_token`; `create_session_state` branches on `is_programmatic_token(config.storage_token)` — deployed exchanges it via the resolver, local forwards it as a Bearer with `X-KBC-ProjectId`. It is not triggered off `config.bearer_token`.)_ Everything downstream (`client.py:169` `bearer_or_sapi_token`, `base.py:38-42` header selection) then uses the resolved/forwarded token unchanged.
  - Note: with a resolved legacy Storage token we should set `storage_token` and leave `bearer_token` unset, so all service clients (including Queue/AI/sync-actions that don't speak Bearer) work uniformly. The programmatic token never goes downstream.
- **Project id:** required by the resolver, resolved **per request/session**, not a single static config value. It comes from a session→project mapping injected in middleware (`SessionStateMiddleware`), driven by user/query filtering — the same place that will later host **token-scoping tooling** (issue increasingly tight PATs scoped to a project/session on top of the session mapping). Source order for v1: `X-KBC-ProjectId` header (HTTP) → session mapping → `KBC_PROJECT_ID` env / CLI (stdio) → from `tokenDetail`/login `user` if unambiguous. Add `project_id` to `Config` (`config.py:17-138`, same `KBC_*` / `X-*` resolution pattern) as the plumbing; the mapping/scoping tooling is a follow-up.

### PKCE login + storage + refresh (Part B)

- **New** `auth_login.py`: PKCE crypto (stdlib `hashlib`, `secrets`, `base64`), loopback `http.server` callback, the two HTTP calls (`/admin/auth/pkce/authorize` open-in-browser, `POST /v1/auth/pkce/token`). Mirror `scripts/auth-demo-cli/pkce.ts` from ui#6061. `clientId` is configurable via env/secret (`KBC_PKCE_CLIENT_ID`), defaulting to the demo value `keboola-cli-demo` for now; tolerate a blank value (injected as a secret later) and swap the real MCP client id when allocated.
- **Credential store + refresh:** load/save the mode-`600` JSON file; `async def get_access_token()` that refreshes via `POST /v1/auth/token/refresh` when within skew of `expiresAt` and persists the rotated pair. _(as-built: this lives in `auth_login.py` alongside the PKCE flow — no separate `credentials.py` module.)_ Note: file-based store, single-user; no keyring/DB until a real multi-account need appears.
- **CLI:** add `login` subcommand in `cli.py` (`parse_args` `cli.py:28-61`, `run_server` dispatch). On normal server start, if no `KBC_STORAGE_TOKEN` and no inbound bearer, load stored creds → refresh-if-needed → feed the access token into the Part A path.
- Refresh concurrency: guard with an `asyncio.Lock` so concurrent sessions don't double-refresh and invalidate each other's rotated token.

### Non-obvious trade-offs

- **One exchange per request, no cache (v1):** matches the issue's explicit "no caching" and the SA-token-per-request rotation requirement. The resolver round-trip adds latency to every tool call; v2 may cache keyed by `(subject_token, project_id)` until `expiresAt`. Flag, don't build.
- **Resolved token replaces bearer downstream:** simpler and uniform across all service clients vs. teaching every client to forward the programmatic bearer (Queue/AI/sync-actions don't accept Bearer today).
- **Existing OAuth (`SimpleOAuthProvider`) left intact:** it already mints a SAPI token. Folding it into the resolver path is a follow-up, not this RFC.

## Scope

**In scope**
- Detect `kbc_at_*` / `kbc_pat_*` inbound tokens and exchange them via the Connection resolver (Part A), in both stdio and HTTP modes.
- Per-request SA-JWT read; resolver error mapping; no token logging; no exchange caching.
- PKCE `login` command, credential storage, and access-token auto-refresh (Part B).
- `project_id` config plumbing.
- Unit + integration tests; `TOOLS.md` regen only if tool signatures change (they shouldn't).

**Delivered beyond the original Part A/B design** _(as-built — originally listed out of scope, since built)_
- Multi-project session scope with per-project fan-out and the `get_accessible_projects` / `set_project_scope` tools.
- Session-scoped token exchange (`/v1/auth/pat/exchange`) for narrowing scope at runtime.
- MFA / sudo flow (`/v1/auth/sudo`) and PAT creation (`/v1/auth/pat`) behind `login --pat/--totp/--recovery`.

**Out of scope**
- kbc-stacks SA-subject → `internal:auth-bridge:resolve-storage-token` mapping and projected-token mount (PSGO-261 Part 2, **separate repo**).
- Replacing/retiring `SimpleOAuthProvider` OAuth flow.
- Caching exchange results; keyring/DB credential storage.
- Device-code flow; PAT list/revoke management endpoints.

## Testing / Verification

**Unit**
- `is_programmatic_token`: `kbc_at_*`, `kbc_pat_*`, `Bearer ` prefix, legacy token, empty → correct boolean.
- `StorageTokenResolver`: success returns `storageToken`; resolver `400/401/403` pass through, `5xx/timeout/network → 502`; assert no token material in log records or exception strings (capture logs, assert redaction); assert SA file is read on each call.
- PKCE: verifier charset/length, `codeChallenge == base64url(sha256(verifier))`, `state` constant-time compare.
- Credentials: refresh triggered within skew, rotated pair persisted, file mode `600`, concurrent refresh serialized by the lock.

**Integration** (`integtests/`, real stack)
- Tool call authenticated with a `kbc_pat_*` token + `X-KBC-ProjectId` succeeds and hits the right project.
- Same with `kbc_at_*`.
- Legacy `X-StorageAPI-Token` path still works (regression).
- Each resolver error path surfaces the mapped client status.

**Manual**
- `keboola-mcp-server login --api-url https://connection.<stack>.keboola.com` → browser → tokens stored; start server with no `KBC_STORAGE_TOKEN`; run a tool; let the access token expire and confirm transparent refresh.

---

## Architecture: hybrid (Option C) — see brainstorm.md

Two token paths, one per environment:
- **Deployed mcp-server (in-k8s):** detect `kbc_at_*`/`kbc_pat_*`, exchange via the resolver using the projected SA JWT → legacy Storage token → used for all downstream clients (Part A; PSGO-261 AC).
- **Local stdio:** PKCE `login` (stack URL only) leases a whole-stack session token, stored + auto-refreshed; MCP **forwards the bearer** + `X-KBC-ProjectId` downstream and the services exchange (no SA token exists locally).

OAuth is **not** removed (the MCP protocol needs it for HTTP transport). The OAuth→PAT exchange is a **separate PR**; until it lands, the public mcp-server keeps its current OAuth→SAPI-mint path (interim divergence, stated intentionally).

## Decisions

1. **`project_id` is explicit session state (D2)** — not derived from the token (a whole-stack PAT has no implicit project; today `StorageClient.project_id()` reads `tokens/verify`, which only works for project-bound legacy tokens). Default from CLI/env (`KBC_PROJECT_ID`) or HTTP `X-KBC-ProjectId`. _(as-built: rather than a single "select-project tool", the server ships two tools — `get_accessible_projects` (introspect-backed discovery) and `set_project_scope` (narrowing) — and, for local programmatic sessions with no explicit project, auto-leases **all** reachable projects as the default scope, gated by an ask-first confirmation.)_
1b. **Whole-stack on-disk credential; scoped minting at runtime (D1)** — the persisted PKCE credential is the whole-stack session token (mode-600, refresh-rotated, never logged); it is never a project-scoped PAT on disk. _(as-built: `set_project_scope` **does** mint a scoped child token via `/v1/auth/pat/exchange`, but only in memory as session state — it is never persisted — and `login --pat` can mint a real PAT on explicit request.)_
2. **`clientId` for PKCE** — use the demo value `keboola-cli-demo` for now, configurable via `KBC_PKCE_CLIENT_ID` (blank-tolerant, injectable as a secret later); swap the real MCP client id when allocated.
3. **Refresh token** — treated as an opaque string (no prefix assumptions).
4. **SA token path env var** — align with the workspace step-up var (`b971146f`) and the Go services' `*_KUBERNETES_TOKEN_PATH` convention; share one file-read helper.
5. **Refresh + dead token** — the server **always refreshes during usage** when it holds the token pair; when the token is dead (refresh fails), it clears stored credentials and **enforces re-login**.
