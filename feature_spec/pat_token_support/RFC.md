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

---

# Extension: Multi-project scope via introspect + scoped exchange (PSGO-261, increment 2)

> This section extends the RFC above. Parts A/B (programmatic-token exchange, PKCE login) are unchanged
> and are the substrate this builds on. It revises decisions **D1** and **D2** (see below).

## New problem

Parts A/B give the server a programmatic token and a single `project_id`. A whole-stack PAT/AT can
actually reach **many** projects, and the Kai multi-project workflows (the parent driver, PAT-1838)
need one agent session to act across several of them. Two gaps remain:

1. **Discovery.** The server has no way to enumerate which projects the inbound token can reach.
   (Brainstorm left this as an open question: "the exact stack-level endpoint to enumerate
   PAT-accessible projects.")
2. **Scope.** `project_id` is a single value. There is no way to (a) operate over a set of projects,
   nor (b) *narrow* a whole-stack token down to a reviewed subset for the rest of the session.

## Token-contract additions (authoritative, from connection auth API)

### Introspect — enumerate accessible projects

```
GET {connection}/v1/auth/token/introspect
Headers: Authorization: Bearer <kbc_at_* | kbc_pat_*>

200:
{ "sessionId": "...", "user": { "id", "email", "name" },
  "grantType": "authorization_code", "expiresAt": "<ISO8601>",
  "projects": [ { "id": <int>, "name": "...", "role": "admin|..." }, ... ] }
```

This is the discovery endpoint. It works for any programmatic token and is the source of truth for
"which projects can this session touch."

### Scoped exchange — mint a token narrowed to chosen projects

```
POST {connection}/v1/auth/pat/exchange
Headers: Authorization: Bearer <current subject token>
Body:    { "expiresIn": null|<int>, "scope": { "projects": ["<id>",...]|null, "readOnly": true|null } }
         # NB: project ids are sent as STRINGS — the exchange API 400s on integers (auth_login.py:166)

201:
{ "accessToken": "<scoped kbc_at_*>", "tokenType": "Bearer", "expiresIn": <int>,
  "scope": {...}, "readOnly": <bool>, "parentTokenId": "...", "parentTokenType": "session",
  "expiresAt": "<ISO8601>", "pat": { "id", "name", "scope", "projects": [...], "readOnly", ... } }
```

`scope.projects = null` → all projects (whole-stack). A non-null list mints a token that can reach
**only** those projects. `readOnly: true` mints a read-only token. The returned `accessToken` becomes
the session's subject token for all downstream exchange/forwarding.

## Required behavior

1. **Discovery tool.** `get_accessible_projects()` calls introspect and returns the projects list
   (id, name, role) plus the user identity. Read-only, no side effects.
2. **Scope-selection tool.** `set_project_scope(project_ids: list[int] | "all", read_only: bool=false)`:
   - `"all"` → scope = every introspected project id; keep the current (whole-stack) token.
   - a subset → call `/v1/auth/pat/exchange` with `scope.projects=project_ids` (+ `readOnly`), store
     the returned scoped `accessToken` as the **session subject token**, set scope = `project_ids`,
     and clear the per-project client cache so it rebuilds against the scoped token.
3. **Conversation-start nudge.** Server instructions tell the agent: on first interaction call
   `get_accessible_projects`, present them, and ask the user **"work across all of these, or a
   subset?"**; call `set_project_scope` with the answer. (MCP has no protocol-level startup prompt —
   this is the idiomatic discovery-tool + instructions pattern, same shape as `get_project_info`.)
4. **Transparent multi-project execution.** Once scope is set, existing tools run **once per project
   in scope** with no per-tool `projects[]` argument. Mechanism (decision D6):
   - Session state holds `scope` (ordered project_ids), `active_project_id`, and a lazy
     `project_id -> KeboolaClient` cache. `KeboolaClient.from_state(state)` returns the client for
     `active_project_id`. **All 43 existing call sites are unchanged.**
   - A dispatch-layer wrapper (`SessionStateMiddleware.on_call_tool`) reads the scope:
     - **1 project** (or legacy single-project session): set `active_project_id`, call the tool once,
       return its result **raw** — byte-for-byte today's behavior.
     - **N projects, read tool:** loop the scope, set `active_project_id` per iteration, collect into a
       per-project envelope `[{ "project_id": <int>, "result": <tool result> }, ...]`. No semantic
       merge — the envelope preserves each tool's native return shape (this is the answer to the
       "merging arbitrary shapes is lossy" risk: we wrap, we don't merge).
     - **N projects, write tool:** do **not** fan out. Require a single target project; if scope has
       >1 and no explicit single target was confirmed, return a clear error instructing the agent to
       confirm with the user and target one project (decision D8).
   - Per-project clients build lazily: deployed → resolver exchange per project (Part A) using the
     scoped subject token; local → forward bearer + `X-KBC-ProjectId: <project>`.
   - Fan-out is sequential in v1. `# ponytail: sequential fan-out; asyncio.gather if N-project latency bites.`
5. **Read/write classification.** An explicit set of mutating tool names (or a registration-time flag)
   drives the write-policy branch. Explicit list over magic — there are few write tools.

## Mode / availability matrix (additions)

| Inbound credential | Introspect / scope tooling | Multi-project |
| --- | --- | --- |
| programmatic (`kbc_at_*`/`kbc_pat_*`, PKCE or Bearer) | available | yes |
| legacy `KBC_STORAGE_TOKEN` | n/a (project-bound token) | no — single project, unchanged |
| OAuth `SimpleOAuthProvider` (current SAPI mint) | n/a until OAuth→PAT PR lands | no (interim) |

## Revised decisions

- **D1 (revised) — scope narrowing is now token-enforced, not advisory.** The original D1 said
  "no minting; narrowing is runtime-only session state." With the user choosing the `pat/exchange`
  path, narrowing to a subset **mints a scoped token** (in-memory, session-lived, never persisted).
  The *stored* (on-disk PKCE) credential is still whole-stack — D1's storage stance holds — but the
  *active* session token is the scoped one, so a tool can no longer reach an out-of-scope project even
  by bug. Strictly stronger than the original advisory model.
- **D2 (extended) — `project_id` → project scope (a set).** Still explicit session state, never
  silently derived. An explicit `KBC_PROJECT_ID` / `X-KBC-ProjectId` pins a single project
  (backward compatible). _(as-built: when a local programmatic session sets **no** explicit project,
  `SessionStateMiddleware._autolease_default_scope` introspects and defaults to **all** reachable
  projects — multi-project by default — gated by an ask-first confirmation (`SessionScope.confirmed`,
  `_BOOTSTRAP_TOOLS`); it is not single-project-by-default.)_ `get_accessible_projects` +
  `set_project_scope` replace the previously-hypothetical "select-project tool"; introspect closes
  the open enumeration question.
- **D6 (new) — transparent fan-out via active-project indirection.** Tools take no `projects[]` arg;
  the dispatch wrapper swaps `active_project_id` and the per-project client cache. Multi-project
  results use a per-project envelope, never a semantic merge. Zero changes to the 43 `from_state`
  sites. (Chosen over a per-tool `projects[]` param.)
- **D7 (new) — scoped exchange uses `/v1/auth/pat/exchange`.** Not `/v1/auth/pat` (PAT create). The
  exchange yields a child token (`parentTokenType: session`) tied to the current session, which is the
  right lifetime for a session-scoped narrowing.
- **D8 (new) — multi-project writes are user-driven only.** Read/query/search tools fan out freely.
  Mutating tools (create/update/run config, flow, job, data-app, transformation) never fan out
  automatically: with >1 project in scope they require a single confirmed target project. Server
  instructions state: **the agent must never write to more than one project without explicit user
  guidance or confirmation.** Bulk multi-project writes are possible but only on that explicit signal.
  _(How "confirmed target" is expressed evolved — see "Decisions (increment 5)" below: an explicit
  `project_id` tool argument, not the interim active-project/re-scope indirection.)_

## Scope changes (relative to the base RFC)

**Moved into scope:** introspect-based project enumeration; `/v1/auth/pat/exchange` scoped token
minting; multi-project read fan-out; user-confirmed multi-project writes;
`get_accessible_projects` + `set_project_scope` tools; per-project client cache.

**Still out of scope:** OAuth→PAT exchange (separate PR); caching of resolver results; keyring/DB
credential storage; `/v1/auth/pat` PAT lifecycle management (create/list/revoke) tools; parallel
fan-out (sequential in v1).

## Delivery plan (phased, compact)

**Phase 1 — Discovery (low risk, read-only).**
- `clients/auth_bridge.py` (or a new `clients/auth.py`): `introspect(subject_token) -> Introspection`
  (user + projects[]). GET `/v1/auth/token/introspect`, token redaction same as the resolver.
- `tools/project.py`: `get_accessible_projects()` tool.
- Server instructions: add the "ask all-vs-subset at start" nudge.
- Tests: introspect success/parse, 401/timeout mapping, no-token-in-logs; tool returns projects.

**Phase 2 — Scoped exchange + session scope state (revises D1).**
- `exchange_scope(subject_token, project_ids|None, read_only, expires_in) -> scoped token` (POST
  `/v1/auth/pat/exchange`).
- Session scope state: `scope: list[int]`, `read_only: bool`, scoped subject token; default scope from
  `project_id`. `set_project_scope` tool wires exchange → state → cache invalidation.
- Tests: subset → exchange called with right body, scoped token stored; "all" → no exchange; read_only
  propagates; scope defaults to single project when unset.

**Phase 3 — Transparent fan-out (the core refactor, D6/D8).**
- Session state: `active_project_id` + lazy `project_id -> KeboolaClient` cache; `from_state` returns
  the active client (indirection only — call sites unchanged).
- `SessionStateMiddleware.on_call_tool` wrapper: 1-project raw passthrough; N-project read envelope;
  N-project write guard.
- Explicit write-tool name set.
- Tests: single-project unchanged (regression); 2-project read returns enveloped per-project results;
  write tool with N-project scope refuses without a confirmed target; per-project client built with the
  right token/header.

**Cross-cutting:** version bump (minor — new capability), `uv.lock`, `TOOLS.md` regen (new tools +
the per-project envelope shape change the docs), integration tests on a dev stack with a real
`kbc_pat_*` across ≥2 projects.

## Open questions (new)

- [ ] **Envelope vs raw for exactly-1-in-scope-but-explicitly-multi.** Confirm: a scope of exactly one
  project returns raw (not a 1-element envelope) so single-project UX never regresses. (Assumed yes.)
- [ ] **`expiresIn` for the scoped exchange.** Use `null` (inherit parent/default) in v1, or pin to the
  remaining parent lifetime? Affects mid-session expiry of the scoped token.
- [ ] **Scope change mid-session re-introspect.** After `set_project_scope`, do we re-introspect to
  validate the subset is still reachable, or trust the prior introspect? (Lean: trust; resolver/exchange
  will reject an out-of-scope project anyway.)
- [ ] **Write-target confirmation mechanism.** Is the "confirmed single target" a tool argument
  (`project_id` on the write tool), a separate `set_write_target` call, or purely instruction-driven?
  (Lean: explicit `project_id` arg on write tools, honored only when scope >1.)

## Resolutions (2026-06-30) — answers to the increment-2 open questions

- **Scoping requires a dedicated tool (`set_project_scope`); it is the only mechanism.** The MCP
  server receives nothing from the conversation except tool calls — plain chat text never reaches the
  server. Scope is server-side state (scoped token + per-project client cache + active project), so the
  user's in-conversation intent can only change scope by the agent invoking the tool. The tool is
  callable **at any point mid-conversation**, not just at start; the conversation-start nudge is an
  instruction-level suggestion, not a gate. The user drives scope changes by saying so; the agent
  translates that into the tool call. (Resolves the recurring "do I need a tool / is it user-driven"
  question: yes, a tool; driven by the user via conversation, any time.)
- **The tool does not swap a single client — it invalidates the cache (D6).** `set_project_scope`
  stores the new scope + scoped token and **clears the per-project client cache**. `from_state` then
  lazily rebuilds each project's client against the new scoped token. Cleaner than replacing one
  `KeboolaClient` object in state.
- **Q2 (scoped-token lifetime) — resolved: the child token is re-minted, not independently refreshed.**
  The `pat/exchange` response carries `accessToken` + `expiresAt` but **no `refreshToken`** — the child
  (scoped) token is not refreshable on its own. The refreshable credential is the **parent** PKCE
  session token (Part B, `/v1/auth/token/refresh`). MCP **remembers the scope selection**
  (`project_ids`, `readOnly`); when the scoped child nears expiry it **re-runs `pat/exchange`** against
  the still-valid (refresh-backed) parent token to lease a fresh scoped token. `expiresIn: null` at
  exchange time (inherit server default) is fine because we re-mint on demand. *Flag: confirm against
  the auth API that the child token genuinely has no own refresh token.*
- **Q3 (re-introspect on scope change) — resolved: trust prior, let exchange reject.** When the user
  picks a subset, MCP goes straight to `pat/exchange` without re-calling `introspect`. The exchange
  endpoint itself rejects any project the token can't reach, so a pre-check is redundant — one fewer
  round-trip, and the exchange is the authority.
- **Q1 (exactly-1-in-scope) — confirmed: a single-project scope returns the raw result, not a
  1-element envelope.** Single-project UX is byte-for-byte unchanged.
- **Q4 (write-target confirmation) — lean: explicit `project_id` arg on write tools, honored only when
  scope > 1.** (Resolved as leaned — see "Decisions (increment 5)" below.)

# Extension: query fan-out, dialect-aware bootstrap, per-service token gaps (PSGO-261, increment 3)

## Context

Increment 2 delivered read fan-out + scope tools but left three rough edges: `query_data` was
pinned to the active project's workspace, `get_accessible_projects` returned only id/name/role
(forcing a `get_project_info` per project for the SQL dialect), and only Storage + the Query
Service actually honor the multi-project token narrowing. This increment addresses the first two
and documents the third.

## `query_data` fan-out + per-project workspace

`query_data` is now a normal fan-out read tool — it was removed from `_NO_FANOUT_TOOLS`. The fan-out
swaps **both** the `KeboolaClient` **and** a `WorkspaceManager` built on it into session state for
the duration of a call (`MultiProjectMiddleware._swap_project`), so the SQL runs inside the targeted
project's own read-only workspace (its BigQuery dataset / Snowflake schema), not the active
project's.

- Narrow to one project with the `project_ids` filter (`query_data(project_ids=[86])`), or run
  across all scoped projects.
- Per-project workspaces are provisioned lazily on first use. `ponytail:` the manager is rebuilt
  per call; a cache surviving the per-request state rebuild is a follow-up if provisioning latency
  shows up.
- Merged `structured_content` for a fanned-out query keeps the first project's `csv_data` (scalar
  deep-merge); every project's full result is present in the per-project text envelopes. Structured
  multi-CSV merge is deferred.

### Known limitations (accepted, not solved this increment)
- **Read-only scope can't provision a first-time workspace** (workspace creation is a POST). The
  first `query_data` into a project without an existing MCP workspace needs a non-read-only scope.
  Verified: read-only scope → `Forbidden POST operation on a readonly client` on workspace create.
- **No cross-project SQL in a single statement.** BigQuery has no cross-project data access;
  Snowflake reaches another project only via a *materialized* linked-bucket alias. A single
  `query_data` call always executes inside exactly one project's workspace. This is a backend
  constraint, not an MCP limitation — a future increment could add FQN-aware routing, but the join
  itself is impossible in one statement regardless.

## `get_accessible_projects` as the dialect-aware bootstrap call

`get_accessible_projects` now compacts several API calls into one bootstrap result so the assistant
does not need a `get_project_info` per project:

- **Introspection** → reachable projects (id, name, role).
- **Per-project token verify** (parent token narrowed with `X-KBC-ProjectId`, run concurrently) →
  each project's `sql_dialect`, derived from `owner.defaultBackend` — **no workspace provisioned**.
- **Current scope surfaced** → `scoped_project_ids`, `active_project_id`, `read_only`, and
  per-project `in_scope` / `is_active` flags. (There is no separate scope-introspection tool; this
  is the read side of scope state without mutating the token.)
- **Optional base instructions** → `with_llm_instruction=true` returns `base_instructions`: a
  top-level array grouped by SQL dialect (deduplicated, **not** copied per project), e.g.
  `[{project_ids:[18,86], sql_dialect:"BigQuery", instructions:"…"}, {project_ids:[95],
  sql_dialect:"Snowflake", instructions:"…"}]`. Request once at the start of a conversation.

`workspace_id` is intentionally omitted here (not needed for bootstrap). The result keeps the
codebase-wide singular `llm_instruction` field (how-to-use-this-result guidance) distinct from the
plural `base_instructions` (the working system prompts).

### `get_project_info` caveat
`get_project_info` stays in `_NO_FANOUT_TOOLS` and reports only the active project. In a
mixed-dialect scope its single `sql_dialect` / dialect-specific `llm_instruction` is misleading for
the other projects. Prefer `get_accessible_projects` for multi-project bootstrap. Follow-up: fan out
`get_project_info`, or split its static prompt from the per-project dialect/branch/workspace facts.

## Per-service token support under multi-project scope

Fan-out narrows a call to one project via the **`X-KBC-ProjectId` header** on a shared token. Only
services that read that header work under header-narrowing. Current wiring (`clients/client.py`):

| Service | Token today | PAT / multi-project status |
|---|---|---|
| Storage (`connection`) | `bearer_or_sapi_token` + `X-KBC-ProjectId` | ✅ works |
| Query Service | workspace bearer | ✅ per-project workspace |
| Metastore (semantic) | `bearer_or_sapi_token` | ✅ PAT/bearer-first, SAPI fallback (guarded); feature-gated, untested on stacks without `mcp-semantic-tooling` |
| Data Science (sandboxes) | `bearer_or_sapi_token` + `X-KBC-ProjectId` | ✅ PAT + project header (verified: data-app create + deploy) |
| Scheduler | `bearer_or_sapi_token` | ✅ bearer-first (writes only) |
| **Jobs Queue** | `bearer_or_sapi_token` + `X-KBC-ProjectId` | ✅ bearer/PAT-first, SAPI fallback |
| **AI Service** | `bearer_or_sapi_token` | ✅ bearer/PAT-first, SAPI fallback |
| **Sync Actions** | `bearer_or_sapi_token` + `X-KBC-ProjectId` | ✅ bearer/PAT-first, SAPI fallback |

### Resolved: Queue / AI / Sync-Actions now speak bearer/PAT
`jobs_queue`, `ai_service`, and `sync_actions` originally passed the raw `self._token`, so under a
PAT/multi-project session the satellite service rejected it (`get_jobs` → 401 "Invalid access
token" from the Queue API). Fixed in commit `5b8c65ed`: all three are now wired with
`bearer_or_sapi_token` (`clients/client.py:169,184,190,209`), which forwards `Authorization:
Bearer <token>` for programmatic sessions and falls back to `X-StorageAPI-Token` for legacy SAPI —
matching metastore/data-science/scheduler. The queue accepts `Authorization: Bearer kbc_at_…` +
`X-KBC-ProjectId` (verified by hand against the Queue API).

## Decisions (increment 3)

- **`query_data` fans out with a per-project workspace** rather than being pinned to the active
  project. Single-project targeting via the `project_ids` filter; cross-project SQL stays out of
  scope (backend-impossible in one statement).
- **`get_accessible_projects` is the multi-project bootstrap**: per-project dialect via token verify
  (no workspace), current scope surfaced, base instructions grouped by dialect behind
  `with_llm_instruction`.
- **Queue / AI / SyncActions now use the bearer/PAT path** (commit `5b8c65ed`), joining
  metastore + data-science in satisfying the PAT/bearer + `X-KBC-ProjectId` contract.

# Extension: scope-first tool visibility + reviewer feedback (PSGO-261, increment 4)

## Context — reviewer feedback vs. PR #451

An earlier MPA attempt (PR #451, `davidesner`) took a different shape: static numbered SAPI tokens
(`KBC_STORAGE_TOKEN_1..N`) in `.mcp.json`, a middleware that injects a `project_id`/`branch_id`
parameter into every tool schema, and the **agent** passing `project_id` per call (so covering N
projects means the agent calls the tool N times). Two critiques of our fan-out/scope model were
raised against that backdrop. Verdict after analysis:

- **"Fan-out is worse than N explicit calls."** Partly conceded, partly not:
  - *Relevance / context bloat* — not a real differentiator: the user can scope the token or use the
    `project_ids` filter to target one project, and a genuine all-projects request bloats context in
    either design.
  - *Latency* — fixable: the fan-out loop should run **concurrently** (it is currently sequential).
  - *Attribution & error isolation* — the one real gap (see below). Kept as follow-up.
- **"Active project while unscoped feels weird; expect tools to load after the first scope."** —
  Accepted. Implemented as scope-first tool visibility (below).

## Attribution & error isolation (the remaining fan-out gap)

Concrete, with a 2-project read:

- **Attribution.** `get_buckets` fan-out concatenates both projects' `buckets` lists via
  `_deep_merge`; each bucket has `source_project: null`, so the merged `structured_content` cannot
  say which project a bucket came from (only the `=== project N ===` text envelope can, which
  structured-output clients don't parse). Two explicit calls each carry their project by construction.
- **Error isolation.** The fan-out loop is `for p in targets: results.append(await call_next())` —
  if one project raises (e.g. `get_jobs` → Queue 401 on project 95 while 86 succeeds), the exception
  propagates and the **whole** call fails, discarding project 86's good result. Two explicit calls
  isolate the failure (86 returns jobs, 95 returns its 401).

Follow-up (not in this increment): make fan-out concurrent, catch per-project errors into a
per-project `{project_id, ok|error}` envelope, and stamp `source_project` on merged rows.

## Resolved: structured_content attribution (PSGO-261, follow-up to the fan-out gap above)

Error isolation shipped separately (`MultiProjectMiddleware.on_call_tool`'s per-project try/except,
collecting failures into retry-hint text notes rather than failing the whole call — see the code).
This closes the remaining half: attribution in `structured_content`.

- **Field name is `_scope_project_id`, not `source_project` as originally sketched above.**
  `source_project` is already a real field on bucket/table output models (`storage/tools.py:127,331`)
  — Keboola's own cross-project *linked-bucket* provenance (which project a shared/linked bucket
  originated from), a pre-existing and unrelated concept. Stamping that name here would have silently
  overwritten real data on any linked bucket/table in a fanned-out result. `_scope_project_id` (leading
  underscore, MCP-scope-specific name) avoids the collision; no output model in this codebase uses
  that name today.
- **Mechanism:** `MultiProjectMiddleware._tag_items_with_project` stamps `_scope_project_id` onto every
  dict item inside each project's structured payload, before `_deep_merge` concatenates the per-project
  lists together — so the field survives the merge on every list item, not just the top level.
  Non-dict list items (e.g. a plain list of ids) are left untouched — nothing to attribute.
- **Only applies to genuine fan-out (2+ targets).** A single-target call (scope of one, or narrowed to
  one via `project_ids`) returns `call_next()` directly and never reaches `_merge` — it doesn't need
  the tag, the whole session already knows which project it hit.
- **Schema safety:** no output model in this codebase sets `extra='forbid'` (`ConfigDict`), so no
  generated JSON schema declares `additionalProperties: false` — adding this key doesn't violate any
  existing tool's declared output schema.
- Text-content attribution (`=== project N ===`) is unchanged and still emitted alongside — this adds
  the same information to `structured_content` for callers that only read that half of the result.

## Tool gating: call-time, not list-time (why hide-then-reveal was reverted)

We first tried **scope-first tool visibility**: while a programmatic session's scope was unconfirmed,
`on_list_tools` advertised only the scoping tools, and `set_project_scope` emitted
`notifications/tools/list_changed` to reveal the rest. **This does not work on Claude Code** (and
likely other clients): the client does **not re-fetch the tool list** after `list_changed`
mid-session, so the newly-unlocked tools never enter its inventory (and `ToolSearch` can't find them)
until a reconnect. Hiding therefore left the session stuck with only two tools.

**Reverted to call-time gating** (robust on every client, no reconnect):
- **All tools stay listed** from connect. No hide.
- The **call-time ask-first gate** (`on_call_tool`) blocks data tools with a "confirm a scope first"
  error until `set_project_scope` is called. After scoping, the already-listed tools just work.
- `set_project_scope` still emits `notifications/tools/list_changed` — now only meaningful because a
  **confirmed multi-project scope adds the `project_ids` filter param** to read tools (a real schema
  change); clients that honor it refresh, clients that don't still work (the param is optional).
- The `project_ids` filter is injected only for a **confirmed** scope of >1 project.

This keeps the reviewer's other win (no phantom active project *before* a scope exists) without
depending on a client capability that isn't there. The "tools appear after scope" ideal is only
achievable on clients that re-fetch on `list_changed`; we don't rely on it.

## Decisions (increment 4)

- **Call-time gate, not list-time hiding** — hide-then-reveal needs client `list_changed` re-fetch
  (absent in Claude Code mid-session), so all tools stay listed and data tools are gated at call time.
- **No phantom active project before a scope is confirmed**; after `set_project_scope` the
  `active_project_id` is the write / `query_data`-default target and is surfaced intentionally.
  _(Superseded for writes by "Decisions (increment 5)" below: writes now take an explicit
  `project_id` argument instead of implicitly targeting `active_project_id`.)_
- **Fan-out stays**, with the relevance/latency critiques answered by the `project_ids` filter and a
  (follow-up) concurrent loop; per-project error isolation is now implemented (partial results).
- Fixed a latent bug: `set_project_scope` referenced `minted.read_only` on the exchange-failure path
  where `minted` is unbound — now uses the stored scope's `read_only`.

## Scale: count-first fan-out with a safety cap

Fan-out's saving is a *fixed* structural overhead (deduped envelopes/wrappers/turns, ~a few hundred
tokens across N projects) — it does **not** compress data. So as projects grow, the percentage cut
trends to zero and the binding cost becomes raw **data volume**:

| buckets/proj (×6) | data | fan-out | explicit | cut % |
|---|--:|--:|--:|--:|
| 5 | 3,375 tok | 3,658 | 3,978 | 8.0% |
| 50 | 33,750 | 34,033 | 34,353 | 0.9% |
| 500 | 337,500 | 337,783 | 338,103 | 0.1% |

An unbounded enumerator (`get_buckets`/`get_tables` have no `limit`/`offset`) fanned out across N
big projects returns hundreds of thousands of tokens in one tool result — overflowing the context
window in *either* model. Fan-out is a round-trip/turn optimizer, not a data-volume one.

**Fix — `MultiProjectMiddleware._merge` degrades to count-first past a cap** (`_FANOUT_MAX_ITEMS`,
default 200 total items across projects):
- Under the cap: unchanged — per-project text envelopes + fully merged lists.
- Over the cap: return a single guidance note with **per-project item counts**, a **truncated sample**
  (first `_FANOUT_MAX_ITEMS`, schema-safe — a shorter list still validates), and steer the agent to
  **narrow with `project_ids`** or **use `search`**. Counters (e.g. `bucket_counts`, search `total`)
  are summed by `_deep_merge`, so they keep reflecting the true totals even when the item lists are
  truncated. The per-project full text dumps are dropped in this path (that is the context saving).

This makes the multi-project path safe on humongous projects: it can never wedge the session, and it
nudges toward the scalable access patterns (search / per-project drill-down) instead of bulk-listing.
Follow-up: real `limit`/`offset` pagination on the enumerators, and concurrent fan-out.

## Transport note: multi-project scope is carried by the caller, not the session (superseded)

**Superseded.** This section originally assumed multi-project scope had to live in the MCP
**session** state (`ctx.session.state[SCOPE_KEY]`), read back on each request, and that this only
persists when the transport keeps the session alive across requests — fine on stdio (one long-lived
process), broken on the deployed default (`stateless_http=True`, a fresh empty session per request,
confirmed live via Datadog trace evidence: three separate `POST /mcp/` requests sharing one
process/`runtime-id` yet never seeing each other's session state), and only working around that with
`--no-stateless-http` (a single-replica-only workaround, and itself in tension with the direction the
MCP spec is taking: the 2026-07-28 RC removes `Mcp-Session-Id`/session pinning from the protocol
entirely, in favor of stateless-by-default operation).

**As built:** `set_project_scope`/`get_accessible_projects` sign the confirmed `SessionScope` into an
opaque `scope_token` (`SessionScope.to_token`/`from_token`, `mcp.py`; HMAC-JWT, the same
gzip+`jwt.api_jws` mechanism `SimpleOAuthProvider` already uses for OAuth tokens, extracted into
`jwt_utils.py`) and return it to the caller, who resends it as a tool-call argument on every
subsequent call. `SessionStateMiddleware` decodes it fresh from the request each time
(`_read_scope_from_request`) instead of reading `ctx.session.state` from a prior request. This is
stateless by construction: it works identically on stdio, one HTTP replica, or many, with no shared
store, no sticky routing, and no `--no-stateless-http` workaround needed. The signing secret is
`config.jwt_secret` (`KBC_JWT_SECRET`) when set — required to be shared across replicas for the
existing OAuth JWTs already, so scope tokens ride along for free — or a process-local fallback
(fine for stdio, since one process serves exactly one conversation).

Separately, the deployed session no longer needs to be single-project via a resolver exchange at
all: `create_session_state` forwards any programmatic token (`kbc_at_*`/`kbc_pat_*`) as
`Authorization: Bearer`, narrowed by `X-KBC-ProjectId` once a project is known — the
`resolve-storage-token` auth-bridge exchange this section referenced has been removed (see
`oauth_session_exchange/RFC.md` Decision §6). Full multi-project scope now works the same way on
the deployed server as it does locally.

## Decisions (increment 5) — explicit `project_id` on write tools (resolves Q4)

**Q4 (write-target confirmation), previously "still open; not blocking," is now resolved as leaned:
explicit `project_id` argument on every write/modify/delete tool, required once 2+ projects are
scoped.** Superseded is the interim behavior described above (line ~604, "increment 4"): a write
targeting `active_project_id` (the first scoped project) with no per-call target, requiring
`set_project_scope` to change which project a write lands on. That indirection was reported as
confusing in practice — writing to a different scoped project needlessly demanded a re-scope, which
also reorders the scope for every subsequent read fan-out.

- **Every write tool now declares `project_id: str | None = None`** (a real, schema-visible
  parameter — not a middleware-injected one, unlike the read-side `project_ids` filter). The LLM
  states its target explicitly in the conversation.
- **`MultiProjectMiddleware._dispatch_write`** (not the tool body) resolves and swaps the target,
  for the same reason `_swap_project` already runs ahead of `ToolsFilteringMiddleware` for read
  fan-out: role/feature/branch authorization must be evaluated against the *targeted* project's
  client, not whatever was active before the call.
- **Ambiguity is now a hard error, not a silent default:** 2+ scoped projects and no `project_id` →
  `ToolError` naming the scoped projects and asking for one. Exactly one scoped project still
  defaults `project_id` to it (unchanged single-project UX).
- **Read tools are unaffected** — they keep the existing `project_ids`-filtered fan-out; listing
  needs no single target.

This also folds in the one still-useful idea from the earlier, superseded MPA RFC (PR #500,
AI-3027, closed as superseded by this RFC): its "`project_id` as an explicit tool argument, chosen
over a header/middleware-only approach" recommendation, including the ambiguity rule (`from_project`
raising when 2+ projects are active and no `project_id` is given). Everything else in PR #500 (token
taxonomy, append-only project registry, Kai integration flow, 24h idle refresh) is already covered
by this RFC and the as-built code under different names.

---

# Extension: Kai (header-token) session-scope persistence (PSGO-261, increment 6)

## Context

Kai currently authorizes with a legacy, project-bound Storage token and will transition to a
stack-wide programmatic token (`kbc_at_`/`kbc_pat_`), refreshed by Kai's own regime rather than
this server's PKCE store. Once that happens, every request Kai sends carries an **unscoped**
whole-stack token, and `set_project_scope`/`get_accessible_projects` need the same server-side
scope persistence OAuth sessions already get (§"Transport note", increment 5) — pushing the
`scope_token` round-trip onto an LLM-driven client is unreliable (nothing guarantees it survives
compaction, a fresh turn, or simply gets echoed back correctly).

OAuth's persistence trick doesn't transfer directly, though: `SimpleOAuthProvider` mints its own
opaque token at login, so `sha256(opaque_token)` (`session_store/repository.py`) is a stable
Postgres key for the life of the session even as the *real* Keboola credential is refreshed
underneath it. Kai's raw token has no such stability — confirmed against the actual refresh code
in `auth_login.py`: `refresh_tokens()` returns a brand-new access-token string on every rotation,
and `create_pat()`'s response carries no separate token-id to key on either. Hashing the raw
inbound token would therefore silently drop the persisted scope on every Kai-side refresh.

## Required behavior

- **Persistence key:** `sha256(f'{conversation_id}:{user_id}')`, where `conversation_id` is the
  existing `X-Conversation-Id`-derived `Config.conversation_id` (already flowing on every request
  for tracing, confirmed stable for the life of one Kai chat session) and `user_id` is
  `Introspection.user_id` (`auth_login.py`) resolved from the *current* request's token. Binding
  to `user_id` — not just `conversation_id` — closes the gap a low-entropy or client-chosen
  `conversation_id` would otherwise leave open: a collision (or reuse) only matches an existing row
  if it also resolves to the same underlying Keboola identity, so a mismatched identity is a cache
  miss, not a leaked scope, with no separate post-lookup equality check to forget.
- **Stored row:** `project_ids`, `read_only`, `confirmed` only — no `scoped_token`/expiry fields,
  since Kai refreshes its own Keboola credential independently of this table; nothing here needs
  to track the parent token's freshness.
- **Read-time validation, not a superset/subset hash:** a hash can only express exact-match
  equality, not "grew is fine, shrank is not" — so the monotonicity rule is enforced in code, at
  read time, against introspection data already being fetched: if
  `set(row.project_ids) - {p.id for p in introspection.projects}` is non-empty (some previously
  scoped project is no longer reachable), the row is dropped and the scope is treated as
  unconfirmed. Projects *added* to the token's reach never invalidate an existing scope, since the
  subset relation still holds.
- **On invalidation, drop the whole scope** (not auto-narrow to the intersection) — force a full
  `get_accessible_projects` → `set_project_scope` redo so an access change is surfaced to the user
  rather than silently absorbed.
- Applies only to deployed, non-OAuth, programmatic-token sessions with a `conversation_id`
  present (`deployed_sa_token_path()` set, `is_programmatic_token(config.storage_token)`, no
  `AuthenticatedUser`/`ProxyAccessToken` on the request). OAuth sessions keep using
  `oauth_sessions`; local PKCE sessions keep using `ctx.session.state` (`session_state_persists`);
  neither is affected by this table.

## Resolution strategy

- New table `kai_sessions` (migration `0004_kai_sessions.sql`), unpartitioned initially — same
  starting point `oauth_sessions` had before partitioning became necessary (increment/migration
  `0002`); add partitioning here too if/when retention needs it.
- New `session_store/kai_scope.py`: `KaiScope` (data) + `KaiScopeStore` (Protocol) +
  `PostgresKaiScopeStore` (impl), deliberately **not** folded into `SessionStore`/`OAuthSession` —
  different key scheme (composite hash vs. opaque-token hash), no encrypted credential fields (no
  secret is stored, just a project-id list and two flags), different invalidation semantics
  (subset-check + drop vs. revoke). Keeping it a separate small store avoids overloading the
  OAuth-shaped `SessionStore` protocol with a second, structurally different session concept.
  Same lazy-pool-on-first-use pattern as `PostgresSessionStore`.
- `ServerState.kai_scope_store: KaiScopeStore | None`, constructed in `server.py` whenever
  `config.postgres_dsn` is set — **independent of whether OAuth is configured**, since Kai's path
  needs no `oauth_client_id`/`session_encryption_key` (no OAuth login, no encrypted fields here).
- `SessionStateMiddleware.on_request` (`mcp.py`): a new fallback,
  `_read_persisted_kai_scope`, slotted after `_read_persisted_local_scope` and before
  `_autolease_default_scope` — mirrors `_read_persisted_oauth_scope`'s position in the chain but
  reads from `kai_scope_store` instead of the OAuth session row, gated on the "deployed,
  non-OAuth, programmatic, has conversation_id" condition above. Skipped for `/list` like every
  other network-touching step in this chain.
- `tools/project.py`'s `set_project_scope`: a new `_persist_kai_scope`, called alongside the
  existing `_persist_oauth_scope` — whichever one applies persists server-side and suppresses
  `scope_token` in the response (`persisted = await _persist_oauth_scope(...) or await
  _persist_kai_scope(...) or session_state_persists`, unchanged shape, one more branch).

## Decisions (increment 6)

- **Server-side persistence over client-side round-tripping**, confirmed: pushing scope state
  into Kai/the LLM's own context is fragile (no guarantee of faithful round-trip across turns or
  compaction); persisting server-side, looked up automatically on every request, needs no
  cooperation from the calling LLM beyond sending the `conversation_id` header it already sends.
- **Composite key (`conversation_id` + `user_id`) over either alone.** `conversation_id` alone is
  client-supplied and not guaranteed high-entropy; `user_id` alone is not conversation-scoped
  (would incorrectly share scope across unrelated chats from the same person). Together they give
  a key that's both stable across Kai's token refreshes and safe against a `conversation_id`
  collision or reuse.
- **Drop-whole-scope over auto-narrow on a reachability shrink** — an explicit user decision
  (over the friendlier-but-quieter auto-narrow-to-intersection alternative): surfacing an access
  change via a forced re-scope beats silently continuing with whatever subset still works.
- **A new store/table over extending `oauth_sessions`/`SessionStore`** — the two session kinds
  differ enough (key scheme, no encrypted fields, no OAuth-specific lifecycle) that folding Kai
  scope into the OAuth-shaped protocol would blur its single responsibility for no real code
  reuse (the two stores would share almost no method bodies).

---

# Extension: Security hardening — response to review (PSGO-261, increment 7)

## Context

Tomas Fejfar's review of PR #604 (2026-08-07, "Agentic review") raised 9 concerns. Each was
independently re-verified against the as-built code (file:line evidence) and cross-checked with a
second, independent security-review pass before any fix was designed — this section documents
what was actually found, not just what was claimed, since two items turned out different from
the original framing (one narrower, one broader; see below).

## Verified findings

1. **Header injection into `Config` → forgeable `scope_token`, CONFIRMED.**
   `SessionStateMiddleware.apply_request_config` calls `config.replace_by(http_rq.headers)` with
   no allowlist; `Config._read_options` matches *any* dataclass field against an `X-{name}`
   header, including `jwt_secret`. Since `resolve_scope_secret(config)` reads `config.jwt_secret`
   from that same per-request config, an `X-Jwt-Secret` header lets a caller choose the HMAC key
   that both signs and verifies their own `scope_token` — full `project_ids` forgery.
2. **`scope_token` embeds a live bearer token, signed but not encrypted, CONFIRMED — broader than
   first framed.** `jwt_utils.py`'s `encode_jwt`/`decode_jwt` are JWS (signature only) over
   gzip+JSON; the payload is base64+gunzip-recoverable by anyone, without the secret.
   `SessionScope.scoped_token` — a real, live Keboola access token, not just non-secret metadata
   like `project_ids` — is itself a dataclass field, so it's embedded verbatim in the
   client-visible token returned by `set_project_scope`/`get_accessible_projects` and resent as a
   tool-call argument on every subsequent call: it lands in LLM context, client transcripts, and
   client-side logs. No `exp` enforcement; decode failures (tampered, expired, or malformed) all
   collapse into the same "no scope" outcome, with no revocation path.
3. **`read_only=True` fails open, CONFIRMED — broader scope than reported.** Not just
   single-project scopes as originally described: `MultiProjectMiddleware` skips `_swap_project`
   (the only code path that ever passes `readonly=scope.read_only` into a `KeboolaClient`)
   whenever a call targets `scope.active_project_id` — true for every single-project scope *and*
   the first/active project of any multi-project scope. `SessionStateMiddleware.create_session_state`
   never passes `readonly=` at all from `on_request`, regardless of scope. So the active
   project's writes are never locally read-only-restricted — enforcement depends entirely on the
   minted `scoped_token` being genuinely read-only server-side, which doesn't exist when the
   `/v1/auth/pat/exchange` call fails. Only *non-active* projects in a 2+ project scope get real
   local enforcement today (via `client_for_project(readonly=scope.read_only or None)`).
4. **`normalize_storage_api_url` is a prefix check, not a domain allowlist, CONFIRMED.**
   `hostname.startswith('connection.')` lets `connection.attacker.tld` pass. `is_same_stack` is a
   correct exact-host match, but it's only ever applied when the server has its own configured
   stack (`own_stack_storage_api_url` set); a server with no stack of its own (local mode, by
   design, since it must accept the caller's URL) has no equivalent check before a caller-supplied
   `X-Storage-Api-Url` host receives the live bearer token.
5. **`resolve_encryption_key`'s silent process-local fallback — REFUTED, already mitigated.**
   `server.py` already refuses to start (`raise RuntimeError`) if OAuth is configured
   (`oauth_client_id`/`oauth_client_secret` both set) without `KBC_SESSION_ENCRYPTION_KEY`, and
   `PostgresSessionStore` is never constructed via any other path. The cross-replica
   silent-decrypt-failure scenario the review described can't actually happen today. Documented
   here so it isn't re-flagged as a live gap.
6. **MFA codes as CLI arguments, CONFIRMED.** `login --totp`/`--recovery` are plain `argparse`
   string options — visible in shell history and `ps`/`/proc/<pid>/cmdline` for the process
   lifetime. Recovery codes are single-use, high-value.
7. **Verbatim auth-endpoint error bodies, CONFIRMED (minor nuance).** `elevate_session`/
   `create_pat` both raise `RuntimeError` including the raw `response.text` (`create_pat` also
   `{payload=}`, which is `{name, expiresIn, scope}` — the MFA code itself is not in either
   logged payload). Still real: no redaction, contradicting this RFC's general redaction stance.
8. **"Ask-first" is prompt-text, not access control — CONFIRMED, but narrower than it first
   appears.** Re-verified exactly where this matters: the ask-first gate
   (`MultiProjectMiddleware.on_call_tool`) only ever fires because `_autolease_default_scope`
   (gated on a *local* programmatic session) auto-leases an unconfirmed, all-projects
   `SessionScope` by default. OAuth and Kai sessions never do this — they simply have **no** scope
   at all (not an auto-leased one) until `set_project_scope` runs, so neither grants usable
   all-project access before an explicit choice; only the local `login`/env-var-token path has
   this gap, and it's closed structurally rather than by better wording — see §Required behavior
   below.
9. **Cross-process credential race, CONFIRMED.** No `asyncio`/`fcntl`/lock import anywhere in
   `auth_login.py`; `save_tokens` does an unlocked read-modify-write on
   `~/.keboola/mcp/credentials.json` with a rotating refresh token. As-built, `_store_key()` is
   `hostname` alone, so two different local MCP client processes for the *same stack* (e.g. Claude
   Desktop and a terminal `login`) genuinely share one entry today — this is confirmed as a real
   design gap, not just a hypothetical.

## Required behavior

- **Config field allowlist for header-derived values.** `Config` gains an explicit
  `_HEADER_ELIGIBLE_FIELDS` set (the fields legitimately meant to vary per request:
  `storage_api_url`, `storage_token`, `branch_id`, `workspace_schema`, `workspace_id`,
  `bearer_token`, `conversation_id`, `project_id`) and a new `replace_by_headers()` method that
  only resolves `X-{name}` headers for fields in that set. `apply_request_config` uses it instead
  of the unrestricted `replace_by`. Deployment-level fields (`jwt_secret`, `postgres_dsn`,
  `session_encryption_key`, `oauth_client_id`/`oauth_client_secret`, `oauth_server_url`,
  `mcp_server_url`) become permanently unreachable from any request header. Env-var (`KBC_{name}`)
  and CLI-derived resolution is untouched — that input is already operator-trusted.
- **Keboola-domain allowlist for `normalize_storage_api_url`.** Replace the bare `connection.`
  prefix check with a regex requiring both the `connection.` label and a genuine
  `*.keboola.(com|dev)` suffix, mirroring the pattern `oauth.py`'s `_ALLOWED_DOMAINS` already uses
  for redirect URIs. Applies uniformly to deployed (already double-covered by `is_same_stack`)
  and local (previously uncovered) servers alike.
- **`read_only` is enforced locally for the active project too**, not just relying on the remote
  scoped token: `create_session_state` now receives `readonly=(True if scope and scope.read_only
  else None)` from `on_request`, so the base session client is built read-only whenever the
  confirmed scope requests it — success or failure of the token exchange. Workspace provisioning
  (a server-side plumbing GET+POST pair, not a user-visible mutation) is explicitly exempted via a
  new `KeboolaClient.writable_storage_client`, so `query_data` keeps working against a read-only
  scope that has no workspace yet. The `MultiProjectMiddleware` active-project shortcuts (read
  fan-out and the write-dispatch path) are guarded with a `KeboolaClient.readonly` check so they
  only skip the per-project client swap when the base client already matches the scope's
  `read_only` — defense in depth, zero added cost for the common case once the above makes that
  the normal state. `set_project_scope`'s exchange-failure fallback keeps working (some stacks
  lack the exchange endpoint) but its `llm_instruction` now says explicitly whether read-only is
  server-enforced (a real `scoped_token` exists) or only locally enforced (fallback path).
  `KeboolaClient.with_branch_id()` — which rebuilds a fresh client for any non-default-branch
  call (routine on a dev branch, not just adversarial) — is fixed to forward `readonly` into the
  new client; a fresh `security-scanner` pass on the implementation caught this dropping
  `readonly` silently, which would have reopened this exact fail-open bug on every branch switch.
- **`scope_token`'s payload is encrypted, not just signed.** `SessionScope.to_token`/`from_token`
  move from `jwt_utils`'s JWS to AES-GCM authenticated encryption via the already-existing
  `session_store/crypto.py` helpers and `resolve_encryption_key` — the same key OAuth sessions
  already encrypt with. `resolve_scope_secret`/`_FALLBACK_SCOPE_SECRET` are removed in favour of
  `resolve_scope_key`. A new `scope_token` is therefore ciphertext, not a
  base64+gzip-recoverable signed blob; the live `scoped_token` it may carry is no longer readable
  without the key. No backward-compatible legacy-JWS decode path: this feature has not shipped to
  production (main has none of PSGO-261 yet), so there are no live tokens to migrate — a clean
  replacement, not a staged one. (A separate design considered and rejected: a new
  Postgres-backed `scope_sessions` table mirroring `kai_scope.py`, giving every client an opaque
  handle instead of any client-held credential. Rejected as unwarranted complexity —
  `scope_token` is only actually issued in the narrow
  remaining case where neither OAuth nor Kai's Postgres-backed persistence applies; OAuth and Kai
  sessions already never hand the client a live credential at all.)
- **MFA codes: prompt, don't require a CLI argument.** `login --pat` still accepts
  `--totp`/`--recovery` as opt-in overrides for scripted/CI use (documented in `--help` as
  shell-history/`ps`-visible), but when neither is supplied, prompts via `getpass.getpass()` —
  hidden input on a real TTY, and a graceful (though visible, with a stderr warning) read from
  stdin when piped/non-interactive, so scripted input still works without extra plumbing.
- **Auth-endpoint errors are redacted.** `elevate_session`/`create_pat` raise a generic
  `RuntimeError(f'... failed ({status}). See debug logs for details.')`; the raw `response.text`/
  request `payload` move to `LOG.debug(...)` only.
- **Local sessions are scoped at login time, never auto-leased to everything.** This replaces
  "document ask-first as guidance" with a structural fix: `login` (and `login --pat`) now require
  an explicit project choice — prompted interactively (same "show projects, pick all or a subset"
  flow already used in-conversation by `get_accessible_projects`/`set_project_scope`) when run
  from a TTY without `--project-ids`/`--all`, required explicitly otherwise. `lease_pat`, which
  previously always requested every accessible project, takes the same explicit choice. The
  confirmed `project_ids`/`read_only` are persisted alongside the access/refresh tokens in the
  stored credential entry, and the local-session bootstrap in `mcp.py` reads them back as an
  already-`confirmed=True` `SessionScope` — `_autolease_default_scope`'s implicit
  all-projects-then-ask-first default is removed for any session with a persisted choice. Since
  OAuth and Kai sessions already never auto-lease (finding #8), this closes the gap at its actual
  source (a local session existing before any explicit choice) rather than trying to make an
  LLM-facing instruction into an enforcement boundary.
- **Credentials are keyed per interface, not just per stack.** `login` gains a profile identifier
  (`--profile <name>` / `KBC_LOGIN_PROFILE`, defaulting to `'default'` so single-interface setups
  are unaffected) naming which calling interface (Claude Desktop, Cursor, a terminal session) this
  login is for. `_store_key()` becomes `(hostname, profile)`, and the on-disk schema nests entries
  accordingly — removing finding #9's race by construction, since independent interfaces no
  longer share an entry at all. The narrower race that remains — two concurrent requests *within
  one process* both seeing "near expiry" and both refreshing — is closed with a plain
  `asyncio.Lock` per `(hostname, profile)` in `get_access_token` (in-process; no file locking
  needed for this case). A non-blocking `fcntl.flock` on a sibling `.lock` file around the on-disk
  read-modify-write is kept as cheap defense-in-depth insurance (polling `LOCK_EX | LOCK_NB`,
  never a blocking flock — degrades with a warning rather than stalling the event loop/MCP
  handshake; the `fcntl` import is guarded for non-POSIX platforms), covering accidental
  profile-sharing or a `login` run racing an already-running server for the same profile.

## Explicitly out of scope this increment

- Real MCP-elicitation-based (`elicitation/create`) human-in-the-loop confirmation for scoping —
  superseded by login-time scoping, which removes the need for any runtime confirmation gate on
  the local path. Worth revisiting only if a future flow reintroduces an unconfirmed-by-default
  state.
- Windows-native file locking for the credential-lock insurance layer — CI and the documented
  supported platforms are POSIX-only today; the `fcntl` import degrades cleanly rather than
  crashing where absent.

## Decisions (increment 7)

- **Fix the flow, don't just document the gap**, for both #8 (ask-first) and #9 (credential
  race): in both cases a structural fix (scope at login time; key credentials per interface) was
  available and preferred over accepting the gap as a documented limitation.
- **Eliminate shared state before adding a lock**: #9's primary fix is removing the sharing
  (per-profile keying), not the `fcntl.flock` layer, which is retained only as insurance for
  whatever narrow sharing remains (in-process concurrency, accidental profile reuse).
- **Encrypt the existing `scope_token` fallback rather than build new server-side infrastructure**
  for #2/#4: since OAuth and Kai already keep credentials server-side, the client-held-token case
  is narrow enough that AES-GCM-encrypting the existing JWS payload is proportionate; a new
  Postgres table mirroring `kai_scope.py` was considered and rejected as unneeded complexity for
  that narrow remaining surface.
