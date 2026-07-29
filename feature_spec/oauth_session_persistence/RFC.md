# RFC: Postgres-backed OAuth session store (replaces self-contained JWT session)

Linear: [PSGO-261](https://linear.app/keboola/issue/PSGO-261/support-pat-tokens-in-mcp-server-mcp-server)
Parent: PSGO-261. Closes the "keyring/DB credential storage" carve-out `pat_token_support/RFC.md`
explicitly deferred in increment 1 ("Still out of scope: ... keyring/DB credential storage").
Related: `oauth_session_exchange/RFC.md` (the exchange this RFC changes how the result is stored),
`pat_token_support/RFC.md` §"Transport note" (the `scope_token` mechanism this RFC's scope columns
can absorb for OAuth sessions specifically).

## Problem

Today, the deployed OAuth login flow (`oauth.py`, `SimpleOAuthProvider`) stores **nothing**
server-side. Every piece of session state — the OAuth authorize-state, the authorization code, the
access token, the refresh token, and (as of the `scope_token` fix) the confirmed multi-project scope
— is self-encoded into a signed, gzip-compressed JWT (`jwt_utils.py`) and handed to the client, which
resends it on every subsequent request. This was a deliberate choice (see `oauth.py`'s own comment:
*"We don't store the authentication states... instead we encode them to JWT"*) and it is fully
stateless: any replica can decode any token with the shared `KBC_JWT_SECRET`, no shared datastore
needed, correct under the MCP 2026-07-28 RC's stateless-transport direction.

That design has three real costs, all inherent to "the client holds the truth, signed":

1. **No revocation.** A leaked or compromised `ProxyAccessToken`/`ProxyRefreshToken`/`scope_token` is
   valid until its embedded expiry, full stop — there is no server-side list to delete from. Ending a
   session early (logout, incident response, revoking a compromised token) is not possible today.
2. **Client-visible plumbing.** `scope_token` must be threaded through every tool call as an explicit
   argument (see `pat_token_support/RFC.md` "Transport note") because there is nowhere else for the
   confirmed scope to live between requests. This works, but it is visible surface area the calling
   agent has to carry correctly every single call.
3. **Refresh is the client's problem.** The MCP client must notice its access token is nearing
   expiry and call this server's `/oauth/token` with `grant_type=refresh_token` — today's
   `exchange_refresh_token()` only runs when the client initiates that call. There's no way for this
   server to refresh the underlying `kbc_access_token`/`kbc_refresh_token` proactively or transparently.

**Proposed change:** store the OAuth session server-side, in Postgres, encrypted at rest. The token
the MCP client holds becomes a short, opaque, random reference (not a JWT carrying real credentials)
that this server looks up, decrypts, and — if the underlying Keboola token is near expiry — refreshes
transparently before using. This is a deliberate, scoped trade: give up "zero shared infra" for the
OAuth path specifically, in exchange for revocation, a smaller/opaque client-facing token, and
server-managed refresh. It does **not** change the local PKCE `login` flow (`~/.keboola/mcp/credentials.json`,
unaffected) or the header/PAT-supplied-token flow (still fully stateless, unaffected) — see Scope.

## Required Behavior

### Token model change

| | Today (JWT, self-contained) | Proposed (Postgres, opaque reference) |
|---|---|---|
| What the MCP client holds | A JWT with the real `kbc_access_token`/`kbc_refresh_token`/`scope` embedded, HMAC-signed | A random opaque string (e.g. 256 bits, base64url) that is *only* a lookup key |
| How the server validates it | Verify HMAC signature, decode payload | Look up by the opaque string (hashed) in Postgres; row must exist, not be revoked, not be expired |
| Where the real Keboola credentials live | Inside the JWT, in the client's possession | Encrypted (AES-256-GCM) in Postgres only; never sent to the client |
| Revocation | Not possible before natural expiry | `DELETE`/soft-revoke the row; token is dead on the next lookup |
| Refresh | Client-initiated, via `/oauth/token` `grant_type=refresh_token` | Server-initiated, lazily: on lookup, if `kbc_access_token` is near expiry, refresh via `refresh_tokens()` and update the row in place — the client's opaque token does not need to change |
| Multi-project scope (`scope_token`) | Separate signed JWT, resent as a tool argument every call | Columns on the same session row; no `scope_token` argument needed for OAuth sessions at all |

### Schema (new `oauth_sessions` table, one row per logged-in session)

| Column | Type | Notes |
|---|---|---|
| `id` | `uuid`, PK | Internal row id |
| `access_token_hash` | `bytea`, unique, indexed | `sha256` of the opaque access token the client holds — store the hash, not the token, so a DB read alone can't leak a live bearer credential |
| `refresh_token_hash` | `bytea`, unique, indexed, nullable | Same, for the opaque refresh token |
| `client_id` | `text` | The OAuth client (`claude.ai`, etc.) — audit/introspection only |
| `user_email` | `text`, nullable | From the exchange response, for audit/introspection |
| `kbc_access_token_enc` | `bytea` | AES-256-GCM ciphertext of the real `kbc_access_token` |
| `kbc_refresh_token_enc` | `bytea` | AES-256-GCM ciphertext of the real `kbc_refresh_token` |
| `kbc_access_expires_at` | `timestamptz` | Drives the lazy-refresh check |
| `scope_project_ids` | `int[]`, nullable | Confirmed multi-project scope — absorbs `scope_token`'s job for OAuth sessions |
| `scope_read_only` | `boolean`, default `false` | |
| `scope_confirmed` | `boolean`, default `false` | |
| `scope_scoped_token_enc` | `bytea`, nullable | AES-256-GCM ciphertext of the minted scoped token (`/v1/auth/pat/exchange` result) |
| `scope_scoped_expires_at` | `timestamptz`, nullable | |
| `created_at` / `updated_at` / `last_used_at` | `timestamptz` | |
| `revoked_at` | `timestamptz`, nullable | Soft-revoke; a non-null value makes lookup fail as if the row didn't exist |

Every encrypted column uses **AES-256-GCM** (authenticated encryption — tamper-evident, not just
confidential) via the `cryptography` package, already a pinned dependency (`pyproject.toml:22`,
`~= 49.0`) — no new crypto library needed, just a new small module (`session_store/crypto.py`) wrapping
`cryptography.hazmat.primitives.ciphers.aead.AESGCM`.

### New env var

`KBC_SESSION_ENCRYPTION_KEY` — 32 raw bytes, base64-encoded for env-var transport (`base64.b64decode`
on load, fail loudly at startup if it doesn't decode to exactly 32 bytes). Single static key for v1
(see Open Questions on rotation). Mirrors how `KBC_JWT_SECRET` is already handled today
(`config.jwt_secret`) — same "required in production, generate an ephemeral one locally if unset"
posture, so local dev/tests work with zero setup.

### Refresh strategy: lazy, on lookup — not a background job

Every session lookup (equivalent to today's `load_access_token()`) checks `kbc_access_expires_at`
against `is_near_expiry`-style logic (reuse the exact same 60-second-early check `SessionScope`
already uses) and refreshes in place via the existing `refresh_tokens()` (`auth_login.py`) before
returning the decrypted token — the same "check-then-refresh-then-use" shape already implemented for
`scope_token`'s `scoped_token` re-mint in `_resolve_local_tokens`. **No new scheduler, no background
worker, no cron** for v1 — refresh only happens on an actual request, which is simpler to reason
about and test, at the cost of the first request after a long idle period paying one extra refresh
round-trip (acceptable; this is the same trade the current design already makes for `scope_token`).

### What the MCP client actually sees

Nothing changes about the OAuth *dance* — `/oauth/consent`, code exchange, redirect — only what comes
back at the end. `SimpleOAuthProvider.exchange_authorization_code()` mints a session row instead of a
JWT and returns the row's opaque access/refresh token pair as today's `AccessToken`/`RefreshToken`
Pydantic models (same wire shape, different contents — a random string instead of a JWT). Client code
requires zero changes; this is entirely a server-internal storage swap from the MCP client's point of
view. `set_project_scope`/`get_accessible_projects` **stop returning `scope_token`** for OAuth
sessions (scope now lives on the row, found via the same access-token lookup already required on
every request) — the two tools' models keep `scope_token: str | None` for backward compat with
header/PAT sessions (see Scope), just always `None` when the caller authenticated via OAuth.

## Resolution Strategy

- **New package `src/keboola_mcp_server/session_store/`**:
  - `crypto.py` — `encrypt(plaintext: bytes, key: bytes) -> bytes` / `decrypt(...)`, thin AES-256-GCM
    wrapper (nonce prepended to ciphertext, standard practice).
  - `repository.py` — `SessionStore` protocol (`create`, `get_by_access_token`, `get_by_refresh_token`,
    `update_kbc_tokens`, `update_scope`, `revoke`) + a `PostgresSessionStore` implementation using
    `asyncpg` (async-native, matches this codebase's existing all-`httpx`-async style; **no ORM** —
    a single-table store doesn't earn SQLAlchemy's overhead, and the rest of the codebase has zero
    ORM precedent to extend). The protocol exists so `oauth.py`/tests can mock the store without a
    real database (unit tests) while integration tests exercise `PostgresSessionStore` against a real
    one (see Testing).
  - `migrations/0001_oauth_sessions.sql` (+ a ~15-line runner: a `schema_migrations` tracking table,
    apply un-applied numbered `.sql` files in order at startup or via a `keboola-mcp-server migrate`
    CLI subcommand — deliberately not `alembic`; one table doesn't need a migration framework, a
    numbered-SQL-files-plus-tracking-table is the whole mechanism and is trivially testable).
- **`config.py`**: add `postgres_dsn: Optional[str]` and `session_encryption_key: Optional[str]`
  fields, same env-var-mapping mechanism as every other `Config` field.
- **`oauth.py`**: `SimpleOAuthProvider` gains a `session_store: SessionStore` constructor param.
  `exchange_authorization_code`/`exchange_refresh_token`/`load_access_token`/`load_refresh_token`
  are rewritten against the store instead of `self._encode`/`self._decode`. The authorize-state JWT
  (5-minute TTL, `authorize()`) and the authorization-code JWT (`_ExtendedAuthorizationCode`) are
  **unchanged** — they're short-lived, single-use, pre-authentication artifacts with no real
  credentials embedded, and encoding them as JWTs today is already fine; only the *long-lived,
  real-credential-carrying* access/refresh/scope tokens move to Postgres.
- **`mcp.py`**: `SessionStateMiddleware`/`MultiProjectMiddleware` gain a scope-store lookup path for
  OAuth sessions (keyed by the same `AuthenticatedUser.access_token` already resolved by the MCP SDK's
  auth layer) instead of decoding `scope_token` from arguments — `set_project_scope` writes
  `scope_*` columns on the row instead of minting a JWT. The `_read_scope_from_request`/`_SCOPE_TOKEN_ARG`
  path stays exactly as-is for non-OAuth sessions (see Scope).
- **`server.py`**: construct the `SessionStore` (real `PostgresSessionStore` if `postgres_dsn` is set,
  otherwise refuse to start an OAuth-enabled server without one — no silent in-memory fallback for a
  production auth path) and pass it into `SimpleOAuthProvider`.
- **`docker-compose.yml`** (new, repo root): a single `postgres:16` service for local dev/integration
  tests — named volume, healthcheck, default credentials for local use only (never used in any real
  deployment, which gets its own managed Postgres instance via kbc-stacks, out of scope here).

## Scope

**In scope:** the deployed, OAuth-authenticated session path only — `exchange-oauth-token` result
storage, refresh, and (optionally, see Open Questions) the multi-project scope for OAuth sessions.
Postgres schema + migrations + local docker-compose + AES-256-GCM encryption + unit/integration tests.

**Explicitly out of scope, unchanged by this RFC:**
- **Local PKCE `login` flow** (`auth_login.py`, `~/.keboola/mcp/credentials.json`) — keeps using its
  existing mode-600 file. It has no multi-replica concern (one stdio process, one conversation) and
  no revocation need proportionate to the complexity of adding a DB dependency to a local CLI tool.
- **Header/PAT-supplied tokens** (`is_programmatic_token()` path) — stays fully stateless, `scope_token`
  keeps working exactly as today for this path. There is no stable per-conversation identifier to key
  a DB row on for a bare supplied token the way `session_id`/the OAuth access token gives us for free.
- Postgres HA/backup/monitoring in the actual deployed stacks — that's a kbc-stacks-side concern
  (separate repo), same carve-out pattern used for the k8s ServiceAccount scope grants in
  `oauth_session_exchange/RFC.md`.
- Encryption-key rotation (see Open Questions) — single static key for v1.
- Background/proactive refresh — lazy-on-lookup only for v1 (see Required Behavior).

## Delivery plan (phased, compact)

**Phase 1 — Schema, crypto, store, local dev infra (no behavior change yet).**
- `session_store/` package: `crypto.py`, `repository.py` (`SessionStore` protocol +
  `PostgresSessionStore`), `migrations/0001_oauth_sessions.sql` + runner.
- `docker-compose.yml`; `config.py` additions; `KBC_SESSION_ENCRYPTION_KEY` handling (generate
  ephemeral locally if unset, same posture as `KBC_JWT_SECRET`).
- Tests: crypto round-trip (encrypt/decrypt, tamper detection via GCM auth tag), migration runner
  applies-once idempotency, `PostgresSessionStore` CRUD against a real docker-compose Postgres.

**Phase 2 — Wire `SimpleOAuthProvider` to the store (the core swap).**
- Replace `_encode`/`_decode` calls for access/refresh tokens with store lookups; lazy-refresh-on-lookup.
- `exchange_authorization_code`: mint a row instead of a JWT pair.
- Tests: exchange creates a row with encrypted tokens; `load_access_token` decrypts + returns; expired
  access token triggers exactly one `refresh_tokens()` call and updates the row in place; a revoked
  row fails lookup; a tampered ciphertext (flipped bit) fails GCM auth and is treated as invalid, not
  silently decrypted wrong.

**Phase 3 — Move multi-project scope onto the row for OAuth sessions.**
- `set_project_scope`/`get_accessible_projects`: write/read `scope_*` columns via the store when the
  session is OAuth-authenticated; `scope_token` stays `None` in their output for this case.
- `mcp.py`: scope resolution branches on session type — OAuth → store lookup, everything else →
  existing `_read_scope_from_request`/`scope_token` path, unchanged.
- Tests: OAuth session's `set_project_scope` never returns a `scope_token`; a subsequent call with no
  `scope_token` argument still resolves the previously-confirmed scope correctly via the store.

**Cross-cutting:** version bump (minor — new capability + new required infra for OAuth deployments),
`uv.lock`, new `asyncpg` dependency, CI: a `postgres` service container for the integration-test tox
env, `TOOLS.md` regen (the `scope_token` field's description changes to note it's OAuth-session-conditional).

## Testing / Verification

**Unit** — `PostgresSessionStore` mocked out via the `SessionStore` protocol wherever `oauth.py`/`mcp.py`
logic is under test (no real DB needed for these); crypto module tested in full isolation (round-trip,
wrong-key failure, tampered-ciphertext failure) with no DB at all.

**Integration** — a real Postgres via docker-compose (`docker compose up -d postgres` in the
integration-test tox env, matching how `integtests/` already needs real external services): full
`authorize → consent → callback → token → tool call → refresh-after-forced-expiry → revoke → 401`
cycle against it.

**Manual** — the same real-dev-stack OAuth login test used throughout this PR's live debugging,
confirming: no `scope_token` in `set_project_scope`'s output for an OAuth session; killing/restarting
the MCP server process mid-conversation and confirming the session survives (this is the concrete,
demonstrable win over the JWT design — a process restart today does *not* invalidate a signed JWT
either, so this specific test doesn't distinguish them; the real differentiator is **revocation**:
manually deleting the row and confirming the *next* request 401s, which a signed JWT cannot do before
its embedded expiry).

## Open Questions

1. **Does multi-project scope move to Postgres for OAuth sessions in the same delivery, or later?**
   Phase 3 above assumes yes (it's a small addition once the row exists for the access/refresh tokens
   anyway) — confirm before starting Phase 3, since it's the part of this RFC that changes tool-facing
   output shape (`scope_token` becomes conditionally absent), not just internal storage.
2. **Encryption key rotation.** V1 ships a single static `KBC_SESSION_ENCRYPTION_KEY`. Rotating it
   invalidates every stored session (can't decrypt with the old key). Acceptable for v1 (forces
   re-login, not data loss — no Keboola data lives in this table, only session credentials), but worth
   a documented runbook step before this ships, and a versioned-key-prefix scheme (`v1:<ciphertext>`)
   would make future rotation non-disruptive if we want to add it later — flagging now so the column
   format (prefix the ciphertext with a key-version byte) is decided before Phase 1's migration ships,
   not retrofitted after real rows exist.
3. **Session expiry / cleanup.** Rows never get deleted automatically today's plan — need a retention
   policy (e.g. delete rows with `revoked_at` set or `last_used_at` older than N days) — a cron/cleanup
   job, explicitly out of scope for v1 but should be tracked as an immediate follow-up, not forgotten.
4. **Does Postgres downtime take down OAuth login entirely, or degrade gracefully?** With no DB, no
   OAuth session can be created or validated — this is a new hard dependency for the OAuth path (by
   design, per Scope: "no silent in-memory fallback for a production auth path"). Confirm this is
   acceptable given kbc-stacks' Postgres HA posture before shipping, since it changes the failure mode
   of OAuth login from "always works" (today, stateless) to "works iff Postgres is reachable."
