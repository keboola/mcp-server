# RFC: Replace the hardcoded OAuth redirect-URI whitelist with Connection's client registry

Linear: AI-2883
Related: AI-3591, AI-3792, AI-3797
Connection side (merged, released): [keboola/connection#7016](https://github.com/keboola/connection/pull/7016) — `docs/features/oauth-dynamic-client-registration.md`

Supersedes the stale draft on `AI-2883-dynamic-oauth-client-registration` ([PR #453](https://github.com/keboola/mcp-server/pull/453), still open) and the interim hardening in [#580](https://github.com/keboola/mcp-server/pull/580) (hub-subdomain exclusion) — both predate Connection's final, merged `/oauth/clients/validate` contract and don't match it (see Decisions §1). The hub-subdomain exclusion no longer exists in this repo: which hosts may receive a redirect is now decided by Connection's registry and its approval step, not by a host list here.

---

## Problem

`SimpleOAuthProvider` (`src/keboola_mcp_server/oauth.py`) plays OAuth Authorization Server to whatever
AI assistant is connecting (Claude.ai, ChatGPT, Cursor, n8n, custom MCP clients, ...). Today it:

1. Accepts **any** `client_id` at `/register` — `register_client()` is a no-op, `get_client()`
   fabricates a permissive client object for literally any string (`oauth.py:255-272`).
2. Validates the client's `redirect_uri` only against a hardcoded regex table,
   `_ALLOWED_DOMAINS` (`oauth.py:53-80`) — wildcard subdomains under `keboola.(com|dev)`, a fixed
   list of partner domains (`chatgpt.com`, `claude.ai`, `make.com`, several `n8n.groupondev.com`
   hosts, ...), plus `cursor://` and loopback special cases.
3. Cannot revoke a client's access individually — there is no registry, just a shared static list
   every caller matching a domain can use.

**Motivation (AI-3792, AI-3591):** `/register` accepts any client without validation, so the static,
ever-growing domain whitelist (`_ALLOWED_DOMAINS`) is the only redirect check. That check holds today,
but it holds by the choice of which domains are listed, not by design: open registration plus a
static list is an inconsistent trust model.

Connection's PR #7016 (merged 2026-09-10, live on all stacks per this RFC's authoring context)
replaces Connection's own equivalent whitelist with a real, per-stack, DB-backed client registry
(`oauth2_client`) and exposes it to callers like this server via `POST /oauth/clients/validate`.
This RFC adopts that on the MCP server side and deletes `_ALLOWED_DOMAINS` for good.

**Two problems surfaced while designing the adoption that Connection's own doc doesn't cover**,
because they're specific to how *this* server's OAuth-AS role works (see Decisions §2, §3):

- Connection's `oauth2_client.identifier` (and the `pending_mcp_client` payload's `client_id`
  field) is capped at 32 characters. The mcp SDK mints a `uuid4()` (36 characters) as `client_id`
  for every client that dynamically registers via `/register` (`RegistrationHandler.handle`,
  `mcp/server/auth/handlers/register.py:52`). Forwarding that id to Connection verbatim would
  never fit — every DCR'd client would be permanently stuck 404-ing, and the `pending_mcp_client`
  payload itself would fail Connection's own length check, so the approval screen would never
  even appear (silently swallowed by `PendingMcpClientApprovalListener`'s catch-and-ignore on a
  malformed payload).
- `client_name` — required, non-empty, by Connection's `pending_mcp_client` decoder, and the only
  thing an admin sees on the approval screen to judge what they're allowing — is submitted by the
  AI assistant only once, in the `/register` body. It never reappears in the `/authorize` query
  string (`AuthorizationRequest` has no such field, `mcp/server/auth/handlers/authorize.py:24-41`).
  `register_client()` being a no-op today means this value is thrown away before `authorize()`
  ever needs it.

## Required Behavior

### `/oauth/clients/validate` contract (Connection, verified against `origin/master`)

`POST {oauth_server_url}/oauth/clients/validate`, body `{"client_id": str, "redirect_uri": str}`
(exact fields only — extra keys 400). Unauthenticated, IP-rate-limited.

| Response | Meaning |
|---|---|
| `200` | Exact `(client_id, redirect_uri)` pair is registered and the client is **active** |
| `404` | Unknown client_id, mismatched redirect_uri, **or** a deactivated client (Connection deliberately makes "deactivated" indistinguishable from "never registered" here) |
| `400` | Malformed body |
| `429` | Rate limit exceeded (`Retry-After` / `X-RateLimit-*` headers) |

### Redirect-URI shape Connection will ever register

`https://<any host>`, `cursor://<anysphere.cursor-retrieval\|anysphere.cursor-mcp>`, or
`http://<localhost\|127.0.0.1>` (RFC 8252 loopback) — no userinfo, no fragment, no control/bidi/
zero-width characters, ≤2048 chars (`PendingMcpClientDecoder`). Connection alone decides what gets
registered; the MCP server locally mirrors this shape in `validate_redirect_uri()` (see Decisions
§8) only to bound its own synchronous SDK hook, not to duplicate Connection's authority — an
invalid shape just never becomes registered there either, and 404s like any other unknown pair.

### Pre-registered clients (Flow A)

Only `claude-ai` → `https://claude.ai/api/mcp/auth_callback` today
(`PreRegisterClaudeAiOAuthClientMigration20260909170658`, public PKCE client, no secret). ChatGPT
and Make.com are deliberately not pre-registered (RFC 9207 dependency / unconfirmed app name — see
the Connection migration's own docblock). They, and everything else previously in
`_ALLOWED_DOMAINS` (n8n/groupondev hosts, Agnes, librechat, devin.ai, onyx.app, Azure APIM), fall
through to Flow B on first connect after this ships (see Decisions §5 / rollout).

### Dynamic approval (Flow B)

An unregistered `(client_id, redirect_uri)` becomes registered when an authenticated Keboola user
visits `{oauth_server_url}/oauth/authorize?...&pending_mcp_client=<base64url-json>` and clicks
Allow. Gated by `PendingMcpClientApprovalListener`, which only fires when:
- the route is Connection's own `oauth2_authorize` (**not** `/oauth/consent`, which this server
  currently hits directly and would never trigger the gate — see Decisions §4),
- the query's `client_id` equals the payload's `client_id`,
- that `client_id` is not registered yet.

Payload: `{"client_id": str, "client_name": str, "redirect_uri": str}`, base64url JSON, unsigned —
"Connection decodes and validates this payload but does not verify a signature. Security is
provided by the user explicitly choosing to allow or deny, having already authenticated."
(Connection's own doc.) `client_id` ≤32 chars, `client_name` non-empty ≤128 chars, both reject
control/bidi/zero-width characters; `redirect_uri` as above.

**Deny renders an internal Connection page with no redirect back at all** — deliberate open-redirect
protection. The MCP server gets no callback for a denied/never-approved client; whatever waits on
its own `/oauth/callback` must time out on its own (already true today — nothing new to build).

## Resolution Strategy

All changes are in `src/keboola_mcp_server/oauth.py` unless noted. No new Postgres table for the registry
(`config.oauth_server_url` already resolves to Connection's base URL and is already threaded into
`SimpleOAuthProvider`). Two deployment-level `Config` fields were added during review, neither settable from a
request header: `oauth_validate_rate_limit` (Decision §11) and `oauth_dynamic_client_approval` (Decision §12).

### 1. Delete the static table

Remove `_RE_LOCALHOST`, `_ALLOWED_DOMAINS`, and the now-unused `import re`.

### 2. `_OAuthClientInformationFull.validate_redirect_uri` — cheap sanity only

This SDK hook runs **synchronously**, before `authorize()` (`AuthorizationHandler.handle`,
`authorize.py:179` calls it directly, not awaited) — so it cannot itself call Connection. Keep it
to what a sync check can responsibly do: reject a missing `redirect_uri`, reject userinfo/fragment
and an oversized URI, and bound the *shape* to what Connection could ever register -- loopback-only
`http://` (RFC 8252), a fixed `cursor://` host allowlist, or `https://` with any host (Connection
decides that part) -- which also rejects a handful of scripting schemes (`javascript`, `data`,
`vbscript`) as a side effect of not matching any allowed scheme. Move the real trust decision (is
this *specific* client_id + redirect_uri actually registered) to `authorize()` (Decision §8 covers
why the full shape check landed here rather than staying minimal, and Decision §2 explains the
sync/async split itself).

### 3. Persist just enough client metadata to survive `/register` → `/authorize`

Add an in-process (not persisted to Postgres — see Decisions §3), size-bounded LRU-ish dict on
`SimpleOAuthProvider`: `client_id -> client_name`, written in `register_client()`, read in
`authorize()` to fill the `pending_mcp_client.client_name` field. `get_client()` is unchanged — it
still fabricates a client for any `client_id`, it just no longer needs to invent a name (the cache
does that at `authorize()` time, not `get_client()` time — `get_client()` never sees `redirect_uri`
so it can't build a `pending_mcp_client` payload anyway).

### 4. `_connection_client_id(redirect_uri, key) -> str` — map to Connection's identity space, not the SDK's

```python
_WELL_KNOWN_CONNECTION_CLIENT_IDS: dict[str, str] = {
    # redirect_uri -> the literal client_id Keboola pre-registered for it in Connection's
    # oauth2_client table. Not a trust decision -- only picks which row to ask about;
    # /oauth/clients/validate is still the sole authority on whether it's actually registered.
    'https://claude.ai/api/mcp/auth_callback': 'claude-ai',
}

def _connection_client_id(redirect_uri: str, key: bytes) -> str:
    if known := _WELL_KNOWN_CONNECTION_CLIENT_IDS.get(redirect_uri):
        return known
    # Connection caps client_id at 32 chars; the SDK mints a 36-char uuid4() per /register call
    # (Problem section). Derive a short, *stable* id from redirect_uri instead of forwarding the
    # SDK's ephemeral one: the same tool reconnecting (same callback URL) lands on the same
    # Connection identity and reuses an earlier approval, even though the SDK gives it a fresh
    # uuid every time it re-registers.
    digest = hmac.new(key, redirect_uri.encode(), hashlib.sha256).hexdigest()[:24]
    return f'mcp-{digest}'
```

This is the id sent to `/oauth/clients/validate`, embedded in `pending_mcp_client.client_id`, and
used as the outer `client_id` query param on the pending-approval redirect (Connection's listener
requires the two to match exactly). It is never shown to, or expected back from, the AI assistant —
purely an internal Connection-facing identity.

The id is an **HMAC** of the redirect_uri, keyed with a sub-key derived from the session encryption key
(`HMAC(session_encryption_key, "keboola-mcp-server/connection-client-id")`), not a plain hash: Connection accepts
an unsigned `pending_mcp_client` payload from any logged-in user at its public `/oauth/authorize`, so an id anyone
could compute would let a user approve a pair directly in Connection, skipping `OAUTH_DYNAMIC_CLIENT_APPROVAL`,
after which `check_registration()` would answer REGISTERED. The session encryption key is already required (the
server refuses to start without it) and already shared by every replica, so no new setting is needed. The id
reaches a browser only through the approval redirect, which exists only while the switch is on; the well-known
pairs keep their literal ids because a Connection migration, not a user, registers them. Rotating the session
encryption key changes every derived id, so clients approved under the old one need approving again. A client
pre-registered by a migration cannot use a derived id (the migration cannot compute it): add its pair to
`_WELL_KNOWN_CONNECTION_CLIENT_IDS` with the migration's literal identifier.

A loopback client's redirect_uri carries an ephemeral port that changes on every run (RFC 8252 §7.3). For
`127.0.0.1` and `[::1]` the port is therefore stripped before the id is derived, mirroring Connection's own
redirect-URI matching, which ignores the port for exactly those two hosts: the same tool reconnecting on a new port
lands on the same derived id and reuses its earlier approval instead of needing a fresh human approval and leaving
a never-cleaned-up registration behind on every run. `localhost` is deliberately not in that set: Connection
matches it port-exactly, so a derived id that ignored its port could not be matched back. Deriving from
`redirect_uri` (not `client_id`) is also what makes a *stable-port* tool's approval survive it re-registering with
a fresh SDK-minted uuid.

**Accepted: loopback clients share one registration per host and path.** Because the port is not part of the id, two
distinct local applications whose callbacks differ only by port (`http://127.0.0.1:54321/callback` and
`http://127.0.0.1:9999/callback`) derive the same id: once one is approved, the other is REGISTERED too, without a
separate approval. This follows from RFC 8252 §7.3 (a loopback callback's port is not part of its identity), and
Connection's own registry matches these two hosts without the port. A port-sensitive id here would not close it
either: the pair would simply be approved per port, and the SDK's client id cannot stand in for a stable identity
because it is a fresh uuid on every `/register`. Keeping the port would make every reconnect of a loopback tool
(Claude Code, VS Code, Codex, MCP Inspector, ...) a new approval and leave a permanent stack-wide registration row
per run, which the pre-registered and approve-in-a-window paths cannot absorb. What bounds it: a registered pair
only lets the callback proceed to Connection's login and consent for the user who is signing in, so the session is
that user's own and no credential goes to a callback the user did not go through consent for. The residual risk is
a local process that can bind a loopback port presenting itself as an already-approved loopback client of the
same path, which already requires code running on the user's machine.

### 5. `authorize()` — the real trust decision

```python
async def authorize(self, client, params) -> str:
    redirect_uri_str = str(params.redirect_uri)
    connection_client_id = self._connection_client_id(redirect_uri_str)

    status = await self._check_client_registration(connection_client_id, redirect_uri_str)
    if status is _ClientRegistration.ERROR:
        # NOT `raise AuthorizeError(...)` -- the mcp SDK's own error_response() would redirect
        # that to the *caller-supplied* redirect_uri, which validate_redirect_uri above now
        # accepts for any https host. Returning a same-origin redirect to this server's own
        # /oauth/callback instead is what actually ships (Decision §9); the caller gets no
        # callback for this attempt and must retry.
        return construct_redirect_uri(
            self._mcp_callback_url,
            error='temporarily_unavailable',
            error_description='Could not verify OAuth client with Connection.',
        )

    ... existing state/JWT construction, unchanged ...

    if status is _ClientRegistration.NOT_REGISTERED:
        client_name = self._pending_client_names.get(client.client_id) or connection_client_id
        pending_payload = base64.urlsafe_b64encode(json.dumps({
            'client_id': connection_client_id,
            'client_name': _sanitize_for_connection(client_name)[:128],
            'redirect_uri': redirect_uri_str,
        }, separators=(',', ':')).encode()).decode('ascii')
        return construct_redirect_uri(
            self._oauth_authorize_url,           # {server_url}/oauth/authorize, NOT /oauth/consent
            client_id=connection_client_id,
            redirect_uri=redirect_uri_str,
            response_type='code',
            code_challenge=secrets.token_urlsafe(32),   # throwaway PKCE -- see Decisions §6
            code_challenge_method='S256',
            pending_mcp_client=pending_payload,
        )

    # status is REGISTERED: still the MCP server's own fixed oauth_client_id/secret, and every
    # registered client (pre-registered or dynamically approved) gets the same scope and target --
    # see Decisions §10 and §14.
    return construct_redirect_uri(self._oauth_server_auth_url, **{**url_params, 'scope': _CONNECTION_SCOPE})
```

`_check_client_registration()` POSTs to `{oauth_server_url}/oauth/clients/validate` with a short,
independent timeout (`connect=3s, read=5s` — this blocks a live browser redirect; Connection's own
doc frames the endpoint as a fast pre-check and it's rate-limited, so a tight budget is appropriate),
mapping `200 -> REGISTERED`, `404 -> NOT_REGISTERED`, anything else (network error, 429, 5xx,
unexpected body) `-> ERROR`. **Fail closed**: `ERROR` never falls through to "registered" or
silently to "not registered + send them into a doomed approval loop" — it's a distinct branch that
tells the caller plainly that verification failed, rather than either widening trust or wasting a
round trip on an approval screen that's going to fail the same way.

### 6. Deletions

`_WELL_KNOWN_DOMAINS`/`_FORBIDDEN_SCHEMES`-equivalent (`_ALLOWED_DOMAINS`/`_RE_LOCALHOST`) gone
per Connection's own "Deliverables / MCP Server (Phase 2)" list. No other deletions — the code exchange
leg still uses this server's own fixed `oauth_client_id`/`secret` throughout (see Decisions §2 for
why that's correct, not an oversight), and every registered client targets `/oauth/consent` with
`claudai projectless` (Decisions §10, §14).

## Scope

**In scope:** `oauth.py` — client-registration validation, the pending-approval redirect, the
client-id/name mapping problem, tests.

**Out of scope:**
- The code-exchange leg itself (`/oauth/token`, session persistence, project-scope auto-confirm) —
  unaffected; this server's own Connection-facing identity (`config.oauth_client_id`/
  `oauth_client_secret`) doesn't change. (The *authorize* leg's target URL does change based on
  scope — see Decision §14 — but that's a routing fix, not a new exchange mechanism.)
- Hardening `/register` itself (e.g. requiring an RFC 7591 §3.1 Initial Access Token). `register_client()`
  staying a no-op is intentional, not a gap this RFC leaves open — see Decisions §7.
- A user-facing "revoke this MCP client" UI — that's Connection's account-settings surface
  (explicitly flagged as future work in Connection's own doc), not this server's.
- Pre-registering ChatGPT/Make.com/n8n/etc. on the Connection side — a Connection-repo change
  (a registration on the Connection side, like Claude.ai's), tracked
  separately; this RFC only makes the MCP server correctly *use* whatever is or isn't registered.

## Testing / Verification

**Unit** (`tests/test_oauth.py`, extend `TestSimpleOAuthProvider` — new parametrize cases, not new
functions, per project convention):
- `_connection_client_id`: known redirect_uri → literal `claude-ai`; two calls with the same
  arbitrary redirect_uri → identical derived id; two different redirect_uris → different ids;
  derived id always ≤32 chars; a different key, or the old unkeyed hash, gives a different id.
- A pair approved in Connection out of band under the old public id is still refused while the switch is off.
- `authorize()`: 200 from validate for any REGISTERED `connection_client_id` — pre-registered
  (well-known, e.g. `claude-ai`) or dynamically approved — → the `/oauth/consent` URL with
  `scope=claudai projectless` (one parametrized test, Decisions §10 and §14); 404 → redirect targets
  `/oauth/authorize` (not `/oauth/consent`) with `pending_mcp_client` decodable back to
  `{client_id, client_name, redirect_uri}` matching what was sent, plus a `code_challenge`;
  network error / non-200/404 status from validate → a same-origin redirect to this server's own
  `/oauth/callback?error=temporarily_unavailable&...` (**not** a raised `AuthorizeError` — see
  Decision §9 for why raising it would itself be an open redirect), which `server.py`'s
  `oauth_callback_handler` renders as a 400 HTML page (a person's browser lands here, not the MCP
  client — see Decision §9) without ever calling `handle_oauth_callback()`. Never a redirect to the
  caller-supplied `redirect_uri` for this case. The caller-supplied `error_description` is never
  rendered into that page (or logged) — only a fixed message is (Vojtěch Biberle + Devin review,
  content spoofing on this server's own trusted origin).
- `register_client()` → `authorize()`: `client_name` submitted at `/register` shows up in the
  `pending_mcp_client` payload, sanitized and length-capped at storage time (not just at read
  time — an unauthenticated `/register` caller must not be able to inflate this cache's memory via
  an arbitrarily long name); a client that skipped `/register` (or whose name aged out of the
  cache, or sanitized down to empty) falls back to the derived Connection client_id as its name
  rather than crashing or sending an empty string (which Connection's decoder would reject
  outright).
- `validate_redirect_uri`: still rejects `None`, dangerous schemes, userinfo/fragment, an
  oversized URI, a non-loopback `http://`, and an unlisted `cursor://` host; no longer rejects an
  arbitrary `https://` host (that's Connection's job now).
- `oauth_callback_handler` (`server.py`): a route-level test asserting `GET /oauth/callback?error=...`
  returns a 400 HTML page without invoking `handle_oauth_callback()` at all — this is the
  regression test for the open-redirect fix itself, not just a unit test of `authorize()`'s return
  value. A second test pins that a caller-supplied `error_description` never appears in that page
  at all, escaped or not — not just an XSS-escaping check, since unescaped-but-absent is the actual
  requirement (content spoofing, not injection).
- `_SlidingWindowRateLimiter` (`TestSlidingWindowRateLimiter`): allows exactly `max_calls` within
  the window then refuses the next one; recovers once the oldest call falls outside the window.
  `check_registration()`: a caller varying `(client_id, redirect_uri)` on every request (so every
  check misses the cache) still gets refused, locally, without an HTTP call, once the budget is
  spent — the scenario the registration cache alone cannot stop. Left as documented, deliberately
  deferred prose (no Linear ticket filed this round): a fleet-wide/Redis-backed limiter is the real
  fix, since this only bounds one process, not the fleet (Vojtěch Biberle review, AI-2883).
- `_connection_client_id`: two loopback URIs (127.0.0.1 or [::1]) differing only by port derive the
  *same* id (RFC 8252 §7.3 — a loopback tool's ephemeral port changes on every reconnect; without
  this every reconnect needed a fresh human approval and left behind a permanent, never-cleaned-up
  Connection registry row); the same case for `localhost` still derives different ids — a
  documented residual, since the pinned mcp SDK's own league OAuth library doesn't ignore the port
  for `localhost` either, so normalizing it here alone wouldn't help end-to-end (Vojtěch Biberle
  review, AI-2883; matching fix on Connection's own registry in a companion connection PR).
- `exchange_authorization_code` → `_auto_confirm_project_scope`: the existing single/multi-project
  cases above use a `claudai projectless` token; a further case covers a session persisted as
  `claudai`-only (`oauth_projectless=False`, the shape a Flow B session had before Decision §10) and
  asserts it auto-confirms identically for its one pinned project. Verified against Connection's own
  `TokenIntrospectProcessor`: a pinned session's introspect response is always exactly its own frozen
  allow-list, never the admin's broader membership. The live canary-orion click-through for a freshly
  registered client (register → Allow → retry → `/oauth/consent` → token exchange → a real tool call over
  the whole-stack session) was run manually and passed (2026-10-05); it is not automated here.

**Pre-release verification (to run on the next dev stack release; not automated here).** Which
redirect targets are trusted moves from a hardcoded list to Connection's registry, so these scenarios
check that the replacement is not weaker. Results will be recorded here and in the PR before release:

1. Replay the AI-3792 report on dev against `main` and against this PR. Pass: no authorization code
   reaches the reported domain (this PR answers with Connection's approval screen instead of a 400).
2. Redirect sweep on `/authorize`, `/mcp/authorize`, `/register`, `/token` and `/.well-known`, with
   probes for missing PKCE, backslash, userinfo, encoded host, IDN, trailing dot and double slash.
   Fail on any 3xx whose host is not Connection or the MCP host, and on any 200 that echoes an
   attacker URI. Also run Connection-down and rate-limited cases; they must fail closed with no
   redirect to the caller.
3. Callbacks registered through Connection's approval step: check that a callback registered by one
   user cannot be used to obtain another user's session, including with a PKCE challenge supplied by
   the registering user. Record the outcome and whether Connection's approval gating (AI-3936) is
   needed before release.
4. Approval gating: once AI-3936 or an equivalent is deployed, a minimum-role user must be unable to
   approve a new client.
5. PKCE and scope: S256 required and `plain` rejected; check what a stolen session can do, and that
   revoking it and deactivating a client both take effect within the 5-minute cache TTL (a definite "not registered" always wins; the only exception is the claude.ai pair while Connection errors, see Decision §11).
6. CI regression: a test over the real `create_server` and CLI composition that fails if any route can
   3xx to an untrusted host.

**Integration** (`integtests/`, against a real Connection instance, no project lock needed — this
call is unauthenticated and doesn't touch a project): `ConnectionClientRegistry.check_registration()`
against the real, live `/oauth/clients/validate` — the pre-registered `claude-ai` pair maps to
`REGISTERED`, an arbitrary never-registered pair maps to `NOT_REGISTERED`. This is deliberately
narrower than a full Allow/Deny click-through — Connection's own live-stack E2E suite
already covers that interactive path from Connection's side; what was missing, and what Copilot's review flagged, is proof that *this
server's own code* interprets Connection's real response codes correctly, not just a mocked one.

**Manual** — one full run against a real dev stack per client type: (1) `claude-ai` (Flow A,
zero-friction), (2) a fresh synthetic MCP client hitting `/register` then `/authorize` (Flow B,
confirm the approval screen renders the right name/URI, Allow completes the flow end-to-end,
immediate retry hits Flow A), (3) Deny (confirm the MCP client's own timeout fires, no crash), (4)
Connection intentionally unreachable (confirm `temporarily_unavailable`, not a hang or a false
approval).

## Decisions

1. **The `AI-2883-dynamic-oauth-client-registration` branch / PR #453 draft is not reused.** It
   predates Connection's merged contract on every material point: it expects `/oauth/clients/validate`
   to return a JSON body with `status: approved|pending|rejected` (real endpoint returns a bare
   200/404), sends `client_name`/`client_uri` in the validate request body (real endpoint's DTO has
   `allowExtraFields: false` — would 400), and still targets `/oauth/consent` for the pending-approval
   redirect (real gate only fires on the `oauth2_authorize` route, so that draft's Flow B would
   never actually trigger Connection's approval screen). Its fail-closed error handling was sound
   and is carried forward; the request/response shapes are not.

2. **The code-exchange leg keeps using this server's own fixed `oauth_client_id`/`oauth_client_secret`
   — it does not switch to per-AI-assistant Connection identities for the token exchange.**
   Considered switching entirely to the AI assistant's own Connection client_id end-to-end (so
   Connection would redirect directly to the AI assistant, bypassing this server's callback). Rejected
   as a needless redesign: this server never needed to make that switch, since the `/oauth/clients/validate`
   check and the `pending_mcp_client` redirect work perfectly well as an **admission check** — "is
   this client allowed to keep using this server as its proxy" — layered in front of the existing,
   unchanged broker flow. **Correction (post-review):** an earlier draft of this decision justified
   the split by claiming a code issued directly to the AI assistant "would be useless to it" since
   it doesn't know Connection's token endpoint. Adversarial review (see the security-review addendum
   below) proved that claim false — Connection's endpoints are public/discoverable, and a well-
   resourced attacker controlling `redirect_uri` could trivially find and call them. The actual,
   verified reason this doesn't matter is Decision §6 below (the code is unredeemable regardless of
   who receives it, because of PKCE, not because of endpoint obscurity) — not this paragraph's
   original (wrong) reasoning. Left uncorrected in the surrounding prose as a record of what was
   originally believed; do not cite this paragraph's "useless to Claude.ai" claim as a security
   argument anywhere else.

3. **On a 404, the pending-approval round trip through `/oauth/authorize` is expected to end with a
   real, live Connection authorization code landing at whatever `redirect_uri` the caller supplied.**
   This is **not** an inert side effect — adversarial review confirmed the Allow click is a genuine
   OAuth grant on the approving admin's account, delivered to a destination the admin has no
   independent way to verify beyond what this server put on the approval screen (see the
   security-review addendum). It is unredeemable **only** because of Decision §6 (PKCE), which is
   why that decision is marked mandatory, not defensive. The practical UX is still "approve once,
   then retry the connection in the AI tool" — consistent with Connection's own "Deployment Order"
   guidance to monitor `/oauth/clients/validate` 404s post-cutover — but the security reasoning
   this paragraph originally gave (the code being merely "unused") was incomplete; see Decision §6.

4. **Client-metadata cache is in-process, not Postgres-backed, and that's an accepted tradeoff, not
   an oversight.** `client_name` is a purely cosmetic field on the approval screen — Connection's
   real security boundary is the exact `redirect_uri` match plus an authenticated human clicking
   Allow, neither of which depends on this cache. Considered a Postgres table (mirroring
   `session_store`) for cross-replica/restart durability; rejected as disproportionate to a
   display-only field — a multi-replica deployment where `/register` and the first `/authorize` land
   on different pods just shows the derived Connection client_id as the name instead of the DCR-supplied
   one, which is a cosmetic miss, not a security or functional gap (the row still gets created
   correctly on Allow either way).

5. **No fallback path to `_ALLOWED_DOMAINS` during rollout.** The user confirmed Connection's side
   is merged and released on all stacks. Keeping two trust models running simultaneously (even
   temporarily) is exactly the "inconsistent security model" this change removes — a single, clean
   cutover is safer than a flag-guarded dual path here. Operational consequence: every previously-whitelisted
   integration that Connection hasn't separately pre-registered (ChatGPT, Make.com, all the
   n8n/groupondev hosts, Agnes, librechat, devin.ai, onyx.app, Azure APIM) will show its users a
   one-time Connection approval screen on first connect after this ships, where today it connected
   silently. This is flagged for whoever reviews/deploys this RFC to decide whether any of those need
   proactive Connection-side pre-registration before cutover instead of relying on first-use approval.

6. **The pending-approval redirect always sends a fresh, random `code_challenge` (`secrets.token_urlsafe(32)`)
   — this is mandatory, not defensive, and is the entire reason Decision §3's live authorization
   code is unredeemable.** Verified against Connection's actual config and source
   (Connection requires a code challenge for public clients and rejects a token request with a
   missing or wrong verifier) — a dynamically-approved client is always public (no secret), so this
   is always enforced for it. The random value is presented *as if* it were `SHA-256(code_verifier)`;
   since it is not actually derived from anything, no verifier can exist for it, and redeeming the
   code would require a SHA-256 preimage. **Do not remove this parameter, and do not ever replace it
   with a value derived from anything this server or a caller could recompute** (e.g. a hash of
   `redirect_uri` or `connection_client_id`) — that would make the code redeemable by the exact
   party this control needs to keep it from. No `code_verifier` is ever generated or stored, by
   design: nothing in this server's own flow ever needs to redeem this code (Decision §3).

7. **`register_client()` stays a no-op with respect to trust (it only writes to the cosmetic name
   cache from Decision §4) — this is not the gap AI-3792's suggested remediation #1 (require an
   Initial Access Token) is asking to close.** The actual authorization boundary moved to
   `authorize()`'s Connection-backed check; an attacker can still call `/register` freely and get a
   client_id back, exactly as before, but that id now grants nothing until either it happens to
   collide with something already registered (cryptographically implausible given Decision §4's
   derivation) or a real, authenticated Keboola user explicitly approves its specific redirect_uri.
   Gating `/register` itself with an Initial Access Token remains available as independent future
   hardening but isn't required to close AI-3792/AI-3591.

8. **`validate_redirect_uri` (the sync SDK hook) enforces a redirect_uri *shape* allowlist
   (https any host / cursor only the two Anysphere hosts / http loopback-only, no userinfo, no fragment, ≤2048 chars)
   instead of accepting anything but three dangerous schemes.** Added after adversarial review found
   a real open redirect this RFC's first draft introduced (see the security-review addendum below):
   this hook's output is what the mcp SDK's own error-response fallback would redirect to if
   `authorize()` ever raised. The shape allowlist is not a reintroduction of the old per-domain
   trust list — an unknown `https://` host still passes and is still decided by Connection — it only
   bounds what a redirect_uri can *look like*, closing off `file://`, `intent://`, `mailto:`,
   unrecognized custom schemes, and userinfo/fragment tricks that Connection could never register
   anyway and that have no legitimate use here.

9. **`authorize()`'s `ERROR` branch returns a same-origin redirect to this server's own
   `/oauth/callback` (now handling an `error=` query param) instead of raising `AuthorizeError`.**
   Raising it would let the mcp SDK's own `error_response()` 302 to the *caller-supplied*
   `redirect_uri` with the error params — and since Decision §8 still accepts any `https://` host
   there (correctly — Connection is the real authority on hosts, not this server), that redirect
   target is attacker-controlled. An attacker who can force a `check_registration()` ERROR (trivially,
   via Decision §11's DoS, or any real Connection outage) could turn this server's own `/authorize`
   into an on-demand open redirect (CWE-601) to any host. Redirecting to this server's own callback
   endpoint instead keeps the browser on this server's origin; the caller that started this attempt
   gets no callback at all and must time out and retry — the same shape as Connection's own
   "Deny gets no callback" behavior, not a new UX pattern.

10. **Every registered client — pre-registered or dynamically approved — gets `claudai projectless`
    (reverses an earlier draft).** An earlier draft withheld `projectless` from a Flow B client, on the
    grounds that Connection's approval step deliberately omits it from a self-service
    approval (any authenticated user, no elevated role required — Decision §12) and that this server's
    own broker identity (`self._oauth_client_id`, which the broker leg always authenticates as — Decision
    §2) is what Connection checks for `projectless`, not the dynamically-approved client. That is
    `AuthorizationRequestResolveListener` / `OAuthScopes::mayGrantUnrestrictedScope()` in
    `keboola/connection` `master`: `projectless` is granted when it is in both the request scope and the
    requesting client's own registration, and the requesting client is the broker. The broker already has
    it (it is how every client got it before this registry), so requesting it for every registered client
    preserves the pre-registry behaviour: no new Connection change, and no per-client project picker for
    clients such as Cursor, ChatGPT or n8n. **Trade-off, accepted:** the trust root for a client changed
    from a reviewed redirect-URI list to one Allow click by any authenticated user (Decision §12), and an
    approved client now receives an unrestricted, every-project grant for that user. Closing that needs
    Connection to gate the approval screen; a narrower alternative is to grant `projectless` only to
    pre-registered clients (Connection migrations plus `_WELL_KNOWN_CONNECTION_CLIENT_IDS`) and leave
    unknown clients project-scoped.

    **Ship decision (2026-10-02): go, with logging as the interim control.** Flow B clients keep
    `projectless` rather than being held to `claudai` until Connection gates approval. Narrowing the
    grant per registration stays possible later at the Connection level and is not needed now.
    - Every registered client sent to consent is logged at INFO: `[authorize] Registered client
      proceeding to consent` with `client_id`, the sanitized `redirect_uri`, `pre_registered` and
      `scope`. The derived `connection_client_id` is deliberately not logged: it is the secret that keeps a user
      from approving a pair out of band (§4), and the callback plus the MCP client id are enough to correlate. A `pre_registered=False` line is a dynamically approved callback
      receiving a whole-stack session request.
    - Connection exposes no listing of registered clients (`POST /oauth/clients/validate` is
      unauthenticated and answers one pair at a time), so the log is the only record, and it lives
      only as long as log retention. An inspection endpoint in Connection is a separate follow-up.

11. **`ConnectionClientRegistry.check_registration()` caches REGISTERED for 5 minutes; NOT_REGISTERED
    and ERROR are never cached** (an earlier draft also cached NOT_REGISTERED briefly, but that
    contradicts the "retry immediately after Allow" UX and buys no real protection — see below — so
    it was dropped; a cached ERROR would prolong a real outage instead of retrying it, so that's
    never cached either). `/authorize` is unauthenticated, and every registration check not served
    from cache costs one call to Connection's `/oauth/clients/validate`, which is itself
    IP-rate-limited (600 calls/60s) — and
    this server's entire egress IP shares that budget across every user of the stack.

    **The cache alone does not stop this** (Copilot review finding): a caller that varies
    `redirect_uri` on every request produces a fresh, never-cached key each time, so an anonymous
    flood of `/authorize` with a different redirect_uri per request bypasses the cache entirely and
    could still exhaust Connection's shared budget — turning fail-closed (correct) into a stack-wide
    OAuth login outage triggered by anyone (and, before Decision §9, arming the open redirect too).
    Closing this needed a control that doesn't depend on the request being one the cache has seen
    before: `ConnectionClientRegistry` now also holds a `_SlidingWindowRateLimiter` (plain
    `collections.deque` of call timestamps, no new dependency) capping outbound calls to
    `/oauth/clients/validate` at a configurable budget **per process** (default 100/60s,
    `OAUTH_VALIDATE_RATE_LIMIT`). Connection's ceiling (600/60s) is per egress IP and shared by every
    replica, so the bound that matters is the fleet's: the per-process budget times the largest replica
    count must stay below it. The default is sized for up to 5 replicas (100 x 5 = 500 < 600), a test
    asserts that arithmetic, and a deployment with more replicas must lower the setting. Review round 3
    found that the previous 300/process default let two replicas use up the whole upstream allowance.
    `_check_registration_uncached` checks this budget *before* making the HTTP call at all, returning
    `ERROR` locally (no network call, still fail-closed) once it's spent. It is still a per-process,
    best-effort bound, not a perfectly fair cross-replica one — a limiter shared across the fleet would
    remove the replica arithmetic; tracked as AI-4005, not blocking this PR.

    Two things around the limiter. Concurrent cache misses for the same `redirect_uri` share one in-flight
    call (single-flight), so a burst cannot fan out past it. And because Connection's own 429 can still
    hit the pre-registered claude.ai pair (it is exempt from the local limiter), that pair alone is served
    from its last known REGISTERED answer for up to an hour when Connection cannot answer (an error or a 429,
    never a definite "not registered"; so an error is the one case that is not fail-closed, and only for this pair, and a deactivation seen by a definite 404 is never undone by it), and after such a failure Connection is not asked again for that pair for
    15 seconds, so a failing Connection is not called once per request, and the same pause follows a definite "not registered" for that pair (a missing, deactivated or not-yet-migrated pre-registration), so repeated requests cannot cost Connection one call each either; the pause is per well-known pair, hence bounded, and the answer during it is "not registered"; no other pair is ever answered from memory. The validate calls share
    one pooled HTTP client, closed in the server's lifespan teardown.

    **Known trade-off (security-scanner finding, accepted):** the limiter's budget is global per
    process, not partitioned by caller. `/authorize` is unauthenticated, so one caller sending ~5
    req/s with a fresh `(client_id, redirect_uri)` pair each time (cheap, no coordination with
    Connection needed) can keep the local deque permanently at capacity, making every *other*
    concurrent caller on that same replica see `temporarily_unavailable` too, for as long as the
    flood continues. This is still a strict improvement over having no local limiter at all: the
    blast radius shrinks from stack-wide (exhausting Connection's real, shared 600/60s-per-IP
    ceiling, which every other replica and every other user depends on) to single-replica, and it
    remains fail-closed the entire time for every pair but claude.ai (no bypass, no false REGISTERED; claude.ai's one-hour stale answer is the documented exception in Decision §11). Properly fixing the
    unpartitioned-budget gap needs the same missing ingredient Decision §11's parent paragraph
    already deferred for a different reason -- `authorize()` has no access to the caller's IP (the
    mcp SDK's `authorize(client, params)` interface doesn't pass the request through), so a per-IP
    sub-limit needs a new ASGI middleware ahead of the route (like the existing
    `DatabaseUnavailableMiddleware`), not a change inside `check_registration()`. Tracked as AI-4005
    (fleet-wide, per-caller limiter) alongside the cross-replica-fairness gap above, not blocking this PR.

12. **Who may approve a new client is decided by Connection, and no role restriction is deployed
    there yet.** Connection's dynamic-approval step does not require an elevated role, and once a
    client is approved `check_registration()` returns REGISTERED for every user on that stack (the
    approval is not scoped to the approver). Connection's gap is not new and is not changed by this PR;
    what this PR changes is that it becomes load-bearing for MCP logins. Before, adding a trusted
    redirect target required a reviewed change to this repo; now Connection's approval is the MCP
    server's trust root, so an approved callback receives the consent of every later user of that
    client, as a project-less (whole-stack) session. This is not fixable from this repo, which is why
    approval is **off by default** (see the deployment switch below) until Connection has an elevated or
    per-user approval boundary. Decision §10 means a registered client is not limited to a single project,
    so the approval gate matters for every registered client.

    **Status (2026-09-16): still open.** A project-admin gate on approval was drafted (AI-3936,
    `keboola/connection#8497`), but that PR is closed and unmerged, so no such gate is deployed.
    Restricting who may approve a new client is Connection's gate (AI-3936); whether and how to
    narrow the grant per registration is tracked in AI-4007. Verification before release is tracked
    in AI-4008 (see "Pre-release verification" above).

    **Deployment switch.** `OAUTH_DYNAMIC_CLIENT_APPROVAL` decides whether a client Connection does not
    know yet is sent to its approval screen. It is **off unless set to `true`**: such a client is refused
    on a short page of this server's own origin, and only clients already registered with Connection can log
    in; a registered client still gets `claudai projectless` either way. Pre-registering a client in
    Connection (a migration, as for claude.ai) is the way to onboard a known client without turning it on.
    Turn it on only for a short, supervised window (for example on a dev stack, to verify the approval
    flow), with someone who knows the correct redirect URI doing the approving, because for that window any
    authenticated user of the stack can register a callback. It is deployment-level configuration and is
    never read from a request header. Turning it off again stops *new* approvals only: a client approved
    earlier stays registered in Connection, and this server cannot list or deactivate those (there is no
    listing endpoint), so cleaning them up is a Connection-side step. The INFO line "Registered client
    proceeding to consent" shows which callbacks are in use.

    **The switch is a rollout gate for this server's own approval redirect, not a barrier against Connection
    itself.** A user can still send Connection an approval payload directly, so what keeps such an approval from
    registering a pair *for this server* is that the Connection client id is keyed with a secret only this
    deployment holds (§4): a pair approved under any id a user can compute is not the id this server asks about.
    What a user can still do is approve a pair under an id of their own choosing in Connection, which does not
    affect this server, and Connection's own gap (above) is unchanged.

    **An id handed out during an approval window outlives the window (accepted).** While the switch is on,
    the redirect to Connection's approval screen carries the keyed client id, and anyone who requests `/authorize`
    can read it. Connection's `pending_mcp_client` payload is unsigned and has no expiry, so an id harvested in the
    window can still be sent to Connection after the switch is turned off, and that approves the pair. Turning the
    switch off therefore stops this server from handing out ids and from sending anyone to the approval screen; it
    does not retract ids already handed out. This adds nothing the window did not already allow (during it any
    authenticated user can approve any callback), it only lets that ability outlast it for the URIs someone
    asked about. Only a signed, expiring approval verified by Connection closes it, which is a Connection-side
    change (AI-4020, together with the role gate of AI-3936); this server cannot rotate the id per window without
    losing every earlier approval, because the id is also the registered client's stable identity. Until then the
    mitigations are operational: keep the switch off except for a short, supervised window, treat what the
    window exposed as approved, and change the session encryption key (which re-derives every id) if a window
    must be revoked, at the cost of approving the legitimate dynamic clients again.

    **Rollout: clients the hardcoded list used to accept — a release gate.** The list this PR removes accepted clients by domain
    (besides claude.ai: ChatGPT, Make, Devin, Onyx, n8n instances, Azure API Management's consent host, a few
    customer-specific hosts, and Keboola's own domains). Connection matches a full redirect URI, not a domain, so
    after this ships each of those that is still in use is refused unless it is **registered in Connection first**
    (a pre-registration migration, as for claude.ai, once its exact redirect URI is known, plus an entry in `_WELL_KNOWN_CONNECTION_CLIENT_IDS` here so this server asks about the migration's literal identifier) or approved in a short,
    supervised window with the switch above turned on. Neither happens by itself: with approval off by default,
    the first sign of a missed client is a user seeing the "not registered" page. Before enabling this on a
    stack, list the callbacks actually in use there (the INFO line above, from the previous release) and register
    the ones that matter. ChatGPT cannot be pre-registered as a constant: its callback is per connector.
    This inventory and the pre-registration plan are an explicit **gate for releasing this change to production**: do not
    release it before the callbacks in use have been listed and each one that matters is registered (or its approval
    window is scheduled).

13. **The REGISTERED cache's 5-minute TTL is also a revocation-latency window (Copilot review
    finding, accepted).** Connection's contract deliberately maps a deactivated client to the same
    404 as "never registered" (see the Contract table above) -- but for up to 5 minutes after
    deactivation, a cached REGISTERED result still lets `authorize()` admit new logins for that
    client. This is a real, previously-undiscussed latency, not a hypothetical one; it's accepted
    rather than fixed here because there is currently no way to *trigger* a deactivation at all --
    the "revoke this MCP client" UI is explicitly out of scope for this RFC (see Scope, above) and
    doesn't exist on Connection yet. When that UI ships, this tradeoff needs revisiting (either an
    invalidation hook the revoke action can call, or shortening the TTL) -- building that now, for a
    revocation path that cannot yet be exercised, would be speculative. Tracked as a follow-up.

14. **Every registered client — including a dynamically-approved (Flow B) one — targets
    `/oauth/consent` with `claudai projectless` (DMD-2180).**
    *Superseded history:* a first fix for the loop described below routed a non-`projectless` request to
    Connection's `/oauth/authorize` instead of `/oauth/consent`, because at the time `/oauth/consent`'s
    Approve action (`ConsentSubmissionAction`) never set the `oauth_selected_project_id` session key the
    plain project-selector branch needs, so a Flow B session looped between `/oauth/consent` and
    `/oauth/project-selector`. That split is gone: Connection `master` now lets `/oauth/consent` resolve a
    non-`projectless` MCP request too (`fa9563ba7b`, "let /oauth/consent resolve a non-projectless MCP
    request too"; entry is gated on the client's own `claudai` scope, not the request's `projectless`), and
    Decision §10 grants `projectless` to every registered client anyway. `_authorize()` therefore always
    targets `_oauth_server_auth_url` (`/oauth/consent`) with `_CONNECTION_SCOPE` (`claudai projectless`).
    Verified manually end to end on canary-orion: register → Allow (Flow B, unredeemable code) → retry
    `/authorize` (Flow A, `/oauth/consent`) → token exchange → a tool call over the whole-stack session.

## Security Review Addendum

Post-implementation, this RFC was reviewed by two independent adversarial passes (one general OWASP-
style audit, one threat-model specifically targeting the dynamic-registration/domain-takeover
questions raised during design) plus a correctness/fail-open review, all cross-checked against a real
checkout of `keboola/connection` at `origin/master` rather than assumed. Confirmed findings and their
resolutions:

| Finding | Severity | Resolution |
|---|---|---|
| `authorize()`'s `ERROR` branch raised `AuthorizeError`, which the mcp SDK redirects to the caller-supplied (now host-unrestricted) `redirect_uri` — an open redirect reachable on demand via the rate-limit DoS below | High | Fixed — Decisions §8, §9 |
| Unauthenticated `/authorize` amplifies 1:1 into Connection's IP-shared `/oauth/clients/validate` rate limit — a stack-wide OAuth login DoS, and the enabler for the open redirect above | High | Fixed — Decision §11 (registration cache + a local per-process rate limiter; a caller varying `redirect_uri` per request defeats the cache alone, so the limiter is what actually bounds it) |
| `check_registration()` used `follow_redirects=True`; any 200 at the end of a redirect chain (misconfigured proxy, login page, ...) would read as REGISTERED | Medium | Fixed (`follow_redirects=False` on this call) |
| A dynamically-approved client inherits the same unrestricted `projectless` grant Connection withholds from self-service approvals | Medium-High | **Accepted, reversed** — Decision §10: every registered client gets `projectless` to preserve pre-registry behaviour; the exposure is Decision §12's, now for every client |
| `client_name` sanitizing to `''` (all control/zero-width chars) silently dropped the whole approval payload instead of falling back to the derived id | Low | Fixed (sanitize before, not after, the fallback) |
| RFC Decisions §2/§3/§6 justified the throwaway-PKCE code's safety with an incorrect claim ("the AI assistant doesn't know Connection's endpoints") | Low (documentation) | Corrected — Decisions §2, §3, §6 |
| `except httpx.HTTPError` didn't cover `httpx.InvalidURL`; a misconfigured `server_url` degraded to the mcp SDK's generic error instead of this code's own specific warning (fail-closed either way, via the SDK's own catch-all) | Low (debuggability only) | Fixed (broadened the except) |
| A user approving the pair directly in Connection under the id this server derived (a plain hash anyone could compute), skipping the approval switch | High | Fixed — the id is an HMAC under a key derived from the session encryption key (§4, Decision §12) |
| `_connection_client_id`'s 96-bit truncated hash, `claude-ai` impersonation via the literal redirect_uri string, and approval-reuse by an unrelated party presenting the same redirect_uri | — | Reviewed, confirmed **not exploitable** — Connection matches the exact pair, and whoever "reuses" an approval must still control the redirect_uri to receive anything from it |
| Only a mocked `check_registration()` was tested; the real Connection response contract (200/404 mapping) was unverified | Medium | Fixed — a live-Connection integration test (`integtests/`, see Testing/Verification) now exercises the real endpoint; the interactive Allow/Deny click-through remains covered by Connection's own E2E suite, not duplicated here |
| Any authenticated user (no elevated role) can register a stack-global trusted client via Connection's approval screen | High | **Not fixable here** — flagged, Decision §12. The drafted Connection-side fix (AI-3936 / `keboola/connection#8497`) is currently closed, unmerged — still open in production, tracked on the Linear issue for a Connection-side owner |
| A 200 from Connection was trusted purely on status code; a misconfigured intermediary answering 200 at the same URL (health check, SSO page, WAF challenge) without ever reaching Connection would read as REGISTERED | Medium | Fixed — the body must also equal Connection's real, documented `{}` empty-JSON contract, or it's treated as `ERROR` |
| `validate_redirect_uri`'s userinfo/fragment checks used truthiness; an empty-but-present component (`https://evil.example/cb#` → `fragment=''`) parses as falsy and slipped through | Low-Medium | Fixed (`is not None`, not truthiness) |
| `register_client()`'s debug log interpolated the raw, unauthenticated `client_name` directly — unbounded length and control characters bypassed the sanitize-at-insertion protection for this one log line | Low | Fixed (logs the already-sanitized stored value instead) |
| Invalid `/authorize` requests (e.g. missing `code_challenge`) made the mcp SDK's `error_response()` redirect to the caller-supplied `https` `redirect_uri` before `authorize()` ran — an unauthenticated open redirect | High | Fixed — `UntrustedAuthorizeRedirectMiddleware` allows `/authorize` to redirect only to Connection or this server. It guards both the mounted `/mcp` app (`get_middleware()`) and the outer app's root OAuth routes (`CustomRoutes.add_to_starlette`); a first version covered only the former (Vojtěch Biberle review), pinned by `test_add_to_starlette_guards_root_authorize_route` |
| The REGISTERED cache's 5-minute TTL is also a revocation-latency window — a deactivated client stays admitted for up to 5 minutes | Low (no live trigger yet — Connection has no revoke UI) | Accepted, documented — Decision §13; revisit when Connection ships a revoke path |
| A dynamically-approved (Flow B) client's authorize request targeted `/oauth/consent` while omitting `projectless`, which Connection's consent flow did not resolve at the time — the session looped between `/oauth/consent` and `/oauth/project-selector`; found via manual end-to-end testing against a real dev stack | High (feature-breaking, not exploitable) | Superseded — Decision §14 (DMD-2180): every registered client now gets `projectless` and `/oauth/consent`, so the split this finding led to was removed |
| Two RFC "Resolution Strategy" sections (§2's sync-hook description, §5's `authorize()` code sketch) described an earlier, narrower design (minimal scheme rejection only; `raise AuthorizeError`) that the final Decisions (§8, §9) superseded, making the RFC internally contradictory | Low (documentation) | Fixed — both sections rewritten to match the shipped behavior |
