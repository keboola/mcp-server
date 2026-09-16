# RFC: Replace the hardcoded OAuth redirect-URI whitelist with Connection's client registry

Linear: [AI-2883](https://linear.app/keboola/issue/AI-2883/mcp-server-implement-dynamic-oauth-client-registration-via-connection)
Related: [AI-3591](https://linear.app/keboola/issue/AI-3591/mcp-server-bypasses-connection-oauth-client-registration-with) (RISK-76), [AI-3792](https://linear.app/keboola/issue/AI-3792/support-17501-critical-unauthenticated-oauth-dynamic-client) (SUPPORT-17501, critical DCR report), [AI-3797](https://linear.app/keboola/issue/AI-3797/support-17488-request-to-allowlist-aieuromediacz-for-keboola-mcp-oauth) (SUPPORT-17488, allowlist request this replaces the need for)
Connection side (merged, released): [keboola/connection#7016](https://github.com/keboola/connection/pull/7016) — `docs/features/oauth-dynamic-client-registration.md`

Supersedes the stale draft on `AI-2883-dynamic-oauth-client-registration` ([PR #453](https://github.com/keboola/mcp-server/pull/453), still open) and the interim hardening in [#580](https://github.com/keboola/mcp-server/pull/580) (RISK-76 hub-subdomain exclusion) — both predate Connection's final, merged `/oauth/clients/validate` contract and don't match it (see Decisions §1).

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

**Confirmed exploitable pattern (AI-3792 / SUPPORT-17501):** a security researcher registered an
arbitrary client via `/register` with `redirect_uri=https://attacker.evil-test.example/cb` and got
a `201` with no validation at all. The attack chain happened to dead-end at `/authorize` today
because `attacker.evil-test.example` doesn't match `_ALLOWED_DOMAINS` — but the underlying model
(open registration + a static, ever-growing domain whitelist as the only redirect check) is exactly
the "inconsistent trust model" risk RISK-76 flags, and it fails closed only by accident of which
domains happen to be listed, not by design.

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
zero-width characters, ≤2048 chars (`PendingMcpClientDecoder`). The MCP server does not need to
re-implement this — an invalid shape just never becomes registered, and 404s like any other
unknown pair.

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

All changes are in `src/keboola_mcp_server/oauth.py` unless noted. No new Postgres table, no new
`Config` field (`config.oauth_server_url` already resolves to Connection's base URL and is already
threaded into `SimpleOAuthProvider`).

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

### 4. `_connection_client_id(redirect_uri) -> str` — map to Connection's identity space, not the SDK's

```python
_WELL_KNOWN_CONNECTION_CLIENT_IDS: dict[str, str] = {
    # redirect_uri -> the literal client_id Keboola pre-registered for it in Connection's
    # oauth2_client table. Not a trust decision -- only picks which row to ask about;
    # /oauth/clients/validate is still the sole authority on whether it's actually registered.
    'https://claude.ai/api/mcp/auth_callback': 'claude-ai',
}

def _connection_client_id(redirect_uri: str) -> str:
    if known := _WELL_KNOWN_CONNECTION_CLIENT_IDS.get(redirect_uri):
        return known
    # Connection caps client_id at 32 chars; the SDK mints a 36-char uuid4() per /register call
    # (Problem section). Derive a short, *stable* id from redirect_uri instead of forwarding the
    # SDK's ephemeral one: the same tool reconnecting (same callback URL) lands on the same
    # Connection identity and reuses an earlier approval, even though the SDK gives it a fresh
    # uuid every time it re-registers.
    digest = hashlib.sha256(redirect_uri.encode()).hexdigest()[:24]
    return f'mcp-{digest}'
```

This is the id sent to `/oauth/clients/validate`, embedded in `pending_mcp_client.client_id`, and
used as the outer `client_id` query param on the pending-approval redirect (Connection's listener
requires the two to match exactly). It is never shown to, or expected back from, the AI assistant —
purely an internal Connection-facing identity.

A loopback client's redirect_uri carries an ephemeral port, so it gets a new derived id — and needs
re-approval — on every run. That's not a regression this RFC introduces: Connection's own doc
already documents exactly this tradeoff for loopback clients and recommends a fixed port for
one-time approval. Deriving from `redirect_uri` (not `client_id`) is what makes a *stable-port*
tool's approval survive it re-registering with a fresh SDK-minted uuid, which is the main point.

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

    # status is REGISTERED: unchanged today's behavior, still the MCP server's own fixed
    # oauth_client_id/secret against /oauth/consent -- Flow A and an already-approved Flow B
    # client are now identical from here on.
    return construct_redirect_uri(self._oauth_server_auth_url, **url_params)
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
leg (`/oauth/consent` → `/oauth/token` using this server's own fixed `oauth_client_id`/`secret`) is
untouched; see Decisions §2 for why that's correct, not an oversight.

## Scope

**In scope:** `oauth.py` — client-registration validation, the pending-approval redirect, the
client-id/name mapping problem, tests.

**Out of scope:**
- The code-exchange leg (`/oauth/consent`, `/oauth/token`, session persistence, project-scope
  auto-confirm) — entirely unaffected; this server's own Connection-facing identity
  (`config.oauth_client_id`/`oauth_client_secret`) doesn't change.
- Hardening `/register` itself (e.g. requiring an RFC 7591 §3.1 Initial Access Token). `register_client()`
  staying a no-op is intentional, not a gap this RFC leaves open — see Decisions §7.
- A user-facing "revoke this MCP client" UI — that's Connection's account-settings surface
  (explicitly flagged as future work in Connection's own doc), not this server's.
- Pre-registering ChatGPT/Make.com/n8n/etc. on the Connection side — a Connection-repo change
  (`php bin/console league:oauth2-server:create-client` or a migration like Claude.ai's), tracked
  separately; this RFC only makes the MCP server correctly *use* whatever is or isn't registered.

## Testing / Verification

**Unit** (`tests/test_oauth.py`, extend `TestSimpleOAuthProvider` — new parametrize cases, not new
functions, per project convention):
- `_connection_client_id`: known redirect_uri → literal `claude-ai`; two calls with the same
  arbitrary redirect_uri → identical derived id; two different redirect_uris → different ids;
  derived id always ≤32 chars.
- `authorize()`: 200 from validate → today's exact `/oauth/consent` URL (regression check — Flow A
  must be byte-for-byte unchanged for an already-registered client), `projectless` in scope only
  for a well-known (Keboola-vetted) `connection_client_id`; 404 → redirect targets
  `/oauth/authorize` (not `/oauth/consent`) with `pending_mcp_client` decodable back to
  `{client_id, client_name, redirect_uri}` matching what was sent, plus a `code_challenge`;
  network error / non-200/404 status from validate → a same-origin redirect to this server's own
  `/oauth/callback?error=temporarily_unavailable&...` (**not** a raised `AuthorizeError` — see
  Decision §9 for why raising it would itself be an open redirect), which `server.py`'s
  `oauth_callback_handler` renders as a 400 JSON response without ever calling
  `handle_oauth_callback()`. Never a redirect to the caller-supplied `redirect_uri` for this case.
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
  returns 400 JSON without invoking `handle_oauth_callback()` at all — this is the regression test
  for the open-redirect fix itself, not just a unit test of `authorize()`'s return value.
- `_SlidingWindowRateLimiter` (`TestSlidingWindowRateLimiter`): allows exactly `max_calls` within
  the window then refuses the next one; recovers once the oldest call falls outside the window.
  `check_registration()`: a caller varying `(client_id, redirect_uri)` on every request (so every
  check misses the cache) still gets refused, locally, without an HTTP call, once the budget is
  spent — the scenario the registration cache alone cannot stop.

**Integration** (`integtests/`, against a real Connection instance, no project lock needed — this
call is unauthenticated and doesn't touch a project): `ConnectionClientRegistry.check_registration()`
against the real, live `/oauth/clients/validate` — the pre-registered `claude-ai` pair maps to
`REGISTERED`, an arbitrary never-registered pair maps to `NOT_REGISTERED`. This is deliberately
narrower than a full Allow/Deny click-through — Connection's own live-stack E2E suite
(`connection/tests/E2E/Auth/McpClientValidationTest.php`) already covers that interactive path from
Connection's side; what was missing, and what Copilot's review flagged, is proof that *this
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
   temporarily) is exactly the "inconsistent security model" risk RISK-76 named — a single, clean
   cutover is safer than a flag-guarded dual path here. Operational consequence: every previously-whitelisted
   integration that Connection hasn't separately pre-registered (ChatGPT, Make.com, all the
   n8n/groupondev hosts, Agnes, librechat, devin.ai, onyx.app, Azure APIM) will show its users a
   one-time Connection approval screen on first connect after this ships, where today it connected
   silently. This is flagged for whoever reviews/deploys this RFC to decide whether any of those need
   proactive Connection-side pre-registration before cutover instead of relying on first-use approval.

6. **The pending-approval redirect always sends a fresh, random `code_challenge` (`secrets.token_urlsafe(32)`)
   — this is mandatory, not defensive, and is the entire reason Decision §3's live authorization
   code is unredeemable.** Verified against Connection's actual config and source
   (`connection/config/packages/league_oauth2_server.yaml`: `require_code_challenge_for_public_clients: true`;
   `league/oauth2-server`'s `AuthCodeGrant::validateCodeChallenge()` rejects a token request with a
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
   (https any host / cursor any host / http loopback-only, no userinfo, no fragment, ≤2048 chars)
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

10. **A dynamically-approved (Flow B) client's authorize request omits the `projectless` scope —
    only a client in `_WELL_KNOWN_CONNECTION_CLIENT_IDS` (today: `claude-ai`) gets it.** Adversarial
    review found that Connection's `AuthorizationRequestResolveListener` decides whether to grant the
    unrestricted, every-project ("projectless") session by checking whether **this server's own
    fixed Connection identity** (`self._oauth_client_id`, which the broker leg always authenticates
    as — see Decision §2) has `projectless` in its own scopes — not whether the *underlying,
    dynamically-approved* client does. Connection's `ClientApprovalProcessor` deliberately withholds
    `projectless` from a self-service-approved client (any authenticated user, no elevated role
    required — see Decision §12) specifically because such approval isn't vetted the way a reviewed
    Keboola migration is. Without this fix, every dynamically-approved client would silently inherit
    the same unrestricted grant as Claude.ai regardless of Connection's intent — verified by tracing
    `AuthorizationRequestResolveListener::scopeRequested()`/`clientAllowsProjectlessScope()`
    (`connection/src/Core/OAuth/EventListener/AuthorizationRequestResolveListener.php`) against a
    real checkout of `origin/master`, not assumed. Omitting `projectless` routes the user through
    normal per-project consent (`ProjectSelectionAction`) instead, matching what Connection's own
    scopes intended for a self-service client.

11. **`ConnectionClientRegistry.check_registration()` caches REGISTERED for 5 minutes; NOT_REGISTERED
    and ERROR are never cached** (an earlier draft also cached NOT_REGISTERED briefly, but that
    contradicts the "retry immediately after Allow" UX and buys no real protection — see below — so
    it was dropped; a cached ERROR would prolong a real outage instead of retrying it, so that's
    never cached either). `/authorize` is unauthenticated, and every registration check not served
    from cache costs one call to Connection's `/oauth/clients/validate`, which is itself
    IP-rate-limited (600 calls/60s, `connection/config/packages/oauth_client_rate_limit.yaml`) — and
    this server's entire egress IP shares that budget across every user of the stack.

    **The cache alone does not stop this** (Copilot review finding): a caller that varies
    `redirect_uri` on every request produces a fresh, never-cached key each time, so an anonymous
    flood of `/authorize` with a different redirect_uri per request bypasses the cache entirely and
    could still exhaust Connection's shared budget — turning fail-closed (correct) into a stack-wide
    OAuth login outage triggered by anyone (and, before Decision §9, arming the open redirect too).
    Closing this needed a control that doesn't depend on the request being one the cache has seen
    before: `ConnectionClientRegistry` now also holds a `_SlidingWindowRateLimiter` (plain
    `collections.deque` of call timestamps, no new dependency) capping outbound calls to
    `/oauth/clients/validate` at 300/60s **per process** -- half of Connection's per-IP ceiling, so
    even the worst case (every check missing the cache) leaves headroom for other replicas sharing
    the same egress IP. `_check_registration_uncached` checks this budget *before* making the HTTP
    call at all, returning `ERROR` locally (no network call, still fail-closed) once it's spent. This
    is deliberately a single-process, best-effort bound, not a perfectly fair cross-replica one — a
    distributed limiter (e.g. Redis-backed) would be needed to cap the whole fleet's *combined* call
    rate precisely; tracked as a follow-up, not blocking this PR.

    **Known trade-off (security-scanner finding, accepted):** the limiter's budget is global per
    process, not partitioned by caller. `/authorize` is unauthenticated, so one caller sending ~5
    req/s with a fresh `(client_id, redirect_uri)` pair each time (cheap, no coordination with
    Connection needed) can keep the local deque permanently at capacity, making every *other*
    concurrent caller on that same replica see `temporarily_unavailable` too, for as long as the
    flood continues. This is still a strict improvement over having no local limiter at all: the
    blast radius shrinks from stack-wide (exhausting Connection's real, shared 600/60s-per-IP
    ceiling, which every other replica and every other user depends on) to single-replica, and it
    remains fail-closed the entire time (no bypass, no false REGISTERED). Properly fixing the
    unpartitioned-budget gap needs the same missing ingredient Decision §11's parent paragraph
    already deferred for a different reason -- `authorize()` has no access to the caller's IP (the
    mcp SDK's `authorize(client, params)` interface doesn't pass the request through), so a per-IP
    sub-limit needs a new ASGI middleware ahead of the route (like the existing
    `DatabaseUnavailableMiddleware`), not a change inside `check_registration()`. Tracked as a
    follow-up alongside the cross-replica-fairness gap above, not blocking this PR.

12. **Flagged, not fixed in this repo: any authenticated Keboola user — no elevated role required —
    can permanently register a stack-global trusted MCP client via Connection's dynamic-approval
    screen.** Adversarial review traced `ClientApprovalProcessor::process()`
    (`connection/src/Core/OAuth/ClientApprovalProcessor.php`) and found it checks only that
    `$context->admin` exists and a per-admin rate limit — no organization, project, or role
    membership check. Once approved, `check_registration()` returns REGISTERED for **every** user on
    that stack (Decision §11's point about "the same handful of registered clients" cuts both ways —
    the approval is not scoped to the approver). This is a genuine widening versus the old model
    (adding a trusted redirect target used to require a reviewed Keboola-engineering PR, globally;
    now it requires one click by any of potentially thousands of tenants on a shared stack, with no
    review). This is **not fixable from this repo** — the missing check is in Connection's PHP, not
    here — and is called out explicitly rather than left as a silent gap for whoever reviews this RFC
    to decide whether it needs to go back to the Connection team before this ships. Decision §10
    limits the *blast radius* of an unvetted approval (no `projectless`) but does not close this gap.

    **Status (2026-09-16): still open.** A project-admin gate for `ClientApprovalProcessor::process()`
    was drafted (AI-3936, `keboola/connection#8497`), but that PR is closed, unmerged, with no
    successor — so the gate is not deployed anywhere. Flagged on the Linear issue for a Connection-
    side owner to pick up; nothing in `keboola/mcp-server` can close it, and this PR does not make
    the underlying gap worse or better (Connection's dynamic-approval screen is reachable today
    independent of whether this PR merges).

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
| A dynamically-approved client silently inherited the same unrestricted `projectless` grant Connection deliberately withholds from self-service approvals | Medium-High | Fixed — Decision §10 |
| `client_name` sanitizing to `''` (all control/zero-width chars) silently dropped the whole approval payload instead of falling back to the derived id | Low | Fixed (sanitize before, not after, the fallback) |
| RFC Decisions §2/§3/§6 justified the throwaway-PKCE code's safety with an incorrect claim ("the AI assistant doesn't know Connection's endpoints") | Low (documentation) | Corrected — Decisions §2, §3, §6 |
| `except httpx.HTTPError` didn't cover `httpx.InvalidURL`; a misconfigured `server_url` degraded to the mcp SDK's generic error instead of this code's own specific warning (fail-closed either way, via the SDK's own catch-all) | Low (debuggability only) | Fixed (broadened the except) |
| `_connection_client_id`'s 96-bit truncated hash, `claude-ai` impersonation via the literal redirect_uri string, and approval-reuse by an unrelated party presenting the same redirect_uri | — | Reviewed, confirmed **not exploitable** — Connection matches the exact pair, and whoever "reuses" an approval must still control the redirect_uri to receive anything from it |
| Only a mocked `check_registration()` was tested; the real Connection response contract (200/404 mapping) was unverified | Medium | Fixed — a live-Connection integration test (`integtests/`, see Testing/Verification) now exercises the real endpoint; the interactive Allow/Deny click-through remains covered by Connection's own E2E suite, not duplicated here |
| Any authenticated user (no elevated role) can register a stack-global trusted client via Connection's approval screen | High | **Not fixable here** — flagged, Decision §12. The drafted Connection-side fix (AI-3936 / `keboola/connection#8497`) is currently closed, unmerged — still open in production, tracked on the Linear issue for a Connection-side owner |
| A 200 from Connection was trusted purely on status code; a misconfigured intermediary answering 200 at the same URL (health check, SSO page, WAF challenge) without ever reaching Connection would read as REGISTERED | Medium | Fixed — the body must also equal Connection's real, documented `{}` empty-JSON contract, or it's treated as `ERROR` |
| `validate_redirect_uri`'s userinfo/fragment checks used truthiness; an empty-but-present component (`https://evil.example/cb#` → `fragment=''`) parses as falsy and slipped through | Low-Medium | Fixed (`is not None`, not truthiness) |
| `register_client()`'s debug log interpolated the raw, unauthenticated `client_name` directly — unbounded length and control characters bypassed the sanitize-at-insertion protection for this one log line | Low | Fixed (logs the already-sanitized stored value instead) |
| The REGISTERED cache's 5-minute TTL is also a revocation-latency window — a deactivated client stays admitted for up to 5 minutes | Low (no live trigger yet — Connection has no revoke UI) | Accepted, documented — Decision §13; revisit when Connection ships a revoke path |
| Two RFC "Resolution Strategy" sections (§2's sync-hook description, §5's `authorize()` code sketch) described an earlier, narrower design (minimal scheme rejection only; `raise AuthorizeError`) that the final Decisions (§8, §9) superseded, making the RFC internally contradictory | Low (documentation) | Fixed — both sections rewritten to match the shipped behavior |
