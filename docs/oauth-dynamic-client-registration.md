# OAuth Dynamic Client Registration

The MCP server acts as an OAuth 2.0 Authorization Server (RFC 8414 / RFC 7591) for whatever AI
assistant connects to it (Claude.ai, Cursor, n8n, a custom MCP client, ...). Trust in a client's
`(client_id, redirect_uri)` pair comes from Connection's OAuth client registry
(`POST /oauth/clients/validate`), not from a hardcoded domain list. See
[`feature_spec/oauth_dynamic_client_registration/RFC.md`](../feature_spec/oauth_dynamic_client_registration/RFC.md)
for the full design.

## Two flows

- **Flow A (pre-registered / previously approved)** — the `(client_id, redirect_uri)` pair is
  already known to Connection (e.g. Claude.ai, or a client approved once before). `/authorize`
  proceeds straight to Connection's `/oauth/consent` screen — no extra step for the user.
- **Flow B (new / unregistered client)** — only while `OAUTH_DYNAMIC_CLIENT_APPROVAL` is turned on (it is off
  by default, see [Operating it](#operating-it)); otherwise an unregistered pair is refused on this server's own
  `/oauth/callback?error=unregistered_client` page. With it on, `/authorize` redirects instead to Connection's own
  `/oauth/authorize` carrying a `pending_mcp_client` payload. A logged-in Keboola user sees an
  approval screen (client name + redirect URI) and must click **Allow** once; the approval is registered against this server's own `/oauth/callback` (the client's real redirect target is shown in the name), and the browser then continues straight to the consent screen, so the client never receives a code from the approval itself (AI-3995, needs Connection's `return_to_client_after_approval`). After that, the same
  `(client_id, redirect_uri)` pair is Flow A on every subsequent connection.

Every client — regardless of flow — must first call `/register` (RFC 7591) to obtain an MCP-side
`client_id`; this server derives its own short, stable id for talking to Connection from the
client's `redirect_uri` (see `_connection_client_id` in `src/keboola_mcp_server/oauth.py`).

## Operating it

Two deployment-level settings (environment or CLI only, never a request header):

| Setting | Meaning | Default |
|---|---|---|
| `OAUTH_DYNAMIC_CLIENT_APPROVAL` | Whether an unregistered client is sent to Connection's approval screen. Only `true` turns it on; unset, empty and `false` all leave it off. | off |
| `OAUTH_DYNAMIC_CLIENT_APPROVAL_DAILY_LIMIT` | With approval on, how many distinct clients per day (a fixed 24-hour UTC window, counted across every replica in the shared database) may be sent to the approval screen. A positive integer; anything else stops startup. A further new client is refused like an unregistered one and logged at WARNING (`Daily approval budget used up, client refused`); the same client retrying costs nothing. | no cap |
| `OAUTH_VALIDATE_FLEET_RATE_LIMIT` | Calls per minute, counted across every replica in the shared database, to Connection's `/oauth/clients/validate`. A positive integer. | 500 |
| `OAUTH_AUTHORIZE_RATE_LIMIT_PER_IP` | `/authorize` requests per minute one caller address may send, across every replica; over it the answer is `429` with `Retry-After`. A positive integer. | no limit |
| `OAUTH_TRUSTED_PROXY_HOPS` | How many proxies in front of the server append to `X-Forwarded-For`; the caller address is the entry that many places from the end (the ones before it are caller-written and never used). | 1 |
| `OAUTH_VALIDATE_RATE_LIMIT` | Calls per minute one server process may make to Connection's `/oauth/clients/validate`; applies only while the shared database counter cannot answer. A positive integer; anything else stops startup. | 100 |

**Approval window.** Leave `OAUTH_DYNAMIC_CLIENT_APPROVAL` off in normal operation and onboard a known client by
pre-registering it in Connection (a migration, as for claude.ai, plus an entry in
`_WELL_KNOWN_CONNECTION_CLIENT_IDS` in `src/keboola_mcp_server/oauth.py` with the migration's literal identifier).
Turn it on only for a short, supervised window, with someone who knows the right redirect URI doing the approving:
for that window any authenticated user of the stack can register a callback, and every later login of that client
then trusts it and gets a whole-stack session. Turning it off again stops *new* approvals only. Clients approved
earlier stay registered in Connection (this server can neither list nor deactivate them), and the Connection client
id this server handed out during the window stays usable in Connection's approval flow until the session
encryption key changes. The only complete fix for that is a signed, expiring approval verified by Connection, which
is a Connection-side change (AI-4020); see the RFC, Decision 12.

**Leaving approval on.** A stack that keeps `OAUTH_DYNAMIC_CLIENT_APPROVAL=true` should also set
`OAUTH_DYNAMIC_CLIENT_APPROVAL_DAILY_LIMIT` (the Kubernetes chart sets 2), so a registration stays rare: at most that
many new clients a day are sent to the approval screen, and an attempt beyond it is a WARNING line that the Datadog
monitors in `keboola/infrastructure` (`terraform/modules/alerts/mcp-server.tf`) alert on, together with every client
sent to approval and a flood against the validation limit. The cap is counted across replicas in the shared database (per process only while the database cannot be reached) and bounds only what this server hands out: Connection still lets any authenticated user click Allow, and a pair
approved earlier stays registered (AI-4020 is the complete fix).

**Shared limits (AI-4005).** The call budget towards Connection, the daily cap on new clients and the per-address
limit on `/authorize` are counted in one small table (`oauth_rate_buckets`, migration 0006) in the same Postgres as the
sessions, with one atomic upsert per check, so they hold across every replica and need no extra service. If the database
cannot answer (it is down, or the migration has not run yet) the call budget and the daily cap fall back to the
per-process limiters below, and the per-address limit lets the request through, since a database outage fails logins
anyway. `gc-sessions` removes the counters of windows that are over. The per-address limit is off unless
`OAUTH_AUTHORIZE_RATE_LIMIT_PER_IP` is set; set `OAUTH_TRUSTED_PROXY_HOPS` to the number of proxies in front of the server
first, or the limit counts the proxy instead of the caller. Windows are fixed (a caller can send up to twice the limit
across a window boundary).

**Sizing the rate limit.** Without the shared counter the limit is per process, but Connection's ceiling is 600 calls per 60 seconds per
egress IP, shared by every replica. Keep `OAUTH_VALIDATE_RATE_LIMIT` times the number of replicas below 600: the
default of 100 is for up to 5 replicas, so lower it when running more (for example 60 for 9 replicas). Cached
answers, single-flight, and the pause after an error or a "not registered" for the pre-registered claude.ai pair
keep real traffic far below the limit; the limit only matters for a flood of distinct redirect URIs.

**Rolling back.** The client id sent to Connection is derived from the session encryption key, so changing that key
also changes every derived id: dynamically approved clients have to be approved again. Rolling back to a release
with the old hardcoded redirect list does not remove anything from Connection; the registrations stay and are
ignored there.

**Release gate.** The removed hardcoded list accepted ChatGPT, Make, Devin, Onyx, n8n, Azure API Management and
some customer-specific hosts by domain. Connection matches a full redirect URI, so before releasing, list the
callbacks in use on each stack (the INFO line `[authorize] Registered client proceeding to consent` of the previous
release) and pre-register the ones that matter, or schedule an approval window. ChatGPT's callback is per
connector and cannot be pre-registered as a constant.

## Endpoints (on the MCP server, `https://mcp.<stack-hostname>`)

| Purpose | Method | Path |
|---|---|---|
| AS metadata | GET | `/.well-known/oauth-authorization-server` |
| Dynamic client registration | POST | `/register` |
| Authorization | GET | `/authorize` |
| Token exchange | POST | `/token` |
| Internal callback (Connection → this server) | GET | `/oauth/callback` |

## Registering and testing a new dynamic client end-to-end

Requires VPN access to the target stack and a Keboola login on Connection for that stack (to click
**Allow** on the approval screen).

**Loopback clients.** The Connection-facing client id is derived from `redirect_uri`. The port is ignored for
`127.0.0.1` and `[::1]` (RFC 8252 §7.3; Connection matches those two hosts the same way), so a tool that binds a new
ephemeral port on every run keeps one approval. `localhost` is **not** normalised: use a fixed port with it, or a
fresh port looks like a brand-new client and needs approval every time. Two different local apps whose callbacks
differ only by the port of one of the two normalised hosts share one registration (RFC, section 4).

This walkthrough needs `OAUTH_DYNAMIC_CLIENT_APPROVAL=true` on the target stack for the first run (step 4 shows the
refusal page otherwise).

```bash
MCP_HOST=mcp.us-central1.gcp.keboola.dev   # or mcp.east-us-2.azure.keboola-testing.com
REDIRECT_URI=http://localhost:9876/callback

# 1. Sanity check — confirm the AS metadata is served
curl -s "https://$MCP_HOST/.well-known/oauth-authorization-server" | python3 -m json.tool

# 2. Dynamic client registration (RFC 7591)
REG_RESPONSE=$(curl -s -X POST "https://$MCP_HOST/register" \
  -H 'Content-Type: application/json' \
  -d @- <<EOF
{
  "redirect_uris": ["$REDIRECT_URI"],
  "client_name": "dcr-test",
  "grant_types": ["authorization_code", "refresh_token"],
  "response_types": ["code"],
  "token_endpoint_auth_method": "none"
}
EOF
)
echo "$REG_RESPONSE" | python3 -m json.tool
CLIENT_ID=$(echo "$REG_RESPONSE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["client_id"])')

# 3. PKCE (S256) — the authorize call requires a code_challenge
CODE_VERIFIER=$(openssl rand -base64 96 | tr -d '=+/\n' | cut -c1-64)
CODE_CHALLENGE=$(printf '%s' "$CODE_VERIFIER" | openssl dgst -sha256 -binary | openssl base64 -A | tr '+/' '-_' | tr -d '=')
STATE=$(openssl rand -hex 8)

# 4. Kick off /authorize — first call, this pair is NOT registered yet, so the response
#    redirects into Connection's Flow B pending-approval screen (not /oauth/consent).
AUTH_URL="https://$MCP_HOST/authorize?client_id=$CLIENT_ID&response_type=code&redirect_uri=$(python3 -c "import urllib.parse,os;print(urllib.parse.quote(os.environ['REDIRECT_URI'],safe=''))")&code_challenge=$CODE_CHALLENGE&code_challenge_method=S256&state=$STATE"
curl -s -i "$AUTH_URL" | head -20
# Look at the "location:" header of the 302 response:
#   - unregistered pair -> Connection's own /oauth/authorize?...pending_mcp_client=... (Flow B)
#   - already-approved pair -> Connection's /oauth/consent?...                          (Flow A)

# 5. Open that Location URL in a browser, log into Connection, and click Allow.
#    Connection redirects the browser to $REDIRECT_URI?code=...&state=...
#    Catch it with a one-shot local listener:
python3 -c "
import http.server, urllib.parse
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        print('code=', q.get('code'), 'state=', q.get('state'))
        self.send_response(200); self.end_headers(); self.wfile.write(b'OK, check the terminal')
        raise SystemExit
http.server.HTTPServer(('localhost', 9876), H).handle_request()
"
CODE=<paste the code printed above>

# 6. Exchange the authorization code for an access token
curl -s -X POST "https://$MCP_HOST/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode "grant_type=authorization_code" \
  --data-urlencode "code=$CODE" \
  --data-urlencode "redirect_uri=$REDIRECT_URI" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "code_verifier=$CODE_VERIFIER" \
  | tee /tmp/token.json | python3 -m json.tool
ACCESS_TOKEN=$(python3 -c 'import json; print(json.load(open("/tmp/token.json"))["access_token"])')

# 7. Verify the token is actually accepted by the protected MCP endpoint
curl -s -i -X POST "https://$MCP_HOST/mcp" \
  -H "Authorization: Bearer $ACCESS_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"dcr-test","version":"0.0.1"}}}'
# 200 + a real initialize result => the dynamically-registered client now has a working session.
# 401 => something in the chain didn't grant a token that this endpoint accepts.
```

Re-running steps 4–7 with the **same** `REDIRECT_URI` (same client, same fixed port) after step 5's
approval should now show Flow A in step 4's `location:` header (`/oauth/consent`, not
`/oauth/authorize?...pending_mcp_client=...`) and require no further Allow click.

### Negative / edge-case checks worth exercising on the same build

- **Deny** the approval screen instead of Allow — the local listener in step 5 should just time out
  (no callback at all), matching Connection's own "Deny gets no callback" behavior.
- **Point `oauth-server-url` at an unreachable host** (or block egress to Connection) and repeat
  step 4 — expect a redirect to this server's own `/oauth/callback?error=temporarily_unavailable`,
  never a hang and never a silently-approved client.
- Repeat step 2 with a `redirect_uri` scheme outside `https` / loopback `http` / the two allow-listed
  `cursor://` hosts — `/register` should still succeed (registration itself doesn't validate
  against Connection), but `/authorize` should reject it before ever reaching Connection.

## Registering your own AI assistant / MCP client against a Keboola stack

For a real integration (not just this test), an MCP client only needs to speak standard OAuth
dynamic client registration against the server's discovery document — no Keboola-specific code:

1. Discover endpoints via `GET https://mcp.<your-stack>/.well-known/oauth-authorization-server`.
2. `POST /register` with your app's real `redirect_uris` and `client_name`.
3. Run the standard authorization-code + PKCE flow (`/authorize` → `/token`) as above.
4. Your client must already be registered in Connection (pre-registered by Keboola) or approved once via the
   pending approval screen, which exists only while the stack's operators have turned approval on; every later
   connection from the same `redirect_uri` is frictionless (Flow A).

If your client's redirect URI is a loopback URL, `127.0.0.1` and `[::1]` keep their approval across ports and
`localhost` needs a fixed port — see the note above.
