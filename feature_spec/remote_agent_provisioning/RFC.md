# RFC: Agent provisioning on the remote MCP server

Linear: _(to be filed — follow-up to [DMD-1939](https://linear.app/keboola/issue/DMD-1939/agent-provisioning-support-in-keboolamcp-server), under [DMD-1801](https://linear.app/keboola/issue/DMD-1801/agentic-project-provisioning))_
Predecessor: [`feature_spec/agent_provisioning/RFC.md`](../agent_provisioning/RFC.md) (shipped, 1.83.0)

## Problem

`create_project` works only on a locally run MCP server. The point of the feature is that **the agent
works in the new project straight away** — the provisioning endpoint hands back a live project-pinned
session, and the agent keeps using it until the human claims the project (at which point Connection
revokes it) or the free PAYG credits run out, roughly two hours of job runtime. A user driving an
agent through the **remote** MCP server gets none of that.

The remote server refuses with:

```python
if deployed_sa_token_path():
    raise ValueError('Creating a Keboola project is only supported by a locally run MCP server. ...')
```

The local design stores the provisioned access/refresh tokens in the user's own mode-600 credential
file. Remotely there is no such file — the pod's store is process-wide and shared — so the shipped
increment fenced the tool off rather than ship a credential leak. This RFC removes the fence.

## Verified facts (2026-10-01)

Checked rather than assumed, because two widely held assumptions about why remote "doesn't work" are
wrong:

| Claim | Reality |
| --- | --- |
| "Agentic creation is not switched on, hence the 401s" | The `agent-provisioning` stack feature is **ON** — `POST /manage/programmatic-projects` with an invalid body answers **400** (validation), not 404, on **both** `connection.canary-orion.keboola.dev` and **production** `connection.keboola.com`. The endpoint also answers **unauthenticated** from the public internet, as designed. |
| "Remote MCP returns 401 because the feature is off" | The 401 is the **MCP server's own OAuth gate** (`create_server()` passes `auth=oauth_provider` whenever `oauth_client_id`/`secret` are configured). It is unrelated to the Connection feature, and enabling the feature on a stack does not change it. |
| "Once enabled on the stack it will work remotely too" | No. Calling `create_project` through the **authenticated** canary-orion remote MCP connector reaches the tool and returns `Creating a Keboola project is only supported by a locally run MCP server.` — our guard, not Connection. Nothing was provisioned. Remote needs the work in this RFC. |

So there are two separate remote gaps: an **unauthenticated** caller cannot reach any tool at all, and
an **authenticated** one reaches `create_project` and is refused by us.

## Decisions taken

1. **Do not make the authorization decision in MCP.** A current Keboola user may legitimately want to
   create a project; Connection simply has no support for it yet. The MCP server therefore stops
   deciding who may provision and lets the backend answer. The one refusal that stays is **not** a
   policy judgement: locally the credential store holds one session per (stack, profile), so
   provisioning into a session that already has a stored login would overwrite it. That is a storage
   collision, and the message should say so rather than implying a permission rule.
2. **The provisioned tokens live in the MCP server's session storage**, encrypted with a secret key
   deployed to every pod. Simple by intent; improvable later.
3. **The agent must be able to work before the claim.** That is the feature. The remote server carries
   the tokens on the session so the agent never sees or handles them.
4. **Connection's rate limits stay strict** for now and may be relaxed later. Anonymous job execution
   in a Keboola project is a real abuse surface (crypto mining, among others), so the strict default is
   correct and this RFC does not ask for it to be loosened.

## Required behavior

| # | Behavior |
| --- | --- |
| 1 | `create_project` works on the remote server and returns project id + `confirm_url`, exactly as locally. |
| 2 | Subsequent tool calls in that conversation work against the new project without the agent handling any credential. |
| 3 | The provisioned access and refresh tokens never appear in a tool result, a log, or the model context. At rest they are encrypted. |
| 4 | The access token is refreshed server-side before it expires (1 h), from the 30 d refresh token. |
| 5 | When the human claims the project, Connection revokes the session; the next call reports that plainly and points the user at signing in with their own account — no stack trace, no silent 401 loop. |
| 6 | A caller who already has working credentials is not blocked by MCP policy; only the local single-slot store collision is refused, with a message that says exactly that. |
| 7 | The anonymous entry path is off unless enabled for that deployment, and never enabled for the In Platform Agent (`mcp-server-agent`). |

## Resolution strategy

### Where the credentials live

Reuse what the OAuth flow already has — no new storage concept:

* `SessionStore.create(client_id, user_email, kbc_access_token, kbc_refresh_token, kbc_access_expires_at)`
  stores exactly the triple a provisioned session consists of and returns an opaque access token.
* Credentials are encrypted at rest with **AES-256-GCM** under `KBC_SESSION_ENCRYPTION_KEY`
  (`session_store/crypto.py`) — precisely decision 2, already built and already deployed per pod.
* `rotate_kbc_tokens()` writes back the rotated pair after a refresh; `revoke()` ends the session.

So provisioning becomes: call the endpoint → `SessionStore.create(...)` → return project id and
`confirm_url` to the model, and the opaque handle to the **transport**, never to the model as a
credential.

### How the session is recognised on the next request

This is the one genuinely new problem, and the obvious answer does not work: the server runs
stateless-HTTP (a fresh, empty `ctx.session.state` per request, any replica), and the MCP
2026-07-28 RC **drops `Mcp-Session-Id` and session pinning from the spec entirely** (see the note on
`SCOPE_TOKEN_ARG` in `scope.py`). There is no transport-level session to hang this on.

The codebase already solved the same problem once, for project scope: `scope_token`, an opaque
AES-256-GCM value the caller echoes back on every tool call — and it already carries a live bearer
credential (`scoped_token`) with AAD binding. Follow that precedent, with one improvement: what comes
back is **not** a credential but the session row's opaque handle, so the transcript holds a revocable
pointer and the Keboola tokens stay in encrypted Postgres.

Mechanically this is the existing `scope_token` plumbing extended to carry the session handle, which
means the agent's part is unchanged from what it already does today: echo one opaque argument. On each
request `SessionStateMiddleware` resolves the handle to the session row, decrypts the credentials,
refreshes them if near expiry (`rotate_kbc_tokens`), and builds the session state exactly as an OAuth
session does. Requirement 3 holds: the model sees an opaque handle, never `kbc_at_*`/`kbc_rt_*`.

### Letting an unauthenticated caller in

Needed only for the newcomer with no Keboola account — the case the feature exists for. Two candidate
mechanisms, to be chosen against fastmcp 4.0.3 (now pinned):

* **(a) Optional auth + per-tool gate.** A request with no credentials yields an "anonymous" principal
  instead of a 401; a middleware rejects every tool except `create_project` for such a session. One
  URL, same endpoint everyone else uses. Touches the auth path, so it needs the careful review.
* **(b) A second, unauthenticated FastMCP app** on a distinct path exposing only `create_project`. No
  auth internals touched, but the newcomer must add a second server URL and then switch.

Recommend (a), (b) as fallback. Gate it on a deployment setting (`KBC_ALLOW_ANONYMOUS_PROVISIONING`,
default off), enabled in kbc-stacks only for the public `mcp-server` chart, never `mcp-server-agent`.

An **authenticated** remote caller needs none of this: they already reach the tool. Dropping the
`deployed_sa_token_path()` guard (decision 1) is all that is required there, and Connection answers
whether their identity may provision.

### When the human claims the project

Connection revokes the agent session at claim time, so its access token starts 401ing while still
unexpired — the refresh path never self-heals it. The shipped `_handle_unauthorized` already
distinguishes a genuinely dead credential (introspection fails 401/403) from a scope-related 401 and
from a transient failure. The remote branch reuses that verdict and, instead of deleting a file,
calls `SessionStore.revoke()` and surfaces: *the project is now yours — sign in with your own Keboola
account to keep working in it*. That is the natural end of the provisioned session's life, not an
error.

## Rate limiting: a consequence to be aware of, not a request to relax

Connection buckets provisioning per client IP and per `clientId`. Locally those are per end user.
Remotely every call arrives from a handful of MCP pod IPs with an identical `clientId`, so both
buckets collapse into one shared bucket: the Nth newcomer in a window is throttled because strangers
went first. The strict limits are correct and stay; this only means that if/when they are revisited,
remote needs a bucket dimension that is not the pod IP — a forwarded client IP honored from a trusted
caller, or a per-conversation key the endpoint accepts. Until then, remote provisioning throughput is
whatever one shared bucket allows, and users will occasionally be told to retry later.

## Scope

In scope: dropping the deployed-server guard, session-backed credentials for provisioned sessions
(store, resolve, refresh, revoke), the anonymous entry path and its setting, and the claim-time
message.

Out of scope:

* Loosening Connection's rate limits.
* Deciding in MCP who may provision — Connection's answer is authoritative.
* **"The confirm page *is* the OAuth authorize step"**: confirming redirects back to the MCP server's
  `/oauth/callback`, so the user comes out authenticated with no second browser trip. A nicer end
  state, needs connection + kbc-ui work, deserves its own RFC.

## Testing / verification

* Unit: the provisioned session is stored encrypted and resolvable; the handle in the tool result is
  not a Keboola token; near-expiry refresh rotates the row; a revoked session produces the claim
  message and revokes the row; the local store-collision refusal still fires and no longer claims to
  be about deployment.
* Integration over the real middleware chain (as `TestBootstrapServerEndToEnd` does): anonymous
  request → `create_project` → a second request carrying the handle builds a client bound to the new
  project; every other tool stays unreachable anonymously.
* Manual E2E on canary-orion (feature confirmed on): connect with no account, provision, run a data
  tool **before** claiming, then claim in the browser and confirm the session ends with the right
  message.
