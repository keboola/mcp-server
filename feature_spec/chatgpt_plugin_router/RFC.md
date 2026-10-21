# RFC: Global MCP router for the ChatGPT plugin

Linear: [DMD-2479](https://linear.app/keboola/issue/DMD-2479/add-router-oauth-endpoints-and-stack-picker-to-mcp-server), [DMD-2480](https://linear.app/keboola/issue/DMD-2480/add-the-streaming-mcp-proxy-to-router-mode), [DMD-2481](https://linear.app/keboola/issue/DMD-2481/accept-router-originated-flows-on-regional-servers), [DMD-2482](https://linear.app/keboola/issue/DMD-2482/serve-the-directory-tool-profile-to-router-sessions) (parent [DMD-2463](https://linear.app/keboola/issue/DMD-2463/keboola-as-a-chatgpt-plugin-rfc))

Full design: [RFC: Keboola as a ChatGPT Plugin](https://github.com/keboola/connection/blob/vb/no-ticket/chatgpt_plugin_rfc/docs/rfc/chatgpt-plugin/chatgpt-plugin.md) in keboola/connection ([PR #9019](https://github.com/keboola/connection/pull/9019)), part 4 "Global router" and tasks 3.1-3.4. This document is the mcp-server summary of that design; the Connection RFC stays the source of truth for the reasoning, the OpenAI evidence and the rollout.

**Gate still open.** The Connection RFC makes written confirmation from OpenAI, that a stack picker before the Keboola login is acceptable for the public Plugin Directory, the gate for this work (its Open Questions). The router is built before that answer, by decision of the RFC owner; nothing routes to it until the deployment task (3.5) and the public submission (4.3). If OpenAI says no, the RFC falls back to one plugin per stack and the router is not deployed.

## Problem

An OpenAI plugin has exactly one MCP server URL, and that URL cannot change after the plugin is published. Keboola runs one hosted MCP server per stack (`https://mcp.<stack>/mcp`, derived from `HOSTNAME_SUFFIX`), each of them its own OAuth authorization server brokering to its own stack's Connection. A single plugin URL can therefore serve the users of one stack only, and EU customers (three of the five multi-tenant stacks) could never use a listing pinned to US AWS.

## Required Behavior

A new run mode of the same image, `--mode=router`, serves the plugin's URL and sends each user to their own stack:

| Endpoint | Behavior |
|---|---|
| `/.well-known/oauth-protected-resource` | `resource` = router `/mcp`, `authorization_servers` = [router issuer] |
| `/.well-known/oauth-authorization-server` | Router issuer and endpoints, `S256`, `authorization_response_iss_parameter_supported: true` |
| `/register` | Answered locally, like the regional server: a fresh `client_id`, nothing stored |
| `/authorize` | The stack picker; on submit, 302 to the chosen stack's `/authorize` with every original parameter, `stack` dropped and `via=router` added. `stack=<key>` from the table skips the picker |
| `/token`, `/revoke` | Route by the stack key prefix of `code`, `refresh_token` or `token` (`eu-aws.<value>`), body forwarded unchanged. Unknown prefix: `/token` answers `invalid_grant`, `/revoke` answers 200 (RFC 7009), no upstream call |
| `/mcp` (3.2) | Reverse proxy by the prefix of the bearer token, streamed; 401 with `resource_metadata` without a known prefix |
| `/.well-known/openai-apps-challenge` | `OPENAI_APPS_CHALLENGE` as text, 404 when unset |

The router holds no Keboola credentials, no database and no session state, and never calls Connection. Its logs carry metadata only (method, path, status, stack key, latency), never a query string, a header or a body.

Regional servers (3.3) treat `/authorize` with `via=router` and `resource` = router URL as a router flow: same validation as today, `iss` = router issuer, the stack key prefixed on the code, access and refresh tokens they issue, prefixed tokens accepted at their `/token`, `/revoke` and `/mcp`, and the session bound to the router resource. Every other request is unchanged. Sessions bound to the router resource get the directory tool profile (3.4): an explicit allow-list without `run_sync_action`, `create_project` and `create_python_js_data_app_git_credential`, applied to `tools/list` on every path and to tool calls.

## Resolution Strategy

- Regional servers do not serve `/revoke` today (revocation is not enabled in their OAuth provider); 3.3 enables it together with the prefixed tokens, which are the only tokens the router forwards there.
- `src/keboola_mcp_server/router/` holds the mode: `stacks.py` (the stack table, the prefix, and `router_issuer()`/`router_resource()`, the one function pair the router and the regional servers both use to derive the router issuer and resource from the router URL, so the two sides cannot disagree), `settings.py` (environment only), `picker.py`, `rate_limit.py` and `app.py` (the Starlette app). `cli.py` adds `--mode`.
- One logical authorization server: the router serves the metadata, `/authorize` (the picker) and `/token`; the regional server that completes the login mints codes and tokens with `iss` = router issuer from the start, so the router never rewrites a response.
- The stack table defaults to the five multi-tenant stacks and can be replaced by `KBC_ROUTER_STACKS` (a JSON list), so a dev stack upstream and the one-by-one production rollout are configuration changes. Upstreams are validated as https origins; a redirect or a forwarded request only ever targets one of them.
- The picker is a server-rendered `GET` form back to `/authorize` whose only visible input is the stack key; every original query parameter rides along as a hidden input, HTML-escaped (a `GET` form drops the query string of its action), and nothing else from the request is rendered. CSP `default-src 'self'; form-action 'self' <stack upstreams>; frame-ancestors 'none'; base-uri 'none'; object-src 'none'` (`form-action` lists the upstreams because browsers apply it to the redirect after the submit, which goes to the chosen stack), `Referrer-Policy: no-referrer` (the page URL carries `state`), no script, the stylesheet served by the router. The choice is a 90-day first-party cookie used only to preselect. No CSRF token: the only state the form changes is the submitting browser's own cookie.
- Rate limits (Connection RFC, Security Considerations "Abuse / DoS") are process-local (no database), in one-minute windows, and keyed by who calls. `/authorize` is a person's browser: per client address, 60 per minute by default (`KBC_ROUTER_AUTHORIZE_RATE_LIMIT_PER_IP`). `/register`, `/token`, `/revoke` and `/mcp` come server to server from the AI client's few shared egress addresses, where a per-address limit sized for one user would refuse every plugin user at once, so `/token` and `/revoke` are limited per credential, by the SHA-256 of the presented code or refresh token (or token), 10 per minute by default (`KBC_ROUTER_TOKEN_RATE_LIMIT_PER_CREDENTIAL`); `/mcp` per hashed bearer token (3.2). `/register` has only an optional per-address ceiling, `KBC_ROUTER_API_RATE_LIMIT_PER_IP`, sized from observed traffic by the deployment (3.5); the router warns at startup while it is unset. No credential reaches the limiter's memory or the logs, only its hash (logged shortened).
- Bodies of `/register`, `/token` and `/revoke` are capped at 64 KiB (413), repeated `grant_type`, `code`, `refresh_token` or `token` parameters are refused (`invalid_request`, so the router never routes by one value while the stack reads another), and Starlette's trailing-slash redirect is off, because it builds its `Location` from the request's `Host` header.
- Uvicorn's access log is off in router mode because it would log the `/authorize` query string (`state`, the PKCE challenge).
- `/token` and `/revoke` forward `Content-Type`, `Accept` and `Authorization` (client authentication) with the verbatim body, and never `X-Forwarded-For`: the regional server sees the router's address, and a forwarded header would let a caller choose the address it counts.

## Scope

In scope: router mode (3.1, 3.2), the regional changes for router flows (3.3), the directory tool profile (3.4).

Out of scope: the deployment, hostname, DNS, TLS and kbc-stacks values (3.5), monitoring (3.6), the plugin package and its URL switch (Phase 1, 4.3), single-tenant stacks, CIMD, multi-region hosting of the router.

## Testing / Verification

- Unit tests per task (`tests/router/`, `tests/test_oauth.py`, `tests/test_mcp.py`): metadata values and issuer equality, picker escaping and CSP, redirect parameters and table-only upstreams, prefix routing and the local answers for unknown prefixes, logs without headers or bodies, rate limits, streamed `/mcp` responses, upstream timeout 504, both regional branches, the directory profile on every `tools/list` path.
- End to end (3.5, needs the deployment): a ChatGPT custom connector on the router URL against a dev stack: picker, consent, tool call, refresh, revoke; ingress logs and traces without `Authorization`, tokens or bodies.
