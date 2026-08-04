# Kubernetes ServiceAccount Authentication (Deployed MCP Server)

The MCP server deployed in Keboola infrastructure can attach its projected Kubernetes
ServiceAccount JWT as a **step-up credential** to Connection Storage API requests
(`X-Kubernetes-Authorization: Bearer <jwt>`).

## How it works

When the `KBC_KUBERNETES_TOKEN_PATH` environment variable points at the projected
ServiceAccount token (mounted into the deployed MCP server), workspace provisioning
requests keep the **user's own Storage token** and additionally carry the SA JWT as
the `X-Kubernetes-Authorization` header:

- creating the billing configuration (`keboola.mcp-server-tool`),
- creating the workspace itself.

The workspace is a regular workspace created under a configuration of the
`keboola.mcp-server-tool` component, and is rediscovered by listing that component's
configurations and fetching each config's workspaces through the config-scoped
workspaces endpoint — no branch-metadata pointer is written anymore.

Connection validates the JWT and, when the ServiceAccount is authorized for workspace
provisioning, waives the permissions the user's token lacks —
workspace provisioning works even for read-only project members. **No privileged token
is ever minted**; every action runs under, and is audited to, the user's own token.
Everything else (queries, reads, tools) is untouched.

Key properties:

- The token file is read when the step-up provisioning client is first built, once per
  session (`WorkspaceManager` lifetime). Provisioning happens at most once per session,
  well within the projected token's rotation window; a new session re-reads the file, so
  kubelet rotation is picked up without restarting the server.
- `KBC_KUBERNETES_TOKEN_PATH` is read from the process environment only; it cannot be
  set or overridden via HTTP headers or per-request config. That alone is not enough,
  because the Storage API URL of a session *can* come from an HTTP header — so the
  destination is checked as well (see the next point).
- The SA JWT is only ever sent to the stack this server belongs to. Before the header is
  attached, the session's Storage API host is compared — exact host match, no prefix or
  pattern matching, with the scheme's default port normalized away — against the server's
  own Storage API URL. If they differ, or if the server has no stack of its own, the
  step-up is skipped and the user's own Storage client is used instead.
- The server's own stack is resolved **once**, when the server starts, from its own
  configuration: the `--api-url` command-line parameter if given, otherwise
  `KBC_STORAGE_API_URL`, otherwise `HOSTNAME_SUFFIX` (the input the Keboola Helm charts
  set). That single value — `ServerState.own_stack_storage_api_url` — is then passed to
  every check, so the step-up check and the per-request URL pinning below cannot disagree
  about which stack is ours. A per-request HTTP header can never influence it.
- Consistently, a per-request Storage API URL (`X-Storage-Api-Url`) pointing at a
  different host than the server's own stack is not honoured: the server keeps its own
  Storage API URL for the request and logs a warning. Servers with no stack of their own
  (locally run servers, stdio transport) are unaffected.
- A missing or empty token file fails loudly — the step-up header is never silently
  dropped when the destination is this server's own stack.
- When the env variable is not set (any locally-run server), the behavior is exactly
  as before — provisioning uses the user's token alone.
- Requires the Connection-side step-up support; until that support covers all
  provisioning endpoints, the workspace-credentials endpoint is the first one
  honoring the header.

## Drawbacks / Limitations

- **Locally-run MCP servers have no projected ServiceAccount token**, so the
  step-up provisioning path only works on the MCP server deployed in Keboola
  infrastructure. A user running the MCP server locally (stdio/custom deployment)
  must connect **at least once to the deployed (remote) MCP server**, so that their
  workspace is created there (with proper billing configuration and credentials).
  After that, the local MCP server works for querying: its `query_data` tool sends
  queries to the in-infrastructure query-service, which attaches its own step-up
  header when obtaining workspace credentials.
- Read-only users who only ever use a locally-run MCP server and never connect to
  the deployed one cannot get a workspace created — their own token lacks the
  required permissions, and there is no ServiceAccount token to step up with.
- For read-only users the step-up is intentionally limited to workspace provisioning
  and storage event emission. Any other write operation still requires adequate
  permissions on the user's own token and keeps failing for a read-only user.
