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

The workspace is a regular workspace created under the `keboola.mcp-server-tool`
configuration and is rediscovered by listing workspaces and matching that component —
no branch-metadata pointer is written anymore.

Connection validates the JWT and, when the ServiceAccount is authorized for workspace
provisioning, waives the permissions the user's token lacks —
workspace provisioning works even for read-only project members. **No privileged token
is ever minted**; every action runs under, and is audited to, the user's own token.
Everything else (queries, reads, tools) is untouched.

Key properties:

- The token file is read per provisioning flow — kubelet rotation is picked up
  automatically (no caching across flows).
- `KBC_KUBERNETES_TOKEN_PATH` is read from the process environment only; it cannot be
  set or overridden via HTTP headers or per-request config.
- A missing or empty token file fails loudly — the step-up header is never silently
  dropped.
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
