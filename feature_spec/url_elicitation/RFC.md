# RFC: Open "unlock" pages via URL-mode elicitation

Linear: [AI-4018](https://linear.app/keboola/issue/AI-4018/mcp-open-data-streams-unlock-page-via-url-mode-elicitation) (follow-up to [AI-4015](../data_streams/RFC.md))

## Problem

When Data Streams aren't enabled, `get_streams`/`create_stream` fail with a link to the project's Data Streams
page. The agent has to repeat the link and the user has to click it. MCP 2025-11-25 added URL-mode elicitation:
a server can ask the client to open a URL, and the client shows its own consent prompt first.

## Required Behavior

| Client | Single-project call without the feature | Multi-project fan-out |
|---|---|---|
| Declares `capabilities.elicitation.url` | JSON-RPC error `-32042` (`URLElicitationRequiredError`), one URL elicitation: the Data Streams page plus a message | Per-project text note with the link (unchanged) |
| Doesn't declare it | `isError` result with the message and the link (unchanged) | Same |

Clients advertise the capability at initialize or, on protocol 2026-07-28, per request. Both are read through
`ctx.session.client_capabilities`, so this also works with the stateless HTTP transport.

## Resolution Strategy

- `elicitation.py`:
  - `UrlActionRequiredError(ToolError)` carries `user_message` and `url`. Its text already includes the link, so
    every non-elicitation path behaves as before.
  - `UrlElicitationMiddleware` catches it and, for capable clients, re-raises `UrlElicitationRequiredError` with a
    fresh `elicitationId`.
- Why a middleware: fastmcp masks every `MCPError` raised inside a tool body into an `isError` result, except the
  missing-capability code. That would drop `-32042` and its payload. Middleware runs outside that masking.
- Registered just outside `MultiProjectMiddleware`. Single-project calls propagate the error unchanged. Fan-out
  keeps collecting per-project failures as text notes, so a URL elicitation can't wipe out the other projects'
  results.
- `tools/streams.py` raises `UrlActionRequiredError` for the missing `data-streams` feature.
- No `notifications/elicitation/complete`: the server can't tell when the feature request is handled.

## Scope

In scope: the middleware, the error type, and the Data Streams unlock.

Out of scope: other "go to the UI" flows (they can raise `UrlActionRequiredError` later), and completion notifications.

## Testing / Verification

- Unit: `tests/test_elicitation.py` runs a real in-memory FastMCP server and client. A capable client gets
  `-32042` with the URL and message. A plain client, or any other tool error, gets an unchanged `isError` result.
- E2E against Azure NE projects 721 (feature off) and 4905 (on), over stdio and stateless streamable-HTTP, with
  capable and plain clients (see PR).
