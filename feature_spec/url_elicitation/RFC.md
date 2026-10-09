# RFC: Open "unlock" pages via URL-mode elicitation

Linear: [AI-4018](https://linear.app/keboola/issue/AI-4018/mcp-open-data-streams-unlock-page-via-url-mode-elicitation) (follow-up to [AI-4015](../data_streams/RFC.md))

## Problem

When Data Streams aren't enabled, `get_streams`/`create_stream` fail with a link to the project's Data Streams
page. The agent has to repeat the link and the user has to click it. MCP 2025-11-25 added URL-mode elicitation:
a server can ask the client to open a URL, and the client shows its own consent prompt first.

## Required Behavior

A client that declares `capabilities.elicitation.url` is asked to open the Data Streams page during the call. It
then gets the normal tool error, so the model still says the feature is locked and how to request it.

| Client / protocol | Single-project call without the feature |
|---|---|
| URL-capable, 2026-07-28 | The call returns an `InputRequiredResult` with one URL `elicitation/create` request. The client opens the page after consent and retries with the answer. The retry returns the tool error. |
| URL-capable, older protocol with a back-channel (stdio, stateful HTTP) | `elicitation/create` URL request during the call, then the tool error |
| Not URL-capable, or no way to ask | The tool error only; its text contains the link |

Multi-project fan-out is unchanged: the per-project text note carries the link.

### Why not `URLElicitationRequiredError` (-32042)

The first version raised -32042. It's built for steps like an OAuth login: the client opens the URL, waits for
`notifications/elicitation/complete`, then retries. In Claude Code 2.1.292 this meant two problems:
- The user was stuck at "Waiting for the server to confirm completion…", because a feature request takes days and
  there's nothing to confirm.
- After Cancel, the model only saw "URL elicitation was canceled", not that Data Streams are locked.

## Resolution Strategy

- `elicitation.py`:
  - `UrlActionRequiredError(ToolError)` carries `user_message` and `url`. Its text already includes the link.
  - `UrlElicitationMiddleware` catches it. For URL-capable clients:
    - 2026-07-28: it returns an `InputRequiredToolResult` keyed `open_url`. On the retry, `ctx.input_responses`
      contains `open_url`, so it re-raises the error.
    - Older protocols: `session.elicit_url(...)` over the back-channel. Any failure, such as no back-channel, is
      logged, then it re-raises the error.
- It's a middleware because it must see the error after `tool_errors` and outside `MultiProjectMiddleware`.
  Single-project calls propagate the error unchanged; fan-out keeps per-project failures as text notes.
- `tools/streams.py` raises `UrlActionRequiredError` naming the feature key (`data-streams`), so agent hosts like
  Kai (AI-4019) can map it to their own feature-request flow.

## Scope

In scope: the middleware, the error type, and the Data Streams unlock.

Out of scope: other "go to the UI" flows (they can raise `UrlActionRequiredError` later).

## Testing / Verification

- Unit: `tests/test_elicitation.py` runs a real in-memory FastMCP server and client on 2026-07-28. Covered:
  - a URL-capable client that accepts or declines: asked once, then gets the tool error
  - a plain client: no request, tool error
  - an unrelated tool error: no request
- E2E, Azure NE project 721 without the feature: stdio and stateless HTTP, URL-capable and plain fastmcp clients.
  Claude Code 2.1.295 shows:
  1. "MCP server keboola wants to open a URL" → Open in browser.
  2. "I'm done, continue".
  3. Claude answers that Data Streams aren't turned on in project 721 and points to Unlock Data Streams.
