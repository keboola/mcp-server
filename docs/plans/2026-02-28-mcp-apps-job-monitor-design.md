# MCP Apps: Job Monitor Dashboard — Design

## Goal

Add the first MCP App to the Keboola MCP Server: an interactive Job Monitor dashboard
that renders inside the conversation as a sandboxed iframe, showing a live-updating
jobs table with expandable log viewers.

## Architecture

### Approach: Raw MCP Primitives on FastMCP 2.14.1

We stay on FastMCP 2.14.1 and wire MCP Apps manually using standard MCP resources and
tool metadata. MCP Apps at the protocol level requires only:

1. A resource with `ui://` scheme and `text/html;profile=mcp-app` MIME type
2. A tool with `_meta.ui.resourceUri` pointing to that resource

No FastMCP 3.0 upgrade needed. We can migrate to FastMCP 3.0's `AppConfig` helpers
later without changing app behavior.

### Components

1. **`job_monitor` tool** (model-visible) — LLM calls this to launch the dashboard.
   Accepts optional filters (job_ids, component_id, status, limit). Returns structured
   job data as tool content, which the app UI renders. Also returns a text fallback for
   clients that don't support MCP Apps.

2. **`poll_job_monitor` tool** (app-only, hidden from LLM) — Called by the iframe every
   5 seconds to fetch fresh data. Returns the same job data structure. Polling
   auto-pauses when all visible jobs are in terminal states.

3. **`ui://keboola/job-monitor` resource** — A self-contained HTML+JS+CSS file served
   as a string. Uses the `@modelcontextprotocol/ext-apps` SDK from CDN. No build step.

### Data Flow

```
LLM calls job_monitor(status="processing")
  → Server fetches jobs from Queue API + logs from Storage API
  → Returns structured content + text fallback to client
  → Client renders iframe with ui://keboola/job-monitor
  → App receives tool result via ontoolresult callback
  → App renders jobs table with expandable log rows
  → Every 5s: App calls poll_job_monitor → gets fresh data → re-renders
  → Polling pauses when all jobs are terminal
  → Polling resumes on manual refresh button click
```

---

## Tool Interface

### `job_monitor` (model-visible)

```python
async def job_monitor(
    ctx: Context,
    job_ids: tuple[str, ...] = (),      # Specific jobs to monitor
    component_id: str | None = None,     # Filter by component
    status: str | None = None,           # Filter by status
    limit: int = 20,                     # Max jobs to show
    include_logs: bool = True,           # Fetch logs for each job
    log_tail_lines: int = 50,            # Last N log events per job
) -> structured job data + text fallback
```

### `poll_job_monitor` (app-only)

Same parameters as `job_monitor`. The app passes the original filter params back so
the poll returns consistent data. Hidden from the LLM via
`_meta.ui.visibility: ["app"]`.

### Structured Output Per Job

```json
{
  "id": "39751913",
  "status": "success",
  "componentId": "keboola.snowflake-transformation",
  "configId": "12345",
  "createdTime": "2026-02-28T10:00:00Z",
  "startTime": "2026-02-28T10:00:05Z",
  "endTime": "2026-02-28T10:01:30Z",
  "durationSeconds": 85,
  "url": "https://connection.../jobs/39751913",
  "logs": [
    {"message": "Processing started", "type": "info", "created": "..."},
    {"message": "Output mapping done", "type": "info", "created": "..."}
  ]
}
```

---

## UI Design

Single self-contained HTML file — no build step, no external CSS frameworks. Vanilla
HTML/CSS/JS with the MCP Apps SDK loaded from CDN (`https://unpkg.com`).

### Layout

- **Header bar**: "Job Monitor" title + last-refreshed timestamp + status indicator
  (green dot = polling active, grey = paused) + manual refresh button
- **Jobs table**: Columns — Status (color-coded badge), Component, Config ID, Duration,
  Created Time
- **Expandable rows**: Click a row to expand and show the log viewer inline
- **Log viewer**: Monospace scrollable area, log events color-coded by type
  (info=grey, warn=amber, error=red, success=green)

### Behavior

- On load: renders initial tool result data immediately
- Every 5s: calls `poll_job_monitor` via `app.callServerTool()`, re-renders table,
  preserves expanded row state
- Polling pauses when all visible jobs are in terminal state
  (success, error, cancelled, terminated, warning)
- Polling resumes on manual refresh button click

### Status Badges

| Status | Style |
|--------|-------|
| `processing` | Blue with pulse animation |
| `waiting`, `created` | Grey |
| `success` | Green |
| `error` | Red |
| `warning` | Amber |
| `terminating`, `terminated`, `cancelled` | Grey strikethrough |

### Theming

Supports light and dark mode via `prefers-color-scheme` CSS media query, overridden
by host context from `app.onhostcontextchanged`. Responsive to container dimensions.

---

## Implementation Details

### File Structure

New files:
- `src/keboola_mcp_server/apps/__init__.py` — Helper to register app resources and
  tools with correct `_meta.ui` metadata and MIME types
- `src/keboola_mcp_server/apps/job_monitor.html` — The self-contained HTML app
- `src/keboola_mcp_server/tools/job_monitor.py` — `job_monitor` and `poll_job_monitor`
  tool functions, reusing job/log fetching logic from `jobs.py`

Modified files:
- `src/keboola_mcp_server/server.py` — Call new app registration on startup
- `src/keboola_mcp_server/tools/jobs.py` — Extract shared job+log fetching into
  reusable helpers

### CSP Declaration

The HTML loads the ext-apps SDK from `https://unpkg.com`, so the tool's `_meta.ui`
must declare:

```json
{
  "csp": {
    "resource_domains": ["https://unpkg.com"]
  }
}
```

### Reuse

The `job_monitor` and `poll_job_monitor` tools reuse the same job-fetching and
log-fetching logic from `get_jobs`. Shared parts are extracted into callable helpers
in `jobs.py` to avoid duplication.

---

## Graceful Degradation

- Clients without MCP Apps support ignore `_meta.ui` metadata and render the text
  content as usual — the tool is still useful as a text-based job summary
- `job_monitor` always returns both `structuredContent` (for the app) and `content`
  (text fallback)
- `poll_job_monitor` has `visibility: ["app"]` so it doesn't appear in tool lists for
  non-app clients — no clutter

---

## Testing

- **Tool function tests**: Test `job_monitor` and `poll_job_monitor` return correct
  structured data (same mocking pattern as `test_jobs.py`)
- **Resource registration test**: Verify `ui://keboola/job-monitor` is registered with
  MIME type `text/html;profile=mcp-app` and returns valid HTML
- **Metadata tests**: Verify `job_monitor` has `_meta.ui.resourceUri` set, and
  `poll_job_monitor` has `visibility: ["app"]`
- **HTML smoke test**: Verify the HTML string contains expected `<script type="module">`
  with `App` import and `app.connect()`
- **No browser tests**: Iframe rendering is the client's responsibility

### TOOLS.md

`job_monitor` appears in generated docs. `poll_job_monitor` is excluded or marked as
internal (app-only).
