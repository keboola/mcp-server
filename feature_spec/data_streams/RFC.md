# RFC: `get_streams` + `create_stream` — Data Streams support

Linear: [AI-4015](https://linear.app/keboola/issue/AI-4015/mcp-data-streams-tools-get-streams-create-stream-with-unlock-link-when)

## Problem

The MCP server has no Data Streams tools. An agent asked to "send our webhooks/OpenTelemetry data to
Keboola" can't list or create streams, and it can't tell the user that the feature exists but is
disabled in the project. Data Streams are gated by the `data-streams` project feature. The UI shows an
"Unlock Data Streams" button for projects without it, and that button sends the feature request.

## Required Behavior

Two tools, tag `streams`, visible to every project (not hidden by `ToolsFilteringMiddleware`):

| Tool | Annotations | Behavior |
|---|---|---|
| `get_streams(source_ids=[])` | `readOnlyHint` | Lists the sources (HTTP/OTLP) with their sinks, ingestion endpoints and UI links. Optional filter by source IDs; unknown IDs fail. |
| `create_stream(name, source_type='http', description, table_id, columns, include_raw_otlp_record, project_id)` | `destructiveHint=False` | Creates a source and its table sinks, waits for the async tasks, returns the created stream. |

`create_stream` defaults mirror the UI (`kbc-ui/src/scripts/modules/stream/constants.ts`):

| Source type | Sinks |
|---|---|
| `http` | One sink into `table_id` (default `in.c-data-stream-<source_id>.events`) with `columns` (default `id`/uuid, `datetime`, `ip`, `body`, `headers`). |
| `otlp` | Three sinks (`logs`, `metrics`, `traces`) with `allowedSignals=[signal]` into `in.c-otlp-<source_id>.<signal>`, UI column templates; `include_raw_otlp_record` adds a `raw` body column. `table_id`/`columns` are rejected. |

If creating a sink fails, the source is deleted (same as the UI), so a failed call leaves no half-made stream.

Availability check (both tools, before any Stream API call):

| Condition | Result |
|---|---|
| Dev branch (`branch_id` set) | `ToolError`: supported only in the main production branch |
| `protected-default-branch` feature | `ToolError`: not available with a protected default branch |
| No `data-streams` feature | `ToolError` with the project's Data Streams page URL (`/admin/projects/<id>/storage/data-streams`) and an instruction to give it to the user, who can click "Unlock Data Streams" |

The returned endpoints carry the source secret (HTTP `url`, OTLP `url`/`secret`). They are returned because
the user needs them to send data (the UI shows them too). The tool descriptions tell the agent to share
them only with the user.

## Resolution Strategy

- `clients/stream.py`: `StreamClient` for `https://stream.<stack>` (branch `default`). Handles paginated
  `aggregation/sources`, `create_source`, `create_table_sink`, `delete_source`, and polling of async tasks
  (`v1/tasks/{id}`, 60 s timeout). Typed `TableColumn` model shared by the client and the tool input.
- `KeboolaClient.stream_client`, authenticated like Storage/scheduler (`bearer_or_sapi_token`).
- `tools/streams.py`: the tools + `ensure_data_streams_available()`.
- `links.py`: Data Streams dashboard/detail/docs links.
- `clients/base.py`: Stream API errors put a code into `error` and the text into `message`; the text is
  now appended to the raised error so the agent sees e.g. "Source already exists in the branch."

The unlock path is a plain link in the error message, which works in every MCP client. URL-mode elicitation
(`URLElicitationRequiredError`, spec 2025-11-25) would let a client open the page directly, but client
support varies, so it's a follow-up.

## Scope

In scope: listing, creating HTTP/OTLP streams, the unlock link.

Out of scope: update/delete/disable tools, sink statistics, source settings, URL-mode elicitation,
creating a support ticket on the user's behalf.

## Testing / Verification

- Unit: `tests/clients/test_stream.py` (pagination, sink payload, task polling/timeout/error) and
  `tests/tools/test_streams.py` (availability matrix for both tools, list/filter mapping, HTTP/OTLP sink
  defaults, OTLP option validation, source rollback on sink failure).
- Integration: `integtests/tools/test_streams.py`. Without the feature, both tools return the unlock link.
  With it, an HTTP stream is created, accepts an event, is listed, and is deleted.
- Manual E2E over MCP against a real project, with and without the feature (see PR).
