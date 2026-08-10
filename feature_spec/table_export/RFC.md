# RFC: Full-table export tool

Linear: [AI-3699](https://linear.app/keboola/issue/AI-3699/support-17247-improvement-to-mcp-server)

## Problem

`query_data` (`src/keboola_mcp_server/tools/sql.py`) is the only tool that returns table
rows today, and it hard-caps results at `MAX_ROWS = 1_000`
(`src/keboola_mcp_server/tools/sql.py:27`). This makes it impossible to build an
automation that pulls a full Storage table through the MCP server for any table larger
than that, and raising `MAX_ROWS` does not fix it in general — `query_data` runs the
query synchronously inside the tool call and returns rows inline, which does not scale
to full-table exports (large payload over the MCP transport, long-running synchronous
call, no resumability).

Storage API already has a purpose-built mechanism for this: table export as an
asynchronous job (`POST /v2/storage/branch/{branchId}/tables/{tableId}/export-async`),
which produces a file in File Storage rather than returning rows inline. Today the MCP
server has no client method or tool that uses this endpoint.

## Required Behavior

A new tool (working name `export_table`) must let an agent:

1. Request an export of a table by `table_id`, with parameters mapped 1:1 to the
   Storage API's `ExportTableRequest2` body (per the `export-async` endpoint docs):

   | Field | Type | Notes |
   |---|---|---|
   | `fileType` | `'csv' \| 'parquet'` (required) | `parquet` only supported on Snowflake backends. |
   | `columns` | `string \| null` | Comma-separated column list. |
   | `format` | `string \| null` | Export format. |
   | `gzip` | `bool` | Gzip the exported file. |
   | `includeInternalTimestamp` | `bool` | Snowflake only. |
   | `limit` | `int \| null` | Max rows to export. |
   | `orderBy` | `object[] \| null` | `{column, order, dataType}`. |
   | `whereFilters` | `object[] \| null` | `{column, operator, values, dataType}`. |
   | `timezone` | `string \| null` | Snowflake only. |
   | `sourceBranchId` | `int \| null` | Honoured only for JSON request bodies. |

   The tool does not need to expose every one of these on day one, but whichever subset
   it exposes must use these exact names/shapes — no invented or guessed parameters.
2. Have the tool wait for the export job to finish (bounded by a timeout, matching the
   pattern already used for workspace creation — see Resolution Strategy) and return a
   download URL for the exported file to the agent (see open question #1).
3. Get a clear, typed error if the job fails or times out, consistent with how
   `run_job`/`get_jobs` (`src/keboola_mcp_server/tools/jobs.py`) surface job failures. On
   timeout the error includes the Storage job ID; recovery is calling the tool again
   (the export submission is cheap to repeat), not polling a separate endpoint — see
   open question #2.

`query_data` and `MAX_ROWS` are **not** changed by this RFC — this is a new, separate
tool for full-table retrieval, not a change to the SQL query path. (AI-2772 tracks
whether `query_data`'s truncation behavior itself should change; that is out of scope
here.)

## Open Design Questions (need agreement before implementation)

These materially affect feasibility/shape and should be resolved in review, not
silently decided during implementation:

1. **How is the exported data delivered back to the agent?** Two options:
   - **Return the file content inline in the tool response** (fetch the exported CSV
     server-side after the export job succeeds, return it as the tool's return value).
     The agent gets the data directly in context, no second fetch, no assumption about
     the agent's network access. Downside: response size is bounded by whatever the MCP
     transport/agent context can hold, so this only works up to some row/byte size —
     ties directly into open question #3 below. It also unconditionally spends context on
     the whole table even when the agent only needs part of it.
   - **Return a Storage file download URL** (from `GET /v2/storage/files/{id}` after the
     job completes) and let the agent fetch it. Cheaper on the MCP response, supports
     `parquet`/`gzip`, and lets the agent read only the parts of the file it actually
     needs instead of the whole table landing in context. Assumes the calling agent can
     make an outbound HTTP request to wherever the file lives (S3/ABS/GCS, or Keboola's
     file-proxy) — not guaranteed for every MCP client.
   - **Recommendation:** default to the URL-based path. A full table dumped inline into
     LLM context is rarely what the agent needs, and the two-step fetch is an acceptable
     MVP tradeoff — it's straightforward to add an inline mode later behind the
     `fileType`/`gzip` params if a client that can't fetch URLs turns out to need one.
2. **Job polling model.** To be precise about what's actually happening: the
   `export-async` endpoint is a Storage API job (`GET /v2/storage/jobs/{id}`, via the
   existing `AsyncStorageClient.job_detail`), **not** a Job Queue job — it is a
   different system from the one `run_job`/`get_jobs` poll
   (`jobs_queue_client`/`src/keboola_mcp_server/tools/jobs.py`). There is no existing
   generic tool for polling Storage API jobs, so this should be a single tool call that
   internally submits the export and polls `job_detail` to completion, bounded by a
   timeout — exactly the pattern `_Workspace._wait_for_new_workspace`
   (`workspace.py:767-794`) already uses for the same job system. No fire-and-poll split
   is needed or currently supported.
   - **On timeout:** the export job keeps running in Storage after the tool call returns
     (submitting it doesn't get cancelled just because the tool stopped waiting). The
     tool returns a typed timeout error containing the Storage job ID, and the documented
     recovery is for the agent to call `export_table` again with the same parameters —
     there's no separate "resume polling this job" tool or job-ID input, since that would
     duplicate the job-submission side-effect handling for a case (very slow exports)
     that's expected to be rare. If this proves too costly in practice (re-submitting
     large exports repeatedly), a resume-by-job-ID path can be added later.
3. **Row limit removed entirely, or just raised?** Exports go through File Storage, so
   there's no inherent row cap the way `query_data` has one — but very large tables may
   still need caller-supplied `limit`/`whereFilters` to keep exports usable. Confirm
   whether unrestricted full-table export is acceptable or whether we want a documented
   max (e.g. warn or reject above some byte/row size) to avoid agents accidentally
   triggering multi-GB exports.

## Resolution Strategy

- `src/keboola_mcp_server/clients/storage.py`: add
  - `table_export_async(table_id, branch_id=None, **export_params) -> JsonDict` — thin
    POST wrapper for `branch/{branch_id}/tables/{table_id}/export-async`, taking the
    `ExportTableRequest2` fields listed above, following the existing method style
    (e.g. `table_detail`, `bucket_table_list`).
  - Reuse existing `job_detail(job_id)` (`clients/storage.py:848`) to poll the resulting
    job — it already hits the correct `jobs/{id}` Storage-API endpoint (the same one
    `_Workspace._wait_for_new_workspace` polls), no new client method needed for that
    part.
  - A `file_detail(file_id) -> JsonDict` wrapper for `GET /v2/storage/files/{id}`, needed
    to resolve the file referenced in the completed job's `results` into a download
    URL/credentials — there is currently no Files API client method at all.
- New tool in `src/keboola_mcp_server/tools/storage/tools.py` (or a new
  `tools/storage/export.py` if it grows large): `export_table`, following the
  `@tool_errors()` + `tool_errors`/`ToolAnnotations` conventions used by the other
  storage tools in that file. A single tool call: submit → poll → fetch → return —
  no job-ID handoff to a separate poll tool (see open question #2).
- Polling/timeout logic modeled directly on
  `_Workspace._wait_for_new_workspace` (`workspace.py:767-794`): loop on `job_detail`,
  check `status == 'success'`, bounded by a timeout, `asyncio.sleep` between polls.
- `export_table` returns a download URL (formatted from `file_detail`), not fetched file
  content — no HTTP client call to download the file server-side is needed.

## Scope

The following describes the scope of the **implementation PR** that follows this RFC —
this RFC PR itself only adds this document.

In scope (implementation PR):

- `table_export_async` + `file_detail` client methods.
- `export_table` tool (exact sync/async shape per the open questions above).
- Unit tests for the new client methods and tool (mocking the Storage API), following
  existing patterns in `tests/tools/test_storage.py` / `tests/clients/test_storage.py`.
- `TOOLS.md` regen via `tox -e check-tools-docs`.
- Version bump (minor — new tool) and `uv lock`.

Out of scope:

- Any change to `query_data` / `MAX_ROWS` (tracked separately in AI-2772).
- Pushing the exported data anywhere on the caller's behalf — this tool only produces
  the export and returns a download URL for it; what the calling agent does with it
  afterward is out of scope.
- General "too many tools" tool-management work raised in the same conversation — that's
  a separate concern from this specific export tool and should get its own RFC/spike if
  pursued.
- Kai-side integration — tracked separately, to be picked up once this RFC is settled.

## Verification

1. `tox` — pytest, black, flake8, check-tools-docs all exit 0.
2. Manual end-to-end via local MCP (`.mcp.json` per project `CLAUDE.md`) against a real
   project: export a table larger than 1,000 rows, confirm the returned download
   URL/credentials actually retrieve the full table content.
3. Confirm behavior on export failure (bad table ID, no read access) surfaces a clear
   tool error rather than a raw job-status dict.
4. Confirm timeout behavior: force a slow export, confirm the tool returns a typed
   timeout error with the Storage job ID, and that re-calling `export_table` succeeds.
