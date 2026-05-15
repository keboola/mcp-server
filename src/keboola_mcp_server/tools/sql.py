import asyncio
import contextlib
import csv
import logging
from io import StringIO
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.server.dependencies import get_http_request
from fastmcp.tools import FunctionTool
from mcp.types import (
    ProgressNotification,
    ProgressNotificationParams,
    ProgressToken,
    ServerNotification,
    ToolAnnotations,
)
from pydantic import BaseModel, Field

from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.workspace import JobSubmittedInfo, SqlSelectData, WorkspaceManager

LOG = logging.getLogger(__name__)

SQL_TOOLS_TAG = 'sql'
MAX_ROWS = 1_000
MAX_CHARS = 50_000
# How often to check whether the HTTP client has disconnected during a long query.
# Mirrors the 1 s job-poll cadence in `_SnowflakeWorkspace.execute_query`.
_DISCONNECT_POLL_INTERVAL = 1.0


async def _watch_for_http_disconnect(poll_interval: float = _DISCONNECT_POLL_INTERVAL) -> None:
    """Return when the underlying HTTP request is torn down, or block forever otherwise.

    In stateless streamable-HTTP mode (`stateless_http=True` in `cli.py`), the MCP
    `notifications/cancelled` payload arrives on a fresh transport instance and cannot
    reach the in-flight tool call's session — so `asyncio.CancelledError` is never
    raised inside the running tool. Watching the underlying ASGI request for an
    `http.disconnect` event lets us notice when the client gave up (closed the tab,
    hit "stop" in Kai, lost network) and trigger the same cancellation path we
    already have for SDK-driven cancels.

    Returns silently when disconnect is detected. Blocks forever if there is no HTTP
    request bound (e.g. stdio transport, background workers) — in that case the caller
    will only stop on normal task completion or its own cancellation.

    Any error from `is_disconnected()` is treated as "still connected" so a transient
    ASGI hiccup never cancels an otherwise-working query.
    """
    try:
        request = get_http_request()
    except RuntimeError:
        # No HTTP request context — never fire (e.g. stdio transport).
        await asyncio.Event().wait()
        return  # unreachable; satisfies the type checker

    while True:
        try:
            if await request.is_disconnected():
                return
        except Exception:
            LOG.debug('HTTP is_disconnected() check failed; treating as still-connected', exc_info=True)
        await asyncio.sleep(poll_interval)


def _client_progress_token(ctx: Context) -> ProgressToken | None:
    """Returns the progress token the client included in the original `tools/call`, or None.

    Per MCP spec, a server may only send `notifications/progress` for a request when the client
    explicitly provided a `progressToken` in that request's `_meta`. Tools that fall through here
    without a token must stay silent — emitting unsolicited progress can confuse strict clients.
    """
    rc = ctx.request_context
    if rc is None or rc.meta is None:
        return None
    # The MCP spec allows `_meta` to omit `progressToken`. The typed `RequestParams.Meta`
    # always carries the attribute (default None), but transports that surface `_meta` as a
    # plain mapping would not — so look it up defensively rather than assume the attribute.
    return getattr(rc.meta, 'progressToken', None)


async def _emit_job_submitted_progress(ctx: Context, progress_token: ProgressToken, info: JobSubmittedInfo) -> None:
    """Surfaces the backend job handle to the client so it can cancel out-of-band by POSTing to
    `info.cancellation_url`. The structured data lives under `params._meta`; the human-readable
    `message` is for clients that surface progress as text only and ignore `_meta`.

    We deliberately call the low-level `ctx.session.send_notification(...)` with
    `related_request_id=ctx.request_id` instead of FastMCP's high-level `ctx.send_notification(...)`.
    The MCP SDK's streamable_http message router (`mcp/server/streamable_http.py`, the
    "Extract related_request_id from meta" branch) uses that field to pick which request's SSE
    response stream receives the notification. Without it, notifications are addressed to
    `GET_STREAM_KEY`, the standalone GET stream — which doesn't exist in `stateless_http=True`
    mode (our deployment shape), so the notification is silently dropped and the client never
    sees the job handle. `ctx.send_notification(...)` does NOT set this field, hence the bypass.
    """
    params = ProgressNotificationParams.model_validate(
        {
            'progressToken': progress_token,
            'progress': 0,
            'message': f'Submitted to {info.backend}',
            '_meta': {
                'keboola.queryJobId': info.job_id,
                'keboola.backend': info.backend,
                # `cancellation_url` may be None when a backend does not expose an out-of-band
                # cancel endpoint; clients should treat the field as optional.
                'keboola.cancellationUrl': info.cancellation_url,
            },
        }
    )
    notification = ProgressNotification(method='notifications/progress', params=params)
    # `ctx.request_id` is a property that RAISES RuntimeError when `request_context` is None —
    # `getattr(ctx, 'request_id', None)` would NOT catch that (its default only suppresses
    # AttributeError). Read the request id off `request_context` directly so a missing context
    # yields None and we hit the graceful-skip branch below instead of raising.
    rc = ctx.request_context
    request_id = str(rc.request_id) if rc is not None and rc.request_id is not None else None
    if request_id is None:
        # Without a request id we cannot route the notification — see the docstring above for why.
        # Sending with `related_request_id=None` reproduces the bug we built this fix to prevent
        # (silent drop onto GET_STREAM_KEY in stateless mode). Skip the emit and warn instead so
        # the failure mode is at least visible in the logs; the query itself continues normally.
        LOG.warning(
            f'Skipping notifications/progress for job_id={info.job_id}: request id is unavailable — '
            f'cannot route to originating SSE stream. Out-of-band cancellation will be unavailable.'
        )
        return
    await ctx.session.send_notification(ServerNotification(notification), related_request_id=request_id)
    LOG.info(
        f'Emitted notifications/progress for job_id={info.job_id} '
        f'related_request_id={request_id!r} backend={info.backend}'
    )


class QueryDataOutput(BaseModel):
    """Output model for SQL query results."""

    query_name: str = Field(description='The name of the executed query')
    csv_data: str = Field(description='The retrieved data in CSV format')
    message: str | None = Field(default=None, description='A message from the query execution')


def add_sql_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            query_data,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SQL_TOOLS_TAG},
        )
    )
    LOG.info('SQL tools added to the MCP server.')


@tool_errors()
async def query_data(
    sql_query: Annotated[str, Field(description='SQL SELECT query to run.')],
    query_name: Annotated[
        str,
        Field(
            description=(
                'A concise, human-readable name for this query based on its purpose and what data it retrieves. '
                'Use normal words with spaces (e.g., "Customer Orders Last Month", "Top Selling Products", '
                '"User Activity Summary").'
            )
        ),
    ],
    ctx: Context,
) -> QueryDataOutput:
    """
    Executes an SQL SELECT query to get the data from the underlying database.

    BEFORE QUERYING:
    * Always verify the table has a non-null fullyQualifiedName from get_tables tool.
      If it does not, the table is not SQL-accessible from this workspace — do not attempt the query and inform user.

    CRITICAL SQL REQUIREMENTS:

    * ALWAYS check the SQL dialect before constructing queries.
    * Do not include any comments in the SQL code
    * Use delimited identifiers and FQN format for the current SQL dialect.

    TABLE AND COLUMN REFERENCES:
    * Always use fully qualified table names in the exact FQN format provided by table information tools
    * Follow the identifier structure exactly as shown by table info tools for the current SQL dialect
    * Always use delimited identifiers when referring to table columns

    CTE (WITH CLAUSE) RULES:
    * ALL column references in main query MUST match exact case used in the CTE
    * If you alias a column in a CTE, reference it under the aliased name in the subsequent queries
    * Define all column aliases explicitly in CTEs
    * Use delimited identifiers in both CTE definition and references to preserve case

    FUNCTION COMPATIBILITY:
    * Check data types before using date functions (DATE_TRUNC, EXTRACT require proper date/timestamp types)
    * Cast VARCHAR columns to appropriate types before using in date/numeric functions

    ERROR PREVENTION:
    * Never pass empty strings ('') where numeric or date values are expected
    * Use NULLIF or CASE statements to handle empty values
    * Always use TRY_CAST or similar safe casting functions when converting data types
    * Check for division by zero using NULLIF(denominator, 0)
    * Always use the LIMIT clause in your SELECT statements when fetching data. There are hard limits imposed
      by this tool on the maximum number of rows that can be fetched and the maximum number of characters.
      The tool will truncate the data if those limits are exceeded.

    DATA VALIDATION:
    * When querying columns with categorical values, use query_data tool to inspect distinct values beforehand
    * Ensure valid filtering by checking actual data values first
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)

    progress_token = _client_progress_token(ctx)

    async def _on_job_submitted(info: JobSubmittedInfo) -> None:
        await _emit_job_submitted_progress(ctx, progress_token, info)

    # Race the query against the HTTP client disconnecting. If the client gives up
    # before the query finishes (Kai kills the sandbox SDK process on STOP, which drops
    # the proxied tools/call socket), cancelling `query_task` triggers the CancelledError
    # branch inside `_Workspace.execute_query`, which fires `cancel_job` on the backend
    # so the scan stops. Needed because in stateless HTTP mode the MCP cancel
    # notification cannot reach this running request — see `_watch_for_http_disconnect`.
    query_task = asyncio.create_task(
        workspace_manager.execute_query(
            sql_query,
            max_rows=MAX_ROWS,
            max_chars=MAX_CHARS,
            on_job_submitted=_on_job_submitted if progress_token is not None else None,
        )
    )
    disconnect_task = asyncio.create_task(_watch_for_http_disconnect())
    try:
        done, _pending = await asyncio.wait(
            [query_task, disconnect_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
    except BaseException:
        query_task.cancel()
        disconnect_task.cancel()
        raise

    if query_task not in done:
        LOG.info(f'HTTP client disconnected during query_data "{query_name}"; cancelling underlying query')
        query_task.cancel()
        with contextlib.suppress(BaseException):
            await query_task
        raise asyncio.CancelledError(f'HTTP client disconnected during query_data "{query_name}"')

    disconnect_task.cancel()
    with contextlib.suppress(BaseException):
        await disconnect_task

    result = query_task.result()
    if result.is_ok:
        LOG.info(' '.join(filter(None, [f'Query "{query_name}" executed successfully.', result.message])))
        if result.data:
            data = result.data
        else:
            # non-SELECT query, this should not really happen, because this tool is for running SELECT queries
            data = SqlSelectData(columns=['message'], rows=[{'message': result.message}])

        # Convert to CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data.columns)
        writer.writeheader()
        writer.writerows(data.rows)

        return QueryDataOutput(query_name=query_name, csv_data=output.getvalue(), message=result.message)

    else:
        # Surface cancellation cleanly: the workspace already produced a precise message
        # ("Query was cancelled") for the cancel-by-client case, so don't wrap it in a
        # generic "Failed to run SQL query, error: ..." prefix that hides what happened.
        # A client-initiated cancel is expected, so log it at INFO; genuine failures at WARNING.
        if result.message == 'Query was cancelled':
            LOG.info(f'Query "{query_name}" was cancelled.')
            raise ValueError('Query was cancelled')
        LOG.warning(' '.join(filter(None, [f'Query "{query_name}" failed.', result.message])))
        raise ValueError(f'Failed to run SQL query, error: {result.message}')
