import asyncio
import contextlib
import csv
import logging
from io import StringIO
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.cancellation import track_request
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.workspace import SqlSelectData, WorkspaceManager

LOG = logging.getLogger(__name__)

SQL_TOOLS_TAG = 'sql'
MAX_ROWS = 1_000
MAX_CHARS = 50_000


def _safe_request_id(ctx: Context) -> str | None:
    """Return the current JSON-RPC request id, or None if unavailable.

    The id is needed to register the running task in the cancellation registry so
    that `notifications/cancelled` can find and abort it. Outside of an active MCP
    request (e.g. some unit tests, or during initialization) the id is missing — in
    that case we skip registration and the call cannot be cancelled by the registry.
    """
    try:
        return str(ctx.request_id)
    except (AttributeError, RuntimeError):
        return None


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

    # Run the query as a separate task and register it in the process-wide cancellation
    # registry. When the client sends `notifications/cancelled`,
    # `CancellationInterceptorMiddleware` peeks at the body, looks the request id up
    # here, and cancels this task. That trips the CancelledError branch inside
    # `_SnowflakeWorkspace.execute_query`, which fires `cancel_job` on the backend.
    # We also cancel the inner task if our own coroutine is cancelled (e.g. by the
    # MCP SDK on a stateful transport) so the inner task isn't left running.
    query_task = asyncio.create_task(workspace_manager.execute_query(sql_query, max_rows=MAX_ROWS, max_chars=MAX_CHARS))
    request_id = _safe_request_id(ctx)
    cancel_tracking = track_request(request_id, query_task) if request_id is not None else contextlib.nullcontext()

    async with cancel_tracking:
        try:
            result = await query_task
        except BaseException:
            if not query_task.done():
                query_task.cancel()
                with contextlib.suppress(BaseException):
                    await query_task
            raise

    LOG.info(' '.join(filter(None, [f'Query "{query_name}" executed successfully.', result.message])))
    if result.is_ok:
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
        raise ValueError(f'Failed to run SQL query, error: {result.message}')
