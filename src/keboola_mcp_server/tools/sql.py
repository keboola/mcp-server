import csv
import logging
from io import StringIO
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ProgressNotification, ProgressNotificationParams, ProgressToken, ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.workspace import JobSubmittedInfo, SqlSelectData, WorkspaceManager

LOG = logging.getLogger(__name__)

SQL_TOOLS_TAG = 'sql'
MAX_ROWS = 1_000
MAX_CHARS = 50_000


def _client_progress_token(ctx: Context) -> ProgressToken | None:
    """Returns the progress token the client included in the original `tools/call`, or None.

    Per MCP spec, a server may only send `notifications/progress` for a request when the client
    explicitly provided a `progressToken` in that request's `_meta`. Tools that fall through here
    without a token must stay silent — emitting unsolicited progress can confuse strict clients.
    """
    rc = ctx.request_context
    if rc is None or rc.meta is None:
        return None
    return rc.meta.progressToken


async def _emit_job_submitted_progress(ctx: Context, progress_token: ProgressToken, info: JobSubmittedInfo) -> None:
    """Surfaces the backend job handle to the client so it can cancel out-of-band by POSTing to
    `info.cancellation_url`. The structured data lives under `params._meta`; the human-readable
    `message` is for clients that surface progress as text only and ignore `_meta`.
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
    await ctx.send_notification(ProgressNotification(method='notifications/progress', params=params))


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

    result = await workspace_manager.execute_query(
        sql_query,
        max_rows=MAX_ROWS,
        max_chars=MAX_CHARS,
        on_job_submitted=_on_job_submitted if progress_token is not None else None,
    )
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
