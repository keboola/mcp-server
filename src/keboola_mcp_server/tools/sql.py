import csv
import logging
from io import StringIO
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from pydantic import Field

from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.workspace import SqlSelectData, WorkspaceManager

LOG = logging.getLogger(__name__)


def add_sql_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(FunctionTool.from_function(query_data))
    mcp.add_tool(FunctionTool.from_function(get_sql_dialect))
    LOG.info('SQL tools added to the MCP server.')


@tool_errors()
@with_session_state()
async def get_sql_dialect(
    ctx: Context,
) -> Annotated[str, Field(description='The SQL dialect of the project database')]:
    """Gets the name of the SQL dialect used by Keboola project's underlying database."""
    return await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()


@tool_errors()
@with_session_state()
async def query_data(
    sql_query: Annotated[str, Field(description='SQL SELECT query to run.')],
    ctx: Context,
) -> Annotated[str, Field(description='The retrieved data in a CSV format.')]:
    """
    Executes an SQL SELECT query to get the data from the underlying database.
    * When constructing the SQL SELECT query make sure to check the SQL dialect
      used by the Keboola project's underlying database.
    * When referring to tables always use fully qualified table names that include the database name,
      schema name and the table name.
    * The fully qualified table name can be found in the table information, use a tool to get the information
      about tables. The fully qualified table name can be found in the response from that tool.
    * Always use quoted column names when referring to table columns. The quoted column names can also be found
      in the response from the table information tool.
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    result = await workspace_manager.execute_query(sql_query)
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

        return output.getvalue()

    else:
        raise ValueError(f'Failed to run SQL query, error: {result.message}')
