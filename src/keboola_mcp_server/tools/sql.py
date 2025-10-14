import csv
import logging
from io import StringIO
from typing import Annotated

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.workspace import SqlSelectData, WorkspaceManager

LOG = logging.getLogger(__name__)

SQL_TOOLS_TAG = 'sql'


class QueryDataOutput(BaseModel):
    """Output model for SQL query results."""

    query_name: str = Field(description='The name of the executed query')
    csv_data: str = Field(description='The retrieved data in CSV format')


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

    CRITICAL SQL REQUIREMENTS:

    * ALWAYS check the SQL dialect before constructing queries. The SQL dialect can be found in the project info.
    * Do not include any comments in the SQL code

    DIALECT-SPECIFIC REQUIREMENTS:
    * Snowflake: Use double quotes for identifiers: "column_name", "table_name"
    * BigQuery: Use backticks for identifiers: `column_name`, `table_name`
    * Never mix quoting styles within a single query

    TABLE AND COLUMN REFERENCES:
    * Always use fully qualified table names that include database name, schema name and table name
    * Get fully qualified table names using table information tools - use exact format shown
    * Snowflake format: "DATABASE"."SCHEMA"."TABLE"
    * BigQuery format: `project`.`dataset`.`table`
    * Always use quoted column names when referring to table columns (exact quotes from table info)

    CTE (WITH CLAUSE) RULES:
    * ALL column references in main query MUST match exact case used in the CTE
    * If you alias a column as "project_id" in CTE, reference it as "project_id" in subsequent queries
    * For Snowflake: Unless columns are quoted in CTE, they become UPPERCASE. To preserve case, use quotes
    * Define all column aliases explicitly in CTEs
    * Quote identifiers in both CTE definition and references to preserve case

    FUNCTION COMPATIBILITY:
    * Snowflake: Use LISTAGG instead of STRING_AGG
    * Check data types before using date functions (DATE_TRUNC, EXTRACT require proper date/timestamp types)
    * Cast VARCHAR columns to appropriate types before using in date/numeric functions

    ERROR PREVENTION:
    * Never pass empty strings ('') where numeric or date values are expected
    * Use NULLIF or CASE statements to handle empty values
    * Always use TRY_CAST or similar safe casting functions when converting data types
    * Check for division by zero using NULLIF(denominator, 0)

    DATA VALIDATION:
    * When querying columns with categorical values, use query_data tool to inspect distinct values beforehand
    * Ensure valid filtering by checking actual data values first
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

        return QueryDataOutput(query_name=query_name, csv_data=output.getvalue())

    else:
        raise ValueError(f'Failed to run SQL query, error: {result.message}')
