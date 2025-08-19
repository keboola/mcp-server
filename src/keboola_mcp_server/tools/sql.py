import csv
import logging
from io import StringIO
from typing import Annotated, Literal

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


class AnomalyDetectionOutput(BaseModel):
    """Output model for anomaly detection results."""

    query_name: str = Field(description='The name of the executed anomaly detection query')
    csv_data: str = Field(description='The anomaly detection results in CSV format')
    summary: str = Field(description='A summary of the anomaly detection analysis')


def add_sql_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            query_data,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SQL_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_sql_dialect,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SQL_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            detect_anomalies,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SQL_TOOLS_TAG},
        )
    )
    LOG.info('SQL tools added to the MCP server.')


@tool_errors()
async def get_sql_dialect(
    ctx: Context,
) -> Annotated[str, Field(description='The SQL dialect of the project database')]:
    """Gets the name of the SQL dialect used by Keboola project's underlying database."""
    return await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()


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
) -> Annotated[QueryDataOutput, Field(description='The query results with name and CSV data.')]:
    """
    Executes an SQL SELECT query to get the data from the underlying database.

    CRITICAL SQL REQUIREMENTS:

    * ALWAYS check the SQL dialect first using get_sql_dialect tool before constructing queries
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


@tool_errors()
async def detect_anomalies(
    table_name: Annotated[str, Field(description='Fully qualified table name (e.g., "DATABASE"."SCHEMA"."TABLE" for Snowflake or `project`.`dataset`.`table` for BigQuery)')],
    numeric_column: Annotated[str, Field(description='The numeric column to analyze for anomalies')],
    date_column: Annotated[str, Field(description='The date/timestamp column to use for time-based grouping')],
    time_window: Annotated[Literal['DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'], Field(description='Time window for aggregating data')],
    ctx: Context,
    anomaly_threshold: Annotated[float, Field(description='Z-score threshold for detecting anomalies (typically 2.5 or 3.0)', ge=1.0, le=5.0)] = 2.5,
) -> Annotated[AnomalyDetectionOutput, Field(description='Anomaly detection results with CSV data and summary')]:
    """
    Performs anomaly detection on time-series data using Z-score analysis.
    
    This tool analyzes a numeric column over time to identify statistical anomalies using the Z-score method.
    It aggregates data by the specified time window, calculates statistical measures, and identifies periods
    where values significantly deviate from the norm.
    
    CRITICAL SQL REQUIREMENTS:
    * ALWAYS check the SQL dialect first using get_sql_dialect tool before using this tool
    * Use proper quoting for table and column names based on the SQL dialect
    * Snowflake: Use double quotes: "column_name", "table_name" 
    * BigQuery: Use backticks: `column_name`, `table_name`
    
    TABLE AND COLUMN REQUIREMENTS:
    * table_name must be fully qualified (database.schema.table)
    * numeric_column must contain numeric data (will be cast to NUMBER/NUMERIC)
    * date_column must contain date/timestamp data (will be parsed with TRY_TO_TIMESTAMP/similar)
    * Empty strings and null values are automatically handled
    
    OUTPUT INTERPRETATION:
    * Z-score > threshold: HIGH_ANOMALY (unusually high values)
    * Z-score < -threshold: LOW_ANOMALY (unusually low values)  
    * |Z-score| > threshold * 0.75: WARNING level
    * Otherwise: NORMAL
    
    PARAMETERS:
    * time_window: Granularity for time-based aggregation
    * anomaly_threshold: Higher values = fewer anomalies detected (more conservative)
    """
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    
    # Get SQL dialect to determine proper function names
    dialect = await workspace_manager.get_sql_dialect()
    
    # Build the SQL query based on dialect
    if dialect.lower() == 'snowflake':
        timestamp_func = 'TRY_TO_TIMESTAMP'
        cast_func = 'NUMBER'
    elif dialect.lower() == 'bigquery':
        timestamp_func = 'SAFE.PARSE_TIMESTAMP'
        cast_func = 'NUMERIC'
    else:
        # Default to Snowflake syntax
        timestamp_func = 'TRY_TO_TIMESTAMP'
        cast_func = 'NUMBER'
    
    # Construct the parameterized SQL query
    sql_query = f"""
WITH aggregated_data AS (
    SELECT 
        DATE_TRUNC('{time_window}', {timestamp_func}({date_column})) AS time_period,
        COUNT(*) AS record_count,
        SUM(NULLIF({numeric_column}, '')::{cast_func}) AS total_value,
        AVG(NULLIF({numeric_column}, '')::{cast_func}) AS avg_value,
        MIN(NULLIF({numeric_column}, '')::{cast_func}) AS min_value,
        MAX(NULLIF({numeric_column}, '')::{cast_func}) AS max_value
    FROM {table_name}
    WHERE NULLIF({numeric_column}, '') IS NOT NULL
        AND {timestamp_func}({date_column}) IS NOT NULL
    GROUP BY 1
),
statistics AS (
    SELECT 
        AVG(total_value) AS mean_value,
        STDDEV(total_value) AS stddev_value,
        COUNT(*) AS period_count
    FROM aggregated_data
),
z_scores AS (
    SELECT 
        a.*,
        s.mean_value,
        s.stddev_value,
        CASE 
            WHEN s.stddev_value = 0 OR s.stddev_value IS NULL THEN 0
            ELSE (a.total_value - s.mean_value) / s.stddev_value
        END AS z_score
    FROM aggregated_data a
    CROSS JOIN statistics s
)
SELECT 
    time_period,
    record_count,
    total_value,
    ROUND(avg_value, 2) AS avg_value,
    min_value,
    max_value,
    ROUND(mean_value, 2) AS mean_value,
    ROUND(stddev_value, 2) AS stddev_value,
    ROUND(z_score, 3) AS z_score,
    ROUND(ABS(z_score), 3) AS abs_z_score,
    CASE 
        WHEN ABS(z_score) > {anomaly_threshold} THEN 'ANOMALY'
        WHEN ABS(z_score) > {anomaly_threshold} * 0.75 THEN 'WARNING'
        ELSE 'NORMAL'
    END AS anomaly_status,
    CASE 
        WHEN z_score > {anomaly_threshold} THEN 'HIGH_ANOMALY'
        WHEN z_score < -{anomaly_threshold} THEN 'LOW_ANOMALY'
        WHEN z_score > {anomaly_threshold} * 0.75 THEN 'HIGH_WARNING'
        WHEN z_score < -{anomaly_threshold} * 0.75 THEN 'LOW_WARNING'
        ELSE 'NORMAL'
    END AS anomaly_direction
FROM z_scores
ORDER BY time_period DESC
""".strip()

    # Execute the query
    result = await workspace_manager.execute_query(sql_query)
    
    if result.is_ok:
        if result.data:
            data = result.data
        else:
            # Should not happen for SELECT queries
            data = SqlSelectData(columns=['message'], rows=[{'message': result.message}])

        # Convert to CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=data.columns)
        writer.writeheader()
        writer.writerows(data.rows)
        
        # Generate summary
        summary = _generate_anomaly_summary(data, time_window, anomaly_threshold)
        
        query_name = f'Anomaly Detection: {numeric_column} by {time_window} (threshold={anomaly_threshold})'
        
        return AnomalyDetectionOutput(
            query_name=query_name,
            csv_data=output.getvalue(),
            summary=summary
        )
    else:
        raise ValueError(f'Failed to run anomaly detection query, error: {result.message}')


def _generate_anomaly_summary(data: SqlSelectData, time_window: str, threshold: float) -> str:
    """Generate a human-readable summary of anomaly detection results."""
    if not data.rows:
        return 'No data found for anomaly analysis.'
    
    total_periods = len(data.rows)
    anomalies = sum(1 for row in data.rows if row.get('anomaly_status') == 'ANOMALY')
    warnings = sum(1 for row in data.rows if row.get('anomaly_status') == 'WARNING')
    normal = total_periods - anomalies - warnings
    
    high_anomalies = sum(1 for row in data.rows if row.get('anomaly_direction') == 'HIGH_ANOMALY')
    low_anomalies = sum(1 for row in data.rows if row.get('anomaly_direction') == 'LOW_ANOMALY')
    
    summary_parts = [
        f'Anomaly Detection Summary (Z-score threshold: {threshold})',
        f'Total {time_window.lower()} periods analyzed: {total_periods}',
        f'• Normal periods: {normal} ({normal/total_periods*100:.1f}%)',
        f'• Warning periods: {warnings} ({warnings/total_periods*100:.1f}%)',
        f'• Anomaly periods: {anomalies} ({anomalies/total_periods*100:.1f}%)',
    ]
    
    if anomalies > 0:
        summary_parts.extend([
            f'  - High anomalies: {high_anomalies}',
            f'  - Low anomalies: {low_anomalies}',
        ])
        
        # Find the most extreme anomaly
        max_abs_z = max((abs(float(row.get('z_score', 0))) for row in data.rows), default=0)
        if max_abs_z > 0:
            summary_parts.append(f'• Maximum absolute Z-score: {max_abs_z:.3f}')
    
    return '\n'.join(summary_parts)
