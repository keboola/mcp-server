import csv
from io import StringIO
from typing import Annotated

import snowflake
from mcp.server.fastmcp import Context
from pydantic import Field

from keboola_mcp_server.database import ConnectionManager


async def query_table(
    sql_query: Annotated[str, Field(description="SQL SELECT query to run.")], ctx: Context
) -> Annotated[str, Field(description="The retrieved data in a CSV format.")]:
    """
    Executes an SQL SELECT query to get the data from the underlying snowflake database.
    * When constructing the SQL SELECT query make sure to use the fully qualified table names
      that include the database name, schema name and the table name.
    * The fully qualified table name can be found in the table information, use a tool to get the information
      about tables. The fully qualified table name can be found in the response for that tool.
    * Snowflake is case-sensitive so always wrap the column names in double quotes.

    Examples:
    * SQL queries must include the fully qualified table names including the database name, e.g.:
      SELECT * FROM "db_name"."db_schema_name"."table_name";
    """
    connection_manager = ctx.session.state["connection_manager"]
    assert isinstance(connection_manager, ConnectionManager)

    with connection_manager.create_snowflake_connection() as conn:
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            # Convert to CSV
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(result)

            return output.getvalue()

        except snowflake.connector.errors.ProgrammingError as e:
            raise ValueError(f"Snowflake query error: {str(e)}")

        except Exception as e:
            raise ValueError(f"Unexpected error during query execution: {str(e)}")
