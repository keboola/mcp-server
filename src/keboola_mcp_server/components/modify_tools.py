import logging
from typing import Annotated, Sequence

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.components.model import ComponentConfiguration
from keboola_mcp_server.components.read_tools import _get_component_details
from keboola_mcp_server.components.utils import (
    _get_sql_transformation_id_from_sql_dialect,
    _get_transformation_configuration,
)
from keboola_mcp_server.sql_tools import get_sql_dialect

LOG = logging.getLogger(__name__)


############################## Add component write tools to the MCP server #########################################


def add_component_write_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(create_sql_transformation)
    LOG.info(f'Added tool {create_sql_transformation.__name__}.')


############################## Write tools #########################################


async def create_sql_transformation(
    ctx: Context,
    name: Annotated[
        str,
        Field(
            description='A short, descriptive name summarizing the purpose of the SQL transformation.',
        ),
    ],
    description: Annotated[
        str,
        Field(
            description=(
                'The detailed description of the SQL transformation capturing the user intent, explaining the '
                'SQL query, and the expected output.'
            ),
        ),
    ],
    sql_statements: Annotated[
        Sequence[str],
        Field(
            description=(
                'The executable SQL query statements written in the current SQL dialect. '
                'Each statement should be a separate item in the list.'
            ),
        ),
    ],
    created_table_names: Annotated[
        Sequence[str],
        Field(
            description=(
                'An empty list or a list of created table names if and only if they are generated within SQL '
                'statements (e.g., using `CREATE TABLE ...`).'
            ),
        ),
    ] = tuple(),
) -> Annotated[
    ComponentConfiguration,
    Field(
        description='Newly created SQL Transformation Configuration.',
    ),
]:
    """
    Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
    description, and optionally a list of created table names if and only if they are generated within the SQL
    statements.
    CONSIDERATIONS:
        - The SQL query statement is executable and must follow the current SQL dialect, which can be retrieved using
        appropriate tool.
        - When referring to the input tables within the SQL query, use fully qualified table names, which can be
          retrieved using appropriate tools.
        - When creating a new table within the SQL query (e.g. CREATE TABLE ...), use only the quoted table name without
          fully qualified table name, and add the plain table name without quotes to the `created_table_names` list.
        - Unless otherwise specified by user, transformation name and description are generated based on the sql query
          and user intent.
    USAGE:
        - Use when you want to create a new SQL transformation.
    EXAMPLES:
        - user_input: `Can you save me the SQL query you generated as a new transformation?`
            -> set the sql_statements to the query, and set other parameters accordingly.
            -> returns the created SQL transformation configuration if successful.
        - user_input: `Generate me an SQL transformation which [USER INTENT]`
            -> set the sql_statements to the query based on the [USER INTENT], and set other parameters accordingly.
            -> returns the created SQL transformation configuration if successful.
    """

    # Get the SQL dialect to use the correct transformation ID (Snowflake or BigQuery)
    # This can raise an exception if workspace is not set or different backend than BigQuery or Snowflake is used
    sql_dialect = await get_sql_dialect(ctx)
    transformation_id = _get_sql_transformation_id_from_sql_dialect(sql_dialect)
    LOG.info(f'SQL dialect: {sql_dialect}, using transformation ID: {transformation_id}')

    # Process the data to be stored in the transformation configuration - parameters(sql statements)
    # and storage(input and output tables)
    transformation_configuration_payload = _get_transformation_configuration(
        statements=sql_statements, transformation_name=name, output_tables=created_table_names
    )

    client = KeboolaClient.from_state(ctx.session.state)
    endpoint = (
        f'branch/{client.storage_client._branch_id}/components/{transformation_id}/configs'
    )

    LOG.info(
        f'Creating new transformation configuration: {name} for component: {transformation_id}.'
    )
    # Try to create the new transformation configuration and return the new object if successful
    # or log an error and raise an exception if not
    try:
        new_raw_transformation_configuration = await client.post(
            endpoint,
            data={
                'name': name,
                'description': description,
                'configuration': transformation_configuration_payload.model_dump(),
            },
        )

        component = await _get_component_details(client=client, component_id=transformation_id)
        new_transformation_configuration = ComponentConfiguration(
            **new_raw_transformation_configuration,
            component_id=transformation_id,
            component=component,
        )

        LOG.info(
            f'Created new transformation "{transformation_id}" with configuration id '
            f'"{new_transformation_configuration.configuration_id}".'
        )
        return new_transformation_configuration
    except Exception as e:
        LOG.exception(f'Error when creating new transformation configuration: {e}')
        raise e


############################## End of component tools #########################################
