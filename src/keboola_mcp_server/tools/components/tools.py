import logging
from typing import Annotated, List, Sequence

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components.model import (
    ComponentConfiguration,
    ComponentType,
    ComponentWithConfigurations,
)
from keboola_mcp_server.tools.components.utils import (
    _get_component_details,
    _get_sql_transformation_id_from_sql_dialect,
    _get_transformation_configuration,
    _handle_component_types,
    _retrieve_components_configurations_by_ids,
    _retrieve_components_configurations_by_types,
)
from keboola_mcp_server.tools.sql import get_sql_dialect

LOG = logging.getLogger(__name__)


############################## Add component tools to the MCP server #########################################

# Regarding the conventional naming of entity models for components and their associated configurations,
# we also unified and shortened function names to make them more intuitive and consistent for both users and LLMs.
# These tool names now reflect their conventional usage, removing redundant parts for users while still
# providing the same functionality as described in the original tool names.
RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME: str = 'retrieve_components'
RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME: str = 'retrieve_transformations'
GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME: str = 'get_component_details'


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(
        get_component_configuration_details, name=GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME
    )
    LOG.info(f'Added tool: {GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME}.')

    mcp.add_tool(
        retrieve_components_configurations, name=RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME
    )
    LOG.info(f'Added tool: {RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME}.')

    mcp.add_tool(
        retrieve_transformations_configurations,
        name=RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
    )
    LOG.info(f'Added tool: {RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME}.')

    mcp.add_tool(create_sql_transformation)
    LOG.info(f'Added tool: {create_sql_transformation.__name__}.')

    mcp.add_tool(create_component_configuration)
    LOG.info(f'Added tool: {create_component_configuration.__name__}.')

    mcp.add_tool(get_component_configuration_examples)
    LOG.info(f'Added tool: {get_component_configuration_examples.__name__}.')

    LOG.info('Component tools initialized.')


############################## read tools #########################################


async def retrieve_components_configurations(
    ctx: Context,
    component_types: Annotated[
        Sequence[ComponentType],
        Field(
            description='List of component types to filter by. If none, return all components.',
        ),
    ] = tuple(),
    component_ids: Annotated[
        Sequence[str],
        Field(
            description='List of component IDs to retrieve configurations for. If none, return all components.',
        ),
    ] = tuple(),
) -> Annotated[
    list[ComponentWithConfigurations],
    Field(
        description='List of objects, each containing a component and its associated configurations.',
    ),
]:
    """
    Retrieves components configurations in the project, optionally filtered by component types or specific component IDs
    If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
    component_types.
    USAGE:
        - Use when you want to see components configurations in the project for given component_types.
        - Use when you want to see components configurations in the project for given component_ids.
    EXAMPLES:
        - user_input: `give me all components`
            -> returns all components configurations in the project
        - user_input: `list me all extractor components`
            -> set types to ["extractor"]
            -> returns all extractor components configurations in the project
        - user_input: `give me configurations for following component/s` | `give me configurations for this component`
            -> set component_ids to list of identifiers accordingly if you know them
            -> returns all configurations for the given components
        - user_input: `give me configurations for 'specified-id'`
            -> set component_ids to ['specified-id']
            -> returns the configurations of the component with ID 'specified-id'
    """
    # If no component IDs are provided, retrieve component configurations by types (default is all types)
    if not component_ids:
        client = KeboolaClient.from_state(ctx.session.state)
        component_types = _handle_component_types(component_types)  # if none, return all types
        return await _retrieve_components_configurations_by_types(client, component_types)
    # If component IDs are provided, retrieve component configurations by IDs
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_ids(client, component_ids)


async def retrieve_transformations_configurations(
    ctx: Context,
    transformation_ids: Annotated[
        Sequence[str],
        Field(
            description='List of transformation component IDs to retrieve configurations for.',
        ),
    ] = tuple(),
) -> Annotated[
    list[ComponentWithConfigurations],
    Field(
        description='List of objects, each containing a transformation component and its associated configurations.',
    ),
]:
    """
    Retrieves transformations configurations in the project, optionally filtered by specific transformation IDs.
    USAGE:
        - Use when you want to see transformation configurations in the project for given transformation_ids.
        - Use when you want to retrieve all transformation configurations, then set transformation_ids to an empty list.
    EXAMPLES:
        - user_input: `give me all transformations`
            -> returns all transformation configurations in the project
        - user_input: `give me configurations for following transformation/s` | `give me configurations for
        this transformation`
            -> set transformation_ids to list of identifiers accordingly if you know the IDs
            -> returns all transformation configurations for the given transformations IDs
        - user_input: `list me transformations for this transformation component 'specified-id'`
            -> set transformation_ids to ['specified-id']
            -> returns the transformation configurations with ID 'specified-id'
    """
    # If no transformation IDs are provided, retrieve transformations configurations by transformation type
    if not transformation_ids:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_types(client, ['transformation'])
    # If transformation IDs are provided, retrieve transformations configurations by IDs
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_ids(client, transformation_ids)


async def get_component_configuration_details(
    component_id: Annotated[
        str, Field(description='Unique identifier of the Keboola component/transformation')
    ],
    configuration_id: Annotated[
        str,
        Field(
            description='Unique identifier of the Keboola component/transformation configuration you want details '
            'about',
        ),
    ],
    ctx: Context,
) -> Annotated[
    ComponentConfiguration,
    Field(
        description='Detailed information about a Keboola component/transformation and its configuration.',
    ),
]:
    """
    Gets detailed information about a specific Keboola component configuration given component/transformation ID and
    configuration ID.
    USAGE:
        - Use when you want to see the details of a specific component/transformation configuration.
    EXAMPLES:
        - user_input: `give me details about this configuration`
            -> set component_id and configuration_id to the specific component/transformation ID and configuration ID
            if you know it
            -> returns the details of the component/transformation configuration pair
    """

    client = KeboolaClient.from_state(ctx.session.state)

    # Get Component Details
    component = await _get_component_details(client=client, component_id=component_id)
    # Get Configuration Details
    raw_configuration = client.storage_client.configurations.detail(component_id, configuration_id)
    LOG.info(
        f'Retrieved configuration details for {component_id} component with configuration {configuration_id}.'
    )

    # Get Configuration Metadata if exists
    endpoint = f'branch/{client.storage_client._branch_id}/components/{component_id}/configs/{configuration_id}/metadata'
    r_metadata = await client.get(endpoint)
    if r_metadata:
        LOG.info(
            f'Retrieved configuration metadata for {component_id} component with configuration {configuration_id}.'
        )
    else:
        LOG.info(
            f'No metadata found for {component_id} component with configuration {configuration_id}.'
        )
    # Create Component Configuration Detail Object
    return ComponentConfiguration.model_validate(
        {
            **raw_configuration,
            'component': component,
            'component_id': component_id,
            'metadata': r_metadata,
        }
    )


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
    endpoint = f'branch/{client.storage_client._branch_id}/components/{transformation_id}/configs'

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


async def create_component_configuration(
    ctx: Context,
    name: Annotated[
        str,
        Field(
            description='A short, descriptive name summarizing the purpose of the component configuration.',
        ),
    ],
    description: Annotated[
        str,
        Field(
            description=(
                'The detailed description of the component configuration explaining its purpose and functionality.'
            ),
        ),
    ],
    component_id: Annotated[
        str,
        Field(
            description='The ID of the component for which to create the configuration.',
        ),
    ],
    configuration: Annotated[
        dict,
        Field(
            description='The configuration JSON object containing the component-specific settings.',
        ),
    ],
    configuration_row: Annotated[
        dict | None,
        Field(
            description='Optional configuration row JSON object. If provided, it will be added to the configuration.',
        ),
    ] = None,
) -> Annotated[
    ComponentConfiguration,
    Field(
        description='Newly created Component Configuration with optional configuration row.',
    ),
]:
    """
    Creates a component configuration using the specified name, component ID, configuration JSON, and description.
    Optionally adds a configuration row if provided.
    CONSIDERATIONS:
        The configuration JSON object must follow the configuration schema of the specified component. The
        configuration JSON object should adhere to the component's configuration examples.
        If configuration_row is provided, it must follow the configuration row schema of the specified component.

    USAGE:
        - Use when you want to create a new configuration for a specific component.
        - Use when you want to create a new configuration with a configuration row.
    EXAMPLES:
        - user_input: `Create a new configuration for component X with these settings`
            -> set the component_id and configuration parameters accordingly
            -> returns the created component configuration if successful.
        - user_input: `Create a new configuration for component X with these settings and this row configuration`
            -> set the component_id, configuration, and configuration_row parameters accordingly
            -> returns the created component configuration with the row if successful.
    """

    client = KeboolaClient.from_state(ctx.session.state)
    endpoint = f'branch/{client.storage_client._branch_id}/components/{component_id}/configs'

    LOG.info(f'Creating new configuration: {name} for component: {component_id}.')
    # Try to create the new configuration and return the new object if successful
    # or log an error and raise an exception if not
    try:
        new_raw_configuration = await client.post(
            endpoint,
            data={
                'name': name,
                'description': description,
                'configuration': configuration,
            },
        )

        component = await _get_component_details(client=client, component_id=component_id)
        new_configuration = ComponentConfiguration(
            **new_raw_configuration,
            component_id=component_id,
            component=component,
        )

        LOG.info(
            f'Created new configuration for component "{component_id}" with configuration id '
            f'"{new_configuration.configuration_id}".'
        )

        # If configuration_row is provided, add it to the configuration
        if configuration_row is not None:
            rows_endpoint = f'branch/{client.storage_client._branch_id}/components/{component_id}/configs/{new_configuration.configuration_id}/rows'
            await client.post(
                rows_endpoint,
                data={
                    'configuration': configuration_row,
                },
            )
            LOG.info(
                f'Added configuration row to configuration "{new_configuration.configuration_id}" for component "{component_id}".'
            )

        return new_configuration
    except Exception as e:
        LOG.exception(f'Error when creating new component configuration: {e}')
        raise e


async def get_component_configuration_examples(
    ctx: Context,
    component_id: Annotated[
        str,
        Field(
            description='The ID of the component to get configuration examples for.',
        ),
    ],
) -> Annotated[
    str,
    Field(
        description='Markdown formatted string containing configuration examples for the component.',
    ),
]:
    """
    Retrieves sample configuration examples for a specific component from a JSONL file.
    USAGE:
        - Use when you want to see example configurations for a specific component.
    EXAMPLES:
        - user_input: `Show me example configurations for component X`
            -> set the component_id parameter accordingly
            -> returns a markdown formatted string with configuration examples
    """
    import json
    from pathlib import Path

    # Construct the path to the JSONL file TODO: fix the path somehow
    jsonl_path = Path("json-schemas/output") / f"sample_data_{component_id}.jsonl"

    if not jsonl_path.exists():
        return f"No configuration examples found for component {component_id}"

    # Read and parse the JSONL file
    examples = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
            try:
                data = json.loads(line)
                if data.get('component_id') == component_id and data.get('config_example'):
                    example = {
                        'config_example': data['config_example'],
                        'config_row_example': data['config_row_example'],
                    }
                    examples.append(example)
            except json.JSONDecodeError:
                continue  # Skip lines that are not valid JSON

    if not examples:
        return f"No configuration examples found for component {component_id}"

    # Format the examples as a markdown list
    markdown = "Configuration examples\n\n"
    for i, example in enumerate(examples, 1):
        markdown += f"{i}. Configuration:\n```json\n{json.dumps(example['config_example'], indent=2)}\n```\n"
        if example['config_row_example']:
            markdown += f"   Configuration Row:\n```json\n{json.dumps(example['config_row_example'], indent=2)}\n```\n"
        markdown += "\n"

    return markdown


############################## End of component tools #########################################
