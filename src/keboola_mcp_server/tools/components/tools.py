import json
import logging
from typing import Annotated, Any, Optional, Sequence, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from keboola_mcp_server.client import JsonDict, KeboolaClient, SuggestedComponent
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.tools.components.model import (
    Component,
    ComponentConfigurationOutput,
    ComponentConfigurationResponse,
    ComponentDetail,
    ComponentRootConfiguration,
    ComponentRowConfiguration,
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

# Add component tools to the MCP server #########################################

# Regarding the conventional naming of entity models for components and their associated configurations,
# we also unified and shortened function names to make them more intuitive and consistent for both users and LLMs.
# These tool names now reflect their conventional usage, removing redundant parts for users while still
# providing the same functionality as described in the original tool names.
RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME: str = 'retrieve_component_configurations'
RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME: str = 'retrieve_transformations'
GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME: str = 'get_component_configuration_details'


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(get_component_configuration_details, name=GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME)
    LOG.info(f'Added tool: {GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME}.')

    mcp.add_tool(retrieve_components_configurations, name=RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME)
    LOG.info(f'Added tool: {RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME}.')

    mcp.add_tool(
        retrieve_transformations_configurations,
        name=RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
    )
    LOG.info(f'Added tool: {RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME}.')

    mcp.add_tool(create_sql_transformation)
    LOG.info(f'Added tool: {create_sql_transformation.__name__}.')

    mcp.add_tool(update_sql_transformation_configuration)
    LOG.info(f'Added tool: {update_sql_transformation_configuration.__name__}.')

    mcp.add_tool(get_component_detail)
    LOG.info(f'Added tool: {get_component_detail.__name__}.')

    mcp.add_tool(create_component_root_configuration)
    LOG.info(f'Added tool: {create_component_root_configuration.__name__}.')

    mcp.add_tool(create_component_row_configuration)
    LOG.info(f'Added tool: {create_component_row_configuration.__name__}.')

    mcp.add_tool(update_component_root_configuration)
    LOG.info(f'Added tool: {update_component_root_configuration.__name__}.')

    mcp.add_tool(update_component_row_configuration)
    LOG.info(f'Added tool: {update_component_row_configuration.__name__}.')

    mcp.add_tool(get_component_configuration_examples)
    LOG.info(f'Added tool: {get_component_configuration_examples.__name__}.')

    mcp.add_tool(find_component_id)
    LOG.info(f'Added tool: {find_component_id.__name__}.')

    LOG.info('Component tools initialized.')


# tools #########################################


@tool_errors()
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
    Retrieves configurations of components present in the project,
    optionally filtered by component types or specific component IDs
    If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
    component_types.

    USAGE:
    - Use when you want to see components configurations in the project for given component_types.
    - Use when you want to see components configurations in the project for given component_ids.

    EXAMPLES:
        - user_input: `give me all components (in the project)`
            -> returns all components configurations in the project
        - user_input: `list me all extractor components (in the project)`
            -> set types to ["extractor"]
            -> returns all extractor components configurations in the project
        - user_input: `give me configurations for following component/s` | `give me configurations for this component`
            -> set component_ids to list of identifiers accordingly if you know them
            -> returns all configurations for the given components in the project
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


@tool_errors()
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
        - returns all transformation configurations in the project
    - user_input: `give me configurations for following transformation/s` | `give me configurations for
      this transformation`
        - set transformation_ids to list of identifiers accordingly if you know the IDs
        - returns all transformation configurations for the given transformations IDs
    - user_input: `list me transformations for this transformation component 'specified-id'`
        - set transformation_ids to ['specified-id']
        - returns the transformation configurations with ID 'specified-id'
    """
    # If no transformation IDs are provided, retrieve transformations configurations by transformation type
    if not transformation_ids:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_types(client, ['transformation'])
    # If transformation IDs are provided, retrieve transformations configurations by IDs
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_ids(client, transformation_ids)


@tool_errors()
async def get_component_detail(
    ctx: Context,
    component_id: Annotated[str, Field(description='Unique identifier of the Keboola component/transformation')],
) -> Annotated[
    ComponentDetail,
    Field(
        description='Detailed information about a Keboola component.',
    ),
]:
    """
    Gets detailed information about a specific Keboola component given component ID.
    USAGE:
        - Use when you want to see the details of a specific component to get its documentation, configuration schemas
        ,etc. Especially in situation when the users asks to create or update a component configuration.
        This tool is mainly for internal use by the agent.
    EXAMPLES:
        - user_input: `Create a generic extractor configuration for x`
            -> Set the component_id if you know it or find the component_id by find_component_id
               or docs use tool and set it
            -> returns the details of the component/transformation configuration pair
    """
    client = KeboolaClient.from_state(ctx.session.state)
    return await _get_component_details(component_id=component_id, client=client)


@tool_errors()
async def get_component_configuration_details(
    component_id: Annotated[str, Field(description='Unique identifier of the Keboola component/transformation')],
    configuration_id: Annotated[
        str,
        Field(
            description='Unique identifier of the Keboola component/transformation configuration you want details '
            'about',
        ),
    ],
    ctx: Context,
) -> Annotated[
    ComponentConfigurationOutput,
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
        - set component_id and configuration_id to the specific component/transformation ID and configuration ID
          if you know it
        - returns the details of the component/transformation configuration pair
    """

    client = KeboolaClient.from_state(ctx.session.state)

    # Get Component Details
    component = await _get_component_details(client=client, component_id=component_id)
    # Get Configuration Details
    raw_configuration = await client.storage_client.configuration_detail(
        component_id=component_id, configuration_id=configuration_id
    )
    LOG.info(f'Retrieved configuration details for {component_id} component with configuration {configuration_id}.')

    # Get Configuration Metadata if exists
    endpoint = (
        f'branch/{client.storage_client.branch_id}/components/{component_id}/configs/{configuration_id}/metadata'
    )
    r_metadata = await client.storage_client.get(endpoint=endpoint)
    if r_metadata:
        LOG.info(
            f'Retrieved configuration metadata for {component_id} component with configuration {configuration_id}.'
        )
    else:
        LOG.info(f'No metadata found for {component_id} component with configuration {configuration_id}.')
    # Create Component Configuration Detail Object
    configuration_response = ComponentConfigurationResponse.model_validate(
        {
            **raw_configuration,
            'component_id': component_id,
            'metadata': r_metadata,
        }
    )
    # Create Component Configuration Output Object
    return ComponentConfigurationOutput.from_component_configuration_response(configuration_response, component)


@tool_errors()
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
    ComponentConfigurationResponse,
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
        - set the sql_statements to the query, and set other parameters accordingly.
        - returns the created SQL transformation configuration if successful.
    - user_input: `Generate me an SQL transformation which [USER INTENT]`
        - set the sql_statements to the query based on the [USER INTENT], and set other parameters accordingly.
        - returns the created SQL transformation configuration if successful.
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
    endpoint = f'branch/{client.storage_client.branch_id}/components/{transformation_id}/configs'

    LOG.info(f'Creating new transformation configuration: {name} for component: {transformation_id}.')
    # Try to create the new transformation configuration and return the new object if successful
    # or log an error and raise an exception if not
    new_raw_transformation_configuration = cast(
        JsonDict,
        await client.storage_client.post(
            endpoint=endpoint,
            data={
                'name': name,
                'description': description,
                'configuration': transformation_configuration_payload.model_dump(),
            },
        )
    )

    component = await _get_component_details(client=client, component_id=transformation_id)
    new_transformation_configuration = ComponentConfigurationResponse.model_validate(
        new_raw_transformation_configuration |
        {
            'component_id': transformation_id,
            'component': Component.from_component_detail(component),
        }
    )

    LOG.info(
        f'Created new transformation "{transformation_id}" with configuration id '
        f'"{new_transformation_configuration.configuration_id}".'
    )
    return new_transformation_configuration


@tool_errors()
async def update_sql_transformation_configuration(
    ctx: Context,
    configuration_id: Annotated[
        str,
        Field(description='Unique identifier of the Keboola transformation configuration you want to update'),
    ],
    change_description: Annotated[
        str,
        Field(
            description='Detailed description of the new changes to the transformation configuration.',
        ),
    ],
    updated_configuration: Annotated[
        dict[str, Any],
        Field(
            description=(
                'Updated transformation configuration JSON object containing both updated settings applied and all '
                'existing settings preserved.'
            ),
        ),
    ],
    updated_description: Annotated[
        str,
        Field(
            description='Updated existing transformation description reflecting the changes made in the behavior of '
            'the transformation. If no behavior changes are made, empty string preserves the original description.',
        ),
    ] = '',
    is_disabled: Annotated[
        bool,
        Field(
            description='Whether to disable the transformation configuration. Default is False.',
        ),
    ] = False,
) -> Annotated[
    ComponentConfigurationResponse,
    Field(
        description='Updated transformation configuration.',
    ),
]:
    """
    Updates an existing SQL transformation configuration, optionally updating the description and disabling the
    configuration.

    CONSIDERATIONS:
    - The configuration JSON data must follow the current Keboola transformation configuration schema.
    - The SQL code statements should follow the current SQL dialect, which can be retrieved using appropriate tool.
    - When the behavior of the transformation is not changed, the updated_description can be empty string.

    EXAMPLES:
    - user_input: `Can you edit this transformation configuration that [USER INTENT]?`
        - set the transformation_id and configuration_id accordingly and update configuration parameters based on
          the [USER INTENT]
        - returns the updated transformation configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    sql_transformation_id = _get_sql_transformation_id_from_sql_dialect(await get_sql_dialect(ctx))
    LOG.info(f'SQL transformation ID: {sql_transformation_id}')

    LOG.info(f'Updating transformation: {sql_transformation_id} with configuration: {configuration_id}.')
    updated_raw_configuration = await client.storage_client.configuration_update(
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        configuration=updated_configuration,
        change_description=change_description,
        updated_description=updated_description if updated_description else None,
        is_disabled=is_disabled,
    )

    transformation = await _get_component_details(client=client, component_id=sql_transformation_id)
    updated_transformation_configuration = ComponentConfigurationResponse.model_validate(
        updated_raw_configuration |
        {
            'component_id': transformation.component_id,
            'component': Component.from_component_detail(transformation),
        }
    )

    LOG.info(
        f'Updated transformation configuration: {updated_transformation_configuration.configuration_id} for '
        f'component: {updated_transformation_configuration.component_id}.'
    )
    return updated_transformation_configuration


@tool_errors()
async def create_component_root_configuration(
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
    root_configuration: Annotated[
        dict[str, Any],
        Field(description='The component configuration parameters, adhering to the root_configuration_schema'),
    ],
    storage: Annotated[
        Optional[dict[str, Any]],
        Field(
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ] = None,
) -> Annotated[
    ComponentRootConfiguration,
    Field(
        description='Created component root configuration object,',
    ),
]:
    """
    Creates a component configuration using the specified name, component ID, configuration JSON, and description.
    CONSIDERATIONS:
        The root_configuration JSON object must follow the root_configuration_schema of the specified component.
        It can also adhere to the appropriate configuration examples if available.

    USAGE:
        - Use when you want to create a new root configuration for a specific component.
    EXAMPLES:
        - user_input: `Create a new configuration for component X with these settings`
            -> set the component_id and configuration parameters accordingly
            -> returns the created component configuration if successful.
    """

    client = KeboolaClient.from_state(ctx.session.state)

    LOG.info(f'Creating new configuration: {name} for component: {component_id}.')

    configuration_payload = {'storage': storage, 'parameters': root_configuration}
    # TODO validate parameters
    # Try to create the new configuration and return the new object if successful
    # or log an error and raise an exception if not
    new_raw_configuration = await client.storage_client.create_component_root_configuration(
        component_id=component_id,
        data={
            'name': name,
            'description': description,
            'configuration': configuration_payload,
        },
        branch_id=client.storage_client_sync._branch_id,
    )

    new_configuration = ComponentRootConfiguration(
        **new_raw_configuration,
        component_id=component_id,
        storage=new_raw_configuration['configuration'].get('storage'),
        parameters=new_raw_configuration['configuration'].get('parameters'),
    )

    LOG.info(
        f'Created new configuration for component "{component_id}" with configuration id '
        f'"{new_configuration.configuration_id}".'
    )

    return new_configuration


@tool_errors()
async def create_component_row_configuration(
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
    configuration_id: Annotated[
        str,
        Field(
            description='The ID of the configuration for which to create the configuration row.',
        ),
    ],
    row_configuration: Annotated[
        dict[str, Any],
        Field(description='The component row configuration parameters, adhering to the row_configuration_schema'),
    ],
    storage: Annotated[
        Optional[dict[str, Any]],
        Field(
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ] = None,
) -> Annotated[
    ComponentRowConfiguration,
    Field(
        description='Created component row configuration object,',
    ),
]:
    """
    Creates a component configuration row in the specified configuration_id, using the specified name,
    component ID, configuration JSON, and description.

    CONSIDERATIONS:
        The row_configuration JSON object must follow the row_configuration_schema of the specified component.
        It can also adhere to the appropriate configuration examples if available.

    USAGE:
        - Use when you want to create a new root configuration for a specific component.
    EXAMPLES:
        - user_input: `Create a new configuration for component X with these settings`
            -> set the component_id and configuration parameters accordingly
            -> returns the created component configuration if successful.
    """

    client = KeboolaClient.from_state(ctx.session.state)

    LOG.info(
        f'Creating new configuration row: {name} for component: {component_id} '
        f'and configuration {configuration_id}.'
    )

    configuration_payload = {'storage': storage, 'parameters': row_configuration}
    # TODO validate parameters
    # Try to create the new configuration and return the new object if successful
    # or log an error and raise an exception if not
    new_raw_configuration = await client.storage_client.create_component_row_configuration(
        component_id=component_id,
        config_id=configuration_id,
        branch_id=client.storage_client_sync._branch_id,
        data={
            'name': name,
            'description': description,
            'configuration': configuration_payload,
        },
    )

    new_configuration = ComponentRowConfiguration(
        **new_raw_configuration,
        component_id=component_id,
        storage=new_raw_configuration['configuration'].get('storage'),
        parameters=new_raw_configuration['configuration'].get('parameters'),
    )

    LOG.info(
        f'Created new configuration for component "{component_id}" with configuration id '
        f'"{new_configuration.configuration_id}".'
    )

    return new_configuration


@tool_errors()
async def update_component_root_configuration(
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
    change_description: Annotated[
        str,
        Field(
            description=('Description of the change made to the component configuration.'),
        ),
    ],
    component_id: Annotated[
        str,
        Field(
            description="The ID of the component which you'd like to update",
        ),
    ],
    configuration_id: Annotated[
        str,
        Field(
            description="The ID of the configuration which you'd like to update.",
        ),
    ],
    root_configuration: Annotated[
        dict[str, Any],
        Field(description='The root configuration parameters, adhering to the root_configuration_schema schema'),
    ],
    storage: Annotated[
        Optional[dict[str, Any]],
        Field(
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that are not row-based and have tables or file '
                'input mapping defined'
            ),
        ),
    ] = None,
) -> Annotated[
    ComponentRootConfiguration,
    Field(
        description='Created component root configuration object,',
    ),
]:
    """
    Updates a specific component configuration using given by component ID, and configuration ID.
    CONSIDERATIONS:
        The root_configuration JSON object must follow the root_configuration_schema of the specified component.
        It can also adhere to the appropriate configuration examples if available.
    USAGE:
        - Use when you want to update a root configuration of a specific component.
    EXAMPLES:
        - user_input: `Update a configuration for component X and configuration ID 1234 with these settings`
            -> set the component_id, configuration_id and configuration parameters accordingly.
            -> set the change_description to the description of the change made to the component configuration.
            -> returns the created component configuration if successful.
    """

    client = KeboolaClient.from_state(ctx.session.state)

    LOG.info(f'Updating configuration: {name} for component: {component_id} and configuration ID {configuration_id}.')

    configuration_payload = {'storage': storage, 'parameters': root_configuration}
    # TODO validate parameters
    # Try to create the new configuration and return the new object if successful
    # or log an error and raise an exception if not
    new_raw_configuration = await client.storage_client.update_component_root_configuration(
        component_id=component_id,
        config_id=configuration_id,
        branch_id=client.storage_client_sync._branch_id,
        data={
            'name': name,
            'description': description,
            'changeDescription': change_description,
            'configuration': configuration_payload,
        },
    )

    new_configuration = ComponentRootConfiguration(
        **new_raw_configuration,
        component_id=component_id,
        storage=new_raw_configuration['configuration'].get('storage'),
        parameters=new_raw_configuration['configuration'].get('parameters'),
    )

    LOG.info(
        f'Created new configuration for component "{component_id}" with configuration id '
        f'"{new_configuration.configuration_id}".'
    )

    return new_configuration


@tool_errors()
async def update_component_row_configuration(
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
    change_description: Annotated[
        str,
        Field(
            description=('Description of the change made to the component configuration.'),
        ),
    ],
    component_id: Annotated[
        str,
        Field(
            description="The ID of the component which you'd like to update",
        ),
    ],
    configuration_id: Annotated[
        str,
        Field(
            description="The ID of the configuration which you'd like to update.",
        ),
    ],
    configuration_row_id: Annotated[
        str,
        Field(
            description="The ID of the configuration row which you'd like to update.",
        ),
    ],
    row_configuration: Annotated[
        dict[str, Any],
        Field(description='The row configuration parameters, adhering to the row_configuration_schema'),
    ],
    storage: Annotated[
        Optional[dict[str, Any]],
        Field(
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ] = None,
) -> Annotated[
    ComponentRowConfiguration,
    Field(
        description='Created component row configuration object,',
    ),
]:
    """
    Updates a specific component configuration row in the specified configuration_id, using the specified name,
    component ID, configuration JSON, and description.

    CONSIDERATIONS:
        The row_configuration JSON object must follow the row_configuration_schema of the specified component.
        It can also adhere to the appropriate configuration examples if available.

    USAGE:
        - Use when you want to update a row configuration for a specific component and configuration.
    EXAMPLES:
        - user_input: `Update a configuration row of configuration ID 123 for component X with these settings`
            -> set the component_id, configuration_id, configuration_row_id and configuration parameters accordingly
            -> returns the created component configuration if successful.
    """

    client = KeboolaClient.from_state(ctx.session.state)

    LOG.info(
        f'Updating configuration row: {name} for component: {component_id}, configuration id {configuration_id} '
        f'and row id {configuration_row_id}.'
    )

    configuration_payload = {'storage': storage, 'parameters': row_configuration}
    # TODO validate parameters
    # Try to create the new configuration and return the new object if successful
    # or log an error and raise an exception if not
    new_raw_configuration = await client.storage_client.update_component_row_configuration(
        component_id=component_id,
        config_id=configuration_id,
        configuration_row_id=configuration_row_id,
        branch_id=client.storage_client_sync._branch_id,
        data={
            'name': name,
            'description': description,
            'changeDescription': change_description,
            'configuration': configuration_payload,
        },
    )

    new_configuration = ComponentRowConfiguration(
        **new_raw_configuration,
        component_id=component_id,
        storage=new_raw_configuration['configuration'].get('storage'),
        parameters=new_raw_configuration['configuration'].get('parameters'),
    )

    LOG.info(
        f'Created new configuration for component "{component_id}" with configuration id '
        f'"{new_configuration.configuration_id}".'
    )

    return new_configuration

@tool_errors()
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
    client = KeboolaClient.from_state(ctx.session.state)
    raw_component = await client.ai_service_client.get_component_detail(component_id)
    root_examples = raw_component.get('rootConfigurationExamples') or []
    row_examples = raw_component.get('rowConfigurationExamples') or []

    markdown = f'# Configuration Examples for `{component_id}`\n\n'

    if root_examples:
        markdown += '## Root Configuration Examples\n\n'
        for i, example in enumerate(root_examples, 1):
            markdown += f'{i}. Root Configuration:\n```json\n{json.dumps(example, indent=2)}\n```\n\n'

    if row_examples:
        markdown += '## Row Configuration Examples\n\n'
        for i, example in enumerate(row_examples, 1):
            markdown += f'{i}. Row Configuration:\n```json\n{json.dumps(example, indent=2)}\n```\n\n'

    return markdown


@tool_errors()
async def find_component_id(
    ctx: Context,
    query: Annotated[
        str,
        Field(
            description='Natural language query to find the requested component.',
        ),
    ],
) -> list[SuggestedComponent]:
    """
    Returns list of component IDs that match the given query.
    USAGE:
        - Use when you want to find the component for a specific purpose.
    EXAMPLES:
        - user_input: `I am looking for a salesforce extractor component`
            -> returns a list of component IDs that match the query, ordered by relevance/best match.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    suggestion_response = await client.ai_service_client.suggest_component(query)
    return suggestion_response.components


# End of component tools #########################################
