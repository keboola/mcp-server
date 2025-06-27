import json
import logging
from datetime import datetime
from typing import Annotated, Any, Sequence, cast

from fastmcp import Context
from fastmcp.tools import FunctionTool
from httpx import HTTPStatusError
from pydantic import Field

from keboola_mcp_server.client import JsonDict, KeboolaClient, SuggestedComponent
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, listing_output_serializer, with_session_state
from keboola_mcp_server.tools.components.model import (
    Component,
    ComponentConfigurationOutput,
    ComponentConfigurationResponse,
    ComponentRootConfiguration,
    ComponentRowConfiguration,
    ComponentType,
    ConfigToolOutput,
    ListConfigsOutput,
    ListTransformationsOutput,
)
from keboola_mcp_server.tools.components.utils import (
    TransformationConfiguration,
    _get_component,
    _get_sql_transformation_id_from_sql_dialect,
    _get_transformation_configuration,
    _handle_component_types,
    _list_configs_by_ids,
    _list_configs_by_types,
)
from keboola_mcp_server.tools.sql import get_sql_dialect
from keboola_mcp_server.tools.validation import (
    validate_root_parameters_configuration,
    validate_root_storage_configuration,
    validate_row_parameters_configuration,
    validate_row_storage_configuration,
)

LOG = logging.getLogger(__name__)


# Add component tools to the MCP server #########################################
def add_component_tools(mcp: KeboolaMcpServer) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(FunctionTool.from_function(get_component))
    mcp.add_tool(FunctionTool.from_function(get_config))
    mcp.add_tool(FunctionTool.from_function(list_configs, serializer=listing_output_serializer))
    mcp.add_tool(FunctionTool.from_function(create_config))
    mcp.add_tool(FunctionTool.from_function(add_config_row))
    mcp.add_tool(FunctionTool.from_function(update_config))
    mcp.add_tool(FunctionTool.from_function(update_config_row))
    mcp.add_tool(FunctionTool.from_function(get_config_examples))
    mcp.add_tool(FunctionTool.from_function(find_component_id))
    mcp.add_tool(FunctionTool.from_function(create_sql_transformation))
    mcp.add_tool(FunctionTool.from_function(update_sql_transformation))
    mcp.add_tool(FunctionTool.from_function(list_transformations))

    LOG.info('Component tools added to the MCP server.')


# tools #########################################


@tool_errors()
@with_session_state()
async def list_configs(
    ctx: Context,
    component_types: Annotated[
        Sequence[ComponentType],
        Field(description='List of component types to filter by. If none, return all components.'),
    ] = tuple(),
    component_ids: Annotated[
        Sequence[str],
        Field(description='List of component IDs to retrieve configurations for. If none, return all components.'),
    ] = tuple(),
) -> ListConfigsOutput:
    """
    Retrieves configurations of components present in the project,
    optionally filtered by component types or specific component IDs.
    If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
    component_types.

    USAGE:
    - Use when you want to see components configurations in the project for given component_types.
    - Use when you want to see components configurations in the project for given component_ids.

    EXAMPLES:
    - user_input: `give me all components (in the project)`
        - returns all components configurations in the project
    - user_input: `list me all extractor components (in the project)`
        - set types to ["extractor"]
        - returns all extractor components configurations in the project
    - user_input: `give me configurations for following component/s` | `give me configurations for this component`
        - set component_ids to list of identifiers accordingly if you know them
        - returns all configurations for the given components in the project
    - user_input: `give me configurations for 'specified-id'`
        - set component_ids to ['specified-id']
        - returns the configurations of the component with ID 'specified-id'
    """
    # If no component IDs are provided, retrieve component configurations by types (default is all types)
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    if not component_ids:
        component_types = _handle_component_types(component_types)  # if none, return all types
        components_with_configurations = await _list_configs_by_types(client, component_types)
    # If component IDs are provided, retrieve component configurations by IDs
    else:
        components_with_configurations = await _list_configs_by_ids(client, component_ids)
    links = [links_manager.get_used_components_link()]

    return ListConfigsOutput(
        components_with_configurations=components_with_configurations, links=links
    )


@tool_errors()
@with_session_state()
async def list_transformations(
    ctx: Context,
    transformation_ids: Annotated[
        Sequence[str],
        Field(description='List of transformation component IDs to retrieve configurations for.'),
    ] = tuple(),
) -> ListTransformationsOutput:
    """
    Retrieves transformation configurations in the project, optionally filtered by specific transformation IDs.

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
    # If no transformation IDs are provided, retrieve transformation configurations by transformation type
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if not transformation_ids:
        components_with_configurations = await _list_configs_by_types(client, ['transformation'])
    # If transformation IDs are provided, retrieve transformation configurations by IDs
    else:
        components_with_configurations = await _list_configs_by_ids(client, transformation_ids)

    links = [links_manager.get_transformations_dashboard_link()]

    return ListTransformationsOutput(
        components_with_configurations=components_with_configurations, links=links
    )


@tool_errors()
@with_session_state()
async def get_component(
    ctx: Context,
    component_id: Annotated[str, Field(description='ID of the component/transformation')],
) -> Annotated[Component, Field(description='The component.')]:
    """
    Gets information about a specific component given its ID.

    USAGE:
    - Use when you want to see the details of a specific component to get its documentation, configuration schemas,
      etc. Especially in situation when the users asks to create or update a component configuration.
      This tool is mainly for internal use by the agent.

    EXAMPLES:
    - user_input: `Create a generic extractor configuration for x`
        - Set the component_id if you know it or find the component_id by find_component_id
          or docs use tool and set it
        - returns the component
    """
    client = KeboolaClient.from_state(ctx.session.state)
    return await _get_component(component_id=component_id, client=client)


@tool_errors()
@with_session_state()
async def get_config(
    component_id: Annotated[str, Field(description='ID of the component/transformation')],
    configuration_id: Annotated[
        str,
        Field(
            description='ID of the component/transformation configuration',
        ),
    ],
    ctx: Context,
) -> Annotated[ComponentConfigurationOutput, Field(description='The component/transformation and its configuration.')]:
    """
    Gets information about a specific component/transformation configuration.

    USAGE:
    - Use when you want to see the configuration of a specific component/transformation.

    EXAMPLES:
    - user_input: `give me details about this configuration`
        - set component_id and configuration_id to the specific component/transformation ID and configuration ID
          if you know it
        - returns the component/transformation configuration pair
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    component = await _get_component(client=client, component_id=component_id)
    raw_configuration = cast(
        JsonDict,
        await client.storage_client.configuration_detail(component_id=component_id, configuration_id=configuration_id),
    )
    configuration_response = ComponentConfigurationResponse.model_validate(
        raw_configuration | {'component_id': component_id}
    )

    # Create root configuration
    root_configuration = ComponentRootConfiguration.model_validate(
        configuration_response.model_dump()
        | {
            'parameters': configuration_response.configuration.get('parameters', {}),
            'storage': configuration_response.configuration.get('storage'),
        }
    )

    # Create row configurations if they exist
    row_configurations = None
    if configuration_response.rows:
        row_configurations = []
        for row in configuration_response.rows:
            if row is None:
                continue
            row_configuration = ComponentRowConfiguration.model_validate(
                row
                | {
                    'component_id': configuration_response.component_id,
                    'parameters': row.get('configuration', {}).get('parameters', {}),
                    'storage': row.get('configuration', {}).get('storage'),
                }
            )
            row_configurations.append(row_configuration)

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=raw_configuration.get('name', ''),
    )
    return ComponentConfigurationOutput(
        root_configuration=root_configuration, row_configurations=row_configurations, component=component, links=links
    )


async def _set_cfg_creation_metadata(client: KeboolaClient, component_id: str, configuration_id: str) -> None:
    """Sets configuration metadata to indicate it was created by MCP."""
    try:
        await client.storage_client.configuration_metadata_update(
            component_id=component_id,
            configuration_id=configuration_id,
            metadata={MetadataField.CREATED_BY_MCP: 'true'},
        )
    except HTTPStatusError as e:
        LOG.exception(
            f'Failed to set "{MetadataField.CREATED_BY_MCP}" metadata for configuration {configuration_id}: {e}'
        )


async def _set_cfg_update_metadata(
    client: KeboolaClient,
    component_id: str,
    configuration_id: str,
    configuration_version: int,
) -> None:
    """Sets configuration metadata to indicate it was updated by MCP."""
    updated_by_md_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{configuration_version}'
    try:
        await client.storage_client.configuration_metadata_update(
            component_id=component_id,
            configuration_id=configuration_id,
            metadata={updated_by_md_key: 'true'},
        )
    except HTTPStatusError as e:
        LOG.exception(f'Failed to set "{updated_by_md_key}" metadata for configuration {configuration_id}: {e}')


@tool_errors()
@with_session_state()
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
    sql_code_blocks: Annotated[
        Sequence[TransformationConfiguration.Parameters.Block.Code],
        Field(
            description=(
                'The SQL query code blocks, each containing a descriptive name and a sequence of '
                'semantically related independently executable sql_statements written in the current SQL dialect.'
            ),
        ),
    ],
    created_table_names: Annotated[
        Sequence[str],
        Field(
            description=(
                'A list of created table names if they are generated within the SQL query statements '
                '(e.g., using `CREATE TABLE ...`).'
            ),
        ),
    ] = tuple(),
) -> ConfigToolOutput:
    """
    Creates an SQL transformation using the specified name, SQL query following the current SQL dialect, a detailed
    description, and a list of created table names.

    CONSIDERATIONS:
    - By default, SQL transformation must create at least one table to produce a result; omit only if the user
      explicitly indicates that no table creation is needed.
    - Each SQL code block must include descriptive name that reflects its purpose and group one or more executable
      semantically related SQL statements.
    - Each SQL query statement within a code block must be executable and follow the current SQL dialect, which can be
      retrieved using appropriate tool.
    - When referring to the input tables within the SQL query, use fully qualified table names, which can be
      retrieved using appropriate tools.
    - When creating a new table within the SQL query (e.g. CREATE TABLE ...), use only the quoted table name without
      fully qualified table name, and add the plain table name without quotes to the `created_table_names` list.
    - Unless otherwise specified by user, transformation name and description are generated based on the SQL query
      and user intent.

    USAGE:
    - Use when you want to create a new SQL transformation.

    EXAMPLES:
    - user_input: `Can you create a new transformation out of this sql query?`
        - set the sql_code_blocks to the query, and set other parameters accordingly.
        - returns the created SQL transformation configuration if successful.
    - user_input: `Generate me an SQL transformation which [USER INTENT]`
        - set the sql_code_blocks to the query based on the [USER INTENT], and set other parameters accordingly.
        - returns the created SQL transformation configuration if successful.
    """

    # Get the SQL dialect to use the correct transformation ID (Snowflake or BigQuery)
    # This can raise an exception if workspace is not set or different backend than BigQuery or Snowflake is used
    sql_dialect = await get_sql_dialect(ctx)
    component_id = _get_sql_transformation_id_from_sql_dialect(sql_dialect)
    LOG.info(f'SQL dialect: {sql_dialect}, using transformation ID: {component_id}')

    # Process the data to be stored in the transformation configuration - parameters(sql statements)
    # and storage (input and output tables)
    transformation_configuration_payload = _get_transformation_configuration(
        codes=sql_code_blocks, transformation_name=name, output_tables=created_table_names
    )

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(f'Creating new transformation configuration: {name} for component: {component_id}.')
    new_raw_transformation_configuration = await client.storage_client.configuration_create(
        component_id=component_id,
        name=name,
        description=description,
        configuration=transformation_configuration_payload.model_dump(by_alias=True),
    )

    configuration_id = new_raw_transformation_configuration['id']

    await _set_cfg_creation_metadata(
        client=client,
        component_id=component_id,
        configuration_id=configuration_id,
    )

    LOG.info(f'Created new transformation "{component_id}" with configuration id ' f'"{configuration_id}".')

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=name,
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=description,
        timestamp=datetime.utcnow(),
        success=True,
        links=links,
    )


@tool_errors()
@with_session_state()
async def update_sql_transformation(
    ctx: Context,
    configuration_id: Annotated[str, Field(description='ID of the transformation configuration to update')],
    change_description: Annotated[
        str,
        Field(description='Description of the changes made to the transformation configuration.'),
    ],
    parameters: Annotated[
        TransformationConfiguration.Parameters,
        Field(
            description=(
                'The updated "parameters" part of the transformation configuration that contains the newly '
                'applied settings and preserves all other existing settings.'
            ),
        ),
    ],
    storage: Annotated[
        dict[str, Any],
        Field(
            description=(
                'The updated "storage" part of the transformation configuration that contains the newly '
                'applied settings and preserves all other existing settings.'
            ),
        ),
    ],
    updated_description: Annotated[
        str,
        Field(
            description='Updated transformation description reflecting the changes made in the behavior of '
            'the transformation. If no behavior changes are made, empty string preserves the original description.',
        ),
    ] = '',
    is_disabled: Annotated[
        bool,
        Field(
            description='Whether to disable the transformation configuration. Default is False.',
        ),
    ] = False,
) -> ConfigToolOutput:
    """
    Updates an existing SQL transformation configuration, optionally updating the description and disabling the
    configuration.

    CONSIDERATIONS:
    - The parameters configuration must include blocks with codes of SQL statements. Using one block with many codes of
      SQL statements is preferred and commonly used unless specified otherwise by the user.
    - Each code contains SQL statements that are semantically related and have a descriptive name.
    - Each SQL statement must be executable and follow the current SQL dialect, which can be retrieved using
      appropriate tool.
    - The storage configuration must not be empty, and it should include input or output tables with correct mappings
      for the transformation.
    - When the behavior of the transformation is not changed, the updated_description can be empty string.

    EXAMPLES:
    - user_input: `Can you edit this transformation configuration that [USER INTENT]?`
        - set the transformation configuration_id accordingly and update parameters and storage tool arguments based on
          the [USER INTENT]
        - returns the updated transformation configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    sql_transformation_id = _get_sql_transformation_id_from_sql_dialect(await get_sql_dialect(ctx))
    LOG.info(f'SQL transformation ID: {sql_transformation_id}')

    transformation = await _get_component(client=client, component_id=sql_transformation_id)

    storage = validate_root_storage_configuration(
        component=transformation,
        storage=storage,
        initial_message='The "storage" field is not valid.',
    )

    updated_configuration = {
        'parameters': parameters.model_dump(by_alias=True),
        'storage': storage,
    }

    LOG.info(f'Updating transformation: {sql_transformation_id} with configuration: {configuration_id}.')
    updated_raw_configuration = await client.storage_client.configuration_update(
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        configuration=updated_configuration,
        change_description=change_description,
        updated_description=updated_description if updated_description else None,
        is_disabled=is_disabled,
    )

    await _set_cfg_update_metadata(
        client=client,
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        configuration_version=updated_raw_configuration.get('version'),
    )

    links = links_manager.get_configuration_links(
        component_id=sql_transformation_id,
        configuration_id=str(configuration_id),
        configuration_name=str(updated_raw_configuration.get('name', '')),
    )

    LOG.info(
        f'Updated transformation configuration: {updated_raw_configuration["id"]} for '
        f'component: {sql_transformation_id}.'
    )

    return ConfigToolOutput(
        component_id=sql_transformation_id,
        configuration_id=str(configuration_id),
        description=updated_description or updated_raw_configuration.get('description', ''),
        timestamp=datetime.utcnow(),
        success=True,
        links=links,
    )


@tool_errors()
@with_session_state()
async def create_config(
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
    component_id: Annotated[str, Field(description='The ID of the component for which to create the configuration.')],
    parameters: Annotated[
        dict[str, Any],
        Field(description='The component configuration parameters, adhering to the root_configuration_schema'),
    ],
    storage: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ],
) -> ConfigToolOutput:
    """
    Creates a root component configuration using the specified name, component ID, configuration JSON, and description.

    CONSIDERATIONS:
    - The configuration JSON object must follow the root_configuration_schema of the specified component.
    - Make sure the configuration parameters always adhere to the root_configuration_schema,
      which is available via the component_detail tool.
    - The configuration JSON object should adhere to the component's configuration examples if found.

    USAGE:
    - Use when you want to create a new root configuration for a specific component.

    EXAMPLES:
    - user_input: `Create a new configuration for component X with these settings`
        - set the component_id and configuration parameters accordingly
        - returns the created component configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(f'Creating new configuration: {name} for component: {component_id}.')

    component = await _get_component(client=client, component_id=component_id)

    storage_cfg = validate_root_storage_configuration(
        component=component,
        storage=storage,
        initial_message='The "storage" field is not valid.',
    )
    parameters = validate_root_parameters_configuration(
        component=component,
        parameters=parameters,
        initial_message='The "parameters" field is not valid.',
    )

    configuration_payload = {'storage': storage_cfg, 'parameters': parameters}

    new_raw_configuration = cast(
        dict[str, Any],
        await client.storage_client.configuration_create(
            component_id=component_id,
            name=name,
            description=description,
            configuration=configuration_payload,
        ),
    )

    configuration_id = new_raw_configuration['id']

    LOG.info(f'Created new configuration for component "{component_id}" with configuration id "{configuration_id}".')

    await _set_cfg_creation_metadata(client, component_id, configuration_id)

    links = links_manager.get_configuration_links(
        component_id=component_id, configuration_id=configuration_id, configuration_name=name
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=description,
        timestamp=datetime.utcnow(),
        success=True,
        links=links,
    )


@tool_errors()
@with_session_state()
async def add_config_row(
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
    component_id: Annotated[str, Field(description='The ID of the component for which to create the configuration.')],
    configuration_id: Annotated[
        str,
        Field(
            description='The ID of the configuration for which to create the configuration row.',
        ),
    ],
    parameters: Annotated[
        dict[str, Any],
        Field(description='The component row configuration parameters, adhering to the row_configuration_schema'),
    ],
    storage: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ],
) -> ConfigToolOutput:
    """
    Creates a component configuration row in the specified configuration_id, using the specified name,
    component ID, configuration JSON, and description.

    CONSIDERATIONS:
    - The configuration JSON object must follow the row_configuration_schema of the specified component.
    - Make sure the configuration parameters always adhere to the row_configuration_schema,
      which is available via the component_detail tool.
    - The configuration JSON object should adhere to the component's configuration examples if found.

    USAGE:
    - Use when you want to create a new row configuration for a specific component configuration.

    EXAMPLES:
    - user_input: `Create a new configuration row for component X with these settings`
        - set the component_id, configuration_id and configuration parameters accordingly
        - returns the created component configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(
        f'Creating new configuration row: {name} for component: {component_id} '
        f'and configuration {configuration_id}.'
    )

    component = await _get_component(client=client, component_id=component_id)

    storage_cfg = validate_row_storage_configuration(
        component=component,
        storage=storage,
        initial_message='The "storage" field is not valid.',
    )
    parameters = validate_row_parameters_configuration(
        component=component,
        parameters=parameters,
        initial_message='The "parameters" field is not valid.',
    )

    configuration_payload = {'storage': storage_cfg, 'parameters': parameters}

    new_raw_configuration = cast(
        dict[str, Any],
        await client.storage_client.configuration_row_create(
            component_id=component_id,
            config_id=configuration_id,
            name=name,
            description=description,
            configuration=configuration_payload,
        ),
    )

    LOG.info(
        f'Created new configuration for component "{component_id}" with configuration id ' f'"{configuration_id}".'
    )

    await _set_cfg_update_metadata(
        client=client,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_version=new_raw_configuration['version'],
    )

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=name,
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=description,
        timestamp=datetime.utcnow(),
        success=True,
        links=links,
    )


@tool_errors()
@with_session_state()
async def update_config(
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
        Field(description='Description of the change made to the component configuration.'),
    ],
    component_id: Annotated[str, Field(description='The ID of the component the configuration belongs to.')],
    configuration_id: Annotated[str, Field(description='The ID of the configuration to update.')],
    parameters: Annotated[
        dict[str, Any],
        Field(description='The component configuration parameters, adhering to the root_configuration_schema schema'),
    ],
    storage: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that are not row-based and have tables or file '
                'input mapping defined'
            ),
        ),
    ],
) -> ConfigToolOutput:
    """
    Updates a specific root component configuration using given by component ID, and configuration ID.

    CONSIDERATIONS:
    - The configuration JSON object must follow the root_configuration_schema of the specified component.
    - Make sure the configuration parameters always adhere to the root_configuration_schema,
      which is available via the component_detail tool.
    - The configuration JSON object should adhere to the component's configuration examples if found

    USAGE:
    - Use when you want to update a root configuration of a specific component.

    EXAMPLES:
    - user_input: `Update a configuration for component X and configuration ID 1234 with these settings`
        - set the component_id, configuration_id and configuration parameters accordingly.
        - set the change_description to the description of the change made to the component configuration.
        - returns the updated component configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(f'Updating configuration: {name} for component: {component_id} and configuration ID {configuration_id}.')

    component = await _get_component(client=client, component_id=component_id)

    storage_cfg = validate_root_storage_configuration(
        component=component,
        storage=storage,
        initial_message='The "storage" field is not valid.',
    )
    parameters = validate_root_parameters_configuration(
        component=component,
        parameters=parameters,
        initial_message='The "parameters" field is not valid.',
    )

    configuration_payload = {'storage': storage_cfg, 'parameters': parameters}

    new_raw_configuration = cast(
        dict[str, Any],
        await client.storage_client.configuration_update(
            component_id=component_id,
            configuration_id=configuration_id,
            configuration=configuration_payload,
            change_description=change_description,
            updated_name=name,
            updated_description=description,
        ),
    )

    LOG.info(f'Updated configuration for component "{component_id}" with configuration id ' f'"{configuration_id}".')

    await _set_cfg_update_metadata(
        client=client,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_version=new_raw_configuration['version'],
    )

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=name,
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=description,
        timestamp=datetime.utcnow(),
        success=True,
        links=links,
    )


@tool_errors()
@with_session_state()
async def update_config_row(
    ctx: Context,
    name: Annotated[
        str,
        Field(description='A short, descriptive name summarizing the purpose of the component configuration.'),
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
            description='Description of the change made to the component configuration.',
        ),
    ],
    component_id: Annotated[str, Field(description='The ID of the component to update.')],
    configuration_id: Annotated[str, Field(description='The ID of the configuration to update.')],
    configuration_row_id: Annotated[str, Field(description='The ID of the configuration row to update.')],
    parameters: Annotated[
        dict[str, Any],
        Field(description='The component row configuration parameters, adhering to the row_configuration_schema'),
    ],
    storage: Annotated[
        dict[str, Any],
        Field(
            default_factory=dict,
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ],
) -> ConfigToolOutput:
    """
    Updates a specific component configuration row in the specified configuration_id, using the specified name,
    component ID, configuration JSON, and description.

    CONSIDERATIONS:
    - The configuration JSON object must follow the row_configuration_schema of the specified component.
    - Make sure the configuration parameters always adhere to the row_configuration_schema,
      which is available via the component_detail tool.

    USAGE:
    - Use when you want to update a row configuration for a specific component and configuration.

    EXAMPLES:
    - user_input: `Update a configuration row of configuration ID 123 for component X with these settings`
        - set the component_id, configuration_id, configuration_row_id and configuration parameters accordingly
        - returns the updated component configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(
        f'Updating configuration row: {name} for component: {component_id}, configuration id {configuration_id} '
        f'and row id {configuration_row_id}.'
    )

    component = await _get_component(client=client, component_id=component_id)

    storage_cfg = validate_row_storage_configuration(
        component=component,
        storage=storage,
        initial_message='The "storage" field is not valid.',
    )
    parameters = validate_row_parameters_configuration(
        component=component,
        parameters=parameters,
        initial_message='Field "parameters" is not valid.\n',
    )

    configuration_payload = {'storage': storage_cfg, 'parameters': parameters}

    new_raw_configuration = cast(
        dict[str, Any],
        await client.storage_client.configuration_row_update(
            component_id=component_id,
            config_id=configuration_id,
            configuration_row_id=configuration_row_id,
            configuration=configuration_payload,
            change_description=change_description,
            updated_name=name,
            updated_description=description,
        ),
    )

    LOG.info(f'Updated configuration for component "{component_id}" with configuration id ' f'"{configuration_id}".')

    await _set_cfg_update_metadata(
        client=client,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_version=new_raw_configuration['version'],
    )

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=name,
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=description,
        timestamp=datetime.utcnow(),
        success=True,
        links=links,
    )


@tool_errors()
@with_session_state()
async def get_config_examples(
    ctx: Context,
    component_id: Annotated[str, Field(description='The ID of the component to get configuration examples for.')],
) -> Annotated[
    str,
    Field(description='Markdown formatted string containing configuration examples for the component.'),
]:
    """
    Retrieves sample configuration examples for a specific component.

    USAGE:
    - Use when you want to see example configurations for a specific component.

    EXAMPLES:
    - user_input: `Show me example configurations for component X`
        - set the component_id parameter accordingly
        - returns a markdown formatted string with configuration examples
    """
    client = KeboolaClient.from_state(ctx.session.state)
    try:
        raw_component = await client.ai_service_client.get_component_detail(component_id)
    except HTTPStatusError:
        LOG.exception(f'Error when getting component details: {component_id}')
        return ''

    root_examples = raw_component.get('rootConfigurationExamples') or []
    row_examples = raw_component.get('rowConfigurationExamples') or []
    assert isinstance(root_examples, list)  # pylance check
    assert isinstance(row_examples, list)  # pylance check

    markdown = f'# Configuration Examples for `{component_id}`\n\n'

    if root_examples:
        markdown += '## Root Configuration Examples\n\n'
        for i, example in enumerate(root_examples, start=1):
            markdown += f'{i}. Root Configuration:\n```json\n{json.dumps(example, indent=2)}\n```\n\n'

    if row_examples:
        markdown += '## Row Configuration Examples\n\n'
        for i, example in enumerate(row_examples, start=1):
            markdown += f'{i}. Row Configuration:\n```json\n{json.dumps(example, indent=2)}\n```\n\n'

    return markdown


@tool_errors()
@with_session_state()
async def find_component_id(
    ctx: Context,
    query: Annotated[str, Field(description='Natural language query to find the requested component.')],
) -> list[SuggestedComponent]:
    """
    Returns list of component IDs that match the given query.

    USAGE:
    - Use when you want to find the component for a specific purpose.

    EXAMPLES:
    - user_input: `I am looking for a salesforce extractor component`
        - returns a list of component IDs that match the query, ordered by relevance/best match.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    suggestion_response = await client.ai_service_client.suggest_component(query)
    return suggestion_response.components
