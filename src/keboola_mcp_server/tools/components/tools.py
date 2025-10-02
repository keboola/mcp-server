"""
Keboola Component Management Tools for MCP Server.

This module provides the core tools for managing Keboola components and their configurations
through the Model Context Protocol (MCP) interface. It serves as the main entry point for
component-related operations in the MCP server.

## Tool Categories

### Component/Configuration Discovery
- `get_component`: Retrieve detailed component information including schemas
- `find_component_id`: Search for components by natural language query
- `get_config`: Retrieve detailed configuration with root + row structure
- `list_configs`: List all component configurations (with filtering)
- `get_config_examples`: Get sample configuration examples for a component

### Configuration Management
- `create_config`: Create new root component configurations
- `update_config`: Update existing root configurations
- `add_config_row`: Add new configuration rows to existing configurations
- `update_config_row`: Update existing configuration rows

### SQL Transformations
- `list_transformations`: List transformation configurations
- `create_sql_transformation`: Create new SQL transformations with code blocks
- `update_sql_transformation`: Update existing SQL transformation configurations
"""

import json
import logging
from datetime import datetime, timezone
from typing import Annotated, Any, Sequence, cast

from fastmcp import Context
from fastmcp.tools import FunctionTool
from httpx import HTTPStatusError
from mcp.types import ToolAnnotations
from pydantic import Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import ConfigurationAPIResponse, JsonDict
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.components.model import (
    Component,
    ComponentSummary,
    ComponentType,
    ConfigParamUpdate,
    ConfigToolOutput,
    Configuration,
    ListConfigsOutput,
    ListTransformationsOutput,
)
from keboola_mcp_server.tools.components.utils import (
    TransformationConfiguration,
    fetch_component,
    get_sql_transformation_id_from_sql_dialect,
    get_transformation_configuration,
    handle_component_types,
    list_configs_by_ids,
    list_configs_by_types,
    set_cfg_creation_metadata,
    set_cfg_update_metadata,
    update_params,
)
from keboola_mcp_server.tools.validation import (
    validate_root_parameters_configuration,
    validate_root_storage_configuration,
    validate_row_parameters_configuration,
    validate_row_storage_configuration,
)
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

COMPONENT_TOOLS_TAG = 'components'


# ============================================================================
# TOOL REGISTRATION
# ============================================================================


def add_component_tools(mcp: KeboolaMcpServer) -> None:
    """Add tools to the MCP server."""
    # Component/Configuration discovery tools
    mcp.add_tool(
        FunctionTool.from_function(
            get_component,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_config,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            list_configs,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_config_examples,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )

    # Configuration management tools
    mcp.add_tool(
        FunctionTool.from_function(
            create_config,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            update_config,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            add_config_row,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            update_config_row,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )

    # SQL transformation tools
    mcp.add_tool(
        FunctionTool.from_function(
            list_transformations,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(readOnlyHint=True),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            create_sql_transformation,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=False),
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            update_sql_transformation,
            tags={COMPONENT_TOOLS_TAG},
            annotations=ToolAnnotations(destructiveHint=True),
        )
    )

    LOG.info('Component tools added to the MCP server.')


# ============================================================================
# Configuration LISTING TOOLS
# ============================================================================


@tool_errors()
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
        component_types = handle_component_types(component_types)  # if none, return all types
        components_with_configurations = await list_configs_by_types(client, component_types)
    # If component IDs are provided, retrieve component configurations by IDs
    else:
        components_with_configurations = await list_configs_by_ids(client, component_ids)
    links = [links_manager.get_used_components_link()]

    return ListConfigsOutput(components_with_configurations=components_with_configurations, links=links)


@tool_errors()
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
        components_with_configurations = await list_configs_by_types(client, ['transformation'])
    # If transformation IDs are provided, retrieve transformation configurations by IDs
    else:
        components_with_configurations = await list_configs_by_ids(client, transformation_ids)

    links = [links_manager.get_transformations_dashboard_link()]

    return ListTransformationsOutput(components_with_configurations=components_with_configurations, links=links)


# ============================================================================
# COMPONENT DISCOVERY TOOLS
# ============================================================================


@tool_errors()
async def get_component(
    ctx: Context,
    component_id: Annotated[str, Field(description='ID of the component/transformation')],
) -> Component:
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
    api_component = await fetch_component(client=client, component_id=component_id)
    return Component.from_api_response(api_component)


@tool_errors()
async def get_config(
    component_id: Annotated[str, Field(description='ID of the component/transformation')],
    configuration_id: Annotated[
        str,
        Field(
            description='ID of the component/transformation configuration',
        ),
    ],
    ctx: Context,
) -> Configuration:
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

    raw_configuration = cast(
        JsonDict,
        await client.storage_client.configuration_detail(component_id=component_id, configuration_id=configuration_id),
    )

    api_config = ConfigurationAPIResponse.model_validate(raw_configuration | {'component_id': component_id})
    api_component = await fetch_component(client=client, component_id=component_id)
    component_summary = ComponentSummary.from_api_response(api_component)

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=raw_configuration.get('name', ''),
    )

    configuration = Configuration.from_api_response(
        api_config=api_config,
        component=component_summary,
        links=links,
    )

    return configuration


# ============================================================================
# CONFIGURATION MANAGEMENT TOOLS
# ============================================================================


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
    sql_dialect = await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()
    component_id = get_sql_transformation_id_from_sql_dialect(sql_dialect)
    LOG.info(f'SQL dialect: {sql_dialect}, using transformation ID: {component_id}')

    # Process the data to be stored in the transformation configuration - parameters(sql statements)
    # and storage (input and output tables)
    transformation_configuration_payload = get_transformation_configuration(
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

    await set_cfg_creation_metadata(
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
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=links,
        version=new_raw_transformation_configuration['version'],
    )


@tool_errors()
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
                'applied settings and preserves all other existing settings. Only updated if provided.'
            ),
            json_schema_extra={'type': 'object'},
        ),
    ] = None,
    storage: Annotated[
        dict[str, Any],
        Field(
            description=(
                'The updated "storage" part of the transformation configuration that contains the newly '
                'applied settings and preserves all other existing settings. Only updated if provided.'
            ),
        ),
    ] = None,
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
    - SCHEMA CHANGES: If the transformation update results in a destructive
      schema change to the output table (such as removing columns, changing
      column types, or renaming columns), you MUST inform the user that they
      need to
      manually delete the output table completely before running the updated
      transformation. Otherwise, the transformation will fail with a schema
      mismatch error. Non-destructive changes (adding new columns) typically do
      not require table deletion.

    EXAMPLES:
    - user_input: `Can you edit this transformation configuration that [USER INTENT]?`
        - set the transformation configuration_id accordingly and update parameters and storage tool arguments based on
          the [USER INTENT]
        - returns the updated transformation configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    sql_dialect = await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()
    sql_transformation_id = get_sql_transformation_id_from_sql_dialect(sql_dialect)
    LOG.info(f'SQL transformation ID: {sql_transformation_id}')

    current_config = await client.storage_client.configuration_detail(
        component_id=sql_transformation_id, configuration_id=configuration_id
    )
    api_component = await fetch_component(client=client, component_id=sql_transformation_id)
    transformation = Component.from_api_response(api_component)

    updated_configuration = current_config.get('configuration', {})

    if parameters is not None:
        updated_configuration['parameters'] = parameters.model_dump(by_alias=True)

    if storage is not None:
        storage_cfg = validate_root_storage_configuration(
            component=transformation,
            storage=storage,
            initial_message='The "storage" field is not valid.',
        )
        updated_configuration['storage'] = storage_cfg

    LOG.info(f'Updating transformation: {sql_transformation_id} with configuration: {configuration_id}.')

    updated_raw_configuration = await client.storage_client.configuration_update(
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        configuration=updated_configuration,
        change_description=change_description,
        updated_description=updated_description,
        is_disabled=is_disabled,
    )

    await set_cfg_update_metadata(
        client=client,
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        configuration_version=updated_raw_configuration.get('version'),
    )

    links = links_manager.get_configuration_links(
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        configuration_name=updated_raw_configuration.get('name') or '',
    )

    LOG.info(
        f'Updated transformation configuration: {updated_raw_configuration["id"]} for '
        f'component: {sql_transformation_id}.'
    )

    return ConfigToolOutput(
        component_id=sql_transformation_id,
        configuration_id=configuration_id,
        description=updated_raw_configuration.get('description') or '',
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=links,
        version=updated_raw_configuration['version'],
    )


@tool_errors()
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
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ] = None,
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

    api_component = await fetch_component(client=client, component_id=component_id)
    component = Component.from_api_response(api_component)

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

    await set_cfg_creation_metadata(client, component_id, configuration_id)

    links = links_manager.get_configuration_links(
        component_id=component_id, configuration_id=configuration_id, configuration_name=name
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=description,
        version=new_raw_configuration['version'],
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=links,
    )


@tool_errors()
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
            description=(
                'The table and/or file input / output mapping of the component configuration. '
                'It is present only for components that have tables or file input mapping defined'
            ),
        ),
    ] = None,
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

    api_component = await fetch_component(client=client, component_id=component_id)
    component = Component.from_api_response(api_component)

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

    await set_cfg_update_metadata(
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
        version=new_raw_configuration['version'],
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=links,
    )


@tool_errors()
async def update_config(
    ctx: Context,
    change_description: Annotated[
        str,
        Field(
            description=(
                'A clear, human-readable summary of what changed in this update. '
                'Be specific: e.g., "Updated API key", "Added customers table to input mapping".'
            ),
        ),
    ],
    component_id: Annotated[str, Field(description='The ID of the component the configuration belongs to.')],
    configuration_id: Annotated[str, Field(description='The ID of the configuration to update.')],
    name: Annotated[
        str,
        Field(
            description=(
                'New name for the configuration. Only provide if changing the name. '
                'Name should be short (typically under 50 characters) and descriptive.'
            )
        ),
    ] = '',
    description: Annotated[
        str,
        Field(
            description=(
                'New detailed description for the configuration. Only provide if changing the description. '
                'Should explain the purpose, data sources, and behavior of this configuration.'
            ),
        ),
    ] = '',
    parameter_updates: Annotated[
        list[ConfigParamUpdate],
        Field(
            description=(
                'List of granular parameter update operations to apply. '
                'Each operation (set, str_replace, remove) modifies a specific '
                'parameter using JSONPath notation. Only provide if updating parameters -'
                ' do not use for changing description or storage. '
                'Prefer simple dot-delimited JSONPaths '
                'and make the smallest possible updates - only change what needs changing. '
                'In case you need to replace the whole parameters, you can use the `set` operation '
                'with `$` as path.'
            ),
        ),
    ] = None,
    storage: Annotated[
        dict[str, Any],
        Field(
            description=(
                'Complete storage configuration containing input/output table and file mappings. '
                'Only provide if updating storage mappings - this replaces the ENTIRE storage configuration. '
                '\n\n'
                'When to use:\n'
                '- Adding/removing input or output tables\n'
                '- Modifying table/file mappings\n'
                '- Updating table destinations or sources\n'
                '\n'
                'Important:\n'
                '- Not applicable for row-based components (they use row-level storage)\n'
                '- Must conform to the Keboola storage schema\n'
                '- Replaces ALL existing storage config - include all mappings you want to keep\n'
                '- Use get_config first to see current storage configuration\n'
                '- Leave unfilled to preserve existing storage configuration'
            )
        ),
    ] = None,
) -> ConfigToolOutput:
    """
    Updates an existing root component configuration by modifying its parameters, storage mappings, name or description.

    This tool allows PARTIAL parameter updates - you only need to provide the fields you want to change.
    All other fields will remain unchanged.
    Use this tool when modifying existing configurations; for configuration rows, use update_config_row instead.

    WHEN TO USE:
    - Modifying configuration parameters (credentials, settings, API keys, etc.)
    - Updating storage mappings (input/output tables or files)
    - Changing configuration name or description
    - Any combination of the above

    PREREQUISITES:
    - Configuration must already exist (use create_config for new configurations)
    - You must know both component_id and configuration_id
    - For parameter updates: Review the component's root_configuration_schema using get_component.
    - For storage updates: Ensure mappings are valid for the component type

    IMPORTANT CONSIDERATIONS:
    - Parameter updates are PARTIAL - only specify fields you want to change
    - parameter_updates supports granular operations: set individual keys, replace strings, or remove keys
    - Parameters must conform to the component's root_configuration_schema
    - Validate schemas before calling: use get_component to retrieve root_configuration_schema
    - For row-based components, this updates the ROOT only (use update_config_row for individual rows)

    WORKFLOW:
    1. Retrieve current configuration using get_config (to understand current state)
    2. Identify specific parameters/storage mappings to modify
    3. Prepare parameter_updates list with targeted operations
    4. Call update_config with only the fields to change
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(f'Updating configuration for component: {component_id} and configuration ID {configuration_id}.')

    current_config = await client.storage_client.configuration_detail(
        component_id=component_id, configuration_id=configuration_id
    )
    api_component = await fetch_component(client=client, component_id=component_id)
    component = Component.from_api_response(api_component)

    configuration_payload = current_config.get('configuration', {}).copy()

    if storage is not None:
        storage_cfg = validate_root_storage_configuration(
            component=component,
            storage=storage,
            initial_message='The "storage" field is not valid.',
        )
        configuration_payload['storage'] = storage_cfg

    if parameter_updates:
        current_params = configuration_payload.get('parameters', {})
        updated_params = update_params(current_params, parameter_updates)

        parameters_cfg = validate_root_parameters_configuration(
            component=component,
            parameters=updated_params,
            initial_message='Applying the "parameter_updates" resulted in an invalid configuration.',
        )
        configuration_payload['parameters'] = parameters_cfg

    updated_raw_configuration = await client.storage_client.configuration_update(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration=configuration_payload,
        change_description=change_description,
        updated_name=name,
        updated_description=description,
    )

    LOG.info(f'Updated configuration for component "{component_id}" with configuration id ' f'"{configuration_id}".')

    await set_cfg_update_metadata(
        client=client,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_version=updated_raw_configuration['version'],
    )

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=updated_raw_configuration.get('name') or '',
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=updated_raw_configuration.get('description') or '',
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=links,
        version=updated_raw_configuration['version'],
    )


@tool_errors()
async def update_config_row(
    ctx: Context,
    change_description: Annotated[
        str,
        Field(description=('A clear, human-readable summary of what changed in this row update. Be specific.')),
    ],
    component_id: Annotated[str, Field(description='The ID of the component the configuration belongs to.')],
    configuration_id: Annotated[
        str,
        Field(description='The ID of the parent configuration containing the row to update.'),
    ],
    configuration_row_id: Annotated[str, Field(description='The ID of the specific configuration row to update.')],
    name: Annotated[
        str,
        Field(
            description=(
                'New name for the configuration row. Only provide if changing the name. '
                'Name should be short (typically under 50 characters) and descriptive of this specific row.'
            )
        ),
    ] = '',
    description: Annotated[
        str,
        Field(
            description=(
                'New detailed description for the configuration row. Only provide if changing the description. '
                'Should explain the specific purpose and behavior of this individual row.'
            )
        ),
    ] = '',
    parameter_updates: Annotated[
        list[ConfigParamUpdate],
        Field(
            description=(
                'List of granular parameter update operations to apply to this row. '
                'Each operation (set, str_replace, remove) modifies a specific '
                'parameter using JSONPath notation. Only provide if updating parameters - '
                'do not use for changing description or storage. '
                'Prefer simple dot-delimited JSONPaths '
                'and make the smallest possible updates - only change what needs changing. '
                'In case you need to replace the whole parameters, you can use the `set` operation '
                'with `$` as path.'
            ),
        ),
    ] = None,
    storage: Annotated[
        dict[str, Any],
        Field(
            description=(
                'Complete storage configuration for this row containing input/output table and file mappings. '
                'Only provide if updating storage mappings - this replaces the ENTIRE storage configuration '
                'for this row. '
                '\n\n'
                'When to use:\n'
                '- Adding/removing input or output tables for this specific row\n'
                '- Modifying table/file mappings for this row\n'
                '- Updating table destinations or sources for this row\n'
                '\n'
                'Important:\n'
                "- Must conform to the component's row storage schema\n"
                '- Replaces ALL existing storage config for this row - include all mappings you want to keep\n'
                '- Use get_config first to see current row storage configuration\n'
                '- Leave unfilled to preserve existing storage configuration'
            )
        ),
    ] = None,
) -> ConfigToolOutput:
    """
    Updates an existing component configuration row by modifying its parameters, storage mappings, name, or description.

    This tool allows PARTIAL parameter updates - you only need to provide the fields you want to change.
    All other fields will remain unchanged.
    Configuration rows are individual items within a configuration, often representing separate data sources,
    tables, or endpoints that share the same component type and parent configuration settings.

    WHEN TO USE:
    - Modifying row-specific parameters (table sources, filters, credentials, etc.)
    - Updating storage mappings for a specific row (input/output tables or files)
    - Changing row name or description
    - Any combination of the above

    PREREQUISITES:
    - The configuration row must already exist (use add_config_row for new rows)
    - You must know component_id, configuration_id, and configuration_row_id
    - For parameter updates: Review the component's row_configuration_schema using get_component
    - For storage updates: Ensure mappings are valid for row-level storage

    IMPORTANT CONSIDERATIONS:
    - Parameter updates are PARTIAL - only specify fields you want to change
    - parameter_updates supports granular operations: set individual keys, replace strings, or remove keys
    - Parameters must conform to the component's row_configuration_schema (not root schema)
    - Validate schemas before calling: use get_component to retrieve row_configuration_schema
    - Each row operates independently - changes to one row don't affect others
    - Row-level storage is separate from root-level storage configuration

    WORKFLOW:
    1. Retrieve current configuration using get_config to see existing rows
    2. Identify the specific row to modify by its configuration_row_id
    3. Prepare parameter_updates list with targeted operations for this row
    4. Call update_config_row with only the fields to change
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    LOG.info(
        f'Updating configuration row for component: {component_id}, configuration id: {configuration_id}, '
        f'row id: {configuration_row_id}.'
    )

    current_row = await client.storage_client.configuration_row_detail(
        component_id=component_id, config_id=configuration_id, configuration_row_id=configuration_row_id
    )
    api_component = await fetch_component(client=client, component_id=component_id)
    component = Component.from_api_response(api_component)

    configuration_payload = current_row.get('configuration', {}).copy()

    if storage is not None:
        storage_cfg = validate_row_storage_configuration(
            component=component,
            storage=storage,
            initial_message='The "storage" field is not valid.',
        )
        configuration_payload['storage'] = storage_cfg

    if parameter_updates:
        current_params = configuration_payload.get('parameters', {})
        updated_params = update_params(current_params, parameter_updates)

        parameters_cfg = validate_row_parameters_configuration(
            component=component,
            parameters=updated_params,
            initial_message='Applying the "parameter_updates" resulted in an invalid row configuration.',
        )
        configuration_payload['parameters'] = parameters_cfg

    updated_raw_configuration = await client.storage_client.configuration_row_update(
        component_id=component_id,
        config_id=configuration_id,
        configuration_row_id=configuration_row_id,
        configuration=configuration_payload,
        change_description=change_description,
        updated_name=name,
        updated_description=description,
    )

    LOG.info(
        f'Updated configuration row for component: {component_id}, configuration id: {configuration_id}, '
        f'row id: {configuration_row_id}.'
    )

    await set_cfg_update_metadata(
        client=client,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_version=updated_raw_configuration['version'],
    )

    links = links_manager.get_configuration_links(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_name=updated_raw_configuration.get('name') or '',
    )

    return ConfigToolOutput(
        component_id=component_id,
        configuration_id=configuration_id,
        description=updated_raw_configuration.get('description') or '',
        timestamp=datetime.now(timezone.utc),
        success=True,
        links=links,
        version=updated_raw_configuration['version'],
    )


@tool_errors()
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
