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
    SimplifiedTfBlocks,
    TfParamUpdate,
    TransformationConfiguration,
)
from keboola_mcp_server.tools.components.utils import (
    BIGQUERY_TRANSFORMATION_ID,
    SNOWFLAKE_TRANSFORMATION_ID,
    add_ids,
    create_transformation_configuration,
    expand_component_types,
    fetch_component,
    get_sql_transformation_id_from_sql_dialect,
    list_configs_by_ids,
    list_configs_by_types,
    set_cfg_creation_metadata,
    set_cfg_update_metadata,
    set_nested_value,
    update_params,
    update_transformation_parameters,
)
from keboola_mcp_server.tools.validation import (
    validate_processors_configuration,
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
        Field(
            description=(
                'Filter by component types. Options: "application", "extractor", "transformation", "writer". '
                'Empty list [] means ALL component types will be returned '
                '(application, extractor, transformation, writer). '
                'This parameter is IGNORED when component_ids is provided (non-empty).'
            )
        ),
    ] = tuple(),
    component_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'Filter by specific component IDs (e.g., ["keboola.ex-db-mysql", "keboola.wr-google-sheets"]). '
                'Empty list [] uses component_types filtering instead. '
                'When provided (non-empty), this parameter takes PRECEDENCE over component_types '
                'and component_types is IGNORED.'
            )
        ),
    ] = tuple(),
) -> ListConfigsOutput:
    """
    Lists all component configurations in the project with optional filtering by component type or specific
    component IDs.

    Returns a list of components, each containing:
    - Component metadata (ID, name, type, description)
    - All configurations for that component
    - Links to the Keboola UI

    PARAMETER BEHAVIOR:
    - If component_ids is provided (non-empty): Returns ONLY those specific components, component_types is IGNORED
    - If component_ids is empty [] and component_types is empty []: Returns ALL component types
      (application, extractor, transformation, writer)
    - If component_ids is empty [] and component_types has values: Returns components matching ONLY those types

    WHEN TO USE:
    - User asks for "all configurations" or "list configurations" → Use component_types=[], component_ids=[]
    - User asks for specific component types (e.g., "extractors", "writers") → Use component_types with specific types
    - User asks for "all transformations" or "list transformations" → Use component_types=["transformation"]
    - User asks for specific component by ID → Use component_ids with the specific ID(s)

    EXAMPLES:
    - user_input: "Show me all components in the project"
      → component_types=[], component_ids=[]
      → Returns ALL component types (application, extractor, transformation, writer) with their configurations

    - user_input: "List all extractor configurations"
      → component_types=["extractor"], component_ids=[]
      → Returns only extractor component configurations

    - user_input: "Show me all extractors and writers"
      → component_types=["extractor", "writer"], component_ids=[]
      → Returns extractor and writer configurations only

    - user_input: "List all transformations"
      → component_types=["transformation"], component_ids=[]
      → Returns transformation configurations only

    - user_input: "Show me configurations for keboola.ex-db-mysql"
      → component_types=[], component_ids=["keboola.ex-db-mysql"]
      → Returns only configurations for the MySQL extractor (component_types is ignored)

    - user_input: "Get configs for these components: ex-db-mysql and wr-google-sheets"
      → component_types=[], component_ids=["keboola.ex-db-mysql", "keboola.wr-google-sheets"]
      → Returns configurations for both specified components (component_types is ignored)
    """
    # If no component IDs are provided, retrieve component configurations by types (default is all types)
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    if not component_ids:
        component_types = expand_component_types(component_types)
        components_with_configurations = await list_configs_by_types(client, component_types)
    # If component IDs are provided, retrieve component configurations by IDs
    else:
        components_with_configurations = await list_configs_by_ids(client, component_ids)

    links = [links_manager.get_used_components_link(), links_manager.get_transformations_dashboard_link()]

    return ListConfigsOutput(components_with_configurations=components_with_configurations, links=links)


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

    if component_id in (SNOWFLAKE_TRANSFORMATION_ID, BIGQUERY_TRANSFORMATION_ID):
        original_parameters = TransformationConfiguration.Parameters.model_validate(
            configuration.configuration_root.parameters
        )
        simplified_parameters: SimplifiedTfBlocks = await original_parameters.to_simplified_parameters()
        configuration.configuration_root.parameters = add_ids(simplified_parameters.model_dump())

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
        Sequence[SimplifiedTfBlocks.Block.Code],
        Field(
            description=(
                'The SQL query code blocks, each containing a descriptive name and an executable SQL script '
                'written in the current SQL dialect. The query will be automatically reformatted to be more readable.'
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
    LOG.info(f'Creating transformation. SQL dialect: {sql_dialect}, using transformation ID: {component_id}')

    # Process the data to be stored in the transformation configuration - parameters(sql statements)
    # and storage (input and output tables)
    transformation_configuration_payload = await create_transformation_configuration(
        codes=sql_code_blocks, transformation_name=name, output_tables=created_table_names, sql_dialect=sql_dialect
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
    configuration_id: Annotated[str, Field(description='The ID of the transformation configuration to update.')],
    change_description: Annotated[
        str,
        Field(
            description=(
                'A clear, human-readable summary of what changed in this transformation update. '
                'Be specific: e.g., "Added JOIN with customers table", "Updated WHERE clause to filter active records".'
            ),
        ),
    ],
    parameter_updates: Annotated[
        list[TfParamUpdate],
        Field(
            description=(
                'List of operations to apply to the transformation structure (blocks, codes, SQL scripts). '
                'Each operation modifies specific elements using block_id and code_id identifiers. '
                'Only provide if updating SQL code or block structure - do not use for description or storage changes. '
                '\n\n'
                'IMPORTANT: Use get_config first to retrieve the current transformation structure and identify '
                'the block_id and code_id values needed for your operations. IDs are automatically assigned.\n'
                '\n'
                'Available operations:\n'
                '1. add_block: Add a new block to the transformation\n'
                '   - Fields: op="add_block", block={name, codes}, position="start"|"end"\n'
                '2. remove_block: Remove an existing block\n'
                '   - Fields: op="remove_block", block_id (e.g., "b0")\n'
                '3. rename_block: Rename an existing block\n'
                '   - Fields: op="rename_block", block_id (e.g., "b0"), block_name\n'
                '4. add_code: Add a new code block to an existing block\n'
                '   - Fields: op="add_code", block_id (e.g., "b0"), code={name, script}, position="start"|"end"\n'
                '5. remove_code: Remove an existing code block\n'
                '   - Fields: op="remove_code", block_id (e.g., "b0"), code_id (e.g., "b0.c0")\n'
                '6. rename_code: Rename an existing code block\n'
                '   - Fields: op="rename_code", block_id (e.g., "b0"), code_id (e.g., "b0.c0"), code_name\n'
                '7. set_code: Replace the entire SQL script of a code block\n'
                '   - Fields: op="set_code", block_id (e.g., "b0"), code_id (e.g., "b0.c0"), script\n'
                '8. add_script: Append or prepend SQL to a code block\n'
                '   - Fields: op="add_script", block_id (e.g., "b0"), code_id (e.g., "b0.c0"), script,'
                '     position="start"|"end"\n'
                '9. str_replace: Replace substring in SQL scripts\n'
                '   - Fields: op="str_replace", search_for, replace_with, block_id (optional), code_id (optional)\n'
                '   - If block_id omitted: replaces in all blocks\n'
                '   - If code_id omitted: replaces in all codes of the specified block\n'
            ),
        ),
    ] = None,
    storage: Annotated[
        dict[str, Any],
        Field(
            description=(
                'Complete storage configuration for transformation input/output table mappings. '
                'Only provide if updating storage mappings - this replaces the ENTIRE storage configuration. '
                '\n\n'
                'When to use:\n'
                '- Adding/removing input tables for the transformation\n'
                '- Modifying output table mappings and destinations\n'
                '- Changing table aliases used in SQL\n'
                '\n'
                'Important:\n'
                '- Must conform to transformation storage schema (input/output tables)\n'
                '- Replaces ALL existing storage config - include all mappings you want to keep\n'
                '- Use get_config first to see current storage configuration\n'
                '- Leave unfilled to preserve existing storage configuration'
            )
        ),
    ] = None,
    updated_description: Annotated[
        str,
        Field(
            description=(
                'New detailed description for the transformation. Only provide if changing the description. '
                'Should explain what the transformation does, data sources, and business logic. '
                'Leave empty to preserve the original description.'
            ),
        ),
    ] = '',
    is_disabled: Annotated[
        bool,
        Field(
            description=(
                'Whether to disable the transformation. Set to True to disable execution without deleting. '
                'Default is False (transformation remains enabled).'
            ),
        ),
    ] = False,
) -> ConfigToolOutput:
    """
    Updates an existing SQL transformation configuration by modifying its SQL code, storage mappings, or description.

    This tool allows PARTIAL parameter updates for transformation SQL blocks and code - you only need to provide
    the operations you want to perform. All other fields will remain unchanged.
    Use this for modifying SQL transformations created with create_sql_transformation.

    WHEN TO USE:
    - Modifying SQL queries in transformation (add/edit/remove SQL statements)
    - Updating transformation block or code block names
    - Changing input/output table mappings for the transformation
    - Updating the transformation description
    - Enabling or disabling the transformation
    - Any combination of the above

    PREREQUISITES:
    - Transformation must already exist (use create_sql_transformation for new transformations)
    - You must know the configuration_id of the transformation
    - SQL dialect is determined automatically from the workspace
    - CRITICAL: Use get_config first to see the current transformation structure and get block_id/code_id values

    TRANSFORMATION STRUCTURE:
    A transformation has this hierarchy:
      transformation
      └─ blocks[] - List of transformation blocks (each has a unique block_id)
         └─ block.name - Descriptive name for the block
         └─ block.codes[] - List of code blocks within the block (each has a unique code_id)
            └─ code.name - Descriptive name for the code block
            └─ code.script - SQL script (string with SQL statements)

    Example structure from get_config:
    {
      "blocks": [
        {
          "id": "b0",  ← block_id needed for operations (format: b{index})
          "name": "Data Preparation",
          "codes": [
            {
              "id": "b0.c0",  ← code_id needed for operations (format: b{block_index}.c{code_index})
              "name": "Load customers",
              "script": "SELECT * FROM customers WHERE status = 'active';"
            }
          ]
        }
      ]
    }

    PARAMETER UPDATE OPERATIONS:
    All operations use block_id and code_id to identify elements (get these from get_config first).

    ID Format:
    - block_id: "b0", "b1", "b2", etc. (format: b{index})
    - code_id: "b0.c0", "b0.c1", "b1.c0", etc. (format: b{block_index}.c{code_index})

    1. BLOCK OPERATIONS:
       - add_block: Create a new block in the transformation
         {"op": "add_block", "block": {"name": "New Block", "codes": []}, "position": "end"}

       - remove_block: Delete an entire block
         {"op": "remove_block", "block_id": "b0"}

       - rename_block: Change a block's name
         {"op": "rename_block", "block_id": "b2", "block_name": "Updated Name"}

    2. CODE BLOCK OPERATIONS:
       - add_code: Create a new code block within an existing block
         {"op": "add_code", "block_id": "b1", "code": {"name": "New Code", "script": "SELECT 1;"}, "position": "end"}

       - remove_code: Delete a code block
         {"op": "remove_code", "block_id": "b0", "code_id": "b0.c0"}

       - rename_code: Change a code block's name
         {"op": "rename_code", "block_id": "b1", "code_id": "b1.c2", "code_name": "Updated Name"}

    3. SQL SCRIPT OPERATIONS:
       - set_code: Replace the entire SQL script (overwrites existing)
         {"op": "set_code", "block_id": "b0", "code_id": "b0.c0", "script": "SELECT * FROM new_table;"}

       - add_script: Append or prepend SQL to existing script (preserves existing)
         {"op": "add_script", "block_id": "b2", "code_id": "b2.c1", "script": "WHERE date > '2024-01-01'",
          "position": "end"}

       - str_replace: Find and replace text in SQL scripts
         {"op": "str_replace", "search_for": "old_table", "replace_with": "new_table", "block_id": "b0",'
          "code_id": "b0.c0"}
         - Omit code_id to replace in all codes of a block
         - Omit both block_id and code_id to replace everywhere

    IMPORTANT CONSIDERATIONS:
    - Parameter updates are PARTIAL - only the operations you specify are applied
    - All other parts of the transformation remain unchanged
    - Each SQL script must be executable and follow the current SQL dialect
    - Storage configuration is COMPLETE REPLACEMENT - include ALL mappings you want to keep
    - Leave updated_description empty to preserve the original description
    - SCHEMA CHANGES: Destructive schema changes (removing columns, changing types, renaming columns) require
      manually deleting the output table before running the updated transformation to avoid schema mismatch errors.
      Non-destructive changes (adding columns) typically do not require table deletion.

    WORKFLOW:
    1. Call get_config to retrieve current transformation structure and identify block_id/code_id values
    2. Identify what needs to change (SQL code, storage, description)
    3. For SQL changes: Prepare parameter_updates list with targeted operations
    4. For storage changes: Build complete storage configuration (include all mappings)
    5. Call update_sql_transformation with change_description and only the fields to change

    EXAMPLE WORKFLOWS:

    Example 1 - Update SQL script in existing code block:
    Step 1: Get current config
      result = get_config(component_id="keboola.snowflake-transformation", configuration_id="12345")
      # Note the block_id (e.g., "b0") and code_id (e.g., "b0.c1") from result

    Step 2: Update the SQL
      update_sql_transformation(
        configuration_id="12345",
        change_description="Updated WHERE clause to filter active customers only",
        parameter_updates=[
          {
            "op": "set_code",
            "block_id": "b0",      # from step 1
            "code_id": "b0.c0",    # from step 1
            "script": "SELECT * FROM customers WHERE status = 'active' AND region = 'US';"
          }
        ]
      )

    Example 2 - Append a new code block to the second block of an existing transformation:
      update_sql_transformation(
        configuration_id="12345",
        change_description="Added aggregation step",
        parameter_updates=[
          {
            "op": "add_code",
            "block_id": "b1",  # second block
            "code": {
              "name": "Aggregate Sales",
              "script": "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id;"
            },
            "position": "end"
          }
        ]
      )

    Example 3 - Replace table name across all SQL scripts:
      update_sql_transformation(
        configuration_id="12345",
        change_description="Renamed source table from old_customers to customers",
        parameter_updates=[
          {
            "op": "str_replace",
            "search_for": "old_customers",
            "replace_with": "customers"
            # No block_id or code_id = applies to all scripts
          }
        ]
      )

    Example 4 - Update storage mappings:
      update_sql_transformation(
        configuration_id="12345",
        change_description="Added new input table",
        storage={
          "input": {
            "tables": [
              {
                "source": "in.c-main.customers",
                "destination": "customers"
              },
              {
                "source": "in.c-main.orders",
                "destination": "orders"
              }
            ]
          },
          "output": {
            "tables": [
              {
                "source": "result",
                "destination": "out.c-main.customer_summary"
              }
            ]
          }
        }
      )
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    sql_dialect = await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()

    sql_transformation_id = get_sql_transformation_id_from_sql_dialect(sql_dialect)

    config_details = await client.storage_client.configuration_detail(
        component_id=sql_transformation_id, configuration_id=configuration_id
    )
    api_component = await fetch_component(client=client, component_id=sql_transformation_id)
    transformation = Component.from_api_response(api_component)

    updated_configuration = config_details.get('configuration', {})

    msg = ''

    if parameter_updates:
        current_param_dict = updated_configuration.get('parameters', {})
        current_raw_parameters = TransformationConfiguration.Parameters.model_validate(current_param_dict)
        simplified_parameters = await current_raw_parameters.to_simplified_parameters()

        updated_params, msg = update_transformation_parameters(
            parameters=simplified_parameters,
            updates=parameter_updates,
            sql_dialect=sql_dialect,
        )
        updated_raw_parameters = await updated_params.to_raw_parameters()

        parameters_cfg = validate_root_parameters_configuration(
            component=transformation,
            parameters=updated_raw_parameters.model_dump(exclude_none=True),
            initial_message='Applying the "parameter_updates" resulted in an invalid configuration.',
        )
        updated_configuration['parameters'] = parameters_cfg

    if storage is not None:
        storage_cfg = validate_root_storage_configuration(
            component=transformation,
            storage=storage,
            initial_message='The "storage" field is not valid.',
        )
        updated_configuration['storage'] = storage_cfg

    LOG.info(
        f'Updating transformation: {sql_transformation_id} with config ID: {configuration_id}. '
        f'SQL dialect: {sql_dialect}'
    )

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
        change_summary=msg,
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
    processors_before: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run before the configured component runs.'),
    ] = None,
    processors_after: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run after the configured component runs.'),
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

    if processors_before:
        processors_before = await validate_processors_configuration(
            client=client,
            processors=processors_before,
            initial_message='The "processors_before" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.before', processors_before)

    if processors_after:
        processors_after = await validate_processors_configuration(
            client=client,
            processors=processors_after,
            initial_message='The "processors_after" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.after', processors_after)

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
    processors_before: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run before the configured component row runs.'),
    ] = None,
    processors_after: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run after the configured component row runs.'),
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

    if processors_before:
        processors_before = await validate_processors_configuration(
            client=client,
            processors=processors_before,
            initial_message='The "processors_before" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.before', processors_before)

    if processors_after:
        processors_after = await validate_processors_configuration(
            client=client,
            processors=processors_after,
            initial_message='The "processors_after" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.after', processors_after)

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
                'Each operation (set, str_replace, remove, list_append) modifies a specific '
                'value using JSONPath notation. Only provide if updating parameters -'
                ' do not use for changing description, storage or processors. '
                'Prefer simple dot-delimited JSONPaths '
                'and make the smallest possible updates - only change what needs changing. '
                'In case you need to replace the whole parameters section, you can use the `set` operation '
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
    processors_before: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run before the configured component row runs.'),
    ] = None,
    processors_after: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run after the configured component row runs.'),
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
    - parameter_updates supports granular operations: set keys, replace strings, remove keys, or append to lists
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

    configuration_payload = cast(JsonDict, current_config.get('configuration', {})).copy()

    if storage is not None:
        storage_cfg = validate_root_storage_configuration(
            component=component,
            storage=storage,
            initial_message='The "storage" field is not valid.',
        )
        configuration_payload['storage'] = storage_cfg

    if processors_before is not None:
        processors_before = await validate_processors_configuration(
            client=client,
            processors=processors_before,
            initial_message='The "processors_before" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.before', processors_before)

    if processors_after is not None:
        processors_after = await validate_processors_configuration(
            client=client,
            processors=processors_after,
            initial_message='The "processors_after" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.after', processors_after)

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
                'Each operation (set, str_replace, remove, list_append) modifies a specific '
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
    processors_before: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run before the configured component row runs.'),
    ] = None,
    processors_after: Annotated[
        list[dict[str, Any]],
        Field(description='The list of processors that will run after the configured component row runs.'),
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

    configuration_payload = cast(JsonDict, current_row.get('configuration', {})).copy()

    if storage is not None:
        storage_cfg = validate_row_storage_configuration(
            component=component,
            storage=storage,
            initial_message='The "storage" field is not valid.',
        )
        configuration_payload['storage'] = storage_cfg

    if processors_before is not None:
        processors_before = await validate_processors_configuration(
            client=client,
            processors=processors_before,
            initial_message='The "processors_before" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.before', processors_before)

    if processors_after is not None:
        processors_after = await validate_processors_configuration(
            client=client,
            processors=processors_after,
            initial_message='The "processors_after" field is not valid.',
        )
        set_nested_value(configuration_payload, 'processors.after', processors_after)

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
