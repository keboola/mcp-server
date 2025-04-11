import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast, get_args

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator, validator

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)


############################## Add tools to the MCP server #########################################

RETRIEVE_COMPONENT_CONFIGURATIONS_TOOL_NAME: str = "retrieve_components_in_project"
RETRIEVE_TRANSFORMATION_CONFIGURATIONS_TOOL_NAME: str = "retrieve_transformations_in_project"
GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME: str = "get_component_details"


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(
        get_component_configuration_details, name=GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME
    )
    logger.info(f"Added tool {GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME} to the MCP server.")

    mcp.add_tool(
        retrieve_components_configurations, name=RETRIEVE_COMPONENT_CONFIGURATIONS_TOOL_NAME
    )
    logger.info(f"Added tool {RETRIEVE_COMPONENT_CONFIGURATIONS_TOOL_NAME} to the MCP server.")

    mcp.add_tool(
        retrieve_transformations_configurations,
        name=RETRIEVE_TRANSFORMATION_CONFIGURATIONS_TOOL_NAME,
    )
    logger.info(f"Added tool {RETRIEVE_TRANSFORMATION_CONFIGURATIONS_TOOL_NAME} to the MCP server.")

    mcp.add_tool(
        create_snowflake_transformation
    )
    logger.info(f"Added tool {create_snowflake_transformation.__name__} to the MCP server.")

    logger.info("Component tools initialized.")


############################## Base Models to #########################################

FULLY_QUALIFIED_ID_SEPARATOR: str = "::"


class ReducedComponent(BaseModel):
    """
    A Reduced Component containing reduced information about the Keboola Component used in a list.
    """

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("id", "component_id", "componentId", "component-id"),
        serialization_alias="componentId",
    )
    component_name: str = Field(
        description="The name of the component",
        validation_alias=AliasChoices(
            "name",
            "component_name",
            "componentName",
            "component-name",
        ),
        serialization_alias="componentName",
    )
    component_type: str = Field(
        description="The type of the component",
        validation_alias=AliasChoices("type", "component_type"),
        serialization_alias="componentType",
    )
    component_description: Optional[str] = Field(
        description="The description of the component",
        default=None,
        validation_alias=AliasChoices("description", "component_description"),
        serialization_alias="componentDescription",
    )


class ReducedComponentConfigurationPair(BaseModel):
    """
    A Reduced Component Configuration containing Keboola Component ID and the reduced information about configuration
    used in a list.
    """

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("component_id", "componentId"),
        serialization_alias="componentId",
    )
    configuration_id: str = Field(
        description="The ID of the component configuration",
        validation_alias=AliasChoices(
            "id",
            "configuration_id",
            "configurationId",
            "configuration-id",
        ),
        serialization_alias="configurationId",
    )
    configuration_name: str = Field(
        description="The name of the component configuration",
        validation_alias=AliasChoices(
            "name",
            "configuration_name",
            "configurationName",
            "configuration-name",
        ),
        serialization_alias="configurationName",
    )
    configuration_description: Optional[str] = Field(
        description="The description of the component configuration",
        validation_alias=AliasChoices(
            "description",
            "configuration_description",
            "configurationDescription",
            "configuration-description",
        ),
        serialization_alias="configurationDescription",
    )
    is_disabled: bool = Field(
        description="Whether the component configuration is disabled",
        validation_alias=AliasChoices("isDisabled", "is_disabled", "is-disabled"),
        serialization_alias="isDisabled",
        default=False,
    )
    is_deleted: bool = Field(
        description="Whether the component configuration is deleted",
        validation_alias=AliasChoices("isDeleted", "is_deleted", "is-deleted"),
        serialization_alias="isDeleted",
        default=False,
    )
    fully_qualified_id: Optional[str] = Field(
        description=(
            "The fully qualified ID of the component configuration by which it can be uniquely identified. "
            "It is a concatenation of the component ID and the configuration ID, separated by a `::`."
        ),
        validation_alias=AliasChoices(
            "fullyQualifiedId", "fully_qualified_id", "fully-qualified-id"
        ),
        serialization_alias="fullyQualifiedId",
        default=None,
    )

    def __init__(self, **data):
        super().__init__(**data)
        if self.fully_qualified_id is None:
            if self.configuration_id and self.component_id:
                self.fully_qualified_id = f"{self.component_id}::{self.configuration_id}"


class ComponentWithConfigurations(BaseModel):
    """
    Grouping of a Keboola Component and its associated configurations.
    """

    component: ReducedComponent = Field(description="The Keboola component.")
    configurations: List[ReducedComponentConfigurationPair] = Field(
        description="The list of component configurations for the given component."
    )


class Component(ReducedComponent):
    """
    Detailed information about a Keboola Component, containing all the relevant details.
    """

    long_description: Optional[str] = Field(
        description="The long description of the component",
        default=None,
        validation_alias=AliasChoices("longDescription", "long_description", "long-description"),
        serialization_alias="longDescription",
    )
    categories: List[str] = Field(description="The categories of the component", default=[])
    version: int = Field(description="The version of the component")
    data: Optional[Dict[str, Any]] = Field(description="The data of the component", default=None)
    flags: Optional[List[str]] = Field(description="The flags of the component", default=None)
    configuration_schema: Optional[Dict[str, Any]] = Field(
        description=(
            "The configuration schema of the component, detailing the structure and requirements of the "
            "configuration."
        ),
        validation_alias=AliasChoices(
            "configurationSchema", "configuration_schema", "configuration-schema"
        ),
        serialization_alias="configurationSchema",
        default=None,
    )
    configuration_description: Optional[str] = Field(
        description="The configuration description of the component",
        validation_alias=AliasChoices(
            "configurationDescription", "configuration_description", "configuration-description"
        ),
        serialization_alias="configurationDescription",
        default=None,
    )
    empty_configuration: Optional[Dict[str, Any]] = Field(
        description="The empty configuration of the component",
        validation_alias=AliasChoices(
            "emptyConfiguration", "empty_configuration", "empty-configuration"
        ),
        serialization_alias="emptyConfiguration",
        default=None,
    )


class ComponentConfigurationPair(ReducedComponentConfigurationPair):
    """
    Detailed information about a Keboola Component Configuration, containing all the relevant details.
    """

    version: int = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(description="The configuration of the component")
    rows: Optional[List[Dict[str, Any]]] = Field(
        description="The rows of the component configuration", default=None
    )
    configuration_metadata: List[Dict[str, Any]] = Field(
        description="The metadata of the component configuration",
        default=[],
        validation_alias=AliasChoices(
            "metadata", "configuration_metadata", "configurationMetadata"
        ),
        serialization_alias="configurationMetadata",
    )
    component: Optional[Component] = Field(
        description="The Keboola component.",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
        default=None,
    )


############################## End of Base Models #########################################

############################## Utility functions #########################################

ComponentType = Literal["application", "extractor", "writer", "all"]
TransformationType = Literal["transformation"]
AllComponentTypes = Union[ComponentType, TransformationType]


def _handle_component_types(
    types: Union[ComponentType, List[ComponentType]],
) -> List[ComponentType]:
    """
    Utility function to handle the component types [extractors, writers, applications, all]
    If the types include "all", it will be removed and the remaining types will be returned.
    :param types: The component types/type to process.
    :return: The processed component types.
    """
    if isinstance(types, str):
        types = [types]
    if "all" in types:
        return [c_type for c_type in get_args(ComponentType) if c_type != "all"]
    return types


async def _retrieve_components_configurations_by_types(
    client: KeboolaClient, component_types: List[AllComponentTypes]
) -> List[ComponentWithConfigurations]:
    """
    Utility function to retrieve components with configurations by types - used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations
    :param client: The Keboola client
    :param component_types: The component types/type to retrieve
    :return: a list of items, each containing a component and its associated configurations
    """

    endpoint = f"branch/{client.storage_client._branch_id}/components"
    # retrieve components by types - unable to use list of types as parameter, we need to iterate over types

    raw_components_with_configs = []
    for type in component_types:
        # retrieve components by type with configurations
        params = {
            "include": "configuration",
            "componentType": type,
        }
        raw_components_configs_by_type = await client.get(endpoint, params=params)
        # extend the list with the raw components with configurations
        raw_components_with_configs.extend(cast(List[Dict[str, Any]], raw_components_configs_by_type))

    # build components with configurations list, each item contains a component and its associated configurations
    components_with_configs = [
        ComponentWithConfigurations(
            component=ReducedComponent.model_validate(raw_component),
            configurations=[
                ReducedComponentConfigurationPair.model_validate(
                    {**raw_config, "component_id": raw_component["id"]}
                )
                for raw_config in raw_component.get("configurations", [])
            ],
        )
        for raw_component in raw_components_with_configs
    ]

    # perform logging
    total_configurations = sum(
        len(component.configurations) for component in components_with_configs
    )
    logger.info(
        f"Found {len(components_with_configs)} components with total of {total_configurations} configurations "
        f"for types {component_types}."
    )
    return components_with_configs


async def _retrieve_components_configurations_by_ids(
    client: KeboolaClient, component_ids: List[str]
) -> List[ComponentWithConfigurations]:
    """
    Utility function to retrieve components configurations by ids - used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations
    :param client: The Keboola client
    :param component_ids: The component IDs to retrieve
    :return: a list of items, each containing a component and its associated configurations
    """
    components_with_configs = []
    for component_id in component_ids:
        # retrieve configurations for component ids
        raw_configurations = client.storage_client.configurations.list(component_id)
        # retrieve component details
        endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}"
        raw_component = await client.get(endpoint)
        # build component configurations list grouped by components
        components_with_configs.append(
            ComponentWithConfigurations(
                component=ReducedComponent.model_validate(raw_component),
                configurations=[
                    ReducedComponentConfigurationPair.model_validate(
                        {**raw_config, "component_id": raw_component["id"]}
                    )
                    for raw_config in raw_configurations
                ],
            )
        )

    # perform logging
    total_configurations = sum(
        len(component.configurations) for component in components_with_configs
    )
    logger.info(
        f"Found {len(components_with_configs)} components with total of {total_configurations} configurations "
        f"for ids {component_ids}."
    )
    return components_with_configs


async def _get_component_details(
    client: KeboolaClient,
    component_id: str,
) -> Component:
    """
    Utility function to retrieve the component details by component ID, used in tools:
    - get_component_configuration_details
    :param component_id: The ID of the Keboola component/transformation you want details about
    :param client: The Keboola client
    :return: The component details
    """

    endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}"
    raw_component = await client.get(endpoint)
    logger.info(f"Retrieved component details for component {component_id}.")
    return Component.model_validate(raw_component)


############################## End of utility functions #########################################

############################## Component tools #########################################


async def retrieve_components_configurations(
    ctx: Context,
    component_types: Annotated[
        List[ComponentType],
        Field(
            description="Array of component types to filter by, default is ['all']",
            default=["all"],
        ),
    ] = ["all"],
    component_ids: Annotated[
        List[str],
        Field(
            description="List of component IDs to retrieve configurations for, default is []",
            default=[],
        ),
    ] = [],
) -> Annotated[
    List[ComponentWithConfigurations],
    Field(
        List[ComponentWithConfigurations],
        description="List of objects, each containing a component and its associated configurations.",
    ),
]:
    """
    Retrieve components configurations in the project based on specified component_types or component_ids.
    If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding component_types.
    USAGE:
        - Use when you want to see components configurations in the project for given component_types.
        - Use when you want to see components configurations in the project for given component_ids.
    EXAMPLES:
        - user_input: `give me all components`
            -> set types to ["all"]
            -> returns all components configurations in the project
        - user_input: `list me all extractor components`
            -> set types to ["extractor"]
            -> returns all extractor components configurations in the project
        - user_input: `give me configurations for following component/s` | `give me configurations for this component`
            -> set types to ["all"], component_ids to list of identifiers accordingly if you know them
            -> returns all configurations for the given components
        - user_input: `give me configurations for 'specified-id'`
            -> set component_ids to ['specified-id']
            -> returns the configurations of the component with ID 'specified-id'
    """
    # If no component IDs are provided, retrieve component configurations by types (default is all types)
    if not component_ids:
        client = KeboolaClient.from_state(ctx.session.state)
        component_types = _handle_component_types(component_types)
        return await _retrieve_components_configurations_by_types(
            client, cast(List[AllComponentTypes], component_types)
        )
    # If component IDs are provided, retrieve component configurations by IDs
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_ids(client, component_ids)


async def retrieve_transformations_configurations(
    ctx: Context,
    transformation_ids: Annotated[
        List[str],
        Field(
            description="List of transformation component IDs to retrieve configurations for, default is []",
            default=[],
        ),
    ] = [],
) -> Annotated[
    List[ComponentWithConfigurations],
    Field(
        List[ComponentWithConfigurations],
        description="List of objects, each containing a transformation component and its associated configurations.",
    ),
]:
    """
    Retrieve transformations in the project for specific transformation IDs or all.
    USAGE:
        - Use when you want to see transformation configurations in the project for given transformation_ids.
        - Use when you want to retrieve all transformation configurations, then set transformation_ids to an empty list.
    EXAMPLES:
        - user_input: `give me all transformations`
            -> set transformation_ids to []
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
        return await _retrieve_components_configurations_by_types(client, ["transformation"])
    # If transformation IDs are provided, retrieve transformations configurations by IDs
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_ids(client, transformation_ids)


async def get_component_configuration_details(
    component_id: Annotated[
        str, Field(str, description="Unique identifier of the Keboola component/transformation")
    ],
    configuration_id: Annotated[
        str,
        Field(
            str,
            description="Unique identifier of the Keboola component/transformation configuration you want details about",
        ),
    ],
    ctx: Context,
) -> Annotated[
    ComponentConfigurationPair,
    Field(
        ComponentConfigurationPair,
        description="Detailed information about a Keboola component/transformation and its configuration.",
    ),
]:
    """
    Get detailed information about a specific Keboola component configuration given component/transformation ID and
    configuration ID. Those IDs can be retrieved from fully_qualified_id of the component configuration which are
    seperated by `::`.
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
    logger.info(
        f"Retrieved configuration details for component configuration {component_id}::{configuration_id}."
    )

    # Get Configuration Metadata if exists
    endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}/configs/{configuration_id}/metadata"
    r_metadata = await client.get(endpoint)
    if r_metadata:
        logger.info(
            f"Retrieved configuration metadata for component configuration {component_id}::{configuration_id}."
        )
    else:
        logger.info(
            f"No metadata found for component configuration {component_id}::{configuration_id}."
        )

    # Create Component Configuration Detail Object
    return ComponentConfigurationPair.model_validate(
        {
            **raw_configuration,
            "component": component,
            "component_id": component_id,
            "metadata": r_metadata,
        }
    )

class SnowFlakeConfigurationParameters(BaseModel):
    """The parameters for the snowflake sql transformation."""
    class Block(BaseModel):
        """The block for the snowflake sql transformation."""
        class Code(BaseModel):
            """
            The code for the snowflake sql transformation block.
            - each sql statement should end with a semicolon
            - each table name should be quoted using double quotes
            """
            name: str = Field(description="The name of the current code script")
            script: list[str] = Field(description="List of SQL statements")
        name: str = Field(description="The name of the current block")
        codes: list[Code] = Field(description="The code scripts")
    blocks: list[Block] = Field(description="The blocks for the transformation")


class SnowFlakeConfigurationStorage(BaseModel):
    """The storage configuration for the transformation.
    Stores the input and output tables for the transformation.
    """
    class Destination(BaseModel):
        class Table(BaseModel):
            """The table to be used for the transformation"""
            destination: str = Field(description="The destination of the table for the transformation")
            source: str = Field(description="The source table name for the transformation (mapping used in the sql query)")
        tables: list[Table] = Field(description="The tables to be used for the transformation")

    input: Optional[Destination] = Field(description="The input tables for the transformation", default = None)
    output: Optional[Destination] = Field(description="The output tables for the transformation", default = None)


async def create_snowflake_transformation(
    ctx: Context,
    name: Annotated[
        str,
        Field(
            str,
            description="The name of the snowflake transformation",
        ),
    ],
    sql_statements: Annotated[
        list[str],
        Field(
            list[str],
            description="The snowflake exacutable sql query statemenets each ending with a semicolon and using double quotes for table names.",
        ),
    ],
    description: Annotated[
        str,
        Field(
            str,
            description="The description of the snowflake transformation",
        ),
    ] = '',
    input_table_names: Annotated[
        list[tuple[str, str]],
        Field(
            list[tuple[str, str]],
            description="List of tuples, each containing the full input table name (bucket.table) and its table name (mapping) used in the sql transformation query",
        ),
    ] = [],
    output_table_names: Annotated[
        list[str],
        Field(
            list[str],
            description="The names of the output tables which are used in and created by the sql transformation",
        ),
    ] = [],
    bucket_name: Annotated[
        str,
        Field(
            str,
            description="Only the name of the bucket to use for the output tables",
        ),
    ] = "experimental-bucket",
) -> Optional[ComponentConfigurationPair]:
    """
    Create a snowflake sql transformation from the given name, sql query, description, and output table names
    CONSIDERATIONS:
        - Each statement in the query is executable and must end with a semicolon.
        - Each table name in the query should be quoted using double quotes.
        - Each created table within the query should be added to the output table names list.
        - Each input table used in the query along with its full bucket name should be added to the input table names list.
        - If the user does not specify, transformation name and description are generated based on the sql query and user intent.
    USAGE:
        - Use when you want to create a new snowflake transformation from a sql query.
    EXAMPLE:
        - user_input: `Can you save me the query as transformation you generated?`
            -> set the sql_query to the query if you know it, and set other parameters accordingly.
            -> returns the created snowflake transformation configuration if successful.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    SNOWFLAKE_TRANSFORMATION_ID = 'keboola.snowflake-transformation'
    endpoint = f"branch/{client.storage_client._branch_id}/components/{SNOWFLAKE_TRANSFORMATION_ID}/configs"
    output = None # empty
    input = None # empty
    if output_table_names:
        output = SnowFlakeConfigurationStorage.Destination(
                tables=[
                    SnowFlakeConfigurationStorage.Destination.Table(
                        destination = #"out.c-" + "-".join(name.split()) + "." + table_name,
                        (
                            f"{bucket_name}"
                            if table_name == bucket_name.split('.')[-1] else 
                            f"{bucket_name}.{table_name}"
                        )
                        if "out.c-" in f"{bucket_name}" else
                        (
                            f"out.c-{bucket_name}"
                            if table_name == bucket_name.split('.')[-1] else
                            f"out.c-{bucket_name}.{table_name}"
                        ),
                        source=table_name
                    ) for table_name in output_table_names
                ]
            )
    if input_table_names:
        input = SnowFlakeConfigurationStorage.Destination(
            tables = [
            SnowFlakeConfigurationStorage.Destination.Table(
                source= full_bucket_table_name if "in.c-" in full_bucket_table_name else f"in.c-{full_bucket_table_name}",
                destination=table_name
            )
            for full_bucket_table_name, table_name in input_table_names
        ])

    storage = SnowFlakeConfigurationStorage(
        input=input,
        output=output
    )
    sql_parameters = SnowFlakeConfigurationParameters(
        blocks=[
            SnowFlakeConfigurationParameters.Block(
                name=f"block-0",
                codes=[
                    SnowFlakeConfigurationParameters.Block.Code(
                    name=f"code-0",
                    script=[sql_statements[j] for j in range(len(sql_statements))] 
                )]
            )
        ]
    )
    configuration = {
        "parameters": sql_parameters.model_dump(),
        "storage": {
            "input": storage.input.model_dump() if storage.input else {},
            "output": storage.output.model_dump() if storage.output else {}
        }
    }
    ret = await client.post(endpoint, data={
        "name": name,
        "description": description,
        "configuration": configuration
    })
    return ComponentConfigurationPair(**ret, component_id=SNOWFLAKE_TRANSFORMATION_ID, component=None)

############################## End of component tools #########################################
