import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast, get_args

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.sql_tools import get_sql_dialect

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

    mcp.add_tool(create_sql_transformation)
    logger.info(f"Added tool {create_sql_transformation.__name__} to the MCP server.")

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
        raw_components_with_configs.extend(
            cast(List[Dict[str, Any]], raw_components_configs_by_type)
        )

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


def _get_sql_transformation_id_from_sql_dialect(
    sql_dialect: str,
) -> str:
    """
    Utility function to retrieve the SQL transformation ID from the given SQL dialect.
    :param sql_dialect: The SQL dialect
    :return: The SQL transformation ID
    :raises ValueError: If the SQL dialect is not supported
    """
    if sql_dialect.lower() == "snowflake":
        return "keboola.snowflake-transformation"
    elif sql_dialect.lower() == "bigquery":
        return "keboola.bigquery-transformation"
    else:
        raise ValueError(f"Unsupported SQL dialect: {sql_dialect}")


class TransformationConfiguration(BaseModel):
    """The configuration for the transformation."""

    class Parameters(BaseModel):
        """The parameters for the transformation."""

        class Block(BaseModel):
            """The block for the transformation."""

            class Code(BaseModel):
                """The code for the transformation block."""

                name: str = Field(description="The name of the current code script")
                script: list[str] = Field(description="List of current code statements")

            name: str = Field(description="The name of the current block")
            codes: list[Code] = Field(description="The code scripts")

        blocks: list[Block] = Field(description="The blocks for the transformation")

    class Storage(BaseModel):
        """The storage configuration for the transformation. For now it stores only input and output tables."""

        class Destination(BaseModel):
            """Tables' destinations for the transformation. Either input or output tables."""

            class Table(BaseModel):
                """The table used in the transformation"""

                destination: Optional[str] = Field(
                    description="The destination table name", default=None
                )
                source: Optional[str] = Field(description="The source table name", default=None)

            tables: list[Table] = Field(
                description="The tables used in the transformation", default=[]
            )

        input: Optional[Destination] = Field(
            description="The input tables for the transformation", default=None
        )
        output: Optional[Destination] = Field(
            description="The output tables for the transformation", default=None
        )

    parameters: Parameters = Field(description="The parameters for the transformation")
    storage: Storage = Field(description="The storage configuration for the transformation")


def _get_transformation_configuration(
    sql_statements: list[str],
    input_table_names: list[tuple[str, str]],
    output_table_names: list[str],
    output_bucket_name: str,
) -> TransformationConfiguration:
    """
    Utility function to set the transformation configuration from SQL statements, input and output table names, and
    bucket name. It creates the expected configuration for the transformation, parameters and storage.
    :param sql_statements: The SQL statements
    :param input_table_names: The input table names
    :param output_table_names: The output table names
    :param bucket_name: The bucket name
    :return: The storage configuration - supports input and output tables only
    """
    # handle output bucket name generated/copied by LLM (stochastic), it should follow: out.c-bucket_name
    output_bucket_name = (
        output_bucket_name
        if output_bucket_name.startswith("out.c-")
        else f"out.c-{output_bucket_name}"
    )
    # init Storage Configuration with empty input and output tables
    storage = TransformationConfiguration.Storage()
    # build input table configuration if input table names are provided
    if input_table_names:
        storage.input = TransformationConfiguration.Storage.Destination(
            tables=[
                TransformationConfiguration.Storage.Destination.Table(
                    source=(
                        (
                            full_bucket_table_name
                            if table_name == full_bucket_table_name.split(".")[-1]
                            else f"{full_bucket_table_name}.{table_name}"
                        )
                        if "in.c-" in full_bucket_table_name
                        else (
                            f"in.c-{full_bucket_table_name}"
                            if table_name == full_bucket_table_name.split(".")[-1]
                            else f"in.c-{full_bucket_table_name}.{table_name}"
                        )
                    ),
                    destination=table_name,
                )
                for full_bucket_table_name, table_name in input_table_names
            ]
        )
    # build output table configuration if output table names are provided
    if output_table_names:
        storage.output = TransformationConfiguration.Storage.Destination(
            tables=[
                TransformationConfiguration.Storage.Destination.Table(
                    source=table_name,
                    # handle full bucket table name generated/copied by LLM (stochastic)
                    # it should follow: out.c-bucket_name.table_name
                    destination=(
                        output_bucket_name
                        if table_name == output_bucket_name.split(".")[-1]
                        else f"{output_bucket_name}.{table_name}"
                    ),
                )
                for table_name in output_table_names
            ]
        )
    # build parameters configuration out of SQL statements
    parameters = TransformationConfiguration.Parameters(
        blocks=[
            TransformationConfiguration.Parameters.Block(
                name=f"Block 0",
                codes=[
                    TransformationConfiguration.Parameters.Block.Code(
                        name=f"Code 0", script=[statement for statement in sql_statements]
                    )
                ],
            )
        ]
    )
    return TransformationConfiguration(parameters=parameters, storage=storage)


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
    If component_ids are supplied, only those components identified by the IDs are retrieved, disregarding
    component_types.
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
            description=(
                "Unique identifier of the Keboola component/transformation configuration you want details about"
            ),
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


async def create_sql_transformation(
    ctx: Context,
    name: Annotated[
        str,
        Field(
            str,
            description="The name of the SQL transformation expressing the sql functionality.",
        ),
    ],
    description: Annotated[
        str,
        Field(
            str,
            description=(
                "The detailed description of the SQL transformation capturing the user intent, explaining the "
                "SQL query, and the expected output."
            ),
        ),
    ],
    sql_statements: Annotated[
        list[str],
        Field(
            list[str],
            description=(
                "The SQL exacutable query statemenets following the current SQL dialect. Each statement is a "
                "separate item in the list."
            ),
        ),
    ],
    output_table_names: Annotated[
        list[str],
        Field(
            list[str],
            description=(
                "Optional list of the names of the output tables which are used in and created by the SQL query."
            ),
        ),
    ] = [],
    bucket_name: Annotated[
        str,
        Field(
            str,
            description="The name of the bucket to use for the output tables.",
        ),
    ] = "experimental-bucket",
    input_table_names: Annotated[
        list[tuple[str, str]],
        Field(
            list[tuple[str, str]],
            description=(
                "Optional list of tuples, each containing the full input table name (bucket.table) and its table name "
                "(mapping) used in the SQL query."
            ),
        ),
    ] = [],
) -> Annotated[
    ComponentConfigurationPair,
    Field(
        ComponentConfigurationPair,
        description="Newly created SQL Transformation Configuration.",
    ),
]:
    """
    Create an SQL transformation from the given name, sql query following current SQL dialect, description, output table
    names, and optionally with input tables.
    CONSIDERATIONS:
        - Each statement in the query is executable and must follow the current SQL dialect (BigQuery, Snowflake).
        - Each created table within the query should be added to the output table names list.
        - When using input tables having full bucket name specified within the query, then each table should be added to
        the input table names list along with its full bucket name.
        - Unless otherwise specified by user, transformation name and description are generated based on the sql query
        and user intent.
    USAGE:
        - Use when you want to create a new SQL transformation from a sql query.
    EXAMPLES:
        - user_input: `Can you save me the SQL you generated?`
            -> set the sql_statements to the query, and set other parameters accordingly.
            -> returns the created SQL transformation configuration if successful.
        - user_input: `Generate me an SQL transformation which [USER INTENT]`
            -> generate the query based on the [USER INTENT], and set other parameters accordingly.
            -> returns the created SQL transformation configuration if successful.
    """

    # Get the SQL dialect to use the correct transformation ID (Snowflake or BigQuery)
    # This can raise an exception if workspace is not set or different backend than BigQuery or Snowflake is used
    sql_dialect = await get_sql_dialect(ctx)
    transformation_id = _get_sql_transformation_id_from_sql_dialect(sql_dialect)
    logger.info(f"SQL dialect: {sql_dialect}, using transformation ID: {transformation_id}")

    # Process the data to be stored in the storage configuration
    configuration = _get_transformation_configuration(
        sql_statements, input_table_names, output_table_names, bucket_name
    )

    # Get the transformation configuration dictionary as required by the API
    config_dict = {
        "parameters": configuration.parameters.model_dump(),
        "storage": {
            # specify explicitly the input and output tables because they are required by the API
            # if input or output tables are not provided then we pass empty dicts as required by the API
            "input": (
                configuration.storage.input.model_dump() if configuration.storage.input else {}
            ),
            "output": (
                configuration.storage.output.model_dump() if configuration.storage.output else {}
            ),
        },
    }

    client = KeboolaClient.from_state(ctx.session.state)
    endpoint = f"branch/{client.storage_client._branch_id}/components/{transformation_id}/configs"
    # Try to create the new transformation configuration and return the full object if successful
    # or log an error and raise an exception if not
    try:
        # Create the new transformation configuration
        logger.info(
            f"Creating new transformation configuration: {name} for component: {transformation_id}."
        )
        new_raw_transformation_configuration = await client.post(
            endpoint,
            data={"name": name, "description": description, "configuration": config_dict},
        )
        component = await _get_component_details(client=client, component_id=transformation_id)
        new_transformation_configuration = ComponentConfigurationPair(
            **new_raw_transformation_configuration,
            component_id=transformation_id,
            component=component,
        )
        logger.info(
            f"Created new transformation configuration: {new_transformation_configuration.configuration_id} for "
            f"component: {transformation_id}."
        )
        return new_transformation_configuration
    except Exception as e:
        logger.error(f"Error creating new transformation configuration: {e}")
        raise e


############################## End of component tools #########################################
