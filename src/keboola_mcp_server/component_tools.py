import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Sequence, Union, cast, get_args

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.sql_tools import get_sql_dialect

LOG = logging.getLogger(__name__)


############################## Add tools to the MCP server #########################################

# Regarding the conventional naming of entity models for components and their associated configurations,
# we also unified and shortened function names to make them more intuitive and consistent for both users and LLMs.
# These tool names now reflect their conventional usage, removing redundant parts for users while still
# providing the same functionality as described in the original tool names.
RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME: str = "retrieve_components"
RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME: str = "retrieve_transformations"
GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME: str = "get_component_details"


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    mcp.add_tool(
        get_component_configuration_details, name=GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME
    )
    LOG.info(f"Added tool {GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME} to the MCP server.")

    mcp.add_tool(
        retrieve_components_configurations, name=RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME
    )
    LOG.info(f"Added tool {RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME} to the MCP server.")

    mcp.add_tool(
        retrieve_transformations_configurations,
        name=RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
    )
    LOG.info(f"Added tool {RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME} to the MCP server.")

    mcp.add_tool(create_sql_transformation)
    LOG.info(f"Added tool {create_sql_transformation.__name__} to the MCP server.")

    LOG.info("Component tools initialized.")


############################## Base Models to #########################################


class ReducedComponent(BaseModel):
    """
    A Reduced Component containing reduced information about the Keboola Component used in a list or comprehensive view.
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
        validation_alias=AliasChoices("type", "component_type", "componentType", "component-type"),
        serialization_alias="componentType",
    )
    component_description: Optional[str] = Field(
        description="The description of the component",
        default=None,
        validation_alias=AliasChoices(
            "description", "component_description", "componentDescription", "component-description"
        ),
        serialization_alias="componentDescription",
    )


class ReducedComponentConfiguration(BaseModel):
    """
    A Reduced Component Configuration containing the Keboola Component ID and the reduced information about configuration
    used in a list.
    """

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("component_id", "componentId", "component-id"),
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


class ComponentWithConfigurations(BaseModel):
    """
    Grouping of a Keboola Component and its associated configurations.
    """

    component: ReducedComponent = Field(description="The Keboola component.")
    configurations: List[ReducedComponentConfiguration] = Field(
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


class ComponentConfiguration(ReducedComponentConfiguration):
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
            "metadata", "configuration_metadata", "configurationMetadata", "configuration-metadata"
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

ComponentType = Literal["application", "extractor", "writer"]
TransformationType = Literal["transformation"]
AllComponentTypes = Union[ComponentType, TransformationType]


def _handle_component_types(
    types: Optional[Union[ComponentType, Sequence[ComponentType]]],
) -> Sequence[ComponentType]:
    """
    Utility function to handle the component types [extractors, writers, applications, all]
    If the types include "all", it will be removed and the remaining types will be returned.
    :param types: The component types/type to process.
    :return: The processed component types.
    """
    if not types:
        return [component_type for component_type in get_args(ComponentType)]
    if isinstance(types, str):
        types = [types]
    return types


async def _retrieve_components_configurations_by_types(
    client: KeboolaClient, component_types: Sequence[AllComponentTypes]
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

    raw_components_with_configurations = []
    for type in component_types:
        # retrieve components by type with configurations
        params = {
            "include": "configuration",
            "componentType": type,
        }
        raw_components_with_configurations_by_type = cast(
            List[Dict[str, Any]], await client.get(endpoint, params=params)
        )
        # extend the list with the raw components with configurations
        raw_components_with_configurations.extend(raw_components_with_configurations_by_type)

    # build components with configurations list, each item contains a component and its associated configurations
    components_with_configurations = [
        ComponentWithConfigurations(
            component=ReducedComponent.model_validate(raw_component),
            configurations=[
                ReducedComponentConfiguration.model_validate(
                    {**raw_configuration, "component_id": raw_component["id"]}
                )
                for raw_configuration in raw_component.get("configurations", [])
            ],
        )
        for raw_component in raw_components_with_configurations
    ]

    # perform logging
    total_configurations = sum(
        len(component.configurations) for component in components_with_configurations
    )
    LOG.info(
        f"Found {len(components_with_configurations)} components with total of {total_configurations} configurations "
        f"for types {component_types}."
    )
    return components_with_configurations


async def _retrieve_components_configurations_by_ids(
    client: KeboolaClient, component_ids: Sequence[str]
) -> List[ComponentWithConfigurations]:
    """
    Utility function to retrieve components with configurations by component IDs - used in tools:
    - retrieve_components_configurations
    - retrieve_transformation_configurations
    :param client: The Keboola client
    :param component_ids: The component IDs to retrieve
    :return: a list of items, each containing a component and its associated configurations
    """
    components_with_configurations = []
    for component_id in component_ids:
        # retrieve configurations for component ids
        raw_configurations = client.storage_client.configurations.list(component_id)
        # retrieve component details
        endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}"
        raw_component = await client.get(endpoint)
        # build component configurations list grouped by components
        components_with_configurations.append(
            ComponentWithConfigurations(
                component=ReducedComponent.model_validate(raw_component),
                configurations=[
                    ReducedComponentConfiguration.model_validate(
                        {**raw_configuration, "component_id": raw_component["id"]}
                    )
                    for raw_configuration in raw_configurations
                ],
            )
        )

    # perform logging
    total_configurations = sum(
        len(component.configurations) for component in components_with_configurations
    )
    LOG.info(
        f"Found {len(components_with_configurations)} components with total of {total_configurations} configurations "
        f"for ids {component_ids}."
    )
    return components_with_configurations


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
    LOG.info(f"Retrieved component details for component {component_id}.")
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
    """
    Utility class to create the transformation configuration, a schema for the transformation configuration in the API.
    Currently, the storage configuration uses only input and output tables, excluding files, etc.
    """

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

        input: Destination = Field(
            description="The input tables for the transformation", default=Destination()
        )
        output: Destination = Field(
            description="The output tables for the transformation", default=Destination()
        )

    parameters: Parameters = Field(description="The parameters for the transformation")
    storage: Storage = Field(description="The storage configuration for the transformation")


def _get_transformation_configuration(
    statements: Sequence[str], transformation_name: str, output_tables: Sequence[str]
) -> TransformationConfiguration:
    """
    Utility function to set the transformation configuration from code statements.
    It creates the expected configuration for the transformation, parameters and storage.
    :param statements: The code statements (sql for now)
    :param transformation_name: The name of the transformation from which the bucket name is derived as in the UI
    :param output_tables: The output tables of the transformation, created by the code statements
    :return: dictionary with parameters and storage following the TransformationConfiguration schema
    """
    # init Storage Configuration with empty input and output tables
    storage = TransformationConfiguration.Storage()
    # build parameters configuration out of code statements
    parameters = TransformationConfiguration.Parameters(
        blocks=[
            TransformationConfiguration.Parameters.Block(
                name=f"Block 0",
                codes=[
                    TransformationConfiguration.Parameters.Block.Code(
                        name=f"Code 0", script=list(statements)
                    )
                ],
            )
        ]
    )
    if output_tables:
        # if the query creates new tables, output_table_mappings should contain the table names (llm generated)
        # we create bucket name from the sql query name adding `out.c-` prefix as in the UI and use it as destination
        # expected output table name format is `out.c-<sql_query_name>.<table_name>`
        bucket_name = "-".join(transformation_name.lower().split())
        destination = f"out.c-{bucket_name}"
        storage.output.tables = [
            TransformationConfiguration.Storage.Destination.Table(
                # here the source refers to the table name from the sql statement
                # and the destination to the full bucket table name
                # WARNING: when implementing input.tables, source and destination are swapped.
                source=out_table,
                destination=f"{destination}.{out_table}",
            )
            for out_table in output_tables
        ]
    return TransformationConfiguration(parameters=parameters, storage=storage)


############################## End of utility functions #########################################

############################## Component tools #########################################


async def retrieve_components_configurations(
    ctx: Context,
    component_types: Annotated[
        Sequence[ComponentType],
        Field(
            description="List of component types to filter by. ",
        ),
    ] = tuple(),
    component_ids: Annotated[
        Sequence[str],
        Field(
            description="List of component IDs to retrieve configurations for.",
        ),
    ] = tuple(),
) -> Annotated[
    List[ComponentWithConfigurations],
    Field(
        description="List of objects, each containing a component and its associated configurations.",
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
            description="List of transformation component IDs to retrieve configurations for.",
        ),
    ] = tuple(),
) -> Annotated[
    List[ComponentWithConfigurations],
    Field(
        description="List of objects, each containing a transformation component and its associated configurations.",
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
        return await _retrieve_components_configurations_by_types(client, ["transformation"])
    # If transformation IDs are provided, retrieve transformations configurations by IDs
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await _retrieve_components_configurations_by_ids(client, transformation_ids)


async def get_component_configuration_details(
    component_id: Annotated[
        str, Field(description="Unique identifier of the Keboola component/transformation")
    ],
    configuration_id: Annotated[
        str,
        Field(
            description="Unique identifier of the Keboola component/transformation configuration you want details "
            "about",
        ),
    ],
    ctx: Context,
) -> Annotated[
    ComponentConfiguration,
    Field(
        description="Detailed information about a Keboola component/transformation and its configuration.",
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
        f"Retrieved configuration details for {component_id} component with configuration {configuration_id}."
    )

    # Get Configuration Metadata if exists
    endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}/configs/{configuration_id}/metadata"
    r_metadata = await client.get(endpoint)
    if r_metadata:
        LOG.info(
            f"Retrieved configuration metadata for {component_id} component with configuration {configuration_id}."
        )
    else:
        LOG.info(
            f"No metadata found for {component_id} component with configuration {configuration_id}."
        )

    # Create Component Configuration Detail Object
    return ComponentConfiguration.model_validate(
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
            description="A short, descriptive name summarizing the purpose of the SQL transformation.",
        ),
    ],
    description: Annotated[
        str,
        Field(
            description=(
                "The detailed description of the SQL transformation capturing the user intent, explaining the "
                "SQL query, and the expected output."
            ),
        ),
    ],
    sql_statements: Annotated[
        Sequence[str],
        Field(
            description=(
                "The executable SQL query statements written in the current SQL dialect. "
                "Each statement should be a separate item in the list."
            ),
        ),
    ],
    created_table_names: Annotated[
        Sequence[str],
        Field(
            description="An empty list or a list of created table names if and only if they are generated within SQL "
            "statements (e.g., using `CREATE TABLE ...`).",
        ),
    ] = tuple(),
) -> Annotated[
    ComponentConfiguration,
    Field(
        description="Newly created SQL Transformation Configuration.",
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
    LOG.info(f"SQL dialect: {sql_dialect}, using transformation ID: {transformation_id}")

    # Process the data to be stored in the transformation configuration - parameters(sql statements)
    # and storage(input and output tables)
    transformation_configuration_payload = _get_transformation_configuration(
        statements=sql_statements, transformation_name=name, output_tables=created_table_names
    )

    client = KeboolaClient.from_state(ctx.session.state)
    endpoint = f"branch/{client.storage_client._branch_id}/components/{transformation_id}/configs"

    LOG.info(
        f"Creating new transformation configuration: {name} for component: {transformation_id}."
    )
    # Try to create the new transformation configuration and return the new object if successful
    # or log an error and raise an exception if not
    try:
        new_raw_transformation_configuration = await client.post(
            endpoint,
            data={
                "name": name,
                "description": description,
                "configuration": transformation_configuration_payload.model_dump(),
            },
        )

        component = await _get_component_details(client=client, component_id=transformation_id)
        new_transformation_configuration = ComponentConfiguration(
            **new_raw_transformation_configuration,
            component_id=transformation_id,
            component=component,
        )

        LOG.info(
            f"Created new transformation '{transformation_id}' with configuration id "
            f"'{new_transformation_configuration.configuration_id}'."
        )
        return new_transformation_configuration
    except Exception as e:
        LOG.exception(f"Error when creating new transformation configuration: {e}")
        raise e


############################## End of component tools #########################################
