import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast, get_args

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator, validator

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

############################## Base Models to #########################################

FULLY_QUALIFIED_ID_SEPARATOR: str = "::"


class ReducedComponent(BaseModel):
    """
    A list item representing a Keboola component. This object bears the reduced information about the component.
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
    A list item representing a Keboola component configuration. This object bears the reduced information about the
    component configuration.
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
    Grouping of a Keboola component and its configurations.
    """

    component: ReducedComponent = Field(description="The Keboola component.")
    configurations: List[ReducedComponentConfigurationPair] = Field(
        description="The list of component configurations for the given component."
    )


class Component(ReducedComponent):
    """
    Detailed information about a Keboola component. This object bears the full information about the component.
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
    Detailed information about a Keboola component configuration. This object bears the full information about the
    component configuration.
    """

    version: int = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(description="The configuration of the component")
    rows: Optional[List[Dict[str, Any]]] = Field(
        description="The rows of the component configuration", default=None
    )
    configuration_metadata: List[Dict[str, Any]] = Field(
        description="The metadata of the component configuration",
        default=[],
        validation_alias=AliasChoices("metadata", "configuration_metadata", "configurationMetadata"),
        serialization_alias="configurationMetadata",
    )
    component: Optional[Component] = Field(
        description="The Keboola component.",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
        default=None,
    )


############################## Utility functions #########################################
ComponentType = Literal["application", "extractor", "writer", "all"]
TransformationType = Literal["transformation"]
AllComponentTypes = Union[ComponentType, TransformationType]


def handle_component_types(types: Union[ComponentType, List[ComponentType]]) -> List[ComponentType]:
    """
    Utility function to handle the component types.
    If the types include "all", it will be removed and the remaining types will be returned.
    :param types: The component types to handle.
    :return: The handled component types.
    """
    if isinstance(types, str):
        types = [types]
    if "all" in types:
        return [c_type for c_type in get_args(ComponentType) if c_type != "all"]
    return types


async def retrieve_components_by_types(
    client: KeboolaClient, component_types: List[AllComponentTypes]
) -> List[ComponentWithConfigurations]:
    """
    Utility function to retrieve components by types - used in tools:
    - retrieve_components_in_project
    - retrieve_transformation_components
    :param client: The Keboola client
    :param component_types: The component types to retrieve
    :return: The components with their configurations
    """
    endpoint = "branch/{}/components".format(client.storage_client._branch_id)
    # retrieve components by types - unable to use list of types as parameter, we need to iterate over types
    params = {
        "include": "configuration",
    }
    raw_components_with_configs = []
    for type in component_types:
        # retrieve components by type with configurations
        params = {"componentType": type}
        raw_components_with_configs.extend(
            cast(List[Dict[str, Any]], await client.get(endpoint, params=params))
        )

    # build component configurations list grouped by components for each component found for given types
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


async def retrieve_components_by_ids(
    client: KeboolaClient, component_ids: List[str]
) -> List[ComponentWithConfigurations]:
    """
    Utility function to retrieve components by ids - used in tools:
    - retrieve_components_in_project
    - retrieve_transformation_in_project
    :param client: The Keboola client
    :param component_ids: The component ids to retrieve
    :return: The components with their configurations
    """
    components_with_configs = []
    for component_id in component_ids:
        # retrieve configurations for component ids
        raw_configurations = client.storage_client.configurations.list(component_id)
        # retrieve component details
        endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}"
        raw_component = await client.get(endpoint)
        # build component configurations list grouped by components for each component id
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


async def get_component_details(
    component_id: Annotated[
        str,
        Field(
            str, description="The ID of the Keboola component/transformation you want details about"
        ),
    ],
    client: KeboolaClient,
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


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    component_tools = [
        retrieve_components_in_project,
        retrieve_transformations_in_project,
        get_component_configuration_details,
    ]
    for tool in component_tools:
        logger.info(f"Adding tool {tool.__name__} to the MCP server.")
        mcp.add_tool(tool)

    logger.info("Component tools initialized.")


async def retrieve_components_in_project(
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
        description="List of objects grouping components and their configurations.",
    ),
]:
    """
    Retrieve component configurations in the project based on specified component_types or component_ids.
    If component_ids are supplied, only the configurations for those components are retrieved, disregarding component_types.
    USAGE:
        - Use when you want to see component configurations in the project for given component_types.
        - Use when you want to see component configurations in the project for given component_ids.
    EXAMPLES:
        - user_input: `give me all components`
            -> set types to ["all"]
            -> returns all components in the project
        - user_input: `list me all extractor components`
            -> set types to ["extractor"]
            -> returns all extractor components in the project
        - user_input: `give me configurations for following component/s` | `give me configurations for this component`
            -> set types to ["all"], component_ids to list of identifiers accordingly if you know them
            -> returns all configurations for the given components
    """
    if not component_ids:
        client = KeboolaClient.from_state(ctx.session.state)
        component_types = handle_component_types(component_types)
        return await retrieve_components_by_types(
            client, cast(List[AllComponentTypes], component_types)
        )
    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await retrieve_components_by_ids(client, component_ids)


async def retrieve_transformations_in_project(
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
        description="List of objects grouping components and their configurations.",
    ),
]:
    """
    Retrieve transformation components in the project for specific transformations or for all transformations.
    USAGE:
        - Use when you want to see transformation components in the project for given transformation_ids.
        - If you want to retrieve all transformations, set transformation_ids to an empty list.
    EXAMPLES:
        - user_input: `give me all transformations`
            -> set component_ids to []
            -> returns all transformation components in the project
        - user_input: `give me configurations for following component/s` | `give me configurations for this component`
            -> set transformation_ids to list of identifiers accordingly if you know the IDs
            -> returns all configurations for the given transformations IDs
        - user_input: `give me details about this transformation component 'simulating-id'`
            -> set transformation_ids to ['simulating-id']
            -> returns the details of the transformation component with ID 'simulating-id'
    """
    if not transformation_ids:
        client = KeboolaClient.from_state(ctx.session.state)
        return await retrieve_components_by_types(
            client, cast(List[AllComponentTypes], ["transformation"])
        )

    else:
        client = KeboolaClient.from_state(ctx.session.state)
        return await retrieve_components_by_ids(client, transformation_ids)


async def get_component_configuration_details(
    component_id: Annotated[
        str, Field(str, description="Unique identifier of the Keboola component/transformation")
    ],
    configuration_id: Annotated[
        str,
        Field(
            str,
            description="Unique identifier of the Keboola component configuration you want details about",
        ),
    ],
    ctx: Context,
) -> Annotated[
    ComponentConfigurationPair,
    Field(
        ComponentConfigurationPair,
        description="Detailed information about a Keboola component/transformation configuration.",
    ),
]:
    """
    Get detailed information about a specific Keboola component configuration given component/transformation ID and
    configuration ID. Those IDs can be retrieved from fully_qualified_id of the component configuration which are
    seperated by `::`.
    USAGE:
        - Use when you want to see the details of a specific component/transformation configuration pair.
    EXAMPLES:
        - user_input: `give me details about this configuration`
            -> set component_id and configuration_id to the specific component/transformation ID and configuration ID
            if you know it
            -> returns the details of the component/transformation configuration pair
    """

    client = KeboolaClient.from_state(ctx.session.state)

    # Get Component Details
    component = await get_component_details(component_id, client=client)
    # Get Configuration Details
    raw_configuration = client.storage_client.configurations.detail(component_id, configuration_id)
    logger.info(
        f"Retrieved configuration details for component configuration {component_id}::{configuration_id}."
    )

    # Get Configuration Metadata if exists
    endpoint = "branch/{}/components/{}/configs/{}/metadata".format(
        client.storage_client._branch_id, component_id, configuration_id
    )
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


############################## End of component tools #########################################
