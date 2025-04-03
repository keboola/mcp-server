import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator, validator

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

ID_TYPE = Union[str, int]

############################## Base Models to #########################################

FULLY_QUALIFIED_ID_SEPARATOR: str = "::"


class ComponentListItem(BaseModel):
    """
    A list item representing a reduced core Keboola component serving as a basis for all configurations of the
    component. This object bears the reduced information about the core component, including the component ID, name,
    type, description.
    """

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("id", "component_id", "componentId", "component-id"),
        serialization_alias="component_id",
    )
    component_name: str = Field(
        description="The name of the component",
        validation_alias=AliasChoices(
            "name",
            "component_name",
            "componentName",
            "component-name",
        ),
        serialization_alias="component_name",
    )
    component_type: str = Field(
        description="The type of the component",
        validation_alias=AliasChoices("type", "component_type"),
        serialization_alias="component_type",
    )
    component_description: Optional[str] = Field(
        description="The description of the component",
        default=None,
        validation_alias=AliasChoices("description", "component_description"),
        serialization_alias="component_description",
    )


class ComponentDetail(ComponentListItem):
    """
    Core Keboola component serves as a basis for all configurations of the component.
    This object bears the full information about the core component, including the configuration schema,
    description, long description, categories, version, data, flags.
    """

    long_description: Optional[str] = Field(
        description="The long description of the component",
        default=None,
        validation_alias=AliasChoices("longDescription", "long_description", "long-description"),
        serialization_alias="longDescription",
    )
    categories: List[str] = Field(description="The categories of the component", default=[])
    version: ID_TYPE = Field(description="The version of the component")
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

    def to_list_item(self) -> ComponentListItem:
        """Convert the ComponentDetail to its parent ComponentListItem by removing detailed fields."""
        return ComponentListItem.model_validate(self.model_dump())


class ComponentConfigurationListItem(BaseModel):
    """
    A list item representing a Keboola component configuration which is a modification of the given core component.
    This object bears the reduced information about the component configuration, including the configuration ID,
    name, description, and the reduced core component object.
    """

    component_id: str = Field(
        description="The ID of the core component",
        validation_alias=AliasChoices("component_id", "componentId"),
        serialization_alias="component_id",
    )
    configuration_id: str = Field(
        description="The ID of the component configuration",
        validation_alias=AliasChoices(
            "id",
            "configuration_id",
            "configurationId",
            "configuration-id",
        ),
        serialization_alias="configuration_id",
    )
    configuration_name: str = Field(
        description="The name of the component configuration",
        validation_alias=AliasChoices(
            "name",
            "configuration_name",
            "configurationName",
            "configuration-name",
        ),
        serialization_alias="configuration_name",
    )
    configuration_description: Optional[str] = Field(
        description="The description of the component configuration",
        validation_alias=AliasChoices(
            "description",
            "configuration_description",
            "configurationDescription",
            "configuration-description",
        ),
        serialization_alias="configuration_description",
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

    @field_validator("fully_qualified_id", mode="before")
    def set_fully_qualified_id(cls, v, info):
        # we need to handle cases where the configuration_id is set then we need to use it
        # otherwise we use the default id from the component which is included in the api response
        configuration_id = info.data.get("configuration_id") or info.data.get("id")
        component_id = info.data.get("component_id")
        if component_id and configuration_id:
            return f"{component_id}{FULLY_QUALIFIED_ID_SEPARATOR}{configuration_id}"
        return v


class ComponentConfigurationsList(BaseModel):
    """
    Container for a reduced core Keboola component and its configurations.
    """

    component: ComponentListItem = Field(
        description="The reduced core Keboola component object that serves as the basis for the configurations"
    )
    configurations: List[ComponentConfigurationListItem] = Field(
        description="The list of component configurations for the given component"
    )


class ComponentConfigurationDetail(ComponentConfigurationListItem):
    """
    Detailed information about a Keboola component configuration.
    This object bears the full information about the component configuration, including the configuration,
    rows, metadata, and the core component object from which this configuration was derived.
    """

    version: ID_TYPE = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(description="The configuration of the component")
    rows: Optional[List[Dict[str, Any]]] = Field(
        description="The rows of the component configuration", default=None
    )
    configuration_metadata: List[Dict[str, Any]] = Field(
        description="The metadata of the component configuration",
        default=[],
        validation_alias=AliasChoices("metadata", "configuration_metadata"),
        serialization_alias="configuration_metadata",
    )
    component: Optional[ComponentDetail] = Field(
        description="The core component object that serves as the basis for this configuration",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
        default=None,
    )


############################## Utility functions #########################################
ComponentType = Literal["all", "other", "application", "transformation", "extractor", "writer"]


def handle_component_types(types: Union[ComponentType, List[ComponentType]]) -> List[ComponentType]:
    if isinstance(types, str):
        types = [types]
    if "all" in types:
        types = ["all"]
    return types


def conform_types(component: ComponentListItem, types: List[ComponentType]) -> bool:
    if "all" in types:
        return True
    if "other" in types:
        return not (
            component.component_type
            in [
                "application",
                "transformation",
                "extractor",
                "writer",
            ]
        )
    else:
        return component.component_type in types


############################## End of utility functions #########################################

############################## Component tools #########################################


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    component_tools = [
        list_components,
        list_component_configurations,
        get_component_configuration_details,
        get_core_component_details,
        list_all_component_configurations,
    ]
    for tool in component_tools:
        logger.info(f"Adding tool {tool.__name__} to the MCP server.")
        mcp.add_tool(tool)

    logger.info("Component tools initialized.")


async def list_all_component_configurations(
    ctx: Context,
    types: Annotated[
        List[ComponentType],
        Field(description="Array of component types to filter by", default=["all"]),
    ] = ["all"],
) -> List[ComponentConfigurationsList]:
    """
    Retrieve a list of all available Keboola component configuration pairs in the project filtered by their types.
    PARAMETERS:
        types: Array of component types to filter by, default is ["all"].
    RETURNS:
        List of component configuration pairs as list items.
    USAGE:
        - Use when you want to see all component configuration pairs in the project given a specific type.
    CONSIDERATIONS:
        - Regarding user experience, the individual components and their configurations are unified, as a core 
        component is only relevant when it has an associated configuration unless user explicitly specifies otherwise.
        - **Specific component** When user specifies a specific component, the tool will return all configurations
        - **components** are mostly referring to component configuration pairs that are writers or extractors but when
            user specifies types filter by those types, since from keboola perspective, all those types are components.
        - **transformations** are component configuration pairs that are type of transformations
        - **applications** are component configuration pairs that are type of applications
        - **other** are component configuration pairs that are not writers, extractors, transformations or applications
        - **all** are all component configuration pairs
    EXAMPLES:
    - General:
        - user_input: `list me components` | `give me all component configurations in this project`
            -> set types to ["extractor", "writer"]
            -> returns all component configuration pairs that are extractors or writers.
        - user_input: `list me transformation components` | `give me available transformation configurations`
            -> set types to ["transformation"]
            -> returns all component configuration pairs that are types of transformations
        - user_input: `list me components configurations considering all types` | `give me all components`
            -> set types to ["all"]
            -> returns all component configuration pairs
        - user_input: `list me all special components` | `give me other configurations`
            -> set types to ["other"]
            -> returns all other component configuration pairs that are not writers, extractors, transformations or
            applications
    - Specific components:
        - user_input: `list me snowflake components` | `give me snowflake configurations`
            -> set types to ["all"] because we need to find all components from which we can derive snowflake configurations
            -> returns all component configuration pairs
        - user_input: `list me postgresql components` | `give me postgresql configurations`
            -> set types to ["all"] because we need to find all components from which we can derive postgresql configurations
            -> returns all component configuration pairs
    """

    types = handle_component_types(types)

    # retrieve all core components
    components = await list_components(ctx, types)
    logger.info(f"Found {len(components)} core components for given types {types}.")
    # iterate over all core components and retrieve their configurations
    component_configs: List[ComponentConfigurationsList] = []
    for component in components:
        # check if the component matches the types filter
        if conform_types(component, types):
            # retrieve all configurations for the component
            cur_component_configs = await list_component_configurations(component.component_id, ctx)
            component_configs.append(
                ComponentConfigurationsList(
                    component=component,
                    configurations=cur_component_configs.configurations,
                )
            )
    return sorted(component_configs, key=lambda x: x.component.component_type)


async def list_components(
    ctx: Context,
    types: Annotated[
        List[ComponentType],
        Field(
            description="Array of component types to filter by",
            default=["all"],
        ),
    ] = ["all"],
) -> List[ComponentListItem]:
    """
    Retrieve a list of core Keboola components used in the project that are filtered by their types.
    These components are the basis for all configurations of the component.
    PARAMETERS:
        types: Array of component types to filter by, default is "all".
    RETURNS:
        List of core component objects containing the component ID, name, type, description.
    USAGE:
        - Use when user wants to see core components in the projects given a specific type.
        - Use when you want to find IDs, names, types, descriptions of all core Keboola components used in the project.
    CONSIDERATIONS:
        - **components** are mostly referring to components that are writers or extractors but when
            user specifies types filter by those types, since from keboola perspective - everything is component.
        - **transformations** are components that are transformations
        - **applications** are components that are applications
        - **other** are components that are not writers, extractors, transformations or applications
        - **all** are all components
    EXAMPLES:
    - General:
        - user_input: `list me components schemas` | `give me core components in this project`
            -> set types to ["extractor", "writer"]
            -> returns all core components that are extractors or writers.
        - user_input: `list me all transformation schemas` | `give me all available base transformation components`
        -> set types to ["transformation"]
        -> returns all core components that are types of transformations or applications
    - Specific components:
        - user_input: `give me snowflake core component`
            -> set types to ["all"] because we need to find all components from which we can derive snowflake configurations
            -> returns all core components
        - user_input: `give me postgresql core component`
            -> set types to ["all"] because we need to find all components from which we can derive postgresql configurations
            -> returns all core components
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    types = handle_component_types(types)

    r_components = client.storage_client.components.list()
    logger.info(f"Found {len(r_components)} components for given types {types}.")
    components = [ComponentListItem.model_validate(r_comp) for r_comp in r_components]
    return list(filter(lambda x: conform_types(x, types), components))


async def list_component_configurations(
    component_id: Annotated[
        str,
        Field(
            str, description="The ID of the Keboola component whose configurations you want to list"
        ),
    ],
    ctx: Context,
) -> ComponentConfigurationsList:
    """
    Retrieve all configurations that exist for a specific core Keboola component ID.
    PARAMETERS:
        component_id: The ID of the Keboola component whose configurations you want to list
    RETURNS:
        A list of component configuration pairs as list items.
    USAGE:
        - Use when you want to see all configurations for a specific core component, or component ID.
    EXAMPLES:
        - user_input: `list me all configurations for snowflake`
            -> set component_id to the specific component ID if you know it
            -> returns all configurations for the snowflake component
    """
    client = KeboolaClient.from_state(ctx.session.state)

    component = await get_core_component_details(component_id, ctx)
    r_configs = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} component configurations for component {component_id}.")
    return ComponentConfigurationsList(
        component=component.to_list_item(),
        configurations=[
            ComponentConfigurationListItem.model_validate(
                {**r_config, "component_id": component_id}
            )
            for r_config in r_configs
        ],
    )


async def get_core_component_details(
    component_id: Annotated[
        str, Field(str, description="The ID of the Keboola component you want details about")
    ],
    ctx: Context,
) -> ComponentDetail:
    """
    Retrieve detailed information about a core Keboola component object given component ID.
    PARAMETERS:
        component_id: The ID of the Keboola component you want details about
    RETURNS:
        A component detail object containing the component ID, name, type, description.
    USAGE:
        - Use when you want to see the details of a specific core component, or component ID.
    EXAMPLES:
        - user_input: `give me schema details this component`
            -> set component_id to the specific component ID if you know it
            -> returns the details of the component
    """
    client = KeboolaClient.from_state(ctx.session.state)

    endpoint = "branch/{}/components/{}".format(client.storage_client._branch_id, component_id)
    r_component = await client.get(endpoint)
    logger.info(f"Retrieved component details for component {component_id}.")
    return ComponentDetail.model_validate(r_component)


async def get_component_configuration_details(
    component_id: Annotated[
        str, Field(str, description="Unique identifier of the Keboola component")
    ],
    configuration_id: Annotated[
        str,
        Field(
            str,
            description="Unique identifier of the Keboola component configuration you want details about",
        ),
    ],
    ctx: Context,
) -> ComponentConfigurationDetail:
    """
    Retrieve detailed information about a specific Keboola component configuration given component ID and configuration
    ID. Those IDs can be retrieved from fully_qualified_id of the component configuration which are seperated by `::`.
    Use to get the configuration details, metadata and the core Keboola component.
    PARAMETERS:
        component_id: The ID of the Keboola component you want details about
        configuration_id: The ID of the Keboola component configuration you want details about
    RETURNS:
        A component configuration detail object containing the component ID, name, type, description.
    USAGE:
        - Use when you want to see the details of a specific component configuration pair.
    EXAMPLES:
        - user_input: `give me details about this configuration`
            -> set component_id and configuration_id to the specific component ID and configuration ID if you know it
            -> returns the details of the component configuration pair
    """

    client = KeboolaClient.from_state(ctx.session.state)

    component = await get_core_component_details(component_id, ctx)
    r_config = client.storage_client.configurations.detail(component_id, configuration_id)
    logger.info(
        f"Retrieved configuration details for component configuration {component_id}::{configuration_id}."
    )

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

    return ComponentConfigurationDetail.model_validate(
        {**r_config, "component": component, "component_id": component_id, "metadata": r_metadata}
    )


async def create_snowflake_transformation(
    ctx: Context,
    name: Annotated[
        str,
        Field(
            str,
            description="The name of the snowflake transformation",
        ),
    ],
    sql_query: Annotated[
        Optional[str],
        Field(
            Optional[str],
            description="The SQL query of the snowflake transformation",
        ),
    ] = None,
    description: Annotated[
        Optional[str],
        Field(
            Optional[str],
            description="The description of the snowflake transformation",
        ),
    ] = None,

) -> None:
    """
    Create a snowflake transformation from the given name, sql query and optionally description.
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    raise NotImplementedError("Not implemented yet.")
       
############################## End of component tools #########################################
