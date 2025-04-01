import logging
from typing import Annotated, Any, Dict, List, Optional, Union

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

ID_TYPE = Union[str, int]


class ComponentListItem(BaseModel):
    """A list item representing a Keboola component."""

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("id", "componentId", "component-id", "component_id"),
        serialization_alias="id",
    )
    component_name: str = Field(
        description="The name of the component",
        validation_alias=AliasChoices("name", "componentName", "component-name", "component_name"),
        serialization_alias="name",
    )
    component_type: str = Field(
        description="The type of the component",
        validation_alias=AliasChoices("type", "componentType", "component-type", "component_type"),
        serialization_alias="type",
    )
    component_description: Optional[str] = Field(
        description="The description of the component",
        default=None,
        validation_alias=AliasChoices(
            "description", "componentDescription", "component-description", "component_description"
        ),
        serialization_alias="description",
    )


class Component(ComponentListItem):
    """Detailed information about a Keboola component."""

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
        description="The configuration schema of the component",
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
        """Convert the component to a list item."""
        return ComponentListItem.model_validate(self.model_dump())


class ComponentConfigurationListItem(BaseModel):
    """A list item representing a Keboola component configuration."""

    component: Optional[ComponentListItem] = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
        default=None,
    )
    configuration_id: str = Field(
        description="The ID of the component configuration",
        validation_alias=AliasChoices("id", "configuration-id", "configuration_id"),
        serialization_alias="id",
    )
    configuration_name: str = Field(
        description="The name of the component configuration",
        validation_alias=AliasChoices("name", "configuration-name", "configuration_name"),
        serialization_alias="name",
    )
    configuration_description: Optional[str] = Field(
        description="The description of the component configuration",
        validation_alias=AliasChoices(
            "description", "configuration-description", "configuration_description"
        ),
        serialization_alias="description",
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


class ComponentConfiguration(ComponentConfigurationListItem):
    """Detailed information about a Keboola component configuration."""

    version: ID_TYPE = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(description="The configuration of the component")
    rows: Optional[List[Dict[str, Any]]] = Field(
        description="The rows of the component configuration", default=None
    )
    component: Optional[Component] = Field(
        description="The original component object",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
        default=None,
    )
    metadata: List[Dict[str, Any]] = Field(
        description="The metadata of the component configuration", default=[]
    )


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""

    component_tools = [
        list_components,
        list_component_configurations,
        get_component_configuration_details,
        get_component_details,
    ]
    for tool in component_tools:
        logger.info(f"Adding tool {tool.__name__} to the MCP server.")
        mcp.add_tool(tool)

    logger.info("Component tools initialized.")


async def list_components(ctx: Context) -> List[ComponentListItem]:
    """Retrieve a list of all available Keboola components in the project."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_components = client.storage_client.components.list()
    logger.info(f"Found {len(r_components)} components.")
    return [ComponentListItem.model_validate(r_comp) for r_comp in r_components]


async def list_component_configurations(
    component_id: Annotated[
        str,
        Field(
            str, description="The ID of the Keboola component whose configurations you want to list"
        ),
    ],
    ctx: Context,
) -> List[ComponentConfigurationListItem]:
    """Retrieve all configurations that exist for a specific Keboola component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    component = await get_component_details(component_id, ctx)
    r_configs = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} configurations for component {component_id}.")
    return [
        ComponentConfigurationListItem.model_validate(
            {**r_config, "component": component.to_list_item()}
        )
        for r_config in r_configs
    ]


async def get_component_details(
    component_id: Annotated[
        str, Field(str, description="The ID of the Keboola component you want details about")
    ],
    ctx: Context,
) -> Component:
    """Retrieve detailed information about a original Keboola component object given component ID."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    endpoint = "branch/{}/components/{}".format(client.storage_client._branch_id, component_id)
    r_component = await client.get(endpoint)
    return Component.model_validate(r_component)


async def get_component_configuration_details(
    component_id: Annotated[
        str, Field(str, description="Unique identifier of the Keboola component")
    ],
    config_id: Annotated[
        str,
        Field(
            str,
            description="Unique identifier of the Keboola component configuration you want details about",
        ),
    ],
    ctx: Context,
) -> ComponentConfiguration:
    """
    Retrieve detailed information about a specific Keboola component configuration given component ID and config ID.
    Use to get the configuration details and metadata for a specific configuration and a given component.
    """
    if isinstance(config_id, int):
        config_id = str(config_id)
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    component = await get_component_details(component_id, ctx)
    r_config = client.storage_client.configurations.detail(component_id, config_id)
    endpoint = "branch/{}/components/{}/configs/{}/metadata".format(
        client.storage_client._branch_id, component_id, config_id
    )
    r_metadata = await client.get(endpoint)
    return ComponentConfiguration.model_validate(
        {**r_config, "component": component, "metadata": r_metadata}
    )
