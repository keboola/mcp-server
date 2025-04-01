import logging
from typing import Annotated, Any, Dict, List, Optional, Union

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

ID_TYPE = Union[str, int]


class ComponentListItem(BaseModel):
    """A list item representing a Keboola component."""

    id: str = Field(description="The ID of the component")
    name: str = Field(description="The name of the component")
    type: str = Field(description="The type of the component")
    description: Optional[str] = Field(description="The description of the component", default=None)


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


class ComponentConfigListItem(BaseModel):
    """A list item representing a Keboola component configuration."""

    id: ID_TYPE = Field(description="The ID of the component configuration")
    name: str = Field(description="The name of the component configuration")
    description: Optional[str] = Field(description="The description of the component configuration")
    created: str = Field(description="The creation date of the component configuration")
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


class ComponentConfig(ComponentConfigListItem):
    """Detailed information about a Keboola component configuration."""

    version: ID_TYPE = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(description="The configuration of the component")
    rows: Optional[List[Dict[str, Any]]] = Field(
        description="The rows of the component configuration", default=None
    )


class ComponentConfigMetadata(BaseModel):
    """Custom user created metadata associated with a Keboola component configuration."""

    component_id: str = Field(description="The ID of the component")
    config_id: str = Field(description="The ID of the component configuration")
    metadata: Dict[str, Any] = Field(description="The metadata of the component configuration")


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_components)
    mcp.add_tool(list_component_configs)
    mcp.add_tool(get_component_details)
    mcp.add_tool(get_component_config_details)
    mcp.add_tool(get_component_config_metadata)
    logger.info("Component tools added to the MCP server.")


async def list_components(ctx: Context) -> List[ComponentListItem]:
    """Retrieve a list of all available Keboola components in the project."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_components = client.storage_client.components.list()
    logger.info(f"Found {len(r_components)} components.")
    return [ComponentListItem.model_validate(r_comp) for r_comp in r_components]


async def list_component_configs(
    component_id: Annotated[
        str,
        Field(
            str, description="The ID of the Keboola component whose configurations you want to list"
        ),
    ],
    ctx: Context,
) -> List[ComponentConfigListItem]:
    """Retrieve all configurations that exist for a specific Keboola component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_configs = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} configurations for component {component_id}.")
    return [ComponentConfigListItem.model_validate(r_config) for r_config in r_configs]


async def get_component_details(
    component_id: Annotated[
        str, Field(str, description="The ID of the Keboola component you want details about")
    ],
    ctx: Context,
) -> Component:
    """Retrieve detailed information about a specific Keboola component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    endpoint = "branch/{}/components/{}".format(client.storage_client._branch_id, component_id)
    r_component = await client.get(endpoint)
    return Component.model_validate(r_component)


async def get_component_config_details(
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
) -> ComponentConfig:
    """
    Retrieve detailed information about a specific Keboola component configuration
    given component ID and config ID.
    """
    if isinstance(config_id, int):
        config_id = str(config_id)
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_config = client.storage_client.configurations.detail(component_id, config_id)
    return ComponentConfig.model_validate(r_config)


async def get_component_config_metadata(
    component_id: Annotated[
        str,
        Field(
            str,
            description="Unique identifier of the Keboola component whose configurations you want to list",
        ),
    ],
    config_id: Annotated[
        str,
        Field(
            str,
            description="Unique identifier of the Keboola component configuration you want details about",
        ),
    ],
    ctx: Context,
) -> List[ComponentConfigMetadata]:
    """Retrieve metadata about a specific Keboola component configuration."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    endpoint = "branch/{}/components/{}/configs/{}/metadata".format(
        client.storage_client._branch_id, component_id, config_id
    )
    r_metadata = await client.get(endpoint)
    r_metadata = [
        {"component_id": component_id, "config_id": config_id, "metadata": r_meta}
        for r_meta in r_metadata
    ]
    return [ComponentConfigMetadata.model_validate(r_meta) for r_meta in r_metadata]
