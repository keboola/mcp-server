import logging
from typing import Annotated, Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)


class ComponentListItem(BaseModel):
    """A list item representing a Keboola component."""

    id: str = Field(description="The ID of the component")
    name: str = Field(description="The name of the component")
    type: str = Field(description="The type of the component")
    description: Optional[str] = Field(description="The description of the component")


class Component(ComponentListItem):
    """Detailed information about a Keboola component."""

    long_description: Optional[str] = Field(description="The long description of the component")
    categories: List[str] = Field(description="The categories of the component")
    version: str = Field(description="The version of the component")
    created: str = Field(description="The creation date of the component")


class ComponentConfig(BaseModel):
    """A list item representing a Keboola component configuration."""

    id: str = Field(description="The ID of the component configuration")
    name: str = Field(description="The name of the component configuration")
    description: Optional[str] = Field(description="The description of the component configuration")
    created: str = Field(description="The creation date of the component configuration")
    is_disabled: bool = Field(
        description="Whether the component configuration is disabled", alias="isDisabled"
    )
    is_deleted: bool = Field(
        description="Whether the component configuration is deleted", alias="isDeleted"
    )
    version: int = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(
        description="The configuration of the component configuration"
    )


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_components)
    mcp.add_tool(list_component_configs)
    mcp.add_tool(get_component_details)
    mcp.add_tool(get_component_config_details)

    logger.info("Component tools added to the MCP server.")


async def list_components(ctx: Context) -> List[Component]:
    """Retrieve a list of all available Keboola components in the project."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_components = await client.storage_client.components.list()
    logger.info(f"Found {len(r_components)} components.")
    return [Component.model_validate(r_comp) for r_comp in r_components]


async def list_component_configs(
    component_id: Annotated[
        str, "Unique identifier of the Keboola component whose configurations you want to list"
    ],
    ctx: Context,
) -> List[ComponentConfig]:
    """Retrieve all configurations that exist for a specific Keboola component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_configs = await client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} configurations for component {component_id}.")
    return [ComponentConfig.model_validate(r_config) for r_config in r_configs]


async def get_component_details(
    component_id: Annotated[
        str, "Unique identifier of the Keboola component you want details about"
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
        str, "Unique identifier of the Keboola component whose configurations you want to list"
    ],
    config_id: Annotated[
        str, "Unique identifier of the Keboola component configuration you want details about"
    ],
    ctx: Context,
) -> ComponentConfig:
    """
    Retrieve detailed information about a specific Keboola component configuration
    given component ID and config ID.
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_config = await client.storage_client.configurations.detail(component_id, config_id)
    return ComponentConfig.model_validate(r_config)
