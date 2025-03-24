import logging
from typing import Annotated, Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)


class Component(BaseModel):
    id: str = Field(description="The ID of the component")
    name: str = Field(description="The name of the component")


class ComponentConfig(BaseModel):
    id: str = Field(description="The ID of the component configuration")
    name: str = Field(description="The name of the component configuration")
    description: Optional[str] = Field(description="The description of the component configuration")
    created: str = Field(description="The creation date of the component configuration")


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_components)
    mcp.add_tool(list_component_configs)
    mcp.add_tool(get_component_details)

    logger.info("Component tools added to the MCP server.")


async def list_components(ctx: Context) -> List[Component]:
    """List all available components."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_components = client.storage_client.components.list()
    logger.info(f"Found {len(r_components)} components.")
    return [Component.model_validate(r_comp) for r_comp in r_components]


async def list_component_configs(
    component_id: Annotated[
        str, "The ID of the component for which configurations should be listed."
    ],
    ctx: Context,
) -> List[ComponentConfig]:
    """List all configurations for a given component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_configs = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} configurations for component {component_id}.")
    return [ComponentConfig.model_validate(r_config) for r_config in r_configs]


async def get_component_details(
    component_id: Annotated[str, "The ID of the component for which details should be retrieved."],
    ctx: Context,
) -> Component:
    """Detail a given component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    endpoint = "branch/{}/components/{}".format(client.storage_client._branch_id, component_id)
    r_component = await client.get(endpoint)
    return Component.model_validate(r_component)
