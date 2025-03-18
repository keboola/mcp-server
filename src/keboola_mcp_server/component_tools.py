from typing import Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient


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
    mcp.add_tool(list_components, "list_components", "List all available components.")
    mcp.add_tool(list_component_configs, "list_component_configs", "List all configurations for a given component.")


async def list_components(ctx: Context) -> List[Component]:
    """List all available components."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)
    r_components = client.storage_client.components.list()
    return [Component(id=r_comp["id"], name=r_comp["name"]) for r_comp in r_components]


async def list_component_configs(
    component_id: str,
    ctx: Context,
) -> List[ComponentConfig]:
    """List all configurations for a given component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)
    r_configs = client.storage_client.configurations.list(component_id)
    return [
        ComponentConfig(
            id=r_config["id"],
            name=r_config["name"],
            description=r_config["description"],
            created=r_config["created"],
        )
        for r_config in r_configs
    ]
