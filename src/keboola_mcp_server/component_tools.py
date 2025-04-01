import logging
from typing import Annotated, Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)


class Component(BaseModel):
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


class ComponentConfiguration(BaseModel):
    component: Component = Field(
        description="The original component object",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
    )
    configuration_id: str = Field(
        description="The ID of the component configuration",
        validation_alias=AliasChoices("id", "configuration-id", "configuration_id"),
        serialization_alias="id",
    )
    configuration_name: str = Field(
        description="The name of the component configuration",
        validation_alias=AliasChoices(
            "name", "configuration-name", "configuration_name"
        ),
        serialization_alias="name",
    )
    configuration_description: Optional[str] = Field(
        description="The description of the component configuration",
        validation_alias=AliasChoices(
            "description", "configuration-description", "configuration_description"
        ),
        serialization_alias="description",
    )


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    component_tools = [
        list_components,
        list_component_configurations,
        get_component_configuration_details,
    ]
    for tool in component_tools:
        logger.info(f"Adding tool {tool.__name__} to the MCP server.")
        mcp.add_tool(tool)

    logger.info("Component tools initialized.")


async def list_components(ctx: Context) -> List[Component]:
    """List all available components."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_components = client.storage_client.components.list()
    logger.info(f"Found {len(r_components)} components.")
    return [Component.model_validate(r_comp) for r_comp in r_components]


async def list_component_configurations(
    component_id: Annotated[
        str, "The ID of the component for which configurations should be listed."
    ],
    ctx: Context,
) -> List[ComponentConfiguration]:
    """List all configurations for a given component."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_component = client.storage_client.components.detail(component_id)
    r_configs = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} configurations for component {component_id}.")
    return [
        ComponentConfiguration.model_validate(
            {**r_config, "component": r_component}
        )
        for r_config in r_configs
    ]


async def get_component_configuration_details(
    component_id: Annotated[str, "The ID of the component for which details should be retrieved."],
    configuration_id: Annotated[
        str, "The ID of the configuration for which details should be retrieved."
    ],
    ctx: Context,
) -> ComponentConfiguration:
    """Detail a given component configuration."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_component = client.storage_client.components.detail(component_id)
    r_config = client.storage_client.configurations.detail(component_id, configuration_id)
    return ComponentConfiguration.model_validate(
        {**r_config, "component": r_component}
    )
