import logging
from typing import Annotated, Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)


class Component(BaseModel):
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


class ComponentConfiguration(BaseModel):
    component: Component = Field(
        description="The original component object",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
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
    client = KeboolaClient.from_state(ctx.session.state)

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
    client = KeboolaClient.from_state(ctx.session.state)

    component = await get_component_details(component_id, ctx)
    r_configs = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(r_configs)} configurations for component {component_id}.")
    return [
        ComponentConfiguration.model_validate({**r_config, "component": component})
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
    client = KeboolaClient.from_state(ctx.session.state)

    component = await get_component_details(component_id, ctx)
    r_config = client.storage_client.configurations.detail(component_id, configuration_id)
    return ComponentConfiguration.model_validate({**r_config, "component": component})


async def get_component_details(
    component_id: Annotated[
        str, Field(str, description="The ID of the Keboola component you want details about")
    ],
    ctx: Context,
) -> Component:
    """Retrieve detailed information about a original Keboola component object given component ID."""
    client = KeboolaClient.from_state(ctx.session.state)

    endpoint = "branch/{}/components/{}".format(client.storage_client._branch_id, component_id)
    r_component = await client.get(endpoint)
    return Component.model_validate(r_component)
