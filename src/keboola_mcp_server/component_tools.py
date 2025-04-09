import logging
from typing import Annotated, Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)


class ComponentDetail(BaseModel):
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


class ComponentConfigurationDetail(BaseModel):
    component: ComponentDetail = Field(
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
        retrieve_components,
        retrieve_component_configurations,
        get_component_configuration_detail,
    ]
    for tool in component_tools:
        logger.info(f"Adding tool {tool.__name__} to the MCP server.")
        mcp.add_tool(tool)

    logger.info("Component tools initialized.")


async def retrieve_components(ctx: Context) -> List[ComponentDetail]:
    """Retrieve all available components."""
    client = KeboolaClient.from_state(ctx.session.state)

    raw_components = client.storage_client.components.list()
    logger.info(f"Found {len(raw_components)} components.")
    return [ComponentDetail.model_validate(raw_comp) for raw_comp in raw_components]


async def retrieve_component_configurations(
    component_id: Annotated[
        str, "The ID of the component for which configurations should be listed."
    ],
    ctx: Context,
) -> List[ComponentConfigurationDetail]:
    """Retrieve all configurations for a given component."""
    client = KeboolaClient.from_state(ctx.session.state)

    component = await get_component_details(component_id, ctx)
    raw_configurations = client.storage_client.configurations.list(component_id)
    logger.info(f"Found {len(raw_configurations)} configurations for component {component_id}.")
    return [
        ComponentConfigurationDetail.model_validate({**r_config, "component": component})
        for r_config in raw_configurations
    ]


async def get_component_configuration_detail(
    component_id: Annotated[str, "The ID of the component for which details should be retrieved."],
    configuration_id: Annotated[
        str, "The ID of the configuration for which details should be retrieved."
    ],
    ctx: Context,
) -> ComponentConfigurationDetail:
    """Get details of a given component configuration."""
    client = KeboolaClient.from_state(ctx.session.state)

    component = await get_component_details(component_id, ctx)
    raw_configuration = client.storage_client.configurations.detail(component_id, configuration_id)
    return ComponentConfigurationDetail.model_validate(
        {**raw_configuration, "component": component}
    )


async def get_component_details(
    component_id: Annotated[
        str, Field(str, description="The ID of the Keboola component you want details about")
    ],
    ctx: Context,
) -> ComponentDetail:
    """Get detailed information about a Keboola component given component ID."""
    client = KeboolaClient.from_state(ctx.session.state)

    endpoint = f"branch/{client.storage_client._branch_id}/components/{component_id}"
    raw_component = await client.get(endpoint)
    return ComponentDetail.model_validate(raw_component)
