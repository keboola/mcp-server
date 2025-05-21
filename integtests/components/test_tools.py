import logging

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef
from keboola_mcp_server.tools.components import (
    ComponentConfiguration,
    ComponentType,
    ComponentWithConfigurations,
    get_component_configuration_details,
    retrieve_components_configurations,
)

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_component_configuration_details(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_component_configuration_details` returns a `ComponentConfiguration` instance."""

    for config in configs:
        assert config.configuration_id is not None

        result = await get_component_configuration_details(
            component_id=config.component_id, configuration_id=config.configuration_id, ctx=mcp_context
        )

        assert isinstance(result, ComponentConfiguration)
        assert result.component_id == config.component_id
        assert result.configuration_id == config.configuration_id

        assert result.configuration is not None
        assert result.component is not None

        assert result.component.component_id == config.component_id
        assert result.component.component_type is not None
        assert result.component.component_name is not None


@pytest.mark.asyncio
async def test_retrieve_components_by_ids(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `retrieve_components_configurations` returns components filtered by component IDs."""

    # Get unique component IDs from test configs
    component_ids = list({config.component_id for config in configs})
    assert len(component_ids) > 0

    result = await retrieve_components_configurations(ctx=mcp_context, component_ids=component_ids)

    # Verify result structure and content
    assert isinstance(result, list)
    assert len(result) == len(component_ids)

    for item in result:
        assert isinstance(item, ComponentWithConfigurations)
        assert item.component.component_id in component_ids

        # Check that configurations belong to this component
        for config in item.configurations:
            assert config.component_id == item.component.component_id


@pytest.mark.asyncio
async def test_retrieve_components_by_types(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `retrieve_components_configurations` returns components filtered by component types."""

    # Get unique component IDs from test configs
    component_ids = list({config.component_id for config in configs})
    assert len(component_ids) > 0

    component_types: list[ComponentType] = ['extractor']

    result = await retrieve_components_configurations(ctx=mcp_context, component_types=component_types)

    assert isinstance(result, list)
    # Currently, we only have extractor components in the project
    assert len(result) == len(component_ids)

    for item in result:
        assert isinstance(item, ComponentWithConfigurations)
        assert item.component.component_type == 'extractor'
