import logging

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components import (
    ComponentType,
    ComponentWithConfigurations,
    get_component_configuration,
    retrieve_components_configurations,
)
from keboola_mcp_server.tools.components.model import ComponentConfigurationOutput, ComponentRootConfiguration
from keboola_mcp_server.tools.components.tools import create_component_root_configuration

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_component_configuration(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_component_configuration` returns a `ComponentConfigurationOutput` instance."""

    for config in configs:
        assert config.configuration_id is not None

        result = await get_component_configuration(
            component_id=config.component_id, configuration_id=config.configuration_id, ctx=mcp_context
        )

        assert isinstance(result, ComponentConfigurationOutput)
        assert result.component is not None
        assert result.component.component_id == config.component_id
        assert result.component.component_type is not None
        assert result.component.component_name is not None

        assert result.root_configuration is not None
        assert result.root_configuration.configuration_id == config.configuration_id
        assert result.root_configuration.component_id == config.component_id


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
            assert config.root_configuration.component_id == item.component.component_id


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


@pytest.mark.asyncio
async def test_create_component_root_configuration(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `create_component_root_configuration` creates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    test_name = 'Test Integration Configuration'
    test_description = 'Integration test configuration created by automated test'
    test_parameters = {}
    test_storage = {}

    # Create the configuration
    result = await create_component_root_configuration(
        ctx=mcp_context,
        name=test_name,
        description=test_description,
        component_id=component_id,
        parameters=test_parameters,
        storage=test_storage
    )
    try:
        assert isinstance(result, ComponentRootConfiguration)
        assert result.configuration_name == test_name
        assert result.configuration_description == test_description
        assert result.component_id == component_id
        assert result.configuration_id is not None
        assert result.parameters == test_parameters
        assert result.storage == test_storage

        # Verify the configuration exists in the backend by fetching it
        client = KeboolaClient.from_state(mcp_context.session.state)
        config_detail = await client.storage_client.configuration_detail(
            component_id=component_id,
            configuration_id=result.configuration_id
        )

        assert config_detail['name'] == test_name
        assert config_detail['description'] == test_description
        assert 'configuration' in config_detail

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=component_id,
            configuration_id=result.configuration_id
        )

        # Convert metadata list to dictionary for easier checking
        # metadata is a list of dicts with 'key' and 'value' keys
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert 'KBC.MCP.createdBy' in metadata_dict
        assert metadata_dict['KBC.MCP.createdBy'] == 'true'
    finally:
        # Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=result.configuration_id
        )
