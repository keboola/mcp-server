import logging
from typing import cast

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components import (
    ComponentType,
    ComponentWithConfigurations,
    get_component_configuration,
    retrieve_components_configurations,
)
from keboola_mcp_server.tools.components.model import (
    ComponentConfigurationOutput,
    ComponentRootConfiguration,
    ComponentToolResponse,
    RetrieveComponentsConfigurationsOutput,
)
from keboola_mcp_server.tools.components.tools import (
    create_component_root_configuration,
    update_component_root_configuration,
)

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
        # Check links field
        assert result.links, 'Links list should not be empty.'
        for link in result.links:
            assert isinstance(link, Link)


@pytest.mark.asyncio
async def test_retrieve_components_by_ids(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `retrieve_components_configurations` returns components filtered by component IDs."""

    # Get unique component IDs from test configs
    component_ids = list({config.component_id for config in configs})
    assert len(component_ids) > 0

    result = await retrieve_components_configurations(ctx=mcp_context, component_ids=component_ids)

    # Verify result structure and content
    assert isinstance(result, RetrieveComponentsConfigurationsOutput)
    assert len(result.components_with_configurations) == len(component_ids)

    for item in result.components_with_configurations:
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

    assert isinstance(result, RetrieveComponentsConfigurationsOutput)
    # Currently, we only have extractor components in the project
    assert len(result.components_with_configurations) == len(component_ids)

    for item in result.components_with_configurations:
        assert isinstance(item, ComponentWithConfigurations)
        assert item.component.component_type == 'extractor'


@pytest.mark.asyncio
async def test_create_component_root_configuration(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `create_component_root_configuration` creates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    test_name = 'Test Configuration'
    test_description = 'Test configuration created by automated test'
    test_parameters = {}
    test_storage = {}

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the configuration
    created_config = await create_component_root_configuration(
        ctx=mcp_context,
        name=test_name,
        description=test_description,
        component_id=component_id,
        parameters=test_parameters,
        storage=test_storage,
    )
    try:
        assert isinstance(created_config, ComponentToolResponse)
        assert created_config.component_id == component_id
        assert created_config.configuration_id is not None
        assert created_config.description == test_description
        assert created_config.success is True
        assert created_config.timestamp is not None
        assert len(created_config.links) > 0

        # Verify the configuration exists in the backend by fetching it
        config_detail = await client.storage_client.configuration_detail(
            component_id=component_id, configuration_id=created_config.configuration_id
        )

        assert config_detail['name'] == test_name
        assert config_detail['description'] == test_description
        assert 'configuration' in config_detail
        
        # Verify the parameters and storage were set correctly
        configuration_data = cast(dict, config_detail['configuration'])
        assert configuration_data['parameters'] == test_parameters
        assert configuration_data['storage'] == test_storage

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=component_id, configuration_id=created_config.configuration_id
        )

        # Convert metadata list to dictionary for easier checking
        # metadata is a list of dicts with 'key' and 'value' keys
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'
    finally:
        # Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=created_config.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_update_component_root_configuration(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `update_component_root_configuration` updates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the initial configuration
    created_config = await create_component_root_configuration(
        ctx=mcp_context,
        name='Initial Test Configuration',
        description='Initial test configuration created by automated test',
        component_id=component_id,
        parameters={'initial_param': 'initial_value'},
        storage={'input': {'tables': [{'source': 'in.c-bucket.table', 'destination': 'input.csv'}]}},
    )
    assert created_config.configuration_id is not None

    try:
        updated_name = 'Updated Test Configuration'
        updated_description = 'Updated test configuration by automated test'
        updated_parameters = {'updated_param': 'updated_value'}
        updated_storage = {'output': {'tables': [{'source': 'output.csv', 'destination': 'out.c-bucket.table'}]}}
        change_description = 'Automated test update'

        # Update the configuration
        updated_config = await update_component_root_configuration(
            ctx=mcp_context,
            name=updated_name,
            description=updated_description,
            change_description=change_description,
            component_id=component_id,
            configuration_id=created_config.configuration_id,
            parameters=updated_parameters,
            storage=updated_storage,
        )

        # Test the ComponentToolResponse attributes
        assert isinstance(updated_config, ComponentToolResponse)
        assert updated_config.component_id == component_id
        assert updated_config.configuration_id == created_config.configuration_id
        assert updated_config.description == updated_description
        assert updated_config.success is True
        assert updated_config.timestamp is not None
        assert len(updated_config.links) > 0

        # Verify the configuration exists in the backend by fetching it
        config_detail = await client.storage_client.configuration_detail(
            component_id=component_id, configuration_id=updated_config.configuration_id
        )

        assert config_detail['name'] == updated_name
        assert config_detail['description'] == updated_description
        assert 'configuration' in config_detail
        
        # Cast to dict to help type checker
        configuration_data = cast(dict, config_detail['configuration'])
        assert configuration_data['parameters'] == updated_parameters
        # Storage API might return more keys than what we set, so we check subset
        for k, v in updated_storage.items():
            assert k in configuration_data['storage']
            assert configuration_data['storage'][k] == v

        # Get the current version for metadata verification
        current_version = config_detail['version']

        # Verify the metadata - check that KBC.MCP.updatedBy.version.{version} is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=component_id, configuration_id=updated_config.configuration_id
        )

        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        updated_by_md_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_version}'
        assert updated_by_md_key in metadata_dict
        assert metadata_dict[updated_by_md_key] == 'true'

    finally:
        # Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=created_config.configuration_id,
            skip_trash=True,
        )
