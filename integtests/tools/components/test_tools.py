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
    get_config,
    list_configs,
)
from keboola_mcp_server.tools.components.model import (
    ComponentConfigurationOutput,
    ComponentToolResponse,
    ListConfigsOutput,
)
from keboola_mcp_server.tools.components.tools import create_config, update_config, add_config_row, update_config_row

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_config(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_config` returns a `ComponentConfigurationOutput` instance."""

    for config in configs:
        assert config.configuration_id is not None

        result = await get_config(
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
async def test_list_configs_by_ids(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `list_configs` returns components filtered by component IDs."""

    # Get unique component IDs from test configs
    component_ids = list({config.component_id for config in configs})
    assert len(component_ids) > 0

    result = await list_configs(ctx=mcp_context, component_ids=component_ids)

    # Verify result structure and content
    assert isinstance(result, ListConfigsOutput)
    assert len(result.components_with_configurations) == len(component_ids)

    for item in result.components_with_configurations:
        assert isinstance(item, ComponentWithConfigurations)
        assert item.component.component_id in component_ids

        # Check that configurations belong to this component
        for config in item.configurations:
            assert config.root_configuration.component_id == item.component.component_id


@pytest.mark.asyncio
async def test_list_configs_by_types(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `list_configs` returns components filtered by component types."""

    # Get unique component IDs from test configs
    component_ids = list({config.component_id for config in configs})
    assert len(component_ids) > 0

    component_types: list[ComponentType] = ['extractor']

    result = await list_configs(ctx=mcp_context, component_types=component_types)

    assert isinstance(result, ListConfigsOutput)
    # Currently, we only have extractor components in the project
    assert len(result.components_with_configurations) == len(component_ids)

    for item in result.components_with_configurations:
        assert isinstance(item, ComponentWithConfigurations)
        assert item.component.component_type == 'extractor'


@pytest.mark.asyncio
async def test_create_config(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `create_config` creates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    test_name = 'Test Configuration'
    test_description = 'Test configuration created by automated test'
    test_parameters = {}
    test_storage = {}

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the configuration
    created_config = await create_config(
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
async def test_update_config(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `update_config` updates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the initial configuration
    created_config = await create_config(
        ctx=mcp_context,
        name='Initial Test Configuration',
        description='Initial test configuration created by automated test',
        component_id=component_id,
        parameters={'initial_param': 'initial_value'},
        storage={'input': {'tables': [{'source': 'in.c-bucket.table', 'destination': 'input.csv'}]}},
    )

    try:
        updated_name = 'Updated Test Configuration'
        updated_description = 'Updated test configuration by automated test'
        updated_parameters = {'updated_param': 'updated_value'}
        updated_storage = {'output': {'tables': [{'source': 'output.csv', 'destination': 'out.c-bucket.table'}]}}
        change_description = 'Automated test update'

        # Update the configuration
        updated_config = await update_config(
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


@pytest.mark.asyncio
async def test_create_component_row_configuration(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `create_component_row_configuration` creates a row configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    # First create a root configuration to add row to
    client = KeboolaClient.from_state(mcp_context.session.state)

    root_config = await create_config(
        ctx=mcp_context,
        name='Root Configuration for Row Test',
        description='Root configuration created for row configuration test',
        component_id=component_id,
        parameters={},
        storage={},
    )

    try:
        row_name = 'Test Row Configuration'
        row_description = 'Test row configuration created by automated test'
        row_parameters = {'row_param': 'row_value'}
        row_storage = {}

        # Create the row configuration
        created_row_config = await add_config_row(
            ctx=mcp_context,
            name=row_name,
            description=row_description,
            component_id=component_id,
            configuration_id=root_config.configuration_id,
            parameters=row_parameters,
            storage=row_storage,
        )

        assert isinstance(created_row_config, ComponentToolResponse)
        assert created_row_config.component_id == component_id
        assert created_row_config.configuration_id == root_config.configuration_id
        assert created_row_config.description == row_description
        assert created_row_config.success is True
        assert created_row_config.timestamp is not None
        assert len(created_row_config.links) > 0

        # Verify the row configuration exists by fetching the root configuration and checking its rows
        config_detail = await client.storage_client.configuration_detail(
            component_id=component_id, configuration_id=root_config.configuration_id
        )

        assert 'rows' in config_detail
        rows = cast(list, config_detail['rows'])
        assert len(rows) == 1

        # Find the row we just created
        created_row = None
        for row in rows:
            if isinstance(row, dict) and row.get('name') == row_name:
                created_row = row
                break

        assert created_row is not None
        assert created_row['description'] == row_description
        assert 'configuration' in created_row

        # Verify the parameters and storage were set correctly
        row_configuration_data = cast(dict, created_row['configuration'])
        assert row_configuration_data['parameters'] == row_parameters
        assert row_configuration_data['storage'] == row_storage

        # Verify metadata was set for the parent configuration
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=component_id, configuration_id=root_config.configuration_id
        )

        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        # The updated metadata should be present since we added a row to the configuration
        updated_by_md_keys = [
            key
            for key in metadata_dict.keys()
            if isinstance(key, str) and key.startswith(MetadataField.UPDATED_BY_MCP_PREFIX)
        ]
        assert len(updated_by_md_keys) > 0

    finally:
        # Delete the configuration (this will also delete the rows)
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=root_config.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_update_component_row_configuration(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `update_component_row_configuration` updates a row configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    client = KeboolaClient.from_state(mcp_context.session.state)

    # First create a root configuration
    root_config = await create_config(
        ctx=mcp_context,
        name='Root Configuration for Row Update Test',
        description='Root configuration created for row update test',
        component_id=component_id,
        parameters={},
        storage={},
    )
    assert root_config.configuration_id is not None

    try:
        # Create a row configuration
        initial_row_config = await add_config_row(
            ctx=mcp_context,
            name='Initial Row Configuration',
            description='Initial row configuration for update test',
            component_id=component_id,
            configuration_id=root_config.configuration_id,
            parameters={'initial_row_param': 'initial_row_value'},
            storage={},
        )

        # Get the row ID from the configuration detail
        config_detail = await client.storage_client.configuration_detail(
            component_id=component_id, configuration_id=initial_row_config.configuration_id
        )

        rows = cast(list, config_detail['rows'])
        assert len(rows) > 0
        row_id = rows[0]['id']

        updated_row_name = 'Updated Row Configuration'
        updated_row_description = 'Updated row configuration by automated test'
        updated_row_parameters = {'updated_row_param': 'updated_row_value'}
        updated_row_storage = {}
        change_description = 'Automated row test update'

        # Update the row configuration
        updated_row_config = await update_config_row(
            ctx=mcp_context,
            name=updated_row_name,
            description=updated_row_description,
            change_description=change_description,
            component_id=component_id,
            configuration_id=root_config.configuration_id,
            configuration_row_id=row_id,
            parameters=updated_row_parameters,
            storage=updated_row_storage,
        )

        # Test the ComponentToolResponse attributes
        assert isinstance(updated_row_config, ComponentToolResponse)
        assert updated_row_config.component_id == component_id
        assert updated_row_config.configuration_id == root_config.configuration_id
        assert updated_row_config.description == updated_row_description
        assert updated_row_config.success is True
        assert updated_row_config.timestamp is not None
        assert len(updated_row_config.links) > 0

        # Verify the row configuration was updated
        updated_config_detail = await client.storage_client.configuration_detail(
            component_id=component_id, configuration_id=updated_row_config.configuration_id
        )
        updated_rows = cast(list, updated_config_detail['rows'])
        assert len(updated_rows) > 0

        # Find the updated row
        updated_row = None
        for row in updated_rows:
            if isinstance(row, dict) and row.get('id') == row_id:
                updated_row = row
                break

        assert updated_row is not None
        assert updated_row['name'] == updated_row_name
        assert updated_row['description'] == updated_row_description

        # Verify the parameters and storage were updated correctly
        updated_row_configuration_data = cast(dict, updated_row['configuration'])
        assert updated_row_configuration_data['parameters'] == updated_row_parameters
        assert updated_row_configuration_data['storage'] == updated_row_storage

        current_row_config_version = updated_row['version']

        # Verify the metadata - check that KBC.MCP.updatedBy.version.{version} is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=component_id, configuration_id=root_config.configuration_id
        )

        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        updated_by_md_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_row_config_version}'

        assert updated_by_md_key in metadata_dict
        assert metadata_dict[updated_by_md_key] == 'true'

    finally:
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=root_config.configuration_id,
            skip_trash=True,
        )
