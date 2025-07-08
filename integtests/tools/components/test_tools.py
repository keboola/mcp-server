import logging
from typing import cast

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef, ProjectDef
from keboola_mcp_server.client import KeboolaClient, SuggestedComponent
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components.domain_models import (
    Component,
    ComponentConfigurationOutput,
    ComponentType,
    ComponentWithConfigurations,
    Configuration,
    ConfigToolOutput,
    ListConfigsOutput,
    ListTransformationsOutput,
)
from keboola_mcp_server.tools.components.tools import (
    add_config_row,
    create_config,
    create_sql_transformation,
    find_component_id,
    get_component,
    get_config,
    get_config_examples,
    list_configs,
    list_transformations,
    update_config,
    update_config_row,
    update_sql_transformation,
)
from keboola_mcp_server.tools.components.utils import (
    TransformationConfiguration,
    _get_sql_transformation_id_from_sql_dialect,
)
from keboola_mcp_server.tools.sql import get_sql_dialect

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_config(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_config` returns a `ComponentConfigurationOutput` instance."""

    for config in configs:
        assert config.configuration_id is not None

        configuration = await get_config(
            component_id=config.component_id, configuration_id=config.configuration_id, ctx=mcp_context
        )

        assert isinstance(configuration, Configuration)
        assert configuration.component is not None
        assert configuration.component.component_id == config.component_id
        assert configuration.component.component_type is not None
        assert configuration.component.component_name is not None

        assert configuration.root_configuration is not None
        assert configuration.root_configuration.configuration_id == config.configuration_id
        assert configuration.root_configuration.component_id == config.component_id
        # Check links field
        assert configuration.links, 'Links list should not be empty.'
        for link in configuration.links:
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
async def test_create_config(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests that `create_config` creates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    # Define test configuration data
    test_name = 'Test Configuration'
    test_description = 'Test configuration created by automated test'
    test_parameters = {}
    test_storage = {}

    client = KeboolaClient.from_state(mcp_context.session.state)

    project_id = keboola_project.project_id

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
        # Verify the response structure
        assert isinstance(created_config, ConfigToolOutput)
        assert created_config.component_id == component_id
        assert created_config.configuration_id is not None
        assert created_config.description == test_description
        assert created_config.success is True
        assert created_config.timestamp is not None
        assert frozenset(created_config.links) == frozenset(
            [
                Link(
                    type='ui-detail',
                    title=f'Configuration: {test_name}',
                    url=(
                        f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}/'
                        + f'{created_config.configuration_id}'
                    ),
                ),
                Link(
                    type='ui-dashboard',
                    title=f'{component_id} Configurations Dashboard',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
                ),
            ]
        )

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
        # Clean up: Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=created_config.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_update_config(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests that `update_config` updates a configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    # Define initial configuration test data
    initial_name = 'Initial Test Configuration'
    initial_description = 'Initial test configuration created by automated test'
    initial_parameters = {'initial_param': 'initial_value'}
    initial_storage = {'input': {'tables': [{'source': 'in.c-bucket.table', 'destination': 'input.csv'}]}}

    # Define updated configuration test data
    updated_name = 'Updated Test Configuration'
    updated_description = 'Updated test configuration by automated test'
    updated_parameters = {'updated_param': 'updated_value'}
    updated_storage = {'output': {'tables': [{'source': 'output.csv', 'destination': 'out.c-bucket.table'}]}}
    change_description = 'Automated test update'

    client = KeboolaClient.from_state(mcp_context.session.state)

    project_id = keboola_project.project_id

    # Create the initial configuration
    created_config = await create_config(
        ctx=mcp_context,
        name=initial_name,
        description=initial_description,
        component_id=component_id,
        parameters=initial_parameters,
        storage=initial_storage,
    )

    try:

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

        # Verify the response structure
        assert isinstance(updated_config, ConfigToolOutput)
        assert updated_config.component_id == component_id
        assert updated_config.configuration_id == created_config.configuration_id
        assert updated_config.description == updated_description
        assert updated_config.success is True
        assert updated_config.timestamp is not None
        assert frozenset(updated_config.links) == frozenset(
            [
                Link(
                    type='ui-detail',
                    title=f'Configuration: {updated_name}',
                    url=(
                        f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}/'
                        + f'{updated_config.configuration_id}'
                    ),
                ),
                Link(
                    type='ui-dashboard',
                    title=f'{component_id} Configurations Dashboard',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
                ),
            ]
        )

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
        # Clean up: Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=component_id,
            configuration_id=created_config.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_add_config_row(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests that `add_config_row` creates a row configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    # Define root configuration test data
    root_config_name = 'Root Configuration for Row Test'
    root_config_description = 'Root configuration created for row configuration test'
    root_config_parameters = {}
    root_config_storage = {}

    # Define row configuration test data
    row_name = 'Test Row Configuration'
    row_description = 'Test row configuration created by automated test'
    row_parameters = {'row_param': 'row_value'}
    row_storage = {}

    client = KeboolaClient.from_state(mcp_context.session.state)

    project_id = keboola_project.project_id

    # First create a root configuration to add row to
    root_config = await create_config(
        ctx=mcp_context,
        name=root_config_name,
        description=root_config_description,
        component_id=component_id,
        parameters=root_config_parameters,
        storage=root_config_storage,
    )

    try:

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

        assert isinstance(created_row_config, ConfigToolOutput)
        assert created_row_config.success is True
        assert created_row_config.timestamp is not None
        assert created_row_config.description == row_description
        assert created_row_config.component_id == component_id
        assert created_row_config.configuration_id == root_config.configuration_id
        assert frozenset(created_row_config.links) == frozenset(
            [
                Link(
                    type='ui-detail',
                    title=f'Configuration: {row_name}',
                    url=(
                        f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}/'
                        + f'{root_config.configuration_id}'
                    ),
                ),
                Link(
                    type='ui-dashboard',
                    title=f'{component_id} Configurations Dashboard',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
                ),
            ]
        )

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
async def test_update_config_row(mcp_context: Context, configs: list[ConfigDef], keboola_project: ProjectDef):
    """Tests that `update_config_row` updates a row configuration with correct metadata."""

    # Use the first component from configs for testing
    test_config = configs[0]
    component_id = test_config.component_id

    # Define root configuration test data
    root_config_name = 'Root Configuration for Row Update Test'
    root_config_description = 'Root configuration created for row update test'
    root_config_parameters = {}
    root_config_storage = {}

    # Define initial row configuration test data
    initial_row_name = 'Initial Row Configuration'
    initial_row_description = 'Initial row configuration for update test'
    initial_row_parameters = {'initial_row_param': 'initial_row_value'}
    initial_row_storage = {}

    # Define updated row configuration test data
    updated_row_name = 'Updated Row Configuration'
    updated_row_description = 'Updated row configuration by automated test'
    updated_row_parameters = {'updated_row_param': 'updated_row_value'}
    updated_row_storage = {}
    change_description = 'Automated row test update'

    client = KeboolaClient.from_state(mcp_context.session.state)

    # First create a root configuration
    root_config = await create_config(
        ctx=mcp_context,
        name=root_config_name,
        description=root_config_description,
        component_id=component_id,
        parameters=root_config_parameters,
        storage=root_config_storage,
    )
    assert root_config.configuration_id is not None

    project_id = keboola_project.project_id

    try:
        # Create a row configuration
        initial_row_config = await add_config_row(
            ctx=mcp_context,
            name=initial_row_name,
            description=initial_row_description,
            component_id=component_id,
            configuration_id=root_config.configuration_id,
            parameters=initial_row_parameters,
            storage=initial_row_storage,
        )

        # Get the row ID from the configuration detail
        config_detail = await client.storage_client.configuration_detail(
            component_id=component_id, configuration_id=initial_row_config.configuration_id
        )

        rows = cast(list, config_detail['rows'])
        assert len(rows) == 1
        row_id = rows[0]['id']

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
        assert isinstance(updated_row_config, ConfigToolOutput)
        assert updated_row_config.component_id == component_id
        assert updated_row_config.configuration_id == root_config.configuration_id
        assert updated_row_config.description == updated_row_description
        assert updated_row_config.success is True
        assert updated_row_config.timestamp is not None
        assert frozenset(updated_row_config.links) == frozenset(
            [
                Link(
                    type='ui-detail',
                    title=f'Configuration: {updated_row_name}',
                    url=(
                        f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}/'
                        + f'{root_config.configuration_id}'
                    ),
                ),
                Link(
                    type='ui-dashboard',
                    title=f'{component_id} Configurations Dashboard',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
                ),
            ]
        )

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


@pytest.mark.asyncio
async def test_create_sql_transformation(mcp_context: Context, keboola_project: ProjectDef):
    """Tests that `create_sql_transformation` creates a SQL transformation with correct configuration."""

    test_name = 'Test SQL Transformation'
    test_description = 'Test SQL transformation created by automated test'

    # Define test SQL code blocks
    test_sql_code_blocks = [
        TransformationConfiguration.Parameters.Block.Code(
            name='Main transformation', sql_statements=['SELECT 1 as test_column', 'SELECT 2 as another_column']
        )
    ]

    test_created_table_names = ['test_output_table']

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the SQL transformation
    created_transformation = await create_sql_transformation(
        ctx=mcp_context,
        name=test_name,
        description=test_description,
        sql_code_blocks=test_sql_code_blocks,
        created_table_names=test_created_table_names,
    )
    sql_dialect = await get_sql_dialect(mcp_context)
    expected_component_id = _get_sql_transformation_id_from_sql_dialect(sql_dialect)
    project_id = keboola_project.project_id

    try:
        # Verify the response structure
        assert isinstance(created_transformation, ConfigToolOutput)
        assert created_transformation.success is True
        assert created_transformation.timestamp is not None
        assert created_transformation.description == test_description
        assert created_transformation.component_id == expected_component_id
        assert created_transformation.configuration_id is not None
        expected_links = frozenset([
            Link(
                type='ui-detail',
                title=f'Configuration: {test_name}',
                url=(
                    f'https://connection.keboola.com/admin/projects/{project_id}/components/'
                    f'{expected_component_id}/{created_transformation.configuration_id}'
                ),
            ),
            Link(
                type='ui-dashboard',
                title=f'{expected_component_id} Configurations Dashboard',
                url=(
                    f'https://connection.keboola.com/admin/projects/{project_id}/components/'
                    f'{expected_component_id}'
                ),
            ),
        ])

        assert frozenset(created_transformation.links) == expected_links

        # Verify the configuration exists in the backend by fetching it
        config_detail = await client.storage_client.configuration_detail(
            component_id=created_transformation.component_id, configuration_id=created_transformation.configuration_id
        )

        assert config_detail['name'] == test_name
        assert config_detail['description'] == test_description
        assert 'configuration' in config_detail

        # Verify the configuration structure
        configuration_data = cast(dict, config_detail['configuration'])
        assert 'parameters' in configuration_data
        assert 'storage' in configuration_data

        # Verify the parameters structure
        parameters = configuration_data['parameters']
        assert 'blocks' in parameters
        assert len(parameters['blocks']) == 1

        block = parameters['blocks'][0]
        assert 'codes' in block
        assert len(block['codes']) == len(test_sql_code_blocks)

        code = block['codes'][0]
        assert code['name'] == test_sql_code_blocks[0].name
        assert 'script' in code  # API uses 'script' instead of 'sql_statements'
        assert len(code['script']) == len(test_sql_code_blocks[0].sql_statements)
        assert code['script'][0] == test_sql_code_blocks[0].sql_statements[0]
        assert code['script'][1] == test_sql_code_blocks[0].sql_statements[1]

        # Verify the storage structure contains output tables
        storage = configuration_data['storage']
        assert 'output' in storage
        assert 'tables' in storage['output']
        assert len(storage['output']['tables']) == len(test_created_table_names)

        output_table = storage['output']['tables'][0]
        assert output_table['source'] == test_created_table_names[0]

        # Verify the metadata - check that KBC.MCP.createdBy is set to 'true'
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=created_transformation.component_id, configuration_id=created_transformation.configuration_id
        )

        # Convert metadata list to dictionary for easier checking
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'

    finally:
        # Clean up: Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=created_transformation.component_id,
            configuration_id=created_transformation.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_update_sql_transformation(mcp_context: Context, keboola_project: ProjectDef):
    """Tests that `update_sql_transformation` updates an existing SQL transformation correctly."""
    # First, create an initial transformation
    initial_name = 'Initial SQL Transformation'
    initial_description = 'Initial SQL transformation for update test'

    initial_sql_code_blocks = [
        TransformationConfiguration.Parameters.Block.Code(
            name='Initial transformation', sql_statements=['SELECT 1 as initial_column']
        )
    ]

    initial_created_table_names = ['initial_output_table']

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the initial transformation
    created_transformation = await create_sql_transformation(
        ctx=mcp_context,
        name=initial_name,
        description=initial_description,
        sql_code_blocks=initial_sql_code_blocks,
        created_table_names=initial_created_table_names,
    )

    project_id = keboola_project.project_id
    sql_dialect = await get_sql_dialect(mcp_context)
    sql_component_id = _get_sql_transformation_id_from_sql_dialect(sql_dialect)

    try:
        # Now update the transformation
        updated_description = 'Updated SQL transformation description'
        change_description = 'Automated test update: modified SQL statements and output tables'

        # Define updated parameters and storage
        updated_parameters = TransformationConfiguration.Parameters(
            blocks=[
                TransformationConfiguration.Parameters.Block(
                    name='Updated block',
                    codes=[
                        TransformationConfiguration.Parameters.Block.Code(
                            name='Updated transformation',
                            sql_statements=[
                                'SELECT 1 as updated_column',
                                'SELECT 2 as additional_column',
                                'SELECT 3 as third_column',
                            ],
                        )
                    ],
                )
            ]
        )

        updated_storage = {
            'input': {'tables': [{'source': 'in.c-bucket.input_table', 'destination': 'input.csv'}]},
            'output': {
                'tables': [
                    {'source': 'updated_output_table', 'destination': 'out.c-bucket.updated_output_table'},
                    {'source': 'second_output_table', 'destination': 'out.c-bucket.second_output_table'},
                ]
            },
        }

        # Update the transformation
        updated_transformation = await update_sql_transformation(
            ctx=mcp_context,
            configuration_id=created_transformation.configuration_id,
            change_description=change_description,
            parameters=updated_parameters,
            storage=updated_storage,
            updated_description=updated_description,
            is_disabled=False,
        )

        # Verify the response structure
        assert isinstance(updated_transformation, ConfigToolOutput)

        assert updated_transformation.success is True
        assert updated_transformation.timestamp is not None
        assert updated_transformation.component_id == created_transformation.component_id
        assert updated_transformation.configuration_id == created_transformation.configuration_id
        assert updated_transformation.description == updated_description
        assert frozenset(updated_transformation.links) == frozenset(
            [
                Link(
                    type='ui-detail',
                    title=f'Configuration: {initial_name}',
                    url=(
                        f'https://connection.keboola.com/admin/projects/{project_id}/components/{sql_component_id}/'
                        + f'{updated_transformation.configuration_id}'
                    ),
                ),
                Link(
                    type='ui-dashboard',
                    title=f'{sql_component_id} Configurations Dashboard',
                    url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{sql_component_id}',
                ),
            ]
        )

        # Verify the updated configuration in the backend
        config_detail = await client.storage_client.configuration_detail(
            component_id=updated_transformation.component_id, configuration_id=updated_transformation.configuration_id
        )

        assert config_detail['description'] == updated_description
        assert 'configuration' in config_detail

        # Verify the updated configuration structure
        configuration_data = cast(dict, config_detail['configuration'])
        assert 'parameters' in configuration_data
        assert 'storage' in configuration_data

        # Verify the updated parameters
        parameters = configuration_data['parameters']
        assert 'blocks' in parameters
        assert len(parameters['blocks']) == len(initial_sql_code_blocks)

        block = parameters['blocks'][0]
        assert block['name'] == updated_parameters.blocks[0].name
        assert 'codes' in block
        assert len(block['codes']) == len(updated_parameters.blocks[0].codes)

        code = block['codes'][0]
        assert code['name'] == updated_parameters.blocks[0].codes[0].name
        assert 'script' in code
        assert len(code['script']) == len(updated_parameters.blocks[0].codes[0].sql_statements)
        assert code['script'][0] == updated_parameters.blocks[0].codes[0].sql_statements[0]
        assert code['script'][1] == updated_parameters.blocks[0].codes[0].sql_statements[1]
        assert code['script'][2] == updated_parameters.blocks[0].codes[0].sql_statements[2]

        # Verify the updated storage configuration
        storage = configuration_data['storage']
        assert 'input' in storage
        assert 'output' in storage

        # Check input tables
        assert 'tables' in storage['input']
        assert len(storage['input']['tables']) == len(updated_storage['input']['tables'])
        input_table = storage['input']['tables'][0]
        assert input_table['source'] == updated_storage['input']['tables'][0]['source']
        assert input_table['destination'] == updated_storage['input']['tables'][0]['destination']

        # Check output tables
        assert 'tables' in storage['output']
        assert len(storage['output']['tables']) == len(updated_storage['output']['tables'])

        output_table_1 = storage['output']['tables'][0]
        assert output_table_1['source'] == updated_storage['output']['tables'][0]['source']
        assert output_table_1['destination'] == updated_storage['output']['tables'][0]['destination']

        output_table_2 = storage['output']['tables'][1]
        assert output_table_2['source'] == updated_storage['output']['tables'][1]['source']
        assert output_table_2['destination'] == updated_storage['output']['tables'][1]['destination']

        # Verify the version was incremented
        assert cast(int, config_detail['version']) == 2

        # Verify the update metadata
        metadata = await client.storage_client.configuration_metadata_get(
            component_id=updated_transformation.component_id, configuration_id=updated_transformation.configuration_id
        )

        # Convert metadata list to dictionary for easier checking
        assert isinstance(metadata, list)
        metadata_dict = {item['key']: item['value'] for item in metadata if isinstance(item, dict)}

        # Check that original creation metadata is still there
        assert MetadataField.CREATED_BY_MCP in metadata_dict
        assert metadata_dict[MetadataField.CREATED_BY_MCP] == 'true'

        # Check that update metadata was added
        update_metadata_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{config_detail["version"]}'
        assert update_metadata_key in metadata_dict
        assert metadata_dict[update_metadata_key] == 'true'

    finally:
        # Clean up: Delete the configuration
        await client.storage_client.configuration_delete(
            component_id=created_transformation.component_id,
            configuration_id=created_transformation.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_list_transformations(mcp_context: Context):
    """Tests that `list_transformations` returns transformation configurations."""
    result = await list_transformations(ctx=mcp_context)

    assert isinstance(result, ListTransformationsOutput)
    for item in result.components_with_configurations:
        assert isinstance(item, ComponentWithConfigurations)
        assert item.component.component_type == 'transformation'


@pytest.mark.asyncio
async def test_list_transformations_by_ids(mcp_context: Context):
    """Tests that `list_transformations` returns only the specific transformations when IDs are provided."""
    # First create a SQL transformation to get its ID
    transformation_name = 'Test SQL Transformation for list_transformations_by_ids'
    transformation_description = 'Test transformation created for testing list_transformations with specific IDs'

    sql_code_blocks = [
        TransformationConfiguration.Parameters.Block.Code(
            name='Test transformation block', sql_statements=['SELECT 1 as test_column']
        )
    ]

    created_table_names = ['test_output_table']

    client = KeboolaClient.from_state(mcp_context.session.state)

    # Create the transformation
    created_transformation = await create_sql_transformation(
        ctx=mcp_context,
        name=transformation_name,
        description=transformation_description,
        sql_code_blocks=sql_code_blocks,
        created_table_names=created_table_names,
    )

    try:
        transformation_ids = [created_transformation.component_id]
        result = await list_transformations(ctx=mcp_context, transformation_ids=transformation_ids)

        assert isinstance(result, ListTransformationsOutput)
        assert len(result.components_with_configurations) == 1

        # Verify it's the transformation we specified
        component_with_configs = result.components_with_configurations[0]
        assert isinstance(component_with_configs, ComponentWithConfigurations)
        assert component_with_configs.component.component_id == created_transformation.component_id
        assert component_with_configs.component.component_type == 'transformation'
        assert (
            component_with_configs.configurations[0].root_configuration.configuration_id
            == created_transformation.configuration_id
        )

    finally:
        # Clean up: Delete the transformation
        await client.storage_client.configuration_delete(
            component_id=created_transformation.component_id,
            configuration_id=created_transformation.configuration_id,
            skip_trash=True,
        )


@pytest.mark.asyncio
async def test_get_component(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_component` returns component details."""
    test_config = configs[0]
    component_id = test_config.component_id

    result = await get_component(component_id=component_id, ctx=mcp_context)

    assert isinstance(result, Component)
    assert result.component_id == test_config.component_id


@pytest.mark.asyncio
async def test_get_config_examples(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_config_examples` returns configuration examples in markdown format."""
    test_config = configs[0]
    component_id = test_config.component_id

    result = await get_config_examples(component_id=component_id, ctx=mcp_context)

    # Verify the result is a markdown formatted string
    assert isinstance(result, str)
    assert f'# Configuration Examples for `{component_id}`' in result
    assert f'{component_id}`' in result
    assert 'parameters' in result


@pytest.mark.asyncio
async def test_find_component_id(mcp_context: Context):
    """Tests that `find_component_id` returns relevant component IDs for a query."""
    query = 'generic extractor'
    generic_extractor_id = 'ex-generic-v2'

    result = await find_component_id(query=query, ctx=mcp_context)

    assert isinstance(result, list)
    assert len(result) > 0
    assert generic_extractor_id in [component.component_id for component in result]

    for component in result:
        assert isinstance(component, SuggestedComponent)


@pytest.mark.asyncio
async def test_get_config_examples_with_invalid_component(mcp_context: Context):
    """Tests that `get_config_examples` handles non-existent components properly."""

    result = await get_config_examples(ctx=mcp_context, component_id='completely-non-existent-component-12345')

    assert result == ''
