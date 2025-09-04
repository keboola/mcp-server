import logging
from typing import Any, AsyncGenerator, cast

import pytest
import pytest_asyncio
from fastmcp import Client, FastMCP
from mcp.server.fastmcp import Context

from integtests.conftest import ConfigDef, ProjectDef
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import Config, MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components import (
    add_config_row,
    create_config,
    create_sql_transformation,
    get_component,
    get_config,
    get_config_examples,
    list_configs,
    list_transformations,
)
from keboola_mcp_server.tools.components.model import (
    Component,
    ComponentType,
    ComponentWithConfigurations,
    ConfigToolOutput,
    Configuration,
    ListConfigsOutput,
    ListTransformationsOutput,
)
from keboola_mcp_server.tools.components.utils import (
    TransformationConfiguration,
    get_sql_transformation_id_from_sql_dialect,
)
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_config(mcp_context: Context, configs: list[ConfigDef]):
    """Tests that `get_config` returns a `Configuration` instance."""

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

        assert configuration.configuration_root is not None
        assert configuration.configuration_root.configuration_id == config.configuration_id
        assert configuration.configuration_root.component_id == config.component_id
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
            assert config.configuration_root.component_id == item.component.component_id


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
        assert created_config.version is not None
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


@pytest.fixture
def mcp_server(storage_api_url: str, storage_api_token: str, workspace_schema: str) -> FastMCP:
    config = Config(storage_api_url=storage_api_url, storage_token=storage_api_token, workspace_schema=workspace_schema)
    return create_server(config)


@pytest_asyncio.fixture
async def mcp_client(mcp_server: FastMCP) -> AsyncGenerator[Client, None]:
    async with Client(mcp_server) as client:
        yield client


@pytest_asyncio.fixture
async def initial_cmpconf(
    mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> AsyncGenerator[ConfigToolOutput, None]:
    # Create the initial component configuration test data
    tool_result = await mcp_client.call_tool(
        name='create_config',
        arguments={
            'name': 'Initial Test Configuration',
            'description': 'Initial test configuration created by automated test',
            'component_id': configs[0].component_id,
            'parameters': {'initial_param': 'initial_value'},
            'storage': {'input': {'tables': [{'source': 'in.c-bucket.table', 'destination': 'input.csv'}]}},
        },
    )
    try:
        yield ConfigToolOutput.model_validate(tool_result.structured_content)
    finally:
        # Clean up: Delete the configuration
        await keboola_client.storage_client.configuration_delete(
            component_id=configs[0].component_id,
            configuration_id=tool_result.structured_content['configuration_id'],
            skip_trash=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'updates',
    [
        {
            'name': 'Updated Test Configuration',
            'description': 'Updated test configuration by automated test',
            'parameters': {'updated_param': 'updated_value'},
            'storage': {'output': {'tables': [{'source': 'output.csv', 'destination': 'out.c-bucket.table'}]}},
        },
        {'name': 'Updated just name'},
        {'description': 'Updated just description'},
        {'parameters': {'updated_param': 'Updated just parameters'}},
        {'storage': {'output': {'tables': [{'source': 'output.csv', 'destination': 'out.c-bucket.table'}]}}},
    ],
)
async def test_update_config(
    updates: dict[str, Any],
    initial_cmpconf: ConfigToolOutput,
    mcp_client: Client,
    keboola_project: ProjectDef,
    keboola_client: KeboolaClient,
):
    """Tests that `update_config` updates a configuration with correct metadata."""
    project_id = keboola_project.project_id
    component_id = initial_cmpconf.component_id
    configuration_id = initial_cmpconf.configuration_id
    tool_result = await mcp_client.call_tool(
        name='update_config',
        arguments={
            'change_description': 'Integration test update',
            'component_id': component_id,
            'configuration_id': configuration_id,
            **updates,
        },
    )

    # Check the tool's output
    updated_config = ConfigToolOutput.model_validate(tool_result.structured_content)
    assert updated_config.component_id == component_id
    assert updated_config.configuration_id == configuration_id
    assert updated_config.success is True
    assert updated_config.timestamp is not None
    assert updated_config.version is not None

    expected_name = updates.get('name') or 'Initial Test Configuration'
    expected_description = updates.get('description') or initial_cmpconf.description
    assert updated_config.description == expected_description
    assert frozenset(updated_config.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Configuration: {expected_name}',
                url='https://connection.keboola.com/admin'
                f'/projects/{project_id}/components/{component_id}/{configuration_id}',
            ),
            Link(
                type='ui-dashboard',
                title=f'{component_id} Configurations Dashboard',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
            ),
        ]
    )

    # Verify the configuration was updated
    config_detail = await keboola_client.storage_client.configuration_detail(
        component_id=updated_config.component_id, configuration_id=updated_config.configuration_id
    )

    assert config_detail['name'] == expected_name
    assert config_detail['description'] == expected_description

    config_data = config_detail.get('configuration')
    assert isinstance(config_data, dict), f'Expecting dict, got: {type(config_data)}'

    if (expected_parameters := updates.get('parameters')) is not None:
        assert config_data['parameters'] == expected_parameters

    if (expected_storage := updates.get('storage')) is not None:
        # Storage API might return more keys than what we set, so we check subset
        for k, v in expected_storage.items():
            assert k in config_data['storage']
            assert config_data['storage'][k] == v

    current_version = config_detail['version']
    assert isinstance(current_version, int), f'Expecting int, got: {type(current_version)}'
    assert current_version == 2

    # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
    metadata = await keboola_client.storage_client.configuration_metadata_get(
        component_id=updated_config.component_id, configuration_id=updated_config.configuration_id
    )
    assert isinstance(metadata, list), f'Expecting list, got: {type(metadata)}'

    meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_version}'
    meta_value = get_metadata_property(metadata, meta_key)
    assert meta_value == 'true'
    # Check that the original creation metadata is still there
    assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'


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
        assert created_row_config.version is not None
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


@pytest_asyncio.fixture
async def initial_cmpconf_row(
    initial_cmpconf: ConfigToolOutput, mcp_client: Client, keboola_client: KeboolaClient
) -> ConfigToolOutput:
    # Create initial row configuration test data
    tool_result = await mcp_client.call_tool(
        name='add_config_row',
        arguments={
            'name': 'Initial Test Row Configuration',
            'description': 'Initial row configuration for update test',
            'component_id': initial_cmpconf.component_id,
            'configuration_id': initial_cmpconf.configuration_id,
            'parameters': {'initial_row_param': 'initial_row_value'},
            'storage': {},
        },
    )
    return ConfigToolOutput.model_validate(tool_result.structured_content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'updates',
    [
        {
            'name': 'Updated Row Configuration',
            'description': 'Updated row configuration by automated test',
            'parameters': {'updated_row_param': 'updated_row_value'},
            'storage': {},
        },
        {'name': 'Updated just name'},
        {'description': 'Updated just description'},
        {'parameters': {'updated_row_param': 'Updated just parameters'}},
        {'storage': {'output': {'tables': [{'source': 'output.csv', 'destination': 'out.c-bucket.table'}]}}},
    ],
)
async def test_update_config_row(
    updates: dict[str, Any],
    initial_cmpconf_row: ConfigToolOutput,
    mcp_client: Client,
    keboola_project: ProjectDef,
    keboola_client: KeboolaClient,
):
    """Tests that `update_config_row` updates a row configuration with correct metadata."""
    project_id = keboola_project.project_id
    component_id = initial_cmpconf_row.component_id
    configuration_id = initial_cmpconf_row.configuration_id

    # Get the row ID from the configuration detail
    config_detail = await keboola_client.storage_client.configuration_detail(
        component_id=component_id, configuration_id=configuration_id
    )
    rows = config_detail['rows']
    assert isinstance(rows, list)
    assert len(rows) == 1
    row_id = rows[0]['id']

    tool_result = await mcp_client.call_tool(
        name='update_config_row',
        arguments={
            'change_description': 'Integration test update',
            'component_id': component_id,
            'configuration_id': configuration_id,
            'configuration_row_id': row_id,
            **updates,
        },
    )

    # Check the tool's output
    updated_row_config = ConfigToolOutput.model_validate(tool_result.structured_content)
    assert updated_row_config.component_id == component_id
    assert updated_row_config.configuration_id == configuration_id
    assert updated_row_config.success is True
    assert updated_row_config.timestamp is not None
    assert updated_row_config.version is not None

    expected_row_name = updates.get('name') or 'Initial Test Row Configuration'
    expected_row_description = updates.get('description') or initial_cmpconf_row.description
    assert updated_row_config.description == expected_row_description
    assert frozenset(updated_row_config.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Configuration: {expected_row_name}',
                url='https://connection.keboola.com/admin'
                f'/projects/{project_id}/components/{component_id}/{configuration_id}',
            ),
            Link(
                type='ui-dashboard',
                title=f'{component_id} Configurations Dashboard',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
            ),
        ]
    )

    # Verify the row configuration was updated
    row_config_detail = await keboola_client.storage_client.configuration_detail(
        component_id=updated_row_config.component_id, configuration_id=updated_row_config.configuration_id
    )
    updated_rows = row_config_detail['rows']
    assert isinstance(updated_rows, list), f'Expecting list, got: {type(updated_rows)}'
    # Find the updated row
    updated_row = next(filter(lambda x: x.get('id') == row_id, updated_rows), None)
    assert updated_row, f'No row for row_id: {row_id}'

    assert isinstance(updated_row, dict), f'Expecting dict, got: {type(updated_row)}'
    assert updated_row['name'] == expected_row_name
    assert updated_row['description'] == expected_row_description

    row_config_data = updated_row['configuration']
    assert isinstance(row_config_data, dict), f'Expecting dict, got: {type(row_config_data)}'

    if (expected_row_parameters := updates.get('parameters')) is not None:
        assert row_config_data['parameters'] == expected_row_parameters

    if (expected_storage := updates.get('storage')) is not None:
        # Storage API might return more keys than what we set, so we check subset
        for k, v in expected_storage.items():
            assert k in row_config_data['storage']
            assert row_config_data['storage'][k] == v

    current_version = config_detail['version']
    assert isinstance(current_version, int), f'Expecting int, got: {type(current_version)}'
    assert current_version == 2

    # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
    metadata = await keboola_client.storage_client.configuration_metadata_get(
        component_id=updated_row_config.component_id, configuration_id=updated_row_config.configuration_id
    )
    assert isinstance(metadata, list), f'Expecting list, got: {type(metadata)}'

    meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_version}'
    meta_value = get_metadata_property(metadata, meta_key)
    assert meta_value == 'true'
    # Check that the original creation metadata is still there
    assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'


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
    sql_dialect = await WorkspaceManager.from_state(mcp_context.session.state).get_sql_dialect()
    expected_component_id = get_sql_transformation_id_from_sql_dialect(sql_dialect)
    project_id = keboola_project.project_id

    try:
        # Verify the response structure
        assert isinstance(created_transformation, ConfigToolOutput)
        assert created_transformation.success is True
        assert created_transformation.timestamp is not None
        assert created_transformation.description == test_description
        assert created_transformation.component_id == expected_component_id
        assert created_transformation.configuration_id is not None
        assert created_transformation.version is not None
        expected_links = frozenset(
            [
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
            ]
        )

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


@pytest_asyncio.fixture
async def initial_sqltrfm(
    mcp_client: Client, configs: list[ConfigDef], keboola_client: KeboolaClient
) -> AsyncGenerator[ConfigToolOutput, None]:
    # Create the initial component configuration test data
    tool_result = await mcp_client.call_tool(
        name='create_sql_transformation',
        arguments={
            'name': 'Initial Test SQL Transformation',
            'description': 'Initial SQL transformation for update test',
            'sql_code_blocks': [{'name': 'Initial transformation', 'sql_statements': ['SELECT 1 as initial_column']}],
            'created_table_names': ['initial_output_table'],
        },
    )
    try:
        yield ConfigToolOutput.model_validate(tool_result.structured_content)
    finally:
        # Clean up: Delete the configuration
        await keboola_client.storage_client.configuration_delete(
            component_id=tool_result.structured_content['component_id'],
            configuration_id=tool_result.structured_content['configuration_id'],
            skip_trash=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'updates',
    [
        {
            'updated_description': 'Updated SQL transformation description',
            'parameters': {
                'blocks': [
                    {
                        'name': 'Updated block',
                        'codes': [
                            {
                                'name': 'Updated transformation',
                                'sql_statements': [
                                    'SELECT 1 as updated_column',
                                    'SELECT 2 as additional_column',
                                    'SELECT 3 as third_column',
                                ],
                            }
                        ],
                    }
                ]
            },
            'storage': {
                'input': {'tables': [{'source': 'in.c-bucket.input_table', 'destination': 'input.csv'}]},
                'output': {
                    'tables': [
                        {'source': 'updated_output_table', 'destination': 'out.c-bucket.updated_output_table'},
                        {'source': 'second_output_table', 'destination': 'out.c-bucket.second_output_table'},
                    ]
                },
            },
            'is_disabled': True,
        },
        {'updated_description': 'Updated SQL transformation description'},
        {
            'parameters': {
                'blocks': [
                    {
                        'name': 'Updated block',
                        'codes': [
                            {
                                'name': 'Updated transformation',
                                'sql_statements': [
                                    'SELECT 1 as updated_column',
                                    'SELECT 2 as additional_column',
                                    'SELECT 3 as third_column',
                                ],
                            }
                        ],
                    }
                ]
            }
        },
        {
            'storage': {
                'input': {'tables': [{'source': 'in.c-bucket.input_table', 'destination': 'input.csv'}]},
                'output': {
                    'tables': [
                        {'source': 'updated_output_table', 'destination': 'out.c-bucket.updated_output_table'},
                        {'source': 'second_output_table', 'destination': 'out.c-bucket.second_output_table'},
                    ]
                },
            }
        },
        {'is_disabled': True},
    ],
)
async def test_update_sql_transformation(
    updates: dict[str, Any],
    initial_sqltrfm: ConfigToolOutput,
    mcp_client: Client,
    keboola_project: ProjectDef,
    keboola_client: KeboolaClient,
):
    """Tests that `update_sql_transformation` updates an existing SQL transformation correctly."""
    project_id = keboola_project.project_id
    component_id = initial_sqltrfm.component_id
    configuration_id = initial_sqltrfm.configuration_id
    tool_result = await mcp_client.call_tool(
        name='update_sql_transformation',
        arguments={
            'change_description': 'Integration test update',
            'configuration_id': configuration_id,
            **updates,
        },
    )

    # Check the tool's output
    updated_trfm = ConfigToolOutput.model_validate(tool_result.structured_content)

    assert updated_trfm.component_id == component_id
    assert updated_trfm.configuration_id == configuration_id
    assert updated_trfm.success is True
    assert updated_trfm.timestamp is not None
    assert updated_trfm.version is not None

    expected_name = updates.get('name') or 'Initial Test SQL Transformation'
    expected_description = updates.get('updated_description') or initial_sqltrfm.description
    assert updated_trfm.description == expected_description
    assert frozenset(updated_trfm.links) == frozenset(
        [
            Link(
                type='ui-detail',
                title=f'Configuration: {expected_name}',
                url='https://connection.keboola.com/admin'
                f'/projects/{project_id}/components/{component_id}/{configuration_id}',
            ),
            Link(
                type='ui-dashboard',
                title=f'{component_id} Configurations Dashboard',
                url=f'https://connection.keboola.com/admin/projects/{project_id}/components/{component_id}',
            ),
        ]
    )

    # Verify the transformation was updated
    trfm_detail = await keboola_client.storage_client.configuration_detail(
        component_id=updated_trfm.component_id, configuration_id=updated_trfm.configuration_id
    )

    assert trfm_detail['name'] == expected_name
    assert trfm_detail['description'] == expected_description

    trfm_data = trfm_detail.get('configuration')
    assert isinstance(trfm_data, dict), f'Expecting dict, got: {type(trfm_data)}'

    actual_parameters = trfm_data.get('parameters')
    assert isinstance(actual_parameters, dict), f'Expecting dict, got: {type(actual_parameters)}'
    if (expected_parameters := updates.get('updated_parameters')) is not None:
        assert isinstance(expected_description, TransformationConfiguration.Parameters)
        assert actual_parameters == expected_parameters.model_dump()

    actual_storage = trfm_data.get('storage')
    assert isinstance(actual_storage, dict), f'Expecting dict, got: {type(actual_storage)}'
    if (expected_storage := updates.get('storage')) is not None:
        # Storage API might return more keys than what we set, so we check subset
        for k, v in expected_storage.items():
            assert k in trfm_data['storage']
            assert trfm_data['storage'][k] == v

    if (expected_is_disabled := updates.get('is_disabled')) is not None:
        assert trfm_detail['isDisabled'] == expected_is_disabled

    current_version = trfm_detail['version']
    assert isinstance(current_version, int), f'Expecting int, got: {type(current_version)}'
    assert current_version == 2

    # Check that KBC.MCP.updatedBy.version.{version} is set to 'true'
    metadata = await keboola_client.storage_client.configuration_metadata_get(
        component_id=updated_trfm.component_id, configuration_id=updated_trfm.configuration_id
    )
    assert isinstance(metadata, list), f'Expecting list, got: {type(metadata)}'

    meta_key = f'{MetadataField.UPDATED_BY_MCP_PREFIX}{current_version}'
    meta_value = get_metadata_property(metadata, meta_key)
    assert meta_value == 'true'
    # Check that the original creation metadata is still there
    assert get_metadata_property(metadata, MetadataField.CREATED_BY_MCP) == 'true'


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
            component_with_configs.configurations[0].configuration_root.configuration_id
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
async def test_get_config_examples_with_invalid_component(mcp_context: Context):
    """Tests that `get_config_examples` handles non-existent components properly."""

    result = await get_config_examples(ctx=mcp_context, component_id='completely-non-existent-component-12345')

    assert result == ''
