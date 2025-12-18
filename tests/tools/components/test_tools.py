import asyncio
from typing import Any, Callable

import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components import (
    add_config_row,
    create_config,
    create_sql_transformation,
    get_components,
    get_config_examples,
    get_configs,
    update_config,
    update_config_row,
    update_sql_transformation,
)
from keboola_mcp_server.tools.components.model import (
    Component,
    ComponentCapabilities,
    ComponentSummary,
    ComponentType,
    ComponentWithConfigs,
    ConfigParamRemove,
    ConfigParamReplace,
    ConfigParamSet,
    ConfigParamUpdate,
    ConfigSummary,
    ConfigToolOutput,
    Configuration,
    ConfigurationRootSummary,
    FullConfigId,
    GetComponentsOutput,
    GetConfigsDetailOutput,
    GetConfigsListOutput,
    SimplifiedTfBlocks,
    TfParamUpdate,
    TfRenameBlock,
    TfSetCode,
    TfStrReplace,
)
from keboola_mcp_server.tools.components.sql_utils import format_simplified_tf_code
from keboola_mcp_server.tools.components.utils import clean_bucket_name
from keboola_mcp_server.workspace import WorkspaceManager

# ============================================================================
# get_configs TESTS
# ============================================================================


@pytest.fixture
def assert_get_configs_list() -> Callable[
    [
        GetConfigsListOutput,
        list[dict[str, Any]],
        list[dict[str, Any]],
    ],
    None,
]:
    """Assert that the get_configs tool (list mode) returns the correct components and configurations."""

    def _assert_get_configs_list(
        result: GetConfigsListOutput,
        components: list[dict[str, Any]],
        configurations: list[dict[str, Any]],
    ):
        components_with_configs = result.components_with_configs

        assert len(components_with_configs) == len(components)
        # assert basics
        assert all(isinstance(component, ComponentWithConfigs) for component in components_with_configs)
        assert all(isinstance(component.component, ComponentSummary) for component in components_with_configs)
        assert all(isinstance(component.configs, list) for component in components_with_configs)
        assert all(
            all(isinstance(config, ConfigSummary) for config in component.configs)
            for component in components_with_configs
        )
        # assert component list details
        assert all(
            returned.component.component_id == expected['id']
            for returned, expected in zip(components_with_configs, components)
        )
        assert all(
            returned.component.component_name == expected['name']
            for returned, expected in zip(components_with_configs, components)
        )
        assert all(
            returned.component.component_type == expected['type']
            for returned, expected in zip(components_with_configs, components)
        )
        assert all(not hasattr(returned.component, 'version') for returned in components_with_configs)

        # assert configurations list details
        assert all(len(component.configs) == len(configurations) for component in components_with_configs)
        assert all(
            all(isinstance(config.configuration_root, ConfigurationRootSummary) for config in component.configs)
            for component in components_with_configs
        )
        # use zip to iterate over the result and mock_configurations since we artificially mock the .get method
        assert all(
            all(
                config.configuration_root.configuration_id == expected['id']
                for config, expected in zip(component.configs, configurations)
            )
            for component in components_with_configs
        )
        assert all(
            all(
                config.configuration_root.name == expected['name']
                for config, expected in zip(component.configs, configurations)
            )
            for component in components_with_configs
        )

    return _assert_get_configs_list


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('component_types', 'expected_types', 'expected_mock_comp_idxs'),
    [
        # No filter - should retrieve all component types (including transformation)
        # Order: application, extractor, transformation, writer
        ([], ['application', 'extractor', 'transformation', 'writer'], [2, 0, 3, 1]),
        # Single type - extractor only
        (['extractor'], ['extractor'], [0]),
        # Single type - writer only
        (['writer'], ['writer'], [1]),
        # Single type - application only
        (['application'], ['application'], [2]),
        # Single type - transformation only
        (['transformation'], ['transformation'], [3]),
        # Multiple types - extractor and writer
        # Order: extractor, writer
        (['extractor', 'writer'], ['extractor', 'writer'], [0, 1]),
        # Multiple types - extractor, writer, and application
        # Order: application, extractor, writer
        (['extractor', 'writer', 'application'], ['application', 'extractor', 'writer'], [2, 0, 1]),
    ],
)
async def test_get_configs_by_types(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_components: list[dict[str, Any]],
    mock_configurations: list[dict[str, Any]],
    assert_get_configs_list: Callable[[GetConfigsListOutput, list[dict[str, Any]], list[dict[str, Any]]], None],
    component_types: list[ComponentType],
    expected_types: list[ComponentType],
    expected_mock_comp_idxs: list[int],
):
    """
    Test get_configs (list mode) when component types are provided with various filters.
    The expected_mock_comp_idxs are the indices of mock_components that should be returned.
    """
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Create a mapping of component_type to the matching mock component
    component_type_map = {comp['type']: comp for comp in mock_components}

    # Create a side_effect function that returns the correct component based on component_type
    async def mock_component_list(component_type: ComponentType, include: list[str] | None = None):
        # Return matching component or empty list if no transformation exists
        if component_type in component_type_map:
            return [{**component_type_map[component_type], 'configurations': mock_configurations}]
        return []

    keboola_client.storage_client.component_list = mocker.AsyncMock(side_effect=mock_component_list)

    result = await get_configs(ctx=context, component_types=component_types)

    # Verify we get the list output type
    assert isinstance(result, GetConfigsListOutput)

    # Get the expected components based on the indices
    expected_components = [mock_components[i] for i in expected_mock_comp_idxs]
    assert_get_configs_list(result, expected_components, mock_configurations)

    # Verify the calls were made with the correct arguments (in sorted order)
    expected_calls = [mocker.call(component_type=comp_type, include=['configuration']) for comp_type in expected_types]
    keboola_client.storage_client.component_list.assert_has_calls(expected_calls)


@pytest.mark.asyncio
async def test_get_configs_by_component_ids(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configurations: list[dict[str, Any]],
    mock_component: dict[str, Any],
    assert_get_configs_list: Callable[[GetConfigsListOutput, list[dict[str, Any]], list[dict[str, Any]]], None],
):
    """Test get_configs (list mode) when component IDs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=mock_configurations)
    keboola_client.storage_client.component_detail = mocker.AsyncMock(return_value=mock_component)

    result = await get_configs(ctx=context, component_ids=[mock_component['id']])

    # Verify we get the list output type
    assert isinstance(result, GetConfigsListOutput)

    assert_get_configs_list(result, [mock_component], mock_configurations)

    # Verify the calls were made with the correct arguments
    keboola_client.storage_client.configuration_list.assert_called_once_with(component_id=mock_component['id'])
    keboola_client.storage_client.component_detail.assert_called_once_with(component_id=mock_component['id'])


@pytest.mark.asyncio
async def test_get_configs_detail(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configuration: dict[str, Any],
    mock_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
):
    """Test get_configs (detail mode) when specific configs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Get URL components from context for link assertions
    storage_api_url = keboola_client.storage_api_url
    project_id = await keboola_client.storage_client.project_id()
    base_url = f'{storage_api_url}/admin/projects/{project_id}'

    mock_ai_service = mocker.MagicMock()
    mock_ai_service.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    keboola_client.ai_service_client = mock_ai_service
    # mock the configuration_detail method to return the mock_configuration
    # simulate the response from the API
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
        return_value={**mock_configuration, 'component': mock_component, 'configurationMetadata': mock_metadata}
    )

    configs = [FullConfigId(component_id=mock_component['id'], configuration_id=mock_configuration['id'])]
    result = await get_configs(ctx=context, configs=configs)

    # Verify we get the detail output type
    assert isinstance(result, GetConfigsDetailOutput)
    assert len(result.configs) == 1

    config = result.configs[0]
    assert config.configuration_root.configuration_id == mock_configuration['id']
    assert config.configuration_root.name == mock_configuration['name']
    assert config.component is not None
    assert config.component.component_id == mock_component['id']
    assert config.component.component_name == mock_component['name']

    # Verify links
    assert set(config.links) == {
        Link(
            type='ui-detail',
            title=f'Configuration: {mock_configuration["name"]}',
            url=f'{base_url}/components/{mock_component["id"]}/{mock_configuration["id"]}',
        ),
        Link(
            type='ui-dashboard',
            title=f'Component "{mock_component["id"]}" Configurations Dashboard',
            url=f'{base_url}/components/{mock_component["id"]}',
        ),
    }

    # Verify the calls were made with the correct arguments
    keboola_client.storage_client.configuration_detail.assert_called_once_with(
        component_id=mock_component['id'], configuration_id=mock_configuration['id']
    )


@pytest.mark.asyncio
async def test_get_configs_detail_multiple(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configuration: dict[str, Any],
    mock_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
):
    """Test get_configs (detail mode) when multiple specific configs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Create a second configuration
    mock_configuration_2 = {**mock_configuration, 'id': '456', 'name': 'My Config 2'}

    mock_ai_service = mocker.MagicMock()
    mock_ai_service.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    keboola_client.ai_service_client = mock_ai_service

    # Return different configs based on the configuration_id
    async def mock_config_detail(component_id: str, configuration_id: str):
        if configuration_id == mock_configuration['id']:
            return {**mock_configuration, 'component': mock_component, 'configurationMetadata': mock_metadata}
        else:
            return {**mock_configuration_2, 'component': mock_component, 'configurationMetadata': mock_metadata}

    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

    configs = [
        FullConfigId(component_id=mock_component['id'], configuration_id=mock_configuration['id']),
        FullConfigId(component_id=mock_component['id'], configuration_id=mock_configuration_2['id']),
    ]
    result = await get_configs(ctx=context, configs=configs)

    # Verify we get the detail output type with multiple configs
    assert isinstance(result, GetConfigsDetailOutput)
    assert len(result.configs) == 2

    # Verify both configs are present
    config_ids = {c.configuration_root.configuration_id for c in result.configs}
    assert config_ids == {mock_configuration['id'], mock_configuration_2['id']}

    # Verify each config has the expected data
    for config in result.configs:
        assert isinstance(config, Configuration)
        assert config.component is not None
        assert config.component.component_id == mock_component['id']


@pytest.mark.asyncio
async def test_get_configs_detail_transformation(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_tf_configuration: dict[str, Any],
    mock_tf_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
):
    """
    Test get_configs (detail mode) for transformations.
    We test that the transformation parameters are correctly simplified and IDs are added.
    """
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    mock_ai_service = mocker.MagicMock()
    mock_ai_service.get_component_detail = mocker.AsyncMock(return_value=mock_tf_component)

    keboola_client.ai_service_client = mock_ai_service
    # mock the configuration_detail method to return the mock_configuration
    # simulate the response from the API
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
        return_value={**mock_tf_configuration, 'component': mock_tf_component, 'configurationMetadata': mock_metadata}
    )

    configs = [
        FullConfigId(component_id=mock_tf_component['componentId'], configuration_id=mock_tf_configuration['id'])
    ]
    result = await get_configs(ctx=context, configs=configs)

    # Verify we get the detail output type
    assert isinstance(result, GetConfigsDetailOutput)
    assert len(result.configs) == 1

    config = result.configs[0]
    assert isinstance(config, Configuration)
    assert config.configuration_root.parameters == {
        'blocks': [
            {
                'id': 'b0',
                'name': 'Blocks',
                'codes': [
                    {'id': 'b0.c0', 'name': 'Code 1', 'script': 'SELECT * FROM customers;\n\nSELECT * FROM orders;\n\n'}
                ],
            }
        ],
    }


@pytest.mark.asyncio
async def test_get_configs_detail_ignores_other_params(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configuration: dict[str, Any],
    mock_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
):
    """Test that get_configs (detail mode) ignores component_types and component_ids when configs is provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    mock_ai_service = mocker.MagicMock()
    mock_ai_service.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    keboola_client.ai_service_client = mock_ai_service
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(
        return_value={**mock_configuration, 'component': mock_component, 'configurationMetadata': mock_metadata}
    )
    keboola_client.storage_client.component_list = mocker.AsyncMock()
    keboola_client.storage_client.configuration_list = mocker.AsyncMock()

    configs = [FullConfigId(component_id=mock_component['id'], configuration_id=mock_configuration['id'])]

    # Provide all params, but configs takes precedence
    result = await get_configs(
        ctx=context,
        component_types=['extractor', 'writer'],  # Should be ignored
        component_ids=['some-other-component'],  # Should be ignored
        configs=configs,  # This should be used
    )

    # Verify we get the detail output type
    assert isinstance(result, GetConfigsDetailOutput)
    assert len(result.configs) == 1

    # Verify that component_list and configuration_list were NOT called (because configs takes precedence)
    keboola_client.storage_client.component_list.assert_not_called()
    keboola_client.storage_client.configuration_list.assert_not_called()

    # Verify configuration_detail was called for the specified config
    keboola_client.storage_client.configuration_detail.assert_called_once_with(
        component_id=mock_component['id'], configuration_id=mock_configuration['id']
    )


@pytest.mark.asyncio
async def test_get_components(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_components: list[dict[str, Any]],
):
    """Test get_components tool fetches components concurrently."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    component_ids = [comp['id'] for comp in mock_components[:3]]

    # Get URL components from context
    storage_api_url = keboola_client.storage_api_url
    project_id = await keboola_client.storage_client.project_id()
    base_url = f'{storage_api_url}/admin/projects/{project_id}'

    # Track call order to verify concurrent execution
    call_order: list[str] = []

    async def mock_fetch_component(client: KeboolaClient, component_id: str):
        from keboola_mcp_server.tools.components.api_models import ComponentAPIResponse

        call_order.append(component_id)
        # Find the matching mock component
        for comp in mock_components:
            if comp['id'] == component_id:
                return ComponentAPIResponse.model_validate(comp)
        raise ValueError(f'Component {component_id} not found')

    mocker.patch(
        'keboola_mcp_server.tools.components.tools.fetch_component',
        side_effect=mock_fetch_component,
    )

    result = await get_components(ctx=context, component_ids=component_ids)

    # Verify all components were fetched
    assert set(call_order) == set(component_ids)

    # Build expected components
    expected_components = [
        Component(
            component_id=mock_components[0]['id'],
            component_name=mock_components[0]['name'],
            component_type=mock_components[0]['type'],
            component_categories=[],
            capabilities=ComponentCapabilities(),
            links=[
                Link(
                    type='ui-dashboard',
                    title=f'{mock_components[0]["name"]} Configurations Dashboard',
                    url=f'{base_url}/components/{mock_components[0]["id"]}',
                )
            ],
        ),
        Component(
            component_id=mock_components[1]['id'],
            component_name=mock_components[1]['name'],
            component_type=mock_components[1]['type'],
            component_categories=[],
            capabilities=ComponentCapabilities(),
            links=[
                Link(
                    type='ui-dashboard',
                    title=f'{mock_components[1]["name"]} Configurations Dashboard',
                    url=f'{base_url}/components/{mock_components[1]["id"]}',
                )
            ],
        ),
        Component(
            component_id=mock_components[2]['id'],
            component_name=mock_components[2]['name'],
            component_type=mock_components[2]['type'],
            component_categories=[],
            capabilities=ComponentCapabilities(),
            links=[
                Link(
                    type='ui-dashboard',
                    title=f'{mock_components[2]["name"]} Configurations Dashboard',
                    url=f'{base_url}/components/{mock_components[2]["id"]}',
                )
            ],
        ),
    ]

    expected_output = GetComponentsOutput(
        components=expected_components,
        links=[
            Link(
                type='ui-dashboard',
                title='Used Components Dashboard',
                url=f'{base_url}/components/configurations',
            )
        ],
    )

    assert result == expected_output


@pytest.mark.parametrize(
    ('sql_dialect', 'expected_component_id', 'expected_configuration_id'),
    [
        ('Snowflake', 'keboola.snowflake-transformation', '1234'),
        ('BigQuery', 'keboola.google-bigquery-transformation', '5678'),
    ],
)
@pytest.mark.asyncio
async def test_create_sql_transformation(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
    expected_configuration_id: str,
):
    """Test create_sql_transformation tool."""
    context = mcp_context_components_configs

    # Mock the WorkspaceManager
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)
    # Mock the KeboolaClient
    keboola_client = KeboolaClient.from_state(context.session.state)
    component = mock_component
    component['id'] = expected_component_id
    configuration = mock_configuration
    configuration['id'] = expected_configuration_id

    # Set up the mock for ai_service_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=component)
    keboola_client.storage_client.configuration_create = mocker.AsyncMock(return_value=configuration)

    transformation_name = mock_configuration['name']
    bucket_name = clean_bucket_name(transformation_name)
    description = mock_configuration['description']
    code_blocks = [
        SimplifiedTfBlocks.Block.Code(name='Code 0', script='SELECT * FROM test'),
        SimplifiedTfBlocks.Block.Code(name='Code 1', script='SELECT * FROM test2; SELECT * FROM test3;'),
    ]
    created_table_name = 'test_table_1'

    # Test the create_sql_transformation tool
    new_transformation_configuration = await create_sql_transformation(
        ctx=context,
        name=transformation_name,
        description=description,
        sql_code_blocks=code_blocks,
        created_table_names=[created_table_name],
    )

    assert isinstance(new_transformation_configuration, ConfigToolOutput)
    assert new_transformation_configuration.component_id == expected_component_id
    assert new_transformation_configuration.configuration_id == mock_configuration['id']
    assert new_transformation_configuration.description == mock_configuration['description']
    assert new_transformation_configuration.version == mock_configuration['version']

    formatted_code_blocks = [format_simplified_tf_code(c, sql_dialect)[0] for c in code_blocks]
    raw_code_blocks = await asyncio.gather(*[b.to_raw_code() for b in formatted_code_blocks])
    keboola_client.storage_client.configuration_create.assert_called_once_with(
        component_id=expected_component_id,
        name=transformation_name,
        description=description,
        configuration={
            'parameters': {
                'blocks': [
                    {
                        'name': 'Blocks',
                        'codes': [b.model_dump() for b in raw_code_blocks],
                    }
                ]
            },
            'storage': {
                'input': {'tables': []},
                'output': {
                    'tables': [
                        {
                            'source': created_table_name,
                            'destination': f'out.c-{bucket_name}.{created_table_name}',
                        }
                    ]
                },
            },
        },
    )


@pytest.mark.parametrize('sql_dialect', ['Unknown'])
@pytest.mark.asyncio
async def test_create_sql_transformation_fail(
    mocker: MockerFixture,
    sql_dialect: str,
    mcp_context_components_configs: Context,
):
    """Test create_sql_transformation tool which should raise an error if the sql dialect is unknown."""
    context = mcp_context_components_configs
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)

    with pytest.raises(ValueError, match='Unsupported SQL dialect'):
        _ = await create_sql_transformation(
            ctx=context,
            name='test_name',
            description='test_description',
            sql_code_blocks=[SimplifiedTfBlocks.Block.Code(name='Code 0', script='SELECT * FROM test')],
        )


@pytest.mark.parametrize(
    ('sql_dialect', 'expected_component_id', 'parameter_updates', 'storage', 'expected_config'),
    [
        pytest.param(
            'Snowflake',
            'keboola.snowflake-transformation',
            [
                TfRenameBlock(op='rename_block', block_id='b0', block_name='Updated Blocks'),
                TfSetCode(
                    op='set_code',
                    block_id='b0',
                    code_id='b0.c0',
                    script='SELECT 1;SELECT * FROM new_table;',
                ),
            ],
            {'output': {'tables': []}},
            {
                'parameters': {
                    'blocks': [
                        {
                            'name': 'Updated Blocks',
                            'codes': [
                                {'name': 'Existing Code', 'script': ['SELECT\n  1;', 'SELECT\n  *\nFROM new_table;']}
                            ],
                        }
                    ]
                },
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='snowflake_rename_block_and_set_code',
        ),
        pytest.param(
            'BigQuery',
            'keboola.google-bigquery-transformation',
            [
                TfStrReplace(
                    op='str_replace',
                    block_id='b0',
                    code_id='b0.c0',
                    search_for='SELECT 1',
                    replace_with='SELECT 2',
                ),
            ],
            None,
            {
                'parameters': {
                    'blocks': [{'name': 'Existing', 'codes': [{'name': 'Existing Code', 'script': ['SELECT 2;']}]}]
                },
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='bigquery_str_replace',
        ),
        pytest.param(
            'Snowflake',
            'keboola.snowflake-transformation',
            None,
            {'output': {'tables': []}},
            {
                'parameters': {
                    'blocks': [{'name': 'Existing', 'codes': [{'name': 'Existing Code', 'script': ['SELECT 1;']}]}]
                },
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='snowflake_storage_only',
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_sql_transformation(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
    parameter_updates: list[TfParamUpdate] | None,
    storage: dict[str, Any] | None,
    expected_config: dict[str, Any],
):
    """
    Test update_sql_transformation tool with transformation-specific parameter_updates.
    """
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # Mock the WorkspaceManager
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)

    existing_configuration = {
        'id': mock_configuration['id'],
        'name': 'Existing Transformation',
        'description': 'Existing description',
        'configuration': {
            'parameters': {
                'blocks': [{'name': 'Existing', 'codes': [{'name': 'Existing Code', 'script': ['SELECT 1;']}]}]
            },
            'storage': {'input': {'tables': ['existing_table']}},
            'other_field': 'should_be_preserved',
        },
        'version': 1,
    }

    new_change_description = 'Test transformation update'
    updated_configuration = mock_configuration.copy()
    updated_configuration['version'] = 2
    mock_component['id'] = expected_component_id

    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value=existing_configuration)
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=updated_configuration)
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    updated_result = await update_sql_transformation(
        context,
        mock_configuration['id'],
        new_change_description,
        parameter_updates=parameter_updates,
        storage=storage,
        updated_description=str(),
        is_disabled=False,
    )

    assert isinstance(updated_result, ConfigToolOutput)
    assert updated_result.component_id == expected_component_id
    assert updated_result.configuration_id == mock_configuration['id']
    assert updated_result.description == mock_configuration['description']
    assert updated_result.version == updated_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_with(component_id=expected_component_id)
    keboola_client.storage_client.configuration_update.assert_called_once_with(
        component_id=expected_component_id,
        configuration_id=mock_configuration['id'],
        change_description=new_change_description,
        configuration=expected_config,
        updated_description='',
        is_disabled=False,
    )


@pytest.mark.asyncio
async def test_get_config_examples(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
):
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mock to return test data
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    text = await get_config_examples(component_id='keboola.ex-aws-s3', ctx=context)
    assert (
        text
        == """# Configuration Examples for `keboola.ex-aws-s3`

## Root Configuration Examples

1. Root Configuration:
```json
{
  "foo": "root"
}
```

## Row Configuration Examples

1. Row Configuration:
```json
{
  "foo": "row"
}
```

"""
    )


@pytest.mark.asyncio
async def test_create_config(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
):
    """Test create_component_root_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration = mock_configuration
    configuration['id'] = 'test-config-id'

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_create = mocker.AsyncMock(return_value=configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    name = 'Test Configuration'
    description = 'Test configuration description'
    parameters = {'test_param': 'test_value'}
    storage = {'input': {'tables': []}}

    # Test the create_component_root_configuration tool
    result = await create_config(
        ctx=context,
        name=name,
        description=description,
        component_id=component_id,
        parameters=parameters,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration['id']
    assert result.description == description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_create.assert_called_once_with(
        component_id=component_id,
        name=name,
        description=description,
        configuration={'storage': storage, 'parameters': parameters},
    )


@pytest.mark.asyncio
async def test_add_config_row(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
):
    """Test create_component_row_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration_id = 'test-config-id'
    row_configuration = {'id': 'test-row-id', 'name': 'Test Row', 'version': 1}

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_row_create = mocker.AsyncMock(return_value=row_configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    name = 'Test Row Configuration'
    description = 'Test row configuration description'
    parameters = {'row_param': 'row_value'}
    storage = {}

    # Test the create_component_row_configuration tool
    result = await add_config_row(
        ctx=context,
        name=name,
        description=description,
        component_id=component_id,
        configuration_id=configuration_id,
        parameters=parameters,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration_id
    assert result.description == description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == row_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_row_create.assert_called_once_with(
        component_id=component_id,
        config_id=configuration_id,
        name=name,
        description=description,
        configuration={'storage': storage, 'parameters': parameters},
    )


@pytest.mark.parametrize(
    ('parameter_updates', 'storage', 'expected_config'),
    [
        pytest.param(
            [
                ConfigParamSet(op='set', path='api_key', value='new_api_key'),
                ConfigParamReplace(op='str_replace', path='database.host', search_for='old', replace_with='new'),
                ConfigParamRemove(op='remove', path='deprecated_field'),
            ],
            None,
            {
                'parameters': {
                    'api_key': 'new_api_key',
                    'database': {'host': 'new_host', 'port': 5432},
                    'existing_param': 'existing_value',
                    # 'deprecated_field' is removed
                },
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='parameter_updates_only1',
        ),
        pytest.param(
            [
                ConfigParamRemove(op='remove', path='existing_param'),
                ConfigParamSet(op='set', path='updated_param', value='updated_value'),
            ],
            None,
            {
                'parameters': {
                    'api_key': 'old_api_key',
                    'database': {'host': 'old_host', 'port': 5432},
                    'deprecated_field': 'old_value',
                    'updated_param': 'updated_value',
                    # 'existing_param' is removed
                },
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='parameter_updates_only2',
        ),
        pytest.param(
            [
                ConfigParamRemove(op='remove', path='existing_param'),
                ConfigParamSet(op='set', path='updated_param', value='updated_value'),
            ],
            {'output': {'tables': []}},
            {
                'parameters': {
                    'api_key': 'old_api_key',
                    'database': {'host': 'old_host', 'port': 5432},
                    'deprecated_field': 'old_value',
                    'updated_param': 'updated_value',
                    # 'existing_param' is removed
                },
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='both_parameter_updates_and_storage',
        ),
        pytest.param(
            None,
            {'output': {'tables': []}},
            {
                'parameters': {
                    'api_key': 'old_api_key',
                    'database': {'host': 'old_host', 'port': 5432},
                    'deprecated_field': 'old_value',
                    'existing_param': 'existing_value',
                },
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='storage_only',
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_config(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    parameter_updates: list[ConfigParamUpdate] | None,
    storage: dict[str, Any] | None,
    expected_config: dict[str, Any],
):
    """Test update_component_root_configuration tool with parameter_updates."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration_id = 'test-config-id'

    existing_configuration = {
        'id': configuration_id,
        'name': 'Existing Config',
        'description': 'Existing description',
        'configuration': {
            'parameters': {
                'api_key': 'old_api_key',
                'database': {'host': 'old_host', 'port': 5432},
                'deprecated_field': 'old_value',
                'existing_param': 'existing_value',
            },
            'storage': {'input': {'tables': ['existing_table']}},
            'other_field': 'should_be_preserved',
        },
        'version': 1,
    }

    updated_name = 'Updated Configuration'
    updated_description = 'Updated configuration description'
    updated_configuration = {
        **existing_configuration,
        'name': updated_name,
        'description': updated_description,
        'version': 2,
    }

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value=existing_configuration)
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=updated_configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    change_description = 'Test update with parameter updates'

    # Test the update_component_root_configuration tool with parameter_updates
    result = await update_config(
        ctx=context,
        name=updated_name,
        description=updated_description,
        change_description=change_description,
        component_id=component_id,
        configuration_id=configuration_id,
        parameter_updates=parameter_updates,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration_id
    assert result.description == updated_description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == updated_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_update.assert_called_once_with(
        component_id=component_id,
        configuration_id=configuration_id,
        configuration=expected_config,
        change_description=change_description,
        updated_name=updated_name,
        updated_description=updated_description,
    )


@pytest.mark.parametrize(
    ('parameter_updates', 'storage', 'expected_config'),
    [
        pytest.param(
            [
                ConfigParamRemove(op='remove', path='existing_param'),
                ConfigParamSet(op='set', path='updated_param', value='updated_value'),
            ],
            {'output': {'tables': []}},
            {
                'parameters': {'updated_param': 'updated_value'},
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='both_parameter_updates_and_storage',
        ),
        pytest.param(
            [
                ConfigParamRemove(op='remove', path='existing_param'),
                ConfigParamSet(op='set', path='updated_param', value='updated_value'),
            ],
            None,
            {
                'parameters': {'updated_param': 'updated_value'},
                'storage': {'input': {'tables': ['existing_table']}},
                'other_field': 'should_be_preserved',
            },
            id='parameter_updates_only',
        ),
        pytest.param(
            None,
            {'output': {'tables': []}},
            {
                'parameters': {'existing_param': 'existing_value'},
                'storage': {'output': {'tables': []}},
                'other_field': 'should_be_preserved',
            },
            id='storage_only',
        ),
    ],
)
@pytest.mark.asyncio
async def test_update_config_row(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    parameter_updates: list[ConfigParamUpdate] | None,
    storage: dict[str, Any] | None,
    expected_config: dict[str, Any],
):
    """Test update_component_row_configuration tool with parameter_updates."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    component_id = mock_component['id']
    configuration_id = 'test-config-id'
    configuration_row_id = 'test-row-id'

    existing_row_configuration = {
        'configuration': {
            'parameters': {'existing_param': 'existing_value'},
            'storage': {'input': {'tables': ['existing_table']}},
            'other_field': 'should_be_preserved',
        },
        'version': 1,
    }

    updated_name = 'Updated Row Configuration'
    updated_description = 'Updated row configuration description'
    updated_row_configuration = {
        **existing_row_configuration,
        'name': updated_name,
        'description': updated_description,
        'version': 2,
    }

    # Set up the mock for ai_service_client and storage_client
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_row_detail = mocker.AsyncMock(return_value=existing_row_configuration)
    keboola_client.storage_client.configuration_row_update = mocker.AsyncMock(return_value=updated_row_configuration)
    keboola_client.storage_client.configuration_metadata_update = mocker.AsyncMock()

    change_description = 'Test row update'

    # Test the update_component_row_configuration tool with parameter_updates
    result = await update_config_row(
        ctx=context,
        name=updated_name,
        description=updated_description,
        change_description=change_description,
        component_id=component_id,
        configuration_id=configuration_id,
        configuration_row_id=configuration_row_id,
        parameter_updates=parameter_updates,
        storage=storage,
    )

    assert isinstance(result, ConfigToolOutput)
    assert result.component_id == component_id
    assert result.configuration_id == configuration_id
    assert result.description == updated_description
    assert result.success is True
    assert result.timestamp is not None
    assert result.version == updated_row_configuration['version']

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=component_id)
    keboola_client.storage_client.configuration_row_update.assert_called_once_with(
        component_id=component_id,
        config_id=configuration_id,
        configuration_row_id=configuration_row_id,
        configuration=expected_config,
        change_description=change_description,
        updated_name=updated_name,
        updated_description=updated_description,
    )
