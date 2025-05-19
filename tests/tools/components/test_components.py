from typing import Any, Callable

import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components import (
    ComponentConfiguration,
    ComponentWithConfigurations,
    ReducedComponent,
    ReducedComponentConfiguration,
    create_sql_transformation,
    get_component_configuration_details,
    retrieve_components_configurations,
    retrieve_transformations_configurations,
    update_sql_transformation_configuration,
)
from keboola_mcp_server.tools.sql import WorkspaceManager


@pytest.fixture
def assert_retrieve_components() -> (
    Callable[[list[ComponentWithConfigurations], list[dict[str, Any]], list[dict[str, Any]]], None]
):
    """Assert that the _retrieve_components_in_project tool returns the correct components and configurations."""

    def _assert_retrieve_components(
        result: list[ComponentWithConfigurations],
        components: list[dict[str, Any]],
        configurations: list[dict[str, Any]],
    ):

        assert len(result) == len(components)
        # assert basics
        assert all(isinstance(component, ComponentWithConfigurations) for component in result)
        assert all(isinstance(component.component, ReducedComponent) for component in result)
        assert all(isinstance(component.configurations, list) for component in result)
        assert all(
            all(isinstance(config, ReducedComponentConfiguration) for config in component.configurations)
            for component in result
        )
        # assert component list details
        assert all(returned.component.component_id == expected['id'] for returned, expected in zip(result, components))
        assert all(
            returned.component.component_name == expected['name'] for returned, expected in zip(result, components)
        )
        assert all(
            returned.component.component_type == expected['type'] for returned, expected in zip(result, components)
        )
        assert all(not hasattr(returned.component, 'version') for returned in result)

        # assert configurations list details
        assert all(len(component.configurations) == len(configurations) for component in result)
        assert all(
            all(isinstance(config, ReducedComponentConfiguration) for config in component.configurations)
            for component in result
        )
        # use zip to iterate over the result and mock_configurations since we artificially mock the .get method
        assert all(
            all(
                config.configuration_id == expected['id']
                for config, expected in zip(component.configurations, configurations)
            )
            for component in result
        )
        assert all(
            all(
                config.configuration_name == expected['name']
                for config, expected in zip(component.configurations, configurations)
            )
            for component in result
        )

    return _assert_retrieve_components


@pytest.mark.asyncio
async def test_retrieve_components_configurations_by_types(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_components: list[dict[str, Any]],
    mock_configurations: list[dict[str, Any]],
    mock_branch_id: str,
    assert_retrieve_components: Callable[
        [list[ComponentWithConfigurations], list[dict[str, Any]], list[dict[str, Any]]], None
    ],
):
    """Test retrieve_components_configurations when component types are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # mock the get method to return the mock_component with the mock_configurations
    # simulate the response from the API
    keboola_client.storage_client.get = mocker.AsyncMock(
        side_effect=[[{**component, 'configurations': mock_configurations}] for component in mock_components]
    )

    result = await retrieve_components_configurations(context, component_types=[])

    assert_retrieve_components(result, mock_components, mock_configurations)

    keboola_client.storage_client.get.assert_has_calls(
        [
            mocker.call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'application', 'include': 'configuration'},
            ),
            mocker.call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'extractor', 'include': 'configuration'},
            ),
            mocker.call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'writer', 'include': 'configuration'},
            ),
        ]
    )


@pytest.mark.asyncio
async def test_retrieve_transformations_configurations(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configurations: list[dict[str, Any]],
    mock_branch_id: str,
    assert_retrieve_components: Callable[
        [list[ComponentWithConfigurations], list[dict[str, Any]], list[dict[str, Any]]], None
    ],
):
    """Test retrieve_transformations_configurations."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # mock the get method to return the mock_component with the mock_configurations
    # simulate the response from the API
    keboola_client.storage_client.get = mocker.AsyncMock(
        return_value=[{**mock_component, 'configurations': mock_configurations}]
    )

    result = await retrieve_transformations_configurations(context)

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.get.assert_has_calls(
        [
            mocker.call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'transformation', 'include': 'configuration'},
            ),
        ]
    )


@pytest.mark.asyncio
async def test_retrieve_components_configurations_from_ids(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configurations: list[dict[str, Any]],
    mock_component: dict[str, Any],
    mock_branch_id: str,
    assert_retrieve_components: Callable[
        [list[ComponentWithConfigurations], list[dict[str, Any]], list[dict[str, Any]]], None
    ],
):
    """Test retrieve_components_configurations when component IDs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=mock_configurations)
    keboola_client.storage_client.get = mocker.AsyncMock(return_value=mock_component)

    result = await retrieve_components_configurations(context, component_ids=[mock_component['id']])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configuration_list.assert_called_once_with(component_id=mock_component['id'])
    keboola_client.storage_client.get.assert_called_once_with(
        endpoint=f'branch/{mock_branch_id}/components/{mock_component["id"]}'
    )


@pytest.mark.asyncio
async def test_retrieve_transformations_configurations_from_ids(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configurations: list[dict[str, Any]],
    mock_component: dict[str, Any],
    mock_branch_id: str,
    assert_retrieve_components: Callable[
        [list[ComponentWithConfigurations], list[dict[str, Any]], list[dict[str, Any]]], None
    ],
):
    """Test retrieve_transformations_configurations when transformation IDs are provided."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    keboola_client.storage_client.configuration_list = mocker.AsyncMock(return_value=mock_configurations)
    keboola_client.storage_client.get = mocker.AsyncMock(return_value=mock_component)

    result = await retrieve_transformations_configurations(context, transformation_ids=[mock_component['id']])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configuration_list.assert_called_once_with(component_id=mock_component['id'])
    keboola_client.storage_client.get.assert_called_once_with(
        endpoint=f'branch/{mock_branch_id}/components/{mock_component["id"]}'
    )


@pytest.mark.asyncio
async def test_get_component_configuration_details(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_configuration: dict[str, Any],
    mock_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
    mock_branch_id: str,
):
    """Test get_component_configuration_details tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mock to return test data
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)
    keboola_client.storage_client.configuration_detail = mocker.AsyncMock(return_value=mock_configuration)
    keboola_client.storage_client.get = mocker.AsyncMock(return_value=mock_metadata)

    result = await get_component_configuration_details(
        component_id='keboola.ex-aws-s3', configuration_id='123', ctx=context
    )
    expected = ComponentConfiguration.model_validate(
        {
            **mock_configuration,
            'component_id': mock_component['id'],
            'component': mock_component,
            'metadata': mock_metadata,
        }
    )
    assert isinstance(result, ComponentConfiguration)
    assert result.model_dump() == expected.model_dump()

    keboola_client.storage_client.configuration_detail.assert_called_once_with(
        component_id=mock_component['id'], configuration_id=mock_configuration['id']
    )

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=mock_component['id'])

    keboola_client.storage_client.get.assert_called_once_with(
        endpoint=(
            f'branch/{mock_branch_id}/'
            f'components/{mock_component["id"]}/'
            f'configs/{mock_configuration["id"]}/'
            'metadata'
        )
    )


@pytest.mark.parametrize(
    ('sql_dialect', 'expected_component_id', 'expected_configuration_id'),
    [
        ('Snowflake', 'keboola.snowflake-transformation', '1234'),
        ('BigQuery', 'keboola.bigquery-transformation', '5678'),
    ],
)
@pytest.mark.asyncio
async def test_create_transformation_configuration(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
    expected_configuration_id: str,
    mock_branch_id: str,
):
    """Test create_transformation_configuration tool."""
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
    keboola_client.storage_client.post = mocker.AsyncMock(return_value=configuration)

    transformation_name = mock_configuration['name']
    bucket_name = '-'.join(transformation_name.lower().split())
    description = mock_configuration['description']
    sql_statements = ['SELECT * FROM test', 'SELECT * FROM test2']
    created_table_name = 'test_table_1'

    # Test the create_sql_transformation tool
    new_transformation_configuration = await create_sql_transformation(
        context,
        transformation_name,
        description,
        sql_statements,
        created_table_names=[created_table_name],
    )

    expected_config = ComponentConfiguration.model_validate(
        {**configuration, 'component_id': expected_component_id, 'component': component}
    )

    assert isinstance(new_transformation_configuration, ComponentConfiguration)
    assert new_transformation_configuration.model_dump() == expected_config.model_dump()

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=expected_component_id)

    keboola_client.storage_client.post.assert_called_once_with(
        endpoint=f'branch/{mock_branch_id}/components/{expected_component_id}/configs',
        data={
            'name': transformation_name,
            'description': description,
            'configuration': {
                'parameters': {
                    'blocks': [
                        {
                            'name': 'Block 0',
                            'codes': [{'name': 'Code 0', 'script': sql_statements}],
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
        },
    )


@pytest.mark.parametrize('sql_dialect', ['Unknown'])
@pytest.mark.asyncio
async def test_create_transformation_configuration_fail(
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
            context,
            'test_name',
            'test_description',
            ['SELECT * FROM test'],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('sql_dialect', 'expected_component_id'),
    [('Snowflake', 'keboola.snowflake-transformation'), ('BigQuery', 'keboola.bigquery-transformation')],
)
async def test_update_transformation_configuration(
    mocker: MockerFixture,
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    sql_dialect: str,
    expected_component_id: str,
):
    """Test update_sql_transformation_configuration tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    # Mock the WorkspaceManager
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value=sql_dialect)

    new_config = {'foo': 'foo'}
    new_change_description = 'foo fooo'
    mock_configuration['configuration'] = new_config
    mock_configuration['changeDescription'] = new_change_description
    mock_component['id'] = expected_component_id
    keboola_client.storage_client.configuration_update = mocker.AsyncMock(return_value=mock_configuration)
    keboola_client.ai_service_client = mocker.MagicMock()
    keboola_client.ai_service_client.get_component_detail = mocker.AsyncMock(return_value=mock_component)

    updated_configuration = await update_sql_transformation_configuration(
        context,
        mock_configuration['id'],
        new_change_description,
        new_config,
        updated_description=str(),
        is_disabled=False,
    )

    assert isinstance(updated_configuration, ComponentConfiguration)
    assert updated_configuration.configuration == new_config
    assert updated_configuration.component_id == expected_component_id
    assert updated_configuration.configuration_id == mock_configuration['id']
    assert updated_configuration.change_description == new_change_description

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id=expected_component_id)
    keboola_client.storage_client.configuration_update.assert_called_once_with(
        component_id=expected_component_id,
        configuration_id=mock_configuration['id'],
        change_description=new_change_description,
        configuration=new_config,
        updated_description=None,
        is_disabled=False,
    )
