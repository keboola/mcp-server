from typing import Any, Callable, Sequence, Union
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components.model import (
    ComponentConfiguration,
    ComponentType,
    ComponentWithConfigurations,
    ReducedComponent,
    ReducedComponentConfiguration,
)
from keboola_mcp_server.tools.components.tools import (
    create_sql_transformation,
    get_component_configuration_details,
    retrieve_components_configurations,
    retrieve_transformations_configurations,
)
from keboola_mcp_server.tools.sql import WorkspaceManager


@pytest.mark.parametrize(
    'sql_dialect, expected_component_id, expected_configuration_id',
    [
        ('Snowflake', 'keboola.snowflake-transformation', '1234'),
        ('BigQuery', 'keboola.bigquery-transformation', '5678'),
    ],
)
@pytest.mark.asyncio
async def test_create_transformation_configuration(
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
    workspace_manager.get_sql_dialect = AsyncMock(return_value=sql_dialect)
    # Mock the KeboolaClient
    keboola_client = KeboolaClient.from_state(context.session.state)
    component = mock_component
    component['id'] = expected_component_id
    configuration = mock_configuration
    configuration['id'] = expected_configuration_id

    keboola_client.get = AsyncMock(return_value=component)
    keboola_client.post = AsyncMock(return_value=configuration)

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
    assert isinstance(new_transformation_configuration, ComponentConfiguration)
    assert new_transformation_configuration.component is not None
    assert new_transformation_configuration.component.component_id == expected_component_id
    assert new_transformation_configuration.component_id == expected_component_id
    assert new_transformation_configuration.configuration_id == expected_configuration_id
    assert new_transformation_configuration.configuration_name == transformation_name
    assert new_transformation_configuration.configuration_description == description

    keboola_client.get.assert_called_once_with(
        f'branch/{mock_branch_id}/components/{expected_component_id}'
    )

    keboola_client.post.assert_called_once_with(
        f'branch/{mock_branch_id}/components/{expected_component_id}/configs',
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
    sql_dialect: str,
    mcp_context_components_configs: Context,
):
    """Test create_sql_transformation tool which should raise an error if the sql dialect is unknown."""
    context = mcp_context_components_configs
    workspace_manager = WorkspaceManager.from_state(context.session.state)
    workspace_manager.get_sql_dialect = AsyncMock(return_value=sql_dialect)

    with pytest.raises(ValueError):
        _ = await create_sql_transformation(
            context,
            'test_name',
            'test_description',
            ['SELECT * FROM test'],
        )


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
            all(
                isinstance(config, ReducedComponentConfiguration)
                for config in component.configurations
            )
            for component in result
        )
        # assert component list details
        assert all(
            returned.component.component_id == expected['id']
            for returned, expected in zip(result, components)
        )
        assert all(
            returned.component.component_name == expected['name']
            for returned, expected in zip(result, components)
        )
        assert all(
            returned.component.component_type == expected['type']
            for returned, expected in zip(result, components)
        )
        assert all(
            returned.component.component_description == expected['description']
            for returned, expected in zip(result, components)
        )
        assert all(not hasattr(returned.component, 'version') for returned in result)

        # assert configurations list details
        assert all(len(component.configurations) == len(configurations) for component in result)
        assert all(
            all(
                isinstance(config, ReducedComponentConfiguration)
                for config in component.configurations
            )
            for component in result
        )
        # use zip to iterate over the result and mock_configurations since we artifically mock the .get method
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
    keboola_client.get = AsyncMock(
        side_effect=[
            [{**component, 'configurations': mock_configurations}] for component in mock_components
        ]
    )

    result = await retrieve_components_configurations(context, component_types=[])

    assert_retrieve_components(result, mock_components, mock_configurations)

    keboola_client.get.assert_has_calls(
        [
            call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'application', 'include': 'configuration'},
            ),
            call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'extractor', 'include': 'configuration'},
            ),
            call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'writer', 'include': 'configuration'},
            ),
        ]
    )


@pytest.mark.asyncio
async def test_retrieve_transformations_configurations(
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
    keboola_client.get = AsyncMock(
        return_value=[{**mock_component, 'configurations': mock_configurations}]
    )

    result = await retrieve_transformations_configurations(context)

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.get.assert_has_calls(
        [
            call(
                f'branch/{mock_branch_id}/components',
                params={'componentType': 'transformation', 'include': 'configuration'},
            ),
        ]
    )


@pytest.mark.asyncio
async def test_retrieve_components_configurations_from_ids(
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

    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await retrieve_components_configurations(context, component_ids=[mock_component["id"]])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component["id"])
    keboola_client.get.assert_called_once_with(
        f'branch/{mock_branch_id}/components/{mock_component["id"]}'
    )


@pytest.mark.asyncio
async def test_retrieve_transformations_configurations_from_ids(
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

    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await retrieve_transformations_configurations(
        context, transformation_ids=[mock_component['id']]
    )

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component['id'])
    keboola_client.get.assert_called_once_with(
        f'branch/{mock_branch_id}/components/{mock_component["id"]}'
    )


@pytest.mark.asyncio
async def test_get_component_configuration_details(
    mcp_context_components_configs: Context,
    mock_configuration: dict[str, Any],
    mock_component: dict[str, Any],
    mock_metadata: list[dict[str, Any]],
):
    """Test get_component_configuration_details tool."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client.components = MagicMock()

    # Setup mock to return test data
    keboola_client.storage_client.configurations.detail = MagicMock(return_value=mock_configuration)
    keboola_client.storage_client.components.detail = MagicMock(return_value=mock_component)
    keboola_client.storage_client._branch_id = '123'
    keboola_client.get = AsyncMock(
        side_effect=[mock_component, mock_metadata]
    )  # Mock two results of the .get method first for component and then for metadata

    result = await get_component_configuration_details('keboola.ex-aws-s3', '123', context)

    assert isinstance(result, ComponentConfiguration)
    assert result.component is not None
    assert result.component.component_id == mock_component['id']
    assert result.component.component_name == mock_component['name']
    assert result.component.component_type == mock_component['type']
    assert result.component.component_description == mock_component['description']
    assert result.component.long_description == mock_component['longDescription']
    assert result.component.categories == mock_component['categories']
    assert result.component.version == mock_component['version']
    assert result.configuration_id == mock_configuration['id']
    assert result.configuration_name == mock_configuration['name']
    assert result.configuration_description == mock_configuration['description']
    assert result.is_disabled == mock_configuration['isDisabled']
    assert result.is_deleted == mock_configuration['isDeleted']
    assert result.version == mock_configuration['version']
    assert result.configuration == mock_configuration['configuration']
    assert result.rows == mock_configuration['rows']
    assert result.configuration_metadata == mock_metadata

    keboola_client.storage_client.configurations.detail.assert_called_once_with(
        'keboola.ex-aws-s3', '123'
    )

    keboola_client.get.assert_has_calls(
        [
            call('branch/123/components/keboola.ex-aws-s3'),
            call('branch/123/components/keboola.ex-aws-s3/configs/123/metadata'),
        ]
    )
