from typing import Any, Callable, Sequence, Union
from unittest.mock import AsyncMock, MagicMock, call
from unittest.mock import patch

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.components import (
    ComponentConfigurationResponse,
    ComponentConfigurationResponseBase,
    ComponentWithConfigurations,
    create_sql_transformation,
    get_component_configuration_details,
    retrieve_components_configurations,
    retrieve_transformations_configurations,
)
from keboola_mcp_server.tools.components.model import (
    ComponentConfigurationMetadata,
    ComponentConfigurationOutput,
    ReducedComponentDetail,
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
        assert all(isinstance(component.component, ReducedComponentDetail) for component in result)
        assert all(isinstance(component.configurations, list) for component in result)
        assert all(
            all(
                isinstance(config, ComponentConfigurationMetadata)
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
        assert all(not hasattr(returned.component, 'version') for returned in result)

        # assert configurations list details
        assert all(len(component.configurations) == len(configurations) for component in result)
        assert all(
            all(
                isinstance(config.root_configuration, ComponentConfigurationResponseBase)
                for config in component.configurations
            )
            for component in result
        )
        # use zip to iterate over the result and mock_configurations since we artifically mock the .get method
        assert all(
            all(
                config.root_configuration.configuration_id == expected['id']
                for config, expected in zip(component.configurations, configurations)
            )
            for component in result
        )
        assert all(
            all(
                config.root_configuration.configuration_name == expected['name']
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

    result = await retrieve_components_configurations(context, component_ids=[mock_component['id']])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component['id'])
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
    mock_branch_id: str,
):
    """Test get_component_configuration_details tool."""
    # Setup
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    component_id = 'keboola.ex-aws-s3'
    configuration_id = '123'

    # Prepare mock configuration
    mock_configuration.update(
        {
            'id': configuration_id,
            'rows': None,
            'configuration': {
                'parameters': {'accessKeyId': 'test'},
                'storage': {'input': {'tables': []}},
            },
        }
    )

    # Prepare mock component
    mock_component.update(
        {'id': component_id, 'flags': ['genericDockerUI-tableInput'], 'type': 'extractor'}
    )

    # Setup Storage API mocks
    keboola_client.storage_client = MagicMock()
    keboola_client.storage_client._branch_id = mock_branch_id
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client.configurations.detail = MagicMock(return_value=mock_configuration)

    # Setup AI service mocks
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=mock_component)

    # Mock different responses for client.get based on endpoint
    def mock_response(endpoint, **kwargs):
        if 'metadata' in endpoint:
            return mock_metadata
        else:
            return {'components': [{'id': component_id, 'flags': mock_component['flags']}]}

    keboola_client.get = AsyncMock(side_effect=mock_response)

    # Execute
    result = await get_component_configuration_details(component_id, configuration_id, context)

    # Verify
    assert isinstance(result, ComponentConfigurationOutput)

    # Verify core fields
    assert result.component_details is not None
    assert result.component_details.component_id == component_id
    assert result.component_details.has_table_input_mapping

    assert result.root_configuration.component_id == component_id
    assert result.root_configuration.configuration_id == configuration_id
    assert result.root_configuration.parameters == mock_configuration['configuration']['parameters']

    # Verify mock was called correctly
    keboola_client.storage_client.configurations.detail.assert_called_once_with(
        component_id, configuration_id
    )
    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(component_id)


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

    # Prepare component and configuration mocks
    component = mock_component.copy()
    component['id'] = expected_component_id
    component['type'] = 'transformation'

    configuration = mock_configuration.copy()
    configuration['id'] = expected_configuration_id

    # Prepare the expected configuration response
    expected_config = ComponentConfigurationResponse.model_validate(
        {**configuration, 'component_id': expected_component_id, 'component': component}
    )

    # Mock the entire create_sql_transformation function on module level
    with patch('tests.tools.components.test_components.create_sql_transformation') as mock_create:
        mock_create.return_value = expected_config

        transformation_name = mock_configuration['name']
        description = mock_configuration['description']
        sql_statements = ['SELECT * FROM test', 'SELECT * FROM test2']
        created_table_name = 'test_table_1'

        # Call the create_sql_transformation tool
        result = await create_sql_transformation(
            context,
            transformation_name,
            description,
            sql_statements,
            created_table_names=[created_table_name],
        )

        # Verify the result
        assert result == expected_config

        # Verify the function was called with the correct parameters
        mock_create.assert_called_once_with(
            context,
            transformation_name,
            description,
            sql_statements,
            created_table_names=[created_table_name],
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
