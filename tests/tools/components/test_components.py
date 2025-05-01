from typing import Any, Callable, Sequence, Union
from unittest.mock import AsyncMock, MagicMock, call
from pathlib import Path
import json

import pytest
from mcp.server.fastmcp import Context

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
    create_component_configuration,
    get_component_configuration_examples,
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
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client.components = MagicMock()

    # Setup mock to return test data
    keboola_client.storage_client.configurations.detail = MagicMock(return_value=mock_configuration)
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=mock_component)
    keboola_client.storage_client.components.detail = MagicMock(return_value=mock_component)
    keboola_client.storage_client._branch_id = mock_branch_id
    keboola_client.get = AsyncMock(return_value=mock_metadata)

    result = await get_component_configuration_details('keboola.ex-aws-s3', '123', context)
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

    keboola_client.storage_client.configurations.detail.assert_called_once_with(
        'keboola.ex-aws-s3', '123'
    )

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(
        'keboola.ex-aws-s3'
    )

    keboola_client.get.assert_called_once_with(
        f'branch/{mock_branch_id}/components/{mock_component["id"]}/configs/{mock_configuration["id"]}/metadata'
    )


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

    # Set up the mock for ai_service_client
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=component)
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

    expected_config = ComponentConfiguration.model_validate(
        {**configuration, 'component_id': expected_component_id, 'component': component}
    )

    assert isinstance(new_transformation_configuration, ComponentConfiguration)
    assert new_transformation_configuration.model_dump() == expected_config.model_dump()

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(
        expected_component_id
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


@pytest.mark.asyncio
async def test_create_component_configuration_success(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    mock_branch_id: str,
):
    """Test successful creation of component configuration without configuration row."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mocks
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=mock_component)
    keboola_client.post = AsyncMock(return_value=mock_configuration)

    # Call the function
    result = await create_component_configuration(
        context,
        name="Test Config",
        description="Test Description",
        component_id=mock_component['id'],
        configuration={"param1": "value1"}
    )

    # Verify result
    assert isinstance(result, ComponentConfiguration)
    assert result.component_id == mock_component['id']
    assert result.configuration_id == mock_configuration['id']

    # Verify API calls
    keboola_client.post.assert_called_once()
    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(mock_component['id'])


@pytest.mark.asyncio
async def test_create_component_configuration_with_row(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    mock_branch_id: str,
):
    """Test creation of component configuration with configuration row."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mocks
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=mock_component)
    keboola_client.post = AsyncMock(side_effect=[mock_configuration, {"id": "row-1"}])

    # Call the function with configuration row
    result = await create_component_configuration(
        context,
        name="Test Config",
        description="Test Description",
        component_id=mock_component['id'],
        configuration={"param1": "value1"},
        configuration_row={"row_param": "row_value"}
    )

    # Verify result
    assert isinstance(result, ComponentConfiguration)
    assert result.component_id == mock_component['id']

    # Verify API calls
    assert keboola_client.post.call_count == 2  # One for configuration, one for row


@pytest.mark.asyncio
async def test_create_component_configuration_failure(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_branch_id: str,
):
    """Test error state when creating configuration."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mock to raise exception
    keboola_client.post = AsyncMock(side_effect=Exception("API Error"))

    # Verify that function raises exception
    with pytest.raises(Exception) as exc_info:
        await create_component_configuration(
            context,
            name="Test Config",
            description="Test Description",
            component_id=mock_component['id'],
            configuration={"param1": "value1"}
        )

    assert str(exc_info.value) == "API Error"


@pytest.mark.asyncio
async def test_create_component_configuration_row_failure(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
    mock_branch_id: str,
):
    """Test error state when adding configuration row."""
    context = mcp_context_components_configs
    keboola_client = KeboolaClient.from_state(context.session.state)

    # Setup mocks - first call successful, second raises exception
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=mock_component)
    keboola_client.post = AsyncMock(side_effect=[mock_configuration, Exception("Row API Error")])

    # Verify that function raises exception
    with pytest.raises(Exception) as exc_info:
        await create_component_configuration(
            context,
            name="Test Config",
            description="Test Description",
            component_id=mock_component['id'],
            configuration={"param1": "value1"},
            configuration_row={"row_param": "row_value"}
        )

    assert str(exc_info.value) == "Row API Error"


@pytest.mark.asyncio
async def test_get_component_configuration_examples_success(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
):
    """Test successful retrieval of component configuration examples."""
    context = mcp_context_components_configs

    # Create test JSONL file in the correct location
    jsonl_path = Path("json-schemas/output")
    jsonl_path.mkdir(parents=True, exist_ok=True)
    test_file = jsonl_path / f"sample_data_{mock_component['id']}.jsonl"

    # Write test data to JSONL file
    test_data = {
        "component_id": mock_component['id'],
        "config_example": {"param1": "value1", "param2": "value2"},
        "config_row_example": {"row_param1": "row_value1"}
    }
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(test_data) + "\n")

    try:
        # Call the function
        result = await get_component_configuration_examples(context, mock_component['id'])

        # Verify result
        assert isinstance(result, str)
        assert "Configuration examples" in result
        assert "param1" in result
        assert "value1" in result
        assert "row_param1" in result
        assert "row_value1" in result
    finally:
        # Clean up
        test_file.unlink(missing_ok=True)
        jsonl_path.rmdir()
        Path("json-schemas").rmdir()


@pytest.mark.asyncio
async def test_get_component_configuration_examples_no_file(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
):
    """Test retrieval of component configuration examples when file does not exist."""
    context = mcp_context_components_configs

    # Ensure file does not exist
    jsonl_path = Path("json-schemas/output")
    test_file = jsonl_path / f"sample_data_{mock_component['id']}.jsonl"
    test_file.unlink(missing_ok=True)

    # Call the function
    result = await get_component_configuration_examples(context, mock_component['id'])

    # Verify result
    assert isinstance(result, str)
    assert result == f"No configuration examples found for component {mock_component['id']}"


@pytest.mark.asyncio
async def test_get_component_configuration_examples_invalid_json(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
):
    """Test retrieval of component configuration examples with invalid JSON."""
    context = mcp_context_components_configs

    # Create test JSONL file in the correct location
    jsonl_path = Path("json-schemas/output")
    jsonl_path.mkdir(parents=True, exist_ok=True)
    test_file = jsonl_path / f"sample_data_{mock_component['id']}.jsonl"

    # Write invalid JSON to file
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write("invalid json\n")
        f.write(json.dumps({
            "component_id": mock_component['id'],
            "config_example": {"param1": "value1"},
            "config_row_example": {"row_param1": "row_value1"}
        }) + "\n")

    try:
        # Call the function
        result = await get_component_configuration_examples(context, mock_component['id'])

        # Verify result
        assert isinstance(result, str)
        assert "Configuration examples" in result
        assert "param1" in result
        assert "value1" in result
        assert "row_param1" in result
        assert "row_value1" in result
    finally:
        # Clean up
        test_file.unlink(missing_ok=True)
        jsonl_path.rmdir()
        Path("json-schemas").rmdir()


@pytest.mark.asyncio
async def test_get_component_configuration_examples_no_examples(
    mcp_context_components_configs: Context,
    mock_component: dict[str, Any],
):
    """Test retrieval of component configuration examples when no examples are found."""
    context = mcp_context_components_configs

    # Create test JSONL file in the correct location
    jsonl_path = Path("json-schemas/output")
    jsonl_path.mkdir(parents=True, exist_ok=True)
    test_file = jsonl_path / f"sample_data_{mock_component['id']}.jsonl"

    # Write test data with different component_id
    test_data = {
        "component_id": "different.component.id",
        "config_example": {"param1": "value1"}
    }
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(test_data) + "\n")

    try:
        # Call the function
        result = await get_component_configuration_examples(context, mock_component['id'])

        # Verify result
        assert isinstance(result, str)
        assert result == f"No configuration examples found for component {mock_component['id']}"
    finally:
        # Clean up
        test_file.unlink(missing_ok=True)
        jsonl_path.rmdir()
        Path("json-schemas").rmdir()
