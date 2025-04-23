from typing import Any, Callable, Sequence, Union
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.component_tools import (
    ComponentConfiguration,
    ComponentType,
    ComponentWithConfigurations,
    ReducedComponent,
    ReducedComponentConfiguration,
    TransformationConfiguration,
    _get_transformation_configuration,
    _handle_component_types,
    create_sql_transformation,
    get_component_configuration_details,
    retrieve_components_configurations,
    retrieve_transformations_configurations,
)
from keboola_mcp_server.sql_tools import WorkspaceManager


@pytest.fixture
def mock_components() -> list[dict[str, Any]]:
    """Mock list_components tool."""
    return [
        {
            "id": "keboola.ex-aws-s3",
            "name": "AWS S3 Extractor",
            "type": "extractor",
            "component_categories": [],
            "documentation_url": "https://components.keboola.com/components/keboola.ex-aws-s3",
            "documentation": None,
            "configuration_schema": {},
            "configuration_row_schema": {},
            "configuration_description": None,
        },
        {
            "id": "keboola.wr-google-drive",
            "name": "Google Drive Writer",
            "type": "writer",
            "component_categories": [],
            "documentation_url": "https://components.keboola.com/components/keboola.wr-google-drive",
            "documentation": None,
            "configuration_schema": {},
            "configuration_row_schema": {},
            "configuration_description": None,
        },
        {
            "id": "keboola.app-google-drive",
            "name": "Google Drive Application",
            "type": "application",
            "component_categories": [],
            "documentation_url": "https://components.keboola.com/components/keboola.app-google-drive",
            "documentation": None,
            "configuration_schema": {},
            "configuration_row_schema": {},
            "configuration_description": None,
        },
    ]


@pytest.fixture
def mock_configurations() -> list[dict[str, Any]]:
    """Mock mock_configurations tool."""
    return [
        {
            "id": "123",
            "name": "My Config",
            "description": "Test configuration",
            "created": "2024-01-01T00:00:00Z",
            "isDisabled": False,
            "isDeleted": False,
            "version": 1,
            "configuration": {},
        },
        {
            "id": "456",
            "name": "My Config 2",
            "description": "Test configuration 2",
            "created": "2024-01-01T00:00:00Z",
            "isDisabled": True,
            "isDeleted": True,
            "version": 2,
            "configuration": {},
        },
    ]


@pytest.fixture
def mock_component() -> dict[str, Any]:
    """Mock mock_component tool."""
    return {
        "id": "keboola.ex-aws-s3",
        "name": "AWS S3 Extractor",
        "type": "extractor",
        "componentCategories": ["extractor"],
        "documentationUrl": "https://components.keboola.com/components/keboola.ex-aws-s3",
        "documentation": None,
        "configurationSchema": {},
        "configurationRowSchema": {},
        "configurationDescription": None,
    }


@pytest.fixture
def mock_configuration() -> dict[str, Any]:
    """Mock mock_configuration tool."""
    return {
        "id": "123",
        "name": "My Config",
        "description": "Test configuration",
        "created": "2024-01-01T00:00:00Z",
        "isDisabled": False,
        "isDeleted": False,
        "version": 1,
        "configuration": {},
        "rows": [{"id": "1", "name": "Row 1"}, {"id": "2", "name": "Row 2"}],
    }


@pytest.fixture
def mock_metadata() -> list[dict[str, Any]]:
    """Mock mock_component_configuration tool."""
    return [
        {
            "id": "1",
            "key": "test-key",
            "value": "test-value",
            "provider": "user",
            "timestamp": "2024-01-01T00:00:00Z",
        }
    ]


@pytest.fixture
def mock_branch_id() -> str:
    return "default"


@pytest.fixture
def mcp_context_components_configs(mcp_context_client, mock_branch_id) -> Context:
    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.components = MagicMock()
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client._branch_id = mock_branch_id
    return mcp_context_client


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
            returned.component.component_id == expected["id"]
            for returned, expected in zip(result, components)
        )
        assert all(
            returned.component.component_name == expected["name"]
            for returned, expected in zip(result, components)
        )
        assert all(
            returned.component.component_type == expected["type"]
            for returned, expected in zip(result, components)
        )
        assert all(not hasattr(returned.component, "version") for returned in result)

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
                config.configuration_id == expected["id"]
                for config, expected in zip(component.configurations, configurations)
            )
            for component in result
        )
        assert all(
            all(
                config.configuration_name == expected["name"]
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
            [{**component, "configurations": mock_configurations}] for component in mock_components
        ]
    )

    result = await retrieve_components_configurations(context, component_types=[])

    assert_retrieve_components(result, mock_components, mock_configurations)

    keboola_client.get.assert_has_calls(
        [
            call(
                f"branch/{mock_branch_id}/components",
                params={"componentType": "application", "include": "configuration"},
            ),
            call(
                f"branch/{mock_branch_id}/components",
                params={"componentType": "extractor", "include": "configuration"},
            ),
            call(
                f"branch/{mock_branch_id}/components",
                params={"componentType": "writer", "include": "configuration"},
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
        return_value=[{**mock_component, "configurations": mock_configurations}]
    )

    result = await retrieve_transformations_configurations(context)

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.get.assert_has_calls(
        [
            call(
                f"branch/{mock_branch_id}/components",
                params={"componentType": "transformation", "include": "configuration"},
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
        f"branch/{mock_branch_id}/components/{mock_component['id']}"
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
        context, transformation_ids=[mock_component["id"]]
    )

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component["id"])
    keboola_client.get.assert_called_once_with(
        f"branch/{mock_branch_id}/components/{mock_component['id']}"
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
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=mock_component)
    keboola_client.storage_client.components.detail = MagicMock(return_value=mock_component)
    keboola_client.storage_client._branch_id = "123"
    keboola_client.get = AsyncMock(return_value=mock_metadata)

    result = await get_component_configuration_details("keboola.ex-aws-s3", "123", context)

    assert isinstance(result, ComponentConfiguration)
    assert result.component is not None
    assert result.component.component_id == mock_component["id"]
    assert result.component.component_name == mock_component["name"]
    assert result.component.component_type == mock_component["type"]
    assert result.configuration_id == mock_configuration["id"]
    assert result.configuration_name == mock_configuration["name"]
    assert result.configuration_description == mock_configuration["description"]
    assert result.is_disabled == mock_configuration["isDisabled"]
    assert result.is_deleted == mock_configuration["isDeleted"]
    assert result.version == mock_configuration["version"]
    assert result.configuration == mock_configuration["configuration"]
    assert result.rows == mock_configuration["rows"]
    assert result.configuration_metadata == mock_metadata

    keboola_client.storage_client.configurations.detail.assert_called_once_with(
        "keboola.ex-aws-s3", "123"
    )

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(
        "keboola.ex-aws-s3"
    )

    keboola_client.get.assert_called_once_with(
        "branch/123/components/keboola.ex-aws-s3/configs/123/metadata"
    )


@pytest.mark.parametrize(
    "component_type, expected",
    [
        ("application", ["application"]),
        (["extractor", "writer"], ["extractor", "writer"]),
        (None, ["application", "extractor", "writer"]),
        ([], ["application", "extractor", "writer"]),
    ],
)
def test_handle_component_types(
    component_type: Union[ComponentType, Sequence[ComponentType], None],
    expected: list[ComponentType],
):
    """Test list_component_configurations tool with core component."""
    assert _handle_component_types(component_type) == expected


@pytest.mark.parametrize(
    "sql_dialect, expected_component_id, expected_configuration_id",
    [
        ("Snowflake", "keboola.snowflake-transformation", "1234"),
        ("BigQuery", "keboola.bigquery-transformation", "5678"),
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
    component["id"] = expected_component_id
    configuration = mock_configuration
    configuration["id"] = expected_configuration_id

    # Set up the mock for ai_service_client
    keboola_client.ai_service_client = MagicMock()
    keboola_client.ai_service_client.get_component_detail = MagicMock(return_value=component)
    keboola_client.post = AsyncMock(return_value=configuration)

    transformation_name = mock_configuration["name"]
    bucket_name = "-".join(transformation_name.lower().split())
    description = mock_configuration["description"]
    sql_statements = ["SELECT * FROM test", "SELECT * FROM test2"]
    created_table_name = "test_table_1"

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

    keboola_client.ai_service_client.get_component_detail.assert_called_once_with(
        expected_component_id
    )

    keboola_client.post.assert_called_once_with(
        f"branch/{mock_branch_id}/components/{expected_component_id}/configs",
        data={
            "name": transformation_name,
            "description": description,
            "configuration": {
                "parameters": {
                    "blocks": [
                        {
                            "name": "Block 0",
                            "codes": [{"name": "Code 0", "script": sql_statements}],
                        }
                    ]
                },
                "storage": {
                    "input": {"tables": []},
                    "output": {
                        "tables": [
                            {
                                "source": created_table_name,
                                "destination": f"out.c-{bucket_name}.{created_table_name}",
                            }
                        ]
                    },
                },
            },
        },
    )


@pytest.mark.parametrize("sql_dialect", ["Unknown"])
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
            "test_name",
            "test_description",
            "SELECT * FROM test",
        )


@pytest.mark.parametrize(
    "sql_statements, created_table_names, transformation_name, expected_bucket_id",
    [
        # testing with multiple sql statements and no output table mappings
        # it should not create any output tables
        (["SELECT * FROM test", "SELECT * FROM test2"], [], "test name", "out.c-test-name"),
        # testing with multiple sql statements and output table mappings
        # it should create output tables according to the mappings
        (
            [
                'CREATE OR REPLACE TABLE "test_table_1" AS SELECT * FROM "test";',
                'CREATE OR REPLACE TABLE "test_table_2" AS SELECT * FROM "test";',
            ],
            ["test_table_1", "test_table_2"],
            "test name two",
            "out.c-test-name-two",
        ),
        # testing with single sql statement and output table mappings
        (
            ['CREATE OR REPLACE TABLE "test_table_1" AS SELECT * FROM "test";'],
            ["test_table_1"],
            "test name",
            "out.c-test-name",
        ),
    ],
)
def test_get_transformation_configuration(
    sql_statements: list[str],
    created_table_names: list[str],
    transformation_name: str,
    expected_bucket_id: str,
):
    """Test get_transformation_configuration tool which should return the correct transformation configuration
    given the sql statement created_table_names and transformation_name."""

    configuration = _get_transformation_configuration(
        statements=sql_statements,
        transformation_name=transformation_name,
        output_tables=created_table_names,
    )

    assert configuration is not None
    assert isinstance(configuration, TransformationConfiguration)
    # we expect only one block and one code for the given sql statements
    assert configuration.parameters is not None
    assert len(configuration.parameters.blocks) == 1
    assert len(configuration.parameters.blocks[0].codes) == 1
    assert configuration.parameters.blocks[0].codes[0].name == "Code 0"
    assert configuration.parameters.blocks[0].codes[0].script == sql_statements
    # given output_table_mappings, assert following tables are created
    assert configuration.storage is not None
    assert configuration.storage.input is not None
    assert configuration.storage.output is not None
    assert configuration.storage.input.tables == []
    if not created_table_names:
        assert configuration.storage.output.tables == []
    else:
        assert len(configuration.storage.output.tables) == len(created_table_names)
        for created_table, expected_table_name in zip(
            configuration.storage.output.tables, created_table_names
        ):
            assert created_table.source == expected_table_name
            assert created_table.destination == f"{expected_bucket_id}.{expected_table_name}"
