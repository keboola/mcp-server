from typing import Any, Callable, Union, get_args
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.component_tools import (
    FULLY_QUALIFIED_ID_SEPARATOR,
    ComponentConfigurationDetail,
    ComponentConfigurationListItem,
    ComponentConfigurationsList,
    ComponentDetail,
    ComponentListItem,
    ComponentType,
    get_component_configuration_details,
    handle_component_types,
    retrieve_components_in_project,
    retrieve_transformations_in_project,
)


@pytest.fixture
def mock_components() -> list[dict[str, Any]]:
    """Mock list_components tool."""
    return [
        {
            "id": "keboola.ex-aws-s3",
            "name": "AWS S3 Extractor",
            "type": "extractor",
            "description": "Extract data from AWS S3",
            "version": 1,
        },
        {
            "id": "keboola.wr-google-drive",
            "name": "Google Drive Writer",
            "type": "writer",
            "description": "Write data to Google Drive",
            "version": 1,
        },
        {
            "id": "keboola.app-google-drive",
            "name": "Google Drive Application",
            "type": "application",
            "description": "Application for Google Drive",
            "version": 1,
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
        "description": "Extract data from AWS S3",
        "longDescription": "Extract data from AWS S3 looooooooong",
        "categories": ["extractor"],
        "version": 1,
        "created": "2024-01-01T00:00:00Z",
        "data": {},
        "flags": [],
        "configurationSchema": {},
        "configurationDescription": "Extract data from AWS S3",
        "emptyConfiguration": {},
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
def test_branch_id() -> str:
    return "default"


@pytest.fixture
def mcp_context_components_configs(mcp_context_client, test_branch_id) -> Context:
    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.components = MagicMock()
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client._branch_id = test_branch_id
    return mcp_context_client


@pytest.fixture
def assert_retrieve_components() -> (
    Callable[[list[ComponentConfigurationsList], list[dict[str, Any]], list[dict[str, Any]]], None]
):
    """Assert that the retrieve_components_in_project tool returns the correct components and configurations."""

    def _assert_retrieve_components(
        result: list[ComponentConfigurationsList],
        components: list[dict[str, Any]],
        configurations: list[dict[str, Any]],
    ):

        assert len(result) == len(components)
        # assert basics
        assert all(isinstance(component, ComponentConfigurationsList) for component in result)
        assert all(isinstance(component.component, ComponentListItem) for component in result)
        assert all(isinstance(component.configurations, list) for component in result)
        assert all(
            all(
                isinstance(config, ComponentConfigurationListItem)
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
        assert all(
            returned.component.component_description == expected["description"]
            for returned, expected in zip(result, components)
        )
        assert all(not hasattr(returned.component, "version") for returned in result)
        # assert configurations list details
        assert all(
            all(
                isinstance(config, ComponentConfigurationListItem)
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
async def test_retrieve_components_in_project(
    mcp_context_components_configs,
    mock_components,
    mock_configurations,
    test_branch_id,
    assert_retrieve_components,
):
    """Test retrieve_components_in_project tool."""
    context = mcp_context_components_configs

    # Mock data
    keboola_client = context.session.state["sapi_client"]

    # mock the get method to return the mock_component with the mock_configurations
    # simulate the response from the API
    keboola_client.get = AsyncMock(
        side_effect=[
            [{**component, "configurations": mock_configurations}] for component in mock_components
        ]
    )

    result = await retrieve_components_in_project(context, component_types=["all"])

    assert_retrieve_components(result, mock_components, mock_configurations)

    keboola_client.get.assert_has_calls(
        [
            call(f"branch/{test_branch_id}/components", params={"componentType": "application"}),
            call(f"branch/{test_branch_id}/components", params={"componentType": "extractor"}),
            call(f"branch/{test_branch_id}/components", params={"componentType": "writer"}),
        ]
    )


@pytest.mark.asyncio
async def test_retrieve_transformations_in_project(
    mcp_context_components_configs,
    mock_component,
    mock_configurations,
    test_branch_id,
    assert_retrieve_components,
):
    """Test retrieve_transformations_in_project tool."""
    context = mcp_context_components_configs

    # Mock data
    keboola_client = context.session.state["sapi_client"]

    # mock the get method to return the mock_component with the mock_configurations
    # simulate the response from the API
    keboola_client.get = AsyncMock(return_value=[mock_component])

    result = await retrieve_transformations_in_project(context)

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.get.assert_has_calls(
        [
            call(f"branch/{test_branch_id}/components", params={"componentType": "transformation"}),
        ]
    )


@pytest.mark.asyncio
async def test_retrieve_components_in_project_from_ids(
    mcp_context_components_configs,
    mock_configurations,
    mock_component,
    test_branch_id,
    assert_retrieve_components,
):
    """Test list_component_configurations tool."""
    context = mcp_context_components_configs
    keboola_client = context.session.state["sapi_client"]

    # Mock data
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await retrieve_components_in_project(context, component_ids=[mock_component["id"]])

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component["id"])
    keboola_client.get.assert_called_once_with(
        f"branch/{test_branch_id}/components/{mock_component['id']}"
    )


@pytest.mark.asyncio
async def test_retrieve_transformations_in_project_from_ids(
    mcp_context_components_configs,
    mock_configurations,
    mock_component,
    test_branch_id,
    assert_retrieve_components,
):
    """Test list_component_configurations tool."""
    context = mcp_context_components_configs
    keboola_client = context.session.state["sapi_client"]

    # Mock data
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await retrieve_transformations_in_project(
        context, transformation_ids=[mock_component["id"]]
    )

    assert_retrieve_components(result, [mock_component], mock_configurations)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component["id"])
    keboola_client.get.assert_called_once_with(
        f"branch/{test_branch_id}/components/{mock_component['id']}"
    )


@pytest.mark.asyncio
async def test_get_component_configuration_details(
    mcp_context_client, mock_configuration, mock_component, mock_metadata
):
    """Test get_component_configuration_details tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.configurations = MagicMock()
    mock_client.storage_client.components = MagicMock()

    # Setup mock to return test data
    mock_client.storage_client.configurations.detail = MagicMock(return_value=mock_configuration)
    mock_client.storage_client.components.detail = MagicMock(return_value=mock_component)
    mock_client.storage_client._branch_id = "123"
    mock_client.get = AsyncMock(
        side_effect=[mock_component, mock_metadata]
    )  # Mock two results of the .get method first for component and then for metadata

    result = await get_component_configuration_details("keboola.ex-aws-s3", "123", context)

    assert isinstance(result, ComponentConfigurationDetail)
    assert result.component is not None
    assert result.component.component_id == mock_component["id"]
    assert result.component.component_name == mock_component["name"]
    assert result.component.component_type == mock_component["type"]
    assert result.component.component_description == mock_component["description"]
    assert result.component.long_description == mock_component["longDescription"]
    assert result.component.categories == mock_component["categories"]
    assert result.component.version == mock_component["version"]
    assert result.configuration_id == mock_configuration["id"]
    assert result.configuration_name == mock_configuration["name"]
    assert result.configuration_description == mock_configuration["description"]
    assert result.is_disabled == mock_configuration["isDisabled"]
    assert result.is_deleted == mock_configuration["isDeleted"]
    assert result.version == mock_configuration["version"]
    assert result.configuration == mock_configuration["configuration"]
    assert result.rows == mock_configuration["rows"]
    assert result.configuration_metadata == mock_metadata

    mock_client.storage_client.configurations.detail.assert_called_once_with(
        "keboola.ex-aws-s3", "123"
    )

    mock_client.get.assert_has_calls(
        [
            call("branch/123/components/keboola.ex-aws-s3"),
            call("branch/123/components/keboola.ex-aws-s3/configs/123/metadata"),
        ]
    )


@pytest.mark.parametrize(
    "component_type, expected",
    [
        ("application", ["application"]),
        (["extractor", "writer"], ["extractor", "writer"]),
        (["writer", "all", "extractor"], ["application", "extractor", "writer"]),
    ],
)
def test_handle_component_types(
    component_type: Union[ComponentType, list[ComponentType]], expected: list[ComponentType]
):
    """Test list_component_configurations tool with core component."""
    assert handle_component_types(component_type) == expected


@pytest.mark.parametrize(
    "component_id, configuration_id, expected",
    [
        ("keboola.ex-aws-s3", "123", f"keboola.ex-aws-s3{FULLY_QUALIFIED_ID_SEPARATOR}123"),
        (
            "keboola.wr-google-drive",
            "234",
            f"keboola.wr-google-drive{FULLY_QUALIFIED_ID_SEPARATOR}234",
        ),
    ],
)
def test_set_fully_qualified_id(
    component_id: str,
    configuration_id: str,
    expected: str,
    mock_component: dict[str, Any],
    mock_configuration: dict[str, Any],
):
    """Test set_fully_qualified_id tool."""
    component = mock_component
    configuration = mock_configuration
    component["id"] = component_id
    configuration["id"] = configuration_id
    component_configuration = ComponentConfigurationListItem.model_validate(
        {**configuration, "component_id": component_id}
    )
    assert component_configuration.fully_qualified_id == expected