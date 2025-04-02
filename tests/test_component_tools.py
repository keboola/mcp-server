from typing import Any
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from keboola_mcp_server.component_tools import (
    Component,
    ComponentListItem,
    ComponentConfiguration,
    ComponentConfigurationListItem,
    get_component_configuration_details,
    get_component_details,
    list_component_configurations,
    list_components,
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
            "id": "keboola.ex-google-drive",
            "name": "Google Drive Extractor",
            "type": "extractor",
            "description": "Extract data from Google Drive",
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


@pytest.mark.asyncio
async def test_list_components(mcp_context_client, mock_components):
    """Test list_components tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.components = MagicMock()

    # Mock data
    keboola_client.storage_client.components.list = MagicMock(return_value=mock_components)

    result = await list_components(mcp_context_client)

    assert len(result) == 2

    assert all(isinstance(component, ComponentListItem) for component in result)
    assert all(c.component_id == item["id"] for c, item in zip(result, mock_components))
    assert all(c.component_name == item["name"] for c, item in zip(result, mock_components))
    assert all(c.component_type == item["type"] for c, item in zip(result, mock_components))
    assert all(
        c.component_description == item["description"] for c, item in zip(result, mock_components)
    )
    assert all(not hasattr(c, "version") for c in result)

    keboola_client.storage_client.components.list.assert_called_once()


@pytest.mark.asyncio
async def test_list_component_configurations(
    mcp_context_client, mock_configurations, mock_component
):
    """Test list_component_configurations tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client.components = MagicMock()
    # Mock data

    test_branch_id = "123"
    keboola_client.storage_client._branch_id = test_branch_id
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)

    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await list_component_configurations("keboola.ex-aws-s3", mcp_context_client)

    assert len(result) == 2
    assert all(isinstance(config, ComponentConfigurationListItem) for config in result)
    assert all(
        config.configuration_id == item["id"] for config, item in zip(result, mock_configurations)
    )
    assert all(config.component.component_id == mock_component["id"] for config in result)
    assert all(
        config.configuration_name == item["name"]
        for config, item in zip(result, mock_configurations)
    )
    assert all(
        config.configuration_description == item["description"]
        for config, item in zip(result, mock_configurations)
    )
    assert all(
        config.is_disabled == item["isDisabled"]
        for config, item in zip(result, mock_configurations)
    )
    assert all(
        config.is_deleted == item["isDeleted"] for config, item in zip(result, mock_configurations)
    )
    assert all(not hasattr(config, "version") for config in result)
    assert all(not hasattr(config, "configuration") for config in result)

    keboola_client.storage_client.configurations.list.assert_called_once_with(mock_component["id"])
    keboola_client.get.assert_called_once_with(
        f"branch/{test_branch_id}/components/{mock_component['id']}"
    )


@pytest.mark.asyncio
async def test_get_component_details(mcp_context_client, mock_component):
    """Test get_component_details tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    # Setup mock to return test data

    test_branch_id = "123"
    keboola_client.storage_client._branch_id = test_branch_id
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await get_component_details("keboola.ex-aws-s3", mcp_context_client)

    assert isinstance(result, Component)
    assert result.component_id == mock_component["id"]
    assert result.component_name == mock_component["name"]
    assert result.component_type == mock_component["type"]
    assert result.component_description == mock_component["description"]
    assert result.long_description == mock_component["longDescription"]
    assert result.categories == mock_component["categories"]
    assert result.version == mock_component["version"]
    assert result.data == mock_component["data"]
    assert result.flags == mock_component["flags"]
    assert result.configuration_schema == mock_component["configurationSchema"]
    assert result.configuration_description == mock_component["configurationDescription"]
    assert result.empty_configuration == mock_component["emptyConfiguration"]

    assert not hasattr(result, "created")
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
    )  # Mock two results of the .get method

    result = await get_component_configuration_details("keboola.ex-aws-s3", "123", context)

    assert isinstance(result, ComponentConfiguration)
    assert result.component.component_id == mock_component["id"]
    assert result.component.component_name == mock_component["name"]
    assert result.component.component_type == mock_component["type"]
    assert result.component.component_description == mock_component["description"]
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
    assert result.metadata == mock_metadata

    mock_client.storage_client.configurations.detail.assert_called_once_with(
        "keboola.ex-aws-s3", "123"
    )

    mock_client.get.assert_has_calls(
        [
            call("branch/123/components/keboola.ex-aws-s3"),
            call("branch/123/components/keboola.ex-aws-s3/configs/123/metadata"),
        ]
    )
