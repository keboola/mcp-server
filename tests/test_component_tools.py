from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.component_tools import (
    Component,
    ComponentConfig,
    ComponentConfigMetadata,
    ComponentListItem,
    get_component_config_details,
    get_component_config_metadata,
    get_component_details,
    list_component_configs,
    list_components,
)


@pytest.mark.asyncio
async def test_list_components(mcp_context_client):
    """Test list_components tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.components = MagicMock()

    # Mock data
    mock_components = [
        {
            "id": "keboola.ex-aws-s3",
            "name": "AWS S3 Extractor",
            "type": "extractor",
            "description": "Extract data from AWS S3",
        },
        {
            "id": "keboola.ex-google-drive",
            "name": "Google Drive Extractor",
            "type": "extractor",
            "description": "Extract data from Google Drive",
        },
    ]
    keboola_client.storage_client.components.list = MagicMock(return_value=mock_components)

    result = await list_components(mcp_context_client)

    assert len(result) == 2
    assert all(isinstance(component, ComponentListItem) for component in result)
    assert all(c.id == item["id"] for c, item in zip(result, mock_components))
    assert all(c.name == item["name"] for c, item in zip(result, mock_components))
    assert all(c.type == item["type"] for c, item in zip(result, mock_components))
    assert all(c.description == item["description"] for c, item in zip(result, mock_components))

    keboola_client.storage_client.components.list.assert_called_once()


@pytest.mark.asyncio
async def test_list_component_configs(mcp_context_client):
    """Test list_component_configs tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.configurations = MagicMock()

    # Mock data
    mock_configs = [
        {
            "id": "123",
            "name": "My Config",
            "description": "Test configuration",
            "created": "2024-01-01T00:00:00Z",
            "isDisabled": False,
            "isDeleted": False,
            "version": 1,
            "configuration": {},
        }
    ]
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configs)

    result = await list_component_configs("keboola.ex-aws-s3", mcp_context_client)

    assert len(result) == 1
    assert isinstance(result[0], ComponentConfig)
    assert result[0].id == "123"
    assert result[0].name == "My Config"
    assert result[0].description == "Test configuration"

    keboola_client.storage_client.configurations.list.assert_called_once_with("keboola.ex-aws-s3")


@pytest.mark.asyncio
async def test_get_component_details(mcp_context_client):
    """Test get_component_details tool."""

    # Mock data
    mock_component = {
        "id": "keboola.ex-aws-s3",
        "name": "AWS S3 Extractor",
        "type": "extractor",
        "description": "Extract data from AWS S3",
        "long_description": "Extract data from AWS S3",
        "categories": ["extractor"],
        "version": 1,
        "created": "2024-01-01T00:00:00Z",
        "data": {},
        "flags": [],
        "configurationSchema": {},
        "configurationDescription": "Extract data from AWS S3",
        "emptyConfiguration": {},
    }

    keboola_client = mcp_context_client.session.state["sapi_client"]

    # Setup mock to return test data
    keboola_client.get = AsyncMock(return_value=mock_component)
    keboola_client.storage_client._branch_id = "123"

    result = await get_component_details("keboola.ex-aws-s3", mcp_context_client)

    assert isinstance(result, Component)
    assert result.id == "keboola.ex-aws-s3"
    assert result.name == "AWS S3 Extractor"

    keboola_client.get.assert_called_once_with("branch/123/components/keboola.ex-aws-s3")


@pytest.mark.asyncio
async def test_get_component_config_details(mcp_context_client):
    """Test get_component_config_details tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.configurations = MagicMock()
    # Mock data
    mock_config = {
        "id": "123",
        "name": "My Config",
        "description": "Test configuration",
        "created": "2024-01-01T00:00:00Z",
        "isDisabled": False,
        "isDeleted": False,
        "version": 1,
        "configuration": {},
    }

    # Setup mock to return test data
    mock_client.storage_client.configurations.detail = MagicMock(return_value=mock_config)

    result = await get_component_config_details("keboola.ex-aws-s3", "123", context)

    assert isinstance(result, ComponentConfig)
    assert result.id == "123"
    assert result.name == "My Config"
    assert result.description == "Test configuration"

    mock_client.storage_client.configurations.detail.assert_called_once_with(
        "keboola.ex-aws-s3", "123"
    )


@pytest.mark.asyncio
async def test_get_component_config_metadata(mcp_context_client):
    """Test get_component_config_metadata tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client._branch_id = "123"

    # Mock data
    mock_metadata = [
        {
            "id": "1",
            "key": "test-key",
            "value": "test-value",
            "provider": "user",
            "timestamp": "2024-01-01T00:00:00Z",
        }
    ]

    # Setup mock to return test data
    mock_client.get = AsyncMock(return_value=mock_metadata)

    result = await get_component_config_metadata("keboola.ex-aws-s3", "456", context)

    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ComponentConfigMetadata)
    assert result[0].component_id == "keboola.ex-aws-s3"
    assert result[0].config_id == "456"
    assert result[0].metadata == mock_metadata[0]

    mock_client.get.assert_called_once_with(
        "branch/123/components/keboola.ex-aws-s3/configs/456/metadata"
    )
