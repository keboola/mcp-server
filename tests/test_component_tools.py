from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.component_tools import (
    Component,
    ComponentConfig,
    ComponentListItem,
    get_component_config_details,
    get_component_details,
    list_component_configs,
    list_components,
)


@pytest.mark.asyncio
async def test_list_components(mock_context):
    """Test list_components tool."""
    context, mock_client = mock_context
    mock_client.storage_client.components = MagicMock()

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
    mock_client.storage_client.components.list = MagicMock(return_value=mock_components)

    result = await list_components(context)

    assert len(result) == 2
    assert all(isinstance(component, ComponentListItem) for component in result)
    assert all(c.id == item["id"] for c, item in zip(result, mock_components))
    assert all(c.name == item["name"] for c, item in zip(result, mock_components))
    assert all(c.type == item["type"] for c, item in zip(result, mock_components))
    assert all(c.description == item["description"] for c, item in zip(result, mock_components))

    mock_client.storage_client.components.list.assert_called_once()


@pytest.mark.asyncio
async def test_list_component_configs(mock_context):
    """Test list_component_configs tool."""
    context, mock_client = mock_context
    mock_client.storage_client.configurations = MagicMock()

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
    mock_client.storage_client.configurations.list = MagicMock(return_value=mock_configs)

    result = await list_component_configs("keboola.ex-aws-s3", context)

    assert len(result) == 1
    assert isinstance(result[0], ComponentConfig)
    assert result[0].id == "123"
    assert result[0].name == "My Config"
    assert result[0].description == "Test configuration"

    mock_client.storage_client.configurations.list.assert_called_once_with("keboola.ex-aws-s3")


@pytest.mark.asyncio
async def test_get_component_details(mock_context):
    """Test get_component_details tool."""
    context, mock_client = mock_context

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

    # Setup mock to return test data
    mock_client.get = AsyncMock(return_value=mock_component)
    mock_client.storage_client._branch_id = "123"

    result = await get_component_details("keboola.ex-aws-s3", context)

    assert isinstance(result, Component)
    assert result.id == "keboola.ex-aws-s3"
    assert result.name == "AWS S3 Extractor"

    mock_client.get.assert_called_once_with("branch/123/components/keboola.ex-aws-s3")


@pytest.mark.asyncio
async def test_get_component_config_details(mock_context):
    """Test get_component_config_details tool."""
    context, mock_client = mock_context
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
    mock_client.storage_client.configurations.detail = AsyncMock(return_value=mock_config)

    result = await get_component_config_details("keboola.ex-aws-s3", "123", context)

    assert isinstance(result, ComponentConfig)
    assert result.id == "123"
    assert result.name == "My Config"
    assert result.description == "Test configuration"

    mock_client.storage_client.configurations.detail.assert_called_once_with(
        "keboola.ex-aws-s3", "123"
    )
