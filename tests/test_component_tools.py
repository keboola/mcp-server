from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.component_tools import (
    Component,
    ComponentConfig,
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
        {"id": "keboola.ex-aws-s3", "name": "AWS S3 Extractor"},
        {"id": "keboola.ex-google-drive", "name": "Google Drive Extractor"},
    ]
    keboola_client.storage_client.components.list = MagicMock(return_value=mock_components)

    result = await list_components(mcp_context_client)

    assert len(result) == 2
    assert all(isinstance(component, Component) for component in result)
    assert all(c.id == item["id"] for c, item in zip(result, mock_components))
    assert all(c.name == item["name"] for c, item in zip(result, mock_components))

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
    mock_component = {"id": "keboola.ex-aws-s3", "name": "AWS S3 Extractor"}
    keboola_client = mcp_context_client.session.state["sapi_client"]
    # Setup mock to return test data
    keboola_client.get = AsyncMock(return_value=mock_component)
    keboola_client.storage_client._branch_id = "123"

    result = await get_component_details("keboola.ex-aws-s3", mcp_context_client)

    assert isinstance(result, Component)
    assert result.id == "keboola.ex-aws-s3"
    assert result.name == "AWS S3 Extractor"

    keboola_client.get.assert_called_once_with("branch/123/components/keboola.ex-aws-s3")
