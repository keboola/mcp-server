from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.component_tools import (
    Component,
    ComponentConfiguration,
    get_component_configuration_details,
    retrieve_component_configurations,
    retrieve_components,
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

    result = await retrieve_components(mcp_context_client)

    assert len(result) == 2
    assert all(isinstance(component, Component) for component in result)
    assert all(
        component.component_id == expected["id"]
        for component, expected in zip(result, mock_components)
    )
    assert all(
        component.component_name == expected["name"]
        for component, expected in zip(result, mock_components)
    )

    keboola_client.storage_client.components.list.assert_called_once()


@pytest.mark.asyncio
async def test_list_component_configs(mcp_context_client):
    """Test list_component_configs tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client.components = MagicMock()
    # Mock data
    mock_configs = [
        {
            "id": "123",
            "name": "My Config",
            "description": "Test configuration",
            "created": "2024-01-01T00:00:00Z",
        }
    ]
    mock_component = {
        "id": "keboola.ex-aws-s3",
        "name": "AWS S3 Extractor",
    }
    keboola_client.storage_client._branch_id = "123"
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configs)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await retrieve_component_configurations("keboola.ex-aws-s3", mcp_context_client)

    assert len(result) == 1
    assert isinstance(result[0], ComponentConfiguration)
    assert result[0].component.component_id == "keboola.ex-aws-s3"
    assert result[0].component.component_name == "AWS S3 Extractor"
    assert result[0].configuration_id == "123"
    assert result[0].configuration_name == "My Config"
    assert result[0].configuration_description == "Test configuration"

    keboola_client.storage_client.configurations.list.assert_called_once_with("keboola.ex-aws-s3")
    keboola_client.get.assert_called_once_with("branch/123/components/keboola.ex-aws-s3")


@pytest.mark.asyncio
async def test_get_component_details(mcp_context_client):
    """Test get_component_details tool."""

    # Mock data
    mock_configuration = {
        "id": "123",
        "name": "My Config",
        "description": "Test configuration",
        "created": "2024-01-01T00:00:00Z",
    }
    mock_component = {
        "id": "keboola.ex-aws-s3",
        "name": "AWS S3 Extractor",
    }
    keboola_client = mcp_context_client.session.state["sapi_client"]
    # Setup mock to return test data
    keboola_client.storage_client._branch_id = "123"
    keboola_client.storage_client.configurations = MagicMock()
    keboola_client.storage_client.configurations.detail = MagicMock(return_value=mock_configuration)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await get_component_configuration_details(
        "keboola.ex-aws-s3", "123", mcp_context_client
    )

    assert isinstance(result, ComponentConfiguration)
    assert result.component.component_id == "keboola.ex-aws-s3"
    assert result.configuration_id == "123"
    assert result.configuration_name == "My Config"
    assert result.configuration_description == "Test configuration"

    keboola_client.storage_client.configurations.detail.assert_called_once_with(
        "keboola.ex-aws-s3", "123"
    )
    keboola_client.get.assert_called_once_with("branch/123/components/keboola.ex-aws-s3")