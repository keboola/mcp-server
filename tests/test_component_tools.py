from typing import Any, Union, get_args
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.component_tools import (
    ComponentConfigurationsList,
    ComponentDetail,
    ComponentListItem,
    ComponentConfigurationDetail,
    ComponentConfigurationListItem,
    ComponentType,
    get_component_configuration_details,
    get_core_component_details,
    handle_component_types,
    retrieve_component_configurations,
    list_all_component_configurations,
    retrieve_core_components,
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


@pytest.mark.asyncio
async def test_list_core_components(mcp_context_components_configs, mock_components):
    """Test list_components tool."""
    context = mcp_context_components_configs

    # Mock data
    keboola_client = context.session.state["sapi_client"]
    keboola_client.storage_client.components.list = MagicMock(return_value=mock_components)

    result = await retrieve_core_components(context)

    assert len(result) == 2

    assert all(isinstance(component, ComponentListItem) for component in result)
    assert all(
        component.component_id == expected["id"]
        for component, expected in zip(result, mock_components)
    )
    assert all(
        component.component_name == expected["name"]
        for component, expected in zip(result, mock_components)
    )
    assert all(
        component.component_type == expected["type"]
        for component, expected in zip(result, mock_components)
    )
    assert all(
        component.component_description == expected["description"]
        for component, expected in zip(result, mock_components)
    )
    assert all(not hasattr(component, "version") for component in result)

    keboola_client.storage_client.components.list.assert_called_once()


@pytest.mark.asyncio
async def test_list_components(
    mcp_context_components_configs, mock_components, mock_configurations
):
    """Test list_components tool."""
    context = mcp_context_components_configs

    # Mock data
    keboola_client = context.session.state["sapi_client"]
    keboola_client.storage_client.components.list = MagicMock(return_value=mock_components)
    keboola_client.storage_client.configurations.list = MagicMock(
        #
        side_effect=[[mock_configurations[0]], [mock_configurations[1]]]
    )

    keboola_client.get = AsyncMock(side_effect=mock_components)

    result = await list_all_component_configurations(context)

    assert len(result) == 2

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
    assert all(c.component.component_id == item["id"] for c, item in zip(result, mock_components))
    assert all(
        c.component.component_name == item["name"] for c, item in zip(result, mock_components)
    )
    assert all(
        c.component.component_type == item["type"] for c, item in zip(result, mock_components)
    )
    assert all(
        c.component.component_description == item["description"]
        for c, item in zip(result, mock_components)
    )
    assert all(not hasattr(c.component, "version") for c in result)
    # assert configurations list details
    assert all(
        all(
            isinstance(config, ComponentConfigurationListItem)
            for config in component.configurations
        )
        for component in result
    )
    # use zip to iterate over the result and mock_configurations since we artifically mock the .get method
    # to return configuration at position i in the mock_configurations for component at position i in the
    # mock_components
    assert all(
        all(
            config.configuration_id == item["id"]
            for config, item in zip(component.configurations, [mock_configurations[i]])
        )
        for i, component in enumerate(result)
    )
    assert all(
        all(
            config.configuration_name == item["name"]
            for config, item in zip(component.configurations, [mock_configurations[i]])
        )
        for i, component in enumerate(result)
    )
    assert all(
        all(
            config.configuration_description == item["description"]
            for config, item in zip(component.configurations, [mock_configurations[i]])
        )
        for i, component in enumerate(result)
    )
    assert all(
        all(
            config.is_disabled == item["isDisabled"]
            for config, item in zip(component.configurations, [mock_configurations[i]])
        )
        for i, component in enumerate(result)
    )
    assert all(
        all(
            config.is_deleted == item["isDeleted"]
            for config, item in zip(component.configurations, [mock_configurations[i]])
        )
        for i, component in enumerate(result)
    )

    keboola_client.storage_client.components.list.assert_called_once()


@pytest.mark.asyncio
async def test_list_component_configurations(
    mcp_context_components_configs, mock_configurations, mock_component, test_branch_id
):
    """Test list_component_configurations tool."""
    context = mcp_context_components_configs
    keboola_client = context.session.state["sapi_client"]

    # Mock data
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)
    keboola_client.get = AsyncMock(return_value=mock_component)

    result = await retrieve_component_configurations("keboola.ex-aws-s3", context)

    # assert basics
    assert isinstance(result, ComponentConfigurationsList)
    assert isinstance(result.component, ComponentListItem)
    assert isinstance(result.configurations, list)
    assert len(result.configurations) == 2
    assert all(
        isinstance(config, ComponentConfigurationListItem) for config in result.configurations
    )
    # assert component list details
    assert result.component.component_id == mock_component["id"]
    assert result.component.component_name == mock_component["name"]
    assert result.component.component_type == mock_component["type"]
    assert result.component.component_description == mock_component["description"]
    # assert configurations list details
    assert all(
        config.configuration_id == item["id"]
        for config, item in zip(result.configurations, mock_configurations)
    )
    assert all(
        config.configuration_name == item["name"]
        for config, item in zip(result.configurations, mock_configurations)
    )
    assert all(
        config.configuration_description == item["description"]
        for config, item in zip(result.configurations, mock_configurations)
    )
    assert all(
        config.is_disabled == item["isDisabled"]
        for config, item in zip(result.configurations, mock_configurations)
    )
    assert all(
        config.is_deleted == item["isDeleted"]
        for config, item in zip(result.configurations, mock_configurations)
    )

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

    result = await get_core_component_details("keboola.ex-aws-s3", mcp_context_client)

    assert isinstance(result, ComponentDetail)
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "component_type, expected",
    [
        ("extractor", ["extractor"]),
        (["extractor", "writer"], ["extractor", "writer"]),
        (["writer", "all", "extractor"], ["all"]),
    ],
)
def test_handle_component_types(
    component_type: Union[ComponentType, list[ComponentType]], expected: list[ComponentType]
):
    """Test list_component_configurations tool with core component."""
    assert handle_component_types(component_type) == expected


@pytest.fixture
def mock_types_components() -> list[dict[str, str | int]]:
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
            "id": "keboola.tr-google-drive",
            "name": "Google Drive Transformation",
            "type": "transformation",
            "description": "Transform data from Google Drive",
            "version": 1,
        },
        {
            "id": "keboola.other",
            "name": "Other Component",
            "type": "other",
            "description": "Other Component",
            "version": 1,
        },
        {
            "id": "keboola.orchestrator",
            "name": "Orchestrator",
            "type": "orchestrator",
            "description": "Orchestrator",
            "version": 1,
        },
    ]


@pytest.mark.parametrize(
    "component_types, expected_ids",
    [
        (["extractor"], ["keboola.ex-aws-s3"]),
        (["extractor", "writer"], ["keboola.ex-aws-s3", "keboola.wr-google-drive"]),
        (
            ["all"],
            [
                "keboola.ex-aws-s3",
                "keboola.wr-google-drive",
                "keboola.tr-google-drive",
                "keboola.orchestrator",
                "keboola.other",
            ],
        ),
        (["other"], ["keboola.other", "keboola.orchestrator"]),
    ],
)
@pytest.mark.asyncio
async def test_conform_types(
    mcp_context_components_configs: Context,
    component_types: list[ComponentType],
    expected_ids: list[str],
    mock_types_components: list[dict[str, str]],
    mock_configurations: list[dict[str, str]],
):
    context = mcp_context_components_configs
    keboola_client = context.session.state["sapi_client"]
    keboola_client.storage_client.components.list = MagicMock(return_value=mock_types_components)
    keboola_client.storage_client.configurations.list = MagicMock(return_value=mock_configurations)
    keboola_client.get = AsyncMock(side_effect=mock_types_components)

    # since we artifically mock the .get method to return the mock_configurations for each component,
    # we can use the length of the mock_configurations to determine the number of configurations per component
    expected_n_configurations_per_component = len(mock_configurations)

    component_configs = await list_all_component_configurations(context, types=component_types)

    assert len(component_configs) == len(expected_ids)
    assert all([cf.component.component_id in expected_ids for cf in component_configs])
    assert all(
        [
            len(cf.configurations) == expected_n_configurations_per_component
            for cf in component_configs
        ]
    )
