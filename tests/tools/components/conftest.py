from typing import Any

import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import KeboolaClient


@pytest.fixture
def mock_components() -> list[dict[str, Any]]:
    """Mock list_components tool."""
    return [
        {
            'id': 'keboola.ex-aws-s3',
            'name': 'AWS S3 Extractor',
            'type': 'extractor',
            'description': 'Extract data from AWS S3',
            'version': '1',
        },
        {
            'id': 'keboola.wr-google-drive',
            'name': 'Google Drive Writer',
            'type': 'writer',
            'description': 'Write data to Google Drive',
            'version': '1',
        },
        {
            'id': 'keboola.app-google-drive',
            'name': 'Google Drive Application',
            'type': 'application',
            'description': 'Application for Google Drive',
            'version': '1',
        },
    ]


@pytest.fixture
def mock_configurations() -> list[dict[str, Any]]:
    """Mock mock_configurations tool."""
    return [
        {
            'id': '123',
            'name': 'My Config',
            'description': 'Test configuration',
            'created': '2024-01-01T00:00:00Z',
            'isDisabled': False,
            'isDeleted': False,
            'version': 1,
            'configuration': {},
        },
        {
            'id': '456',
            'name': 'My Config 2',
            'description': 'Test configuration 2',
            'created': '2024-01-01T00:00:00Z',
            'isDisabled': True,
            'isDeleted': True,
            'version': 2,
            'configuration': {},
        },
    ]


@pytest.fixture
def mock_component() -> dict[str, Any]:
    """Mock mock_component tool."""
    return {
        'id': 'keboola.ex-aws-s3',
        'name': 'AWS S3 Extractor',
        'type': 'extractor',
        'description': 'Extract data from AWS S3',
        'longDescription': 'Extract data from AWS S3 looooooooong',
        'categories': ['extractor'],
        'version': 1,
        'created': '2024-01-01T00:00:00Z',
        'data': {'data1': 'data1', 'data2': 'data2'},
        'flags': ['flag1', 'flag2'],
        'configurationSchema': {},
        'configurationDescription': 'Extract data from AWS S3',
        'emptyConfiguration': {},
    }


@pytest.fixture
def mock_configuration() -> dict[str, Any]:
    """Mock mock_configuration tool."""
    return {
        'id': '123',
        'name': 'My Config',
        'description': 'Test configuration',
        'created': '2024-01-01T00:00:00Z',
        'isDisabled': False,
        'isDeleted': False,
        'version': 1,
        'configuration': {},
        'rows': [{'id': '1', 'name': 'Row 1'}, {'id': '2', 'name': 'Row 2'}],
    }


@pytest.fixture
def mock_metadata() -> list[dict[str, Any]]:
    """Mock mock_component_configuration tool."""
    return [
        {
            'id': '1',
            'key': 'test-key',
            'value': 'test-value',
            'provider': 'user',
            'timestamp': '2024-01-01T00:00:00Z',
        }
    ]


@pytest.fixture
def mock_branch_id() -> str:
    return 'default'


@pytest.fixture
def mcp_context_components_configs(mocker: MockerFixture, mcp_context_client: Context, mock_branch_id: str) -> Context:
    keboola_client = mcp_context_client.session.state[KeboolaClient.STATE_KEY]
    keboola_client.storage_client_sync.components = mocker.MagicMock()
    keboola_client.storage_client_sync.configurations = mocker.MagicMock()
    keboola_client.storage_client.branch_id = mock_branch_id

    return mcp_context_client
