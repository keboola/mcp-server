import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import RawKeboolaClient
from keboola_mcp_server.clients.storage import AsyncStorageClient


@pytest.fixture
def storage_client(mocker: MockerFixture) -> AsyncStorageClient:
    raw = mocker.AsyncMock(RawKeboolaClient)
    client = AsyncStorageClient(raw_client=raw, branch_id=None)
    return client


@pytest.mark.asyncio
async def test_list_events_basic(storage_client: AsyncStorageClient):
    """Tests list_events calls the correct endpoint with runId."""
    mock_events = [
        {'uuid': 'evt-1', 'message': 'Processing started', 'type': 'info', 'created': '2024-01-01T00:00:01Z'},
        {'uuid': 'evt-2', 'message': 'Error occurred', 'type': 'error', 'created': '2024-01-01T00:00:02Z'},
    ]
    storage_client.raw_client.get.return_value = mock_events

    result = await storage_client.list_events(job_id='456', limit=50)

    assert result == mock_events
    storage_client.raw_client.get.assert_called_once_with(
        endpoint='events',
        params={'runId': '456', 'limit': 50, 'offset': 0, 'forceUuid': 'true'},
    )


@pytest.mark.asyncio
async def test_list_events_with_offset(storage_client: AsyncStorageClient):
    """Tests list_events passes offset correctly."""
    storage_client.raw_client.get.return_value = []

    await storage_client.list_events(job_id='456', limit=10, offset=100)

    storage_client.raw_client.get.assert_called_once_with(
        endpoint='events',
        params={'runId': '456', 'limit': 10, 'offset': 100, 'forceUuid': 'true'},
    )
