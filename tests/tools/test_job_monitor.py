from typing import Any

import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.job_monitor import job_monitor, poll_job_monitor


@pytest.fixture
def mock_job() -> dict[str, Any]:
    return {
        'id': '123',
        'status': 'success',
        'isFinished': True,
        'componentId': 'keboola.snowflake-transformation',
        'configId': '456',
        'createdTime': '2024-01-01T00:00:00Z',
        'startTime': '2024-01-01T00:00:05Z',
        'endTime': '2024-01-01T00:01:30Z',
        'durationSeconds': 85.0,
        'runId': '789',
        'url': 'https://connection.keboola.com/jobs/123',
        'configData': {},
        'result': {'message': 'ok'},
    }


@pytest.fixture
def mock_events() -> list[dict[str, Any]]:
    return [
        {
            'uuid': 'e2',
            'message': 'Finished',
            'type': 'success',
            'created': '2024-01-01T00:01:30Z',
        },
        {
            'uuid': 'e1',
            'message': 'Started',
            'type': 'info',
            'created': '2024-01-01T00:00:05Z',
        },
    ]


@pytest.mark.asyncio
async def test_job_monitor_returns_structured_content(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_job: dict[str, Any],
    mock_events: list[dict[str, Any]],
):
    """Tests job_monitor returns both jobs and filterParams."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)
    keboola_client.storage_client.list_events = mocker.AsyncMock(return_value=mock_events)

    result = await job_monitor(ctx=context, job_ids=('123',))

    assert 'jobs' in result
    assert 'filterParams' in result
    assert len(result['jobs']) == 1
    assert result['jobs'][0]['id'] == '123'
    assert result['jobs'][0]['status'] == 'success'
    # Logs should be included by default
    assert result['jobs'][0]['logs'] is not None
    assert len(result['jobs'][0]['logs']) == 2


@pytest.mark.asyncio
async def test_job_monitor_filter_params_passthrough(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_job: dict[str, Any],
    mock_events: list[dict[str, Any]],
):
    """Tests that job_monitor includes filterParams so the app can poll with the same filters."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)
    keboola_client.storage_client.list_events = mocker.AsyncMock(return_value=mock_events)

    result = await job_monitor(ctx=context, job_ids=('123',), log_tail_lines=100)

    assert result['filterParams']['job_ids'] == ['123']
    assert result['filterParams']['log_tail_lines'] == 100
    assert result['filterParams']['include_logs'] is True


@pytest.mark.asyncio
async def test_job_monitor_listing_mode(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_job: dict[str, Any],
):
    """Tests job_monitor in listing mode (no job_ids) returns summaries."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.search_jobs_by = mocker.AsyncMock(return_value=[mock_job])

    result = await job_monitor(ctx=context, status='processing', limit=10)

    assert 'jobs' in result
    assert result['filterParams']['status'] == 'processing'
    assert result['filterParams']['limit'] == 10


@pytest.mark.asyncio
async def test_poll_job_monitor_returns_same_format(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_job: dict[str, Any],
    mock_events: list[dict[str, Any]],
):
    """Tests poll_job_monitor returns the same format as job_monitor."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)
    keboola_client.storage_client.list_events = mocker.AsyncMock(return_value=mock_events)

    result = await poll_job_monitor(ctx=context, job_ids=('123',))

    assert 'jobs' in result
    assert 'filterParams' in result
    assert len(result['jobs']) == 1
