import asyncio
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlparse

import pytest
from httpx import HTTPStatusError, Request, Response

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.workspace import WorkspaceManager, _SnowflakeWorkspace


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('bearer_token', 'storage_token', 'expected_token'),
    [
        ('oauth_bearer_123', 'sapi_token_456', 'Bearer oauth_bearer_123'),
        (None, 'sapi_token_456', 'sapi_token_456'),
        ('', 'sapi_token_456', 'sapi_token_456'),
    ],
    ids=['with_bearer_token', 'without_bearer_token', 'empty_bearer_token'],
)
async def test_query_client_token_selection(bearer_token: str | None, storage_token: str, expected_token: str):
    """Test QueryServiceClient uses bearer token when available, falls back to storage token."""
    # Create mock KeboolaClient with different token configurations
    mock_client = Mock(spec=KeboolaClient)
    mock_client.token = storage_token
    mock_client.bearer_token = bearer_token
    mock_client.hostname_suffix = 'keboola.com'
    mock_client.branch_id = '12345'
    mock_client.headers = {}

    # Create a mock storage client to avoid real API calls
    mock_storage_client = Mock()
    mock_client.storage_client = mock_storage_client

    # Create workspace instance
    workspace = _SnowflakeWorkspace(workspace_id=1, schema='test_schema', client=mock_client)

    # Mock QueryServiceClient.create to capture the token parameter
    with patch.object(QueryServiceClient, 'create') as mock_qs_create:
        mock_qs_instance = AsyncMock(spec=QueryServiceClient)
        mock_qs_instance.branch_id = '12345'
        mock_qs_create.return_value = mock_qs_instance

        # Call the method that creates the QueryServiceClient
        result = await workspace._create_qs_client()

        # Verify QueryServiceClient.create was called with the expected token
        mock_qs_create.assert_called_once()
        call_kwargs = mock_qs_create.call_args.kwargs
        assert call_kwargs['token'] == expected_token
        # Use proper URL parsing instead of substring check to avoid security alerts
        parsed_url = urlparse(call_kwargs['root_url'])
        assert parsed_url.scheme == 'https'
        assert parsed_url.netloc == 'query.keboola.com'
        assert call_kwargs['branch_id'] == '12345'
        assert result == mock_qs_instance


@pytest.mark.asyncio
async def test_query_client_token_selection_with_branch_lookup():
    """Test QueryServiceClient token selection when branch_id needs to be looked up."""
    # Create mock KeboolaClient with bearer token but no branch_id
    mock_client = Mock(spec=KeboolaClient)
    mock_client.token = 'sapi_token_456'
    mock_client.bearer_token = 'oauth_bearer_123'
    mock_client.hostname_suffix = 'keboola.com'
    mock_client.branch_id = None  # No branch_id, will trigger lookup
    mock_client.headers = {}

    # Mock storage client with branches_list that returns default branch
    mock_storage_client = AsyncMock()
    mock_storage_client.branches_list.return_value = [
        {'id': '999', 'isDefault': False},
        {'id': '888', 'isDefault': True},  # Default branch
        {'id': '777', 'isDefault': False},
    ]
    mock_client.storage_client = mock_storage_client

    # Create workspace instance
    workspace = _SnowflakeWorkspace(workspace_id=1, schema='test_schema', client=mock_client)

    # Mock QueryServiceClient.create
    with patch.object(QueryServiceClient, 'create') as mock_qs_create:
        mock_qs_instance = AsyncMock(spec=QueryServiceClient)
        mock_qs_instance.branch_id = '888'
        mock_qs_create.return_value = mock_qs_instance

        # Call the method that creates the QueryServiceClient
        await workspace._create_qs_client()

        # Verify branch lookup was performed
        mock_storage_client.branches_list.assert_called_once()

        # Verify QueryServiceClient.create was called with bearer token and correct branch
        mock_qs_create.assert_called_once()
        call_kwargs = mock_qs_create.call_args.kwargs
        assert call_kwargs['token'] == 'Bearer oauth_bearer_123'
        assert call_kwargs['branch_id'] == '888'  # Found default branch


@pytest.mark.asyncio
async def test_workspace_creation_cleans_up_config_on_failure():
    """Test that WorkspaceManager._create_ws cleans up config when workspace creation fails."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client

    mock_storage_client.verify_token.return_value = {'owner': {'defaultBackend': 'snowflake'}}
    mock_storage_client.configuration_create.return_value = {'id': 'test-config-123', 'name': 'test'}

    mock_response = Mock(spec=Response)
    mock_response.status_code = 500
    mock_response.text = 'Workspace creation failed'
    mock_request = Mock(spec=Request)
    mock_request.url = 'https://connection.keboola.com/v2/storage'
    mock_storage_client.workspace_create_for_config = AsyncMock(
        side_effect=HTTPStatusError('Workspace creation failed', request=mock_request, response=mock_response)
    )
    mock_storage_client.configuration_delete = AsyncMock()

    manager = WorkspaceManager(mock_client)

    with pytest.raises(HTTPStatusError):
        await manager._create_ws()

    mock_storage_client.configuration_create.assert_called_once()
    mock_storage_client.configuration_delete.assert_called_once_with(
        WorkspaceManager.MCP_WORKSPACE_COMPONENT_ID, 'test-config-123'
    )


def _make_cancel_test_workspace(*, cancel_job_side_effect=None) -> tuple[_SnowflakeWorkspace, AsyncMock, dict]:
    """Build a `_SnowflakeWorkspace` whose `_qsclient` simulates a long-running query.

    The mocked `get_job_status` returns ``running`` until `cancel_job` is invoked,
    after which it returns ``canceled`` (mimicking Query Service confirming the cancel).
    The ``state`` dict lets the caller inspect whether cancellation was issued.
    """
    workspace = _SnowflakeWorkspace(workspace_id=1, schema='test_schema', client=Mock(spec=KeboolaClient))
    mock_qs = AsyncMock(spec=QueryServiceClient)
    workspace._qsclient = mock_qs

    mock_qs.submit_job.return_value = 'job-abc-123'

    state = {'cancelled': False}

    async def get_status(job_id: str):
        return {'status': 'canceled' if state['cancelled'] else 'running'}

    async def default_cancel(job_id: str, reason: str):
        state['cancelled'] = True
        return {}

    mock_qs.get_job_status.side_effect = get_status
    mock_qs.cancel_job.side_effect = cancel_job_side_effect or default_cancel
    return workspace, mock_qs, state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('cancel_job_side_effect_factory', 'expect_cancel_call'),
    [
        (None, True),
        (
            lambda: HTTPStatusError(
                'cancel failed',
                request=Mock(spec=Request),
                response=Mock(spec=Response, status_code=500, text='boom'),
            ),
            True,
        ),
    ],
    ids=['backend_cancel_succeeds', 'backend_cancel_fails'],
)
async def test_execute_query_cancellation_propagates_to_backend(
    cancel_job_side_effect_factory, expect_cancel_call: bool
):
    """Client cancellation (MCP `notifications/cancelled`) must trigger `cancel_job` on
    the Snowflake side. If the backend cancel itself fails, the original CancelledError
    must still propagate so the SDK can finalize the request cleanly."""
    side_effect = cancel_job_side_effect_factory() if cancel_job_side_effect_factory else None
    workspace, mock_qs, _state = _make_cancel_test_workspace(cancel_job_side_effect=side_effect)

    task = asyncio.create_task(workspace.execute_query('SELECT 1'))
    # Yield to let the task enter the poll loop and issue at least one get_job_status.
    await asyncio.sleep(0.05)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    if expect_cancel_call:
        mock_qs.cancel_job.assert_called_once_with('job-abc-123', reason='Client cancelled the request')
