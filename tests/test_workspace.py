import asyncio
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlparse

import pytest
from httpx import HTTPStatusError, Request, Response

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.workspace import JobSubmittedInfo, WorkspaceManager, _SnowflakeWorkspace


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
@pytest.mark.parametrize('terminal_status', ['canceled', 'cancelled'])
async def test_execute_query_returns_clear_message_when_job_cancelled(terminal_status: str):
    """When the QS poll loop exits because the job reached a CANCELLED terminal state
    (typically because kai-agent POSTed to Query Service's cancel endpoint after the user
    clicked STOP), we must short-circuit the results-fetch and surface a precise
    "Query was cancelled" message — not the generic "Job is still running or not completed
    yet" that QS returns from get_job_results for non-completed jobs.

    Both 'canceled' (US spelling, the QS canonical form) and 'cancelled' (UK spelling,
    seen in the wild) are accepted as terminal-cancel statuses by the poll loop, so both
    must take this fast path.
    """
    workspace, qs_mock = _make_snowflake_workspace_with_mocked_qs(job_id='job-cancel')
    # Override the polling status so the loop exits via the cancelled branch.
    qs_mock.get_job_status.return_value = {
        'status': terminal_status,
        'statements': [{'id': 'stmt-1'}],
    }

    result = await workspace.execute_query('SELECT SYSTEM$WAIT(300)')

    assert result.is_error
    assert result.data is None
    assert result.message == 'Query was cancelled'
    # The fast path must skip the results fetch entirely; no need to ask QS for rows
    # we know don't exist.
    qs_mock.get_job_results.assert_not_called()


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('input_branch_id', 'has_sb_feature', 'workspace_schema', 'expected_bound_branch_id'),
    [
        # default branch: always production, regardless of feature
        (None, True, None, None),
        (None, False, None, None),
        # dev branch + storage-branches feature on: keep dev branch
        ('456', True, None, '456'),
        # dev branch without storage-branches (legacy): fall back to production
        ('456', False, None, None),
        # dev branch + storage-branches + explicit workspace_schema (KBC_WORKSPACE_SCHEMA):
        # stay branch-aware. The user is responsible for ensuring the named workspace
        # exists in the explicitly-bound branch — there is no carve-out for explicit schemas.
        ('456', True, 'WORKSPACE_XYZ', '456'),
        # dev branch + legacy + explicit workspace_schema: still rebinds to production,
        # since branched workspaces don't exist on legacy projects.
        ('456', False, 'WORKSPACE_XYZ', None),
    ],
    ids=[
        'default_branch_with_sb',
        'default_branch_without_sb',
        'dev_branch_with_sb',
        'dev_branch_legacy',
        'dev_branch_with_sb_explicit_schema',
        'dev_branch_legacy_explicit_schema',
    ],
)
async def test_workspace_manager_create_is_branch_aware(
    input_branch_id: str | None,
    has_sb_feature: bool,
    workspace_schema: str | None,
    expected_bound_branch_id: str | None,
):
    """
    WorkspaceManager.create() must keep the client on the dev branch only when the project
    has the `storage-branches` feature; otherwise it must rebind to the production branch.
    The rule applies uniformly whether the workspace is auto-managed or pinned via an
    explicit `workspace_schema` (KBC_WORKSPACE_SCHEMA) — branch context is governed solely
    by KBC_BRANCH_ID and the project's `storage-branches` feature.
    """
    input_client = Mock(spec=KeboolaClient)
    input_client.branch_id = input_branch_id
    input_client.has_feature = AsyncMock(return_value=has_sb_feature)

    # Mirror the real `with_branch_id` semantics: same branch → return self;
    # different branch → return a fresh client bound to the requested branch.
    def _rebind(target_branch_id: str | None) -> Mock:
        if target_branch_id == input_client.branch_id:
            return input_client
        rebound = Mock(spec=KeboolaClient)
        rebound.branch_id = target_branch_id
        return rebound

    input_client.with_branch_id = AsyncMock(side_effect=_rebind)

    manager = await WorkspaceManager.create(input_client, workspace_schema=workspace_schema)

    # noinspection PyProtectedMember
    bound_client = manager._client
    assert bound_client.branch_id == expected_bound_branch_id
    # noinspection PyProtectedMember
    assert manager._workspace_schema == workspace_schema

    # has_feature is only meaningful when the client is on a dev branch — the helper
    # short-circuits otherwise, so on the default branch we should not even ask.
    if input_branch_id is None:
        input_client.has_feature.assert_not_called()
    else:
        input_client.has_feature.assert_awaited_once()


def _make_snowflake_workspace_with_mocked_qs(job_id: str = 'job-abc-123') -> tuple[_SnowflakeWorkspace, AsyncMock]:
    """Builds a _SnowflakeWorkspace whose QueryServiceClient is fully mocked to run a one-row query end to end.

    Returns (workspace, qs_mock) so tests can assert on the mock and access build_cancel_url's return value.
    """
    qs_mock = AsyncMock(spec=QueryServiceClient)
    qs_mock.submit_job.return_value = job_id
    qs_mock.get_job_status.return_value = {
        'status': 'completed',
        'statements': [{'id': 'stmt-1'}],
    }
    # `data` (not `rows`) matches the QS response shape that
    # `_SnowflakeWorkspace.execute_query()` reads via `results.get('data', [])`.
    # Keeping the mock aligned with production prevents regressions where the
    # results-fetch path silently misses a renamed/missing field.
    qs_mock.get_job_results.return_value = {
        'status': 'completed',
        'message': 'ok',
        'numberOfRows': 1,
        'columns': [{'name': 'col'}],
        'data': [['v']],
    }
    qs_mock.build_cancel_url = Mock(return_value=f'https://query.keboola.com/api/v1/queries/{job_id}/cancel')

    workspace = _SnowflakeWorkspace(workspace_id=1, schema='S', client=Mock(spec=KeboolaClient))
    workspace._qsclient = qs_mock
    return workspace, qs_mock


@pytest.mark.asyncio
async def test_execute_query_invokes_on_job_submitted_with_full_info():
    """The callback fires exactly once, immediately after submit_job, carrying the cancel URL."""
    workspace, qs_mock = _make_snowflake_workspace_with_mocked_qs(job_id='job-xyz')

    received: list[JobSubmittedInfo] = []

    async def callback(info: JobSubmittedInfo) -> None:
        received.append(info)

    await workspace.execute_query('SELECT 1', on_job_submitted=callback)

    assert len(received) == 1
    assert received[0] == JobSubmittedInfo(
        job_id='job-xyz',
        cancellation_url='https://query.keboola.com/api/v1/queries/job-xyz/cancel',
        backend='snowflake',
    )
    qs_mock.build_cancel_url.assert_called_once_with('job-xyz')


@pytest.mark.asyncio
async def test_execute_query_without_callback_does_not_call_build_cancel_url():
    """When no callback is supplied, the workspace must not waste a call to build_cancel_url."""
    workspace, qs_mock = _make_snowflake_workspace_with_mocked_qs()

    await workspace.execute_query('SELECT 1')

    qs_mock.build_cancel_url.assert_not_called()


@pytest.mark.asyncio
async def test_execute_query_swallows_callback_exception_and_completes_query():
    """A misbehaving callback (network failure when sending the notification, anything) must
    never abort the underlying query. The query still completes and returns its result."""
    workspace, qs_mock = _make_snowflake_workspace_with_mocked_qs()

    async def boom(info: JobSubmittedInfo) -> None:
        raise RuntimeError('progress send failed')

    result = await workspace.execute_query('SELECT 1', on_job_submitted=boom)

    # The query completed despite the callback error.
    assert result.is_ok
    qs_mock.get_job_results.assert_awaited()


@pytest.mark.asyncio
async def test_execute_query_cancels_backend_when_cancelled_during_callback():
    """If the in-flight task is cancelled while awaiting `on_job_submitted` (the job is already
    submitted by then), the backend job must still be cancelled rather than left running."""
    workspace, qs_mock = _make_snowflake_workspace_with_mocked_qs(job_id='job-cb-cancel')
    # Make QS report the job as cancelled so `_cancel_job_with_timeout` confirms immediately.
    qs_mock.get_job_status.return_value = {'status': 'cancelled', 'statements': [{'id': 'stmt-1'}]}

    async def cancel_during_callback(info: JobSubmittedInfo) -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await workspace.execute_query('SELECT 1', on_job_submitted=cancel_during_callback)

    # The backend cancel must have been issued for the already-submitted job.
    qs_mock.cancel_job.assert_awaited_once()
    assert qs_mock.cancel_job.await_args.args[0] == 'job-cb-cancel'
    # The cancellation short-circuits before any results fetch.
    qs_mock.get_job_results.assert_not_awaited()


def test_build_cancel_url_uses_raw_client_base_api_url():
    """build_cancel_url must produce an absolute URL clients can POST to without further assembly."""
    qs = QueryServiceClient.create(
        root_url='https://query.keboola.com',
        branch_id='42',
        token='Bearer t',
    )
    url = qs.build_cancel_url('job-1')
    assert url == 'https://query.keboola.com/api/v1/queries/job-1/cancel'


@pytest.mark.asyncio
async def test_workspace_manager_execute_query_forwards_callback():
    """WorkspaceManager.execute_query must plumb on_job_submitted through to the active workspace.

    Without this, the tool-layer notification path is silently disabled for any consumer that goes
    through the manager (which is everyone, in practice).
    """
    workspace, qs_mock = _make_snowflake_workspace_with_mocked_qs(job_id='job-fwd')

    manager = WorkspaceManager(Mock(spec=KeboolaClient))
    manager._workspace = workspace

    received: list[JobSubmittedInfo] = []

    async def callback(info: JobSubmittedInfo) -> None:
        received.append(info)

    await manager.execute_query('SELECT 1', on_job_submitted=callback)

    assert len(received) == 1
    assert received[0].job_id == 'job-fwd'


@pytest.mark.asyncio
async def test_workspace_manager_create_skips_feature_lookup_on_default_branch():
    """
    On the default branch `has_storage_branches` short-circuits before calling
    `has_feature`, so we should never trigger the underlying verify_token round trip.
    """
    input_client = Mock(spec=KeboolaClient)
    input_client.branch_id = None
    input_client.has_feature = AsyncMock(return_value=True)

    rebound_client = Mock(spec=KeboolaClient)
    rebound_client.branch_id = None
    input_client.with_branch_id = AsyncMock(return_value=rebound_client)

    await WorkspaceManager.create(input_client)

    input_client.has_feature.assert_not_called()


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
