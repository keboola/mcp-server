import asyncio
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlparse

import pytest
from httpx import HTTPStatusError, Request, Response

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.workspace import JobSubmittedInfo, WorkspaceManager, _SnowflakeWorkspace, _WspInfo


@pytest.mark.parametrize(
    ('elapsed_seconds', 'expected_interval'),
    [(0.0, 1.0), (9.99, 1.0), (10.0, 2.0), (29.99, 2.0), (30.0, 5.0), (119.99, 5.0), (120.0, 20.0), (600.0, 20.0)],
)
def test_next_poll_interval_backs_off_over_time(elapsed_seconds: float, expected_interval: float) -> None:
    """Job-status polling interval must back off as the query keeps running."""
    assert _SnowflakeWorkspace._next_poll_interval(elapsed_seconds) == expected_interval


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
    mock_client.legacy_storage_token = storage_token
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
    mock_client.legacy_storage_token = 'sapi_token_456'
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
    mock_client.writable_storage_client = mock_storage_client

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
async def test_create_ws_does_not_use_prod_branch_fallback() -> None:
    """`_create_ws` provisions through `self._client`, so the workspace is by construction in
    this manager's own branch -- it must fetch the just-created id directly rather than through
    `_find_ws_by_id`, whose production-branch fallback exists for caller-supplied ids, not ones
    this manager just created itself (AI-3669 review, workspace.py:838 thread). A transient 404
    on the fresh id here must surface as "creation failed", not silently resolve to a different
    workspace with the same id on the production branch."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = '456'
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client
    mock_client.writable_storage_client = mock_storage_client
    mock_storage_client.verify_token.return_value = {'owner': {'defaultBackend': 'snowflake'}}
    mock_storage_client.configuration_create.return_value = {'id': 'test-config-123', 'name': 'test'}
    mock_storage_client.workspace_create_for_config.return_value = {'id': 42}
    mock_storage_client.job_detail.return_value = {'status': 'success', 'results': {'id': 42}}

    mock_response = Mock(spec=Response)
    mock_response.status_code = 404
    mock_request = Mock(spec=Request)
    mock_storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('not found', request=mock_request, response=mock_response)
    )
    mock_client.with_branch_id = AsyncMock()

    manager = WorkspaceManager(mock_client)

    result = await manager._create_ws()

    assert result is None
    mock_client.with_branch_id.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'job_detail',
    [
        {'status': 'error'},
        {'status': 'terminated'},
        # 'warning' with no workspace id is still a failure (nothing usable was produced).
        {'status': 'warning'},
        {'status': 'warning', 'results': {}},
    ],
    ids=['error', 'terminated', 'warning_no_results', 'warning_no_id'],
)
async def test_workspace_creation_stops_on_terminal_error_status(job_detail: dict):
    """A job that reaches a terminal failure status must stop polling at once, not spin to timeout."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client
    mock_client.writable_storage_client = mock_storage_client

    mock_storage_client.verify_token.return_value = {'owner': {'defaultBackend': 'snowflake'}}
    mock_storage_client.configuration_create.return_value = {'id': 'cfg-1', 'name': 'test'}
    mock_storage_client.workspace_create_for_config.return_value = {'id': 999}
    mock_storage_client.job_detail.return_value = job_detail

    manager = WorkspaceManager(mock_client)
    result = await manager._create_ws()

    assert result is None
    # Polled exactly once — the terminal status short-circuits the loop.
    mock_storage_client.job_detail.assert_awaited_once()


@pytest.mark.asyncio
async def test_workspace_creation_warning_with_id_uses_workspace(mocker):
    """A 'warning' job that still created a workspace (results.id present) must not be discarded."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client
    mock_client.writable_storage_client = mock_storage_client

    mock_storage_client.verify_token.return_value = {'owner': {'defaultBackend': 'snowflake'}}
    mock_storage_client.configuration_create.return_value = {'id': 'cfg-1', 'name': 'test'}
    mock_storage_client.workspace_create_for_config.return_value = {'id': 999}
    mock_storage_client.job_detail.return_value = {'status': 'warning', 'results': {'id': 999}}

    manager = WorkspaceManager(mock_client)
    sentinel = object()
    mocker.patch.object(manager, '_find_ws_by_id', AsyncMock(return_value=sentinel))
    result = await manager._create_ws()

    assert result is sentinel  # the created workspace is used despite the warning
    manager._find_ws_by_id.assert_awaited_once_with(999)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('input_branch_id', 'has_sb_feature', 'workspace_schema', 'workspace_id', 'expected_bound_branch_id'),
    [
        # default branch: always production, regardless of feature
        (None, True, None, None, None),
        (None, False, None, None, None),
        # dev branch + storage-branches feature on: keep dev branch
        ('456', True, None, None, '456'),
        # dev branch without storage-branches (legacy): fall back to production
        ('456', False, None, None, None),
        # dev branch + storage-branches + explicit workspace_schema (KBC_WORKSPACE_SCHEMA):
        # stay branch-aware. The user is responsible for ensuring the named workspace
        # exists in the explicitly-bound branch — there is no carve-out for explicit schemas.
        ('456', True, 'WORKSPACE_XYZ', None, '456'),
        # dev branch + legacy + explicit workspace_schema: still rebinds to production,
        # since branched workspaces don't exist on legacy projects.
        ('456', False, 'WORKSPACE_XYZ', None, None),
        # dev branch + storage-branches + explicit workspace_id: the pin must reach `cls(...)`
        # on the storage-branches construction path too (mutation-tested: dropping
        # `workspace_id=workspace_id` there leaves the rest of the suite green).
        ('456', True, None, '123', '456'),
        # default branch + explicit workspace_id: same, on the prod-client construction path.
        (None, False, None, '123', None),
    ],
    ids=[
        'default_branch_with_sb',
        'default_branch_without_sb',
        'dev_branch_with_sb',
        'dev_branch_legacy',
        'dev_branch_with_sb_explicit_schema',
        'dev_branch_legacy_explicit_schema',
        'dev_branch_with_sb_explicit_id',
        'default_branch_without_sb_explicit_id',
    ],
)
async def test_workspace_manager_create_is_branch_aware(
    input_branch_id: str | None,
    has_sb_feature: bool,
    workspace_schema: str | None,
    workspace_id: str | None,
    expected_bound_branch_id: str | None,
):
    """
    WorkspaceManager.create() must keep the client on the dev branch only when the project
    has the `storage-branches` feature; otherwise it must rebind to the production branch.
    The rule applies uniformly whether the workspace is auto-managed or pinned via an
    explicit `workspace_schema` (KBC_WORKSPACE_SCHEMA) or `workspace_id` (KBC_WORKSPACE_ID) —
    branch context is governed solely by KBC_BRANCH_ID and the project's `storage-branches`
    feature, and both pins must reach `cls(...)` on either construction path.
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

    manager = await WorkspaceManager.create(input_client, workspace_schema=workspace_schema, workspace_id=workspace_id)

    # noinspection PyProtectedMember
    bound_client = manager._client
    assert bound_client.branch_id == expected_bound_branch_id
    # noinspection PyProtectedMember
    assert manager._workspace_schema == workspace_schema
    # noinspection PyProtectedMember
    assert manager._workspace_id == workspace_id

    # has_feature is only meaningful when the client is on a dev branch — the helper
    # short-circuits otherwise, so on the default branch we should not even ask.
    if input_branch_id is None:
        input_client.has_feature.assert_not_called()
    else:
        input_client.has_feature.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_workspace_resolves_by_id_when_set():
    """An explicit `workspace_id` (e.g. a Data App's own workspace) must be looked up by ID
    and take precedence over `workspace_schema`, instead of falling back to the default
    per-branch MCP-managed workspace."""
    mock_client = Mock(spec=KeboolaClient)
    manager = WorkspaceManager(mock_client, workspace_schema='SOME_SCHEMA', workspace_id='123')

    ws_info = _WspInfo(id=123, schema='APP_SCHEMA', backend='snowflake', credentials=None, readonly=True)
    manager._find_ws_by_id = AsyncMock(return_value=(ws_info, mock_client))  # type: ignore[method-assign]
    manager._find_ws_by_schema = AsyncMock()  # type: ignore[method-assign]

    workspace = await manager._get_workspace()

    assert workspace.id == 123
    manager._find_ws_by_id.assert_awaited_once_with('123')
    manager._find_ws_by_schema.assert_not_called()


@pytest.mark.asyncio
async def test_get_workspace_raises_when_id_not_found():
    """A `workspace_id` that resolves to no workspace must fail loudly rather than silently
    falling back to the default workspace — the caller asked for a specific workspace."""
    mock_client = Mock(spec=KeboolaClient)
    manager = WorkspaceManager(mock_client, workspace_id='999')
    manager._find_ws_by_id = AsyncMock(return_value=None)  # type: ignore[method-assign]

    with pytest.raises(ValueError, match='workspace_id=999'):
        await manager._get_workspace()


@pytest.mark.asyncio
async def test_get_workspace_warns_when_pinned_workspace_is_not_readonly(caplog: pytest.LogCaptureFixture) -> None:
    """A `workspace_id` pin resolving to a writable workspace must not silently pass through --
    at minimum it needs a warning (whether a Data App's platform-provisioned workspace is
    actually read-only is unconfirmed against a real stack; hard-enforcing here without knowing
    that could break the feature outright -- see AI-3669 review, workspace.py:834 thread)."""
    mock_client = Mock(spec=KeboolaClient)
    manager = WorkspaceManager(mock_client, workspace_id='123')
    writable_info = _WspInfo(id=123, schema='APP_SCHEMA', backend='snowflake', credentials=None, readonly=False)
    manager._find_ws_by_id = AsyncMock(return_value=(writable_info, mock_client))  # type: ignore[method-assign]

    with caplog.at_level('WARNING'):
        workspace = await manager._get_workspace()

    assert workspace.id == 123
    assert any('no read-only storage access' in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_get_data_app_workspace_id_ignores_the_pin():
    """`get_data_app_workspace_id()`/`get_data_app_branch_id()`/`get_data_app_sql_dialect()` must
    resolve the *managed* workspace even when a session is pinned via `workspace_id` -- they feed
    into `tools/data_apps.py`, which writes the result into a *different* data app's own
    persisted `WORKSPACE_ID` secret and bakes the dialect into that same app's generated source
    code. If they returned the pinned workspace, creating/updating data app B from a session
    pinned to app A's workspace would permanently point B at A's workspace/dialect."""
    mock_client = Mock(spec=KeboolaClient)
    manager = WorkspaceManager(mock_client, workspace_id='999')

    pinned_info = _WspInfo(
        id=999, schema='PINNED_SCHEMA', backend='bigquery', credentials='{"project_id": "proj"}', readonly=True
    )
    managed_info = _WspInfo(id=111, schema='MANAGED_SCHEMA', backend='snowflake', credentials=None, readonly=True)
    manager._find_ws_by_id = AsyncMock(return_value=(pinned_info, mock_client))  # type: ignore[method-assign]
    manager._find_ws_in_branch = AsyncMock(return_value=managed_info)  # type: ignore[method-assign]

    assert await manager.get_data_app_workspace_id() == 111
    assert await manager.get_data_app_sql_dialect() == 'Snowflake'
    pinned_workspace = await manager._get_workspace()
    assert pinned_workspace.id == 999


@pytest.mark.asyncio
async def test_get_managed_workspace_raises_instead_of_provisioning_when_pinned():
    """A session pinned via `workspace_id` (e.g. a Data App session) must not provision a brand
    new MCP-managed workspace on demand when none exists yet -- that workspace would be billed
    to this session's token, not whoever actually needs it, and provisioning can outright fail on
    a read-only token. It should fail loudly instead of silently creating one."""
    mock_client = Mock(spec=KeboolaClient)
    manager = WorkspaceManager(mock_client, workspace_id='999')
    manager._find_ws_in_branch = AsyncMock(return_value=None)  # type: ignore[method-assign]
    manager._create_ws = AsyncMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError, match='workspace_id=999'):
        await manager._get_managed_workspace()
    manager._create_ws.assert_not_awaited()


@pytest.mark.asyncio
async def test_found_workspace_repr_never_prints_credentials() -> None:
    """`_WspInfo.credentials` holds the backend's credential blob (a service-account JSON for
    BigQuery) -- its repr, which every `LOG.info(f'... {info}')` call site relies on, must
    never include it."""
    secret = 'super-secret-service-account-json'
    info = _WspInfo(id=123, schema='APP_SCHEMA', backend='snowflake', credentials=secret, readonly=True)

    assert secret not in repr(info)
    assert 'credentials=****' in repr(info)


@pytest.mark.asyncio
async def test_find_ws_by_id_falls_back_to_production_branch() -> None:
    """A Data App workspace is not tied to any particular branch, but `workspace_detail` is
    branch-scoped -- a dev-branch session pinned to a workspace that lives on the default
    branch must not 404 just because the first lookup used the wrong branch prefix."""
    dev_client = Mock(spec=KeboolaClient)
    dev_client.branch_id = '456'
    dev_client.storage_client = AsyncMock()
    mock_response = Mock(spec=Response)
    mock_response.status_code = 404
    mock_request = Mock(spec=Request)
    dev_client.storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('not found', request=mock_request, response=mock_response)
    )

    prod_client = Mock(spec=KeboolaClient)
    prod_client.branch_id = None
    prod_client.storage_client = AsyncMock()
    prod_client.storage_client.workspace_detail = AsyncMock(
        return_value={
            'id': 123,
            'connection': {'backend': 'snowflake', 'schema': 'APP_SCHEMA', 'user': None},
            'readOnlyStorageAccess': True,
        }
    )
    dev_client.with_branch_id = AsyncMock(return_value=prod_client)

    manager = WorkspaceManager(dev_client, workspace_id='123')

    result = await manager._find_ws_by_id('123')

    assert result is not None
    info, resolved_client = result
    assert info.id == 123
    dev_client.storage_client.workspace_detail.assert_awaited_once_with('123')
    dev_client.with_branch_id.assert_awaited_once_with(None)
    prod_client.storage_client.workspace_detail.assert_awaited_once_with('123')
    # The workspace was only resolvable via the prod-branch client -- that client is returned for
    # use on this one workspace, but this manager's own client must NOT change: mutating
    # `manager._client` would also redirect every other lookup this manager makes (e.g. the
    # MCP-managed workspace) onto the wrong branch.
    assert resolved_client is prod_client
    assert manager._client is dev_client


@pytest.mark.asyncio
async def test_pin_resolution_via_prod_fallback_does_not_leak_into_managed_lookup() -> None:
    """Regression test for a real bug caught in review: resolving a `workspace_id` pin through
    the prod-branch fallback must not affect the *managed* workspace lookup afterwards -- that
    lookup (feeding `get_data_app_workspace_id()`/`get_data_app_branch_id()`/
    `get_data_app_sql_dialect()`, which get persisted into a Data App's own config) must keep
    running on the manager's original (dev-branch) client, not the prod client the pin happened
    to resolve on."""
    dev_client = Mock(spec=KeboolaClient)
    dev_client.branch_id = '456'
    dev_client.storage_client = AsyncMock()
    mock_response = Mock(spec=Response)
    mock_response.status_code = 404
    mock_request = Mock(spec=Request)
    dev_client.storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('not found', request=mock_request, response=mock_response)
    )

    prod_client = Mock(spec=KeboolaClient)
    prod_client.branch_id = None
    prod_client.storage_client = AsyncMock()
    prod_client.storage_client.workspace_detail = AsyncMock(
        return_value={
            'id': 123,
            'connection': {'backend': 'snowflake', 'schema': 'PINNED_SCHEMA', 'user': None},
            'readOnlyStorageAccess': True,
        }
    )
    dev_client.with_branch_id = AsyncMock(return_value=prod_client)

    manager = WorkspaceManager(dev_client, workspace_id='123')
    managed_info = _WspInfo(id=111, schema='MANAGED_SCHEMA', backend='snowflake', credentials=None, readonly=True)
    manager._find_ws_in_branch = AsyncMock(return_value=managed_info)  # type: ignore[method-assign]

    pinned_workspace = await manager._get_workspace()
    assert pinned_workspace.id == 123

    managed_workspace_id = await manager.get_data_app_workspace_id()

    assert managed_workspace_id == 111
    assert manager._client is dev_client


@pytest.mark.asyncio
async def test_get_workspace_warns_when_pin_resolves_via_prod_fallback(caplog: pytest.LogCaptureFixture) -> None:
    """When a `workspace_id` pin only resolves via the production-branch fallback, `self._client`
    stays on the dev branch for every other lookup (get_tables, get_bucket_detail, ...) while
    queries run against the prod-branch workspace -- nothing else makes that branch mismatch
    diagnosable, so it must at least be logged (AI-3669 review, workspace.py:908 thread)."""
    dev_client = Mock(spec=KeboolaClient)
    dev_client.branch_id = '456'
    dev_client.storage_client = AsyncMock()
    mock_response = Mock(spec=Response)
    mock_response.status_code = 404
    mock_request = Mock(spec=Request)
    dev_client.storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('not found', request=mock_request, response=mock_response)
    )

    prod_client = Mock(spec=KeboolaClient)
    prod_client.branch_id = None
    prod_client.storage_client = AsyncMock()
    prod_client.storage_client.workspace_detail = AsyncMock(
        return_value={
            'id': 123,
            'connection': {'backend': 'snowflake', 'schema': 'APP_SCHEMA', 'user': None},
            'readOnlyStorageAccess': True,
        }
    )
    dev_client.with_branch_id = AsyncMock(return_value=prod_client)

    manager = WorkspaceManager(dev_client, workspace_id='123')

    with caplog.at_level('WARNING'):
        workspace = await manager._get_workspace()

    assert workspace.id == 123
    assert any('production-branch fallback' in r.message for r in caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [400, 403, 404])
async def test_find_ws_by_id_treats_400_403_404_alike(status_code: int, caplog: pytest.LogCaptureFixture) -> None:
    """The header value is unvalidated: a non-numeric id (400), an id the token can't read
    (403), and a nonexistent id (404) must all mean the same thing -- "not usable" -- rather
    than 400/403 bypassing the intended `ValueError` and surfacing a raw `HTTPStatusError`. The
    status code is still logged, so a permissions problem (403) is distinguishable from a typo
    (404) in server logs even though both resolve to the same "not found" `ValueError` (AI-3669
    review, workspace.py:696 thread)."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_client.storage_client = AsyncMock()
    mock_response = Mock(spec=Response)
    mock_response.status_code = status_code
    mock_request = Mock(spec=Request)
    mock_client.storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('error', request=mock_request, response=mock_response)
    )

    manager = WorkspaceManager(mock_client, workspace_id='not-a-valid-id')

    with caplog.at_level('DEBUG'):
        assert await manager._find_ws_by_id('not-a-valid-id') is None
    assert any(str(status_code) in r.message for r in caplog.records)


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [400, 403])
async def test_fetch_ws_strict_reraises_400_403(status_code: int) -> None:
    """`strict=True` (used for the id of a workspace this server just created, e.g. in
    `_create_ws`) must only treat a 404 as "not found" -- a 400/403 on that known-good id is a
    real failure (e.g. the creating token can't read what it just provisioned) that must not be
    silently swallowed into a generic "workspace creation failed" error. `strict` only exists on
    `_fetch_ws` -- `_find_ws_by_id` (the pin-lookup caller) has no legitimate use for it, since a
    caller-supplied id is unvalidated by definition."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_client.storage_client = AsyncMock()
    mock_response = Mock(spec=Response)
    mock_response.status_code = status_code
    mock_request = Mock(spec=Request)
    mock_client.storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('error', request=mock_request, response=mock_response)
    )

    with pytest.raises(HTTPStatusError):
        await WorkspaceManager._fetch_ws(mock_client, '123', strict=True)


@pytest.mark.asyncio
async def test_fetch_ws_raises_without_leaking_credentials() -> None:
    """A workspace whose detail 200s but has an empty `backend`/`schema` (e.g. a non-SQL/sandbox
    workspace) raises `ValueError` -- since `workspace_id` is now caller-supplied via
    `X-Workspace-Id`, this path is reachable with untrusted input, so the raw Storage API payload
    (whose `connection.user` is the backend's credential blob, e.g. a BigQuery service-account
    JSON) must never end up in the error message that server logs / client responses / Storage
    events pick up."""
    secret = 'super-secret-service-account-json'
    mock_client = Mock(spec=KeboolaClient)
    mock_client.storage_client = AsyncMock()
    mock_client.storage_client.workspace_detail = AsyncMock(
        return_value={
            'id': 123,
            'connection': {'backend': '', 'schema': '', 'user': secret},
            'readOnlyStorageAccess': True,
        }
    )

    with pytest.raises(ValueError) as exc_info:
        await WorkspaceManager._fetch_ws(mock_client, '123')

    assert secret not in str(exc_info.value)
    assert 'credentials=****' in str(exc_info.value)


@pytest.mark.asyncio
async def test_find_ws_by_id_reraises_unexpected_status() -> None:
    """A 5xx (or any other unexpected status) is a real failure, not an absent/inaccessible
    workspace -- it must propagate rather than being swallowed into a misleading "not found"."""
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_client.storage_client = AsyncMock()
    mock_response = Mock(spec=Response)
    mock_response.status_code = 500
    mock_request = Mock(spec=Request)
    mock_client.storage_client.workspace_detail = AsyncMock(
        side_effect=HTTPStatusError('error', request=mock_request, response=mock_response)
    )

    manager = WorkspaceManager(mock_client, workspace_id='123')

    with pytest.raises(HTTPStatusError):
        await manager._find_ws_by_id('123')


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
    workspace, _qs_mock = _make_snowflake_workspace_with_mocked_qs(job_id='job-fwd')

    manager = WorkspaceManager(Mock(spec=KeboolaClient))
    manager._workspace = workspace

    received: list[JobSubmittedInfo] = []

    async def callback(info: JobSubmittedInfo) -> None:
        received.append(info)

    await manager.execute_query('SELECT 1', on_job_submitted=callback)

    assert len(received) == 1
    assert received[0].job_id == 'job-fwd'


@pytest.mark.asyncio
async def test_workspace_creation_uses_step_up_client(tmp_path):
    """
    When a Kubernetes SA token path is configured (deployed MCP server), workspace
    provisioning must route its writes through the step-up Storage client
    (KeboolaClient.step_up_storage_client) rather than the user's plain client, so
    Connection can waive the permissions a read-only token lacks. Header construction
    and read-only propagation are covered by the KeboolaClient.step_up_storage_client
    tests.
    """
    token_file = tmp_path / 'token'
    token_file.write_text('sa-jwt\n')

    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client
    mock_storage_client.verify_token.return_value = {'owner': {'id': 123, 'defaultBackend': 'snowflake'}}

    mock_writer = AsyncMock()
    mock_writer.configuration_create.return_value = {'id': 'test-config-123', 'name': 'test'}
    mock_writer.workspace_create_for_config.return_value = {'id': 42}
    mock_client.step_up_storage_client.return_value = mock_writer

    manager = WorkspaceManager(mock_client, kubernetes_token_path=str(token_file))
    # Stop the flow right after the provisioning calls we want to assert on.
    manager._fetch_ws = AsyncMock(return_value=None)  # type: ignore[method-assign]
    mock_storage_client.job_detail.return_value = {'status': 'success', 'results': {'id': 42}}

    await manager._create_ws()

    # The step-up client is built from the projected token path ...
    mock_client.step_up_storage_client.assert_called_once_with(str(token_file))
    # ... and all provisioning writes went through it, not the user's plain client.
    mock_writer.configuration_create.assert_awaited_once()
    mock_writer.workspace_create_for_config.assert_awaited_once()
    mock_storage_client.configuration_create.assert_not_called()
    mock_storage_client.workspace_create_for_config.assert_not_called()


@pytest.mark.asyncio
async def test_provisioning_client_falls_back_to_user_client():
    """Without a Kubernetes token path the provisioning client is the user's own client, but
    always writable (see `KeboolaClient.writable_storage_client`) -- provisioning is server-side
    plumbing, not a user-visible mutation, so it must succeed even under a read-only scope."""
    mock_client = Mock(spec=KeboolaClient)
    mock_writable_client = AsyncMock()
    mock_client.writable_storage_client = mock_writable_client

    manager = WorkspaceManager(mock_client)

    assert await manager._provisioning_storage_client() is mock_writable_client
    mock_client.step_up_storage_client.assert_not_called()


@pytest.mark.asyncio
async def test_provisioning_client_is_cached_per_manager(tmp_path):
    """The step-up provisioning client is built at most once per manager."""
    token_file = tmp_path / 'token'
    token_file.write_text('sa-jwt')

    mock_client = Mock(spec=KeboolaClient)
    mock_client.step_up_storage_client.return_value = AsyncMock()

    manager = WorkspaceManager(mock_client, kubernetes_token_path=str(token_file))

    first = await manager._provisioning_storage_client()
    second = await manager._provisioning_storage_client()

    assert first is second
    mock_client.step_up_storage_client.assert_called_once_with(str(token_file))


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


def _wsp(ws_id: int, readonly: bool) -> dict:
    """Minimal SAPI workspace item (shape per the config-scoped workspaces payload)."""
    return {
        'id': ws_id,
        'connection': {'backend': 'snowflake', 'schema': f'WORKSPACE_{ws_id}'},
        'readOnlyStorageAccess': readonly,
    }


@pytest.mark.asyncio
async def test_find_ws_in_branch_matches_mcp_component_readonly():
    """_find_ws_in_branch returns the read-only workspace found under the MCP component's
    configs, skipping non-read-only workspaces — no project-wide list, no branch-metadata read."""
    mock_client = Mock(spec=KeboolaClient)
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client
    mock_storage_client.configuration_list.return_value = [{'id': 'cfg-1'}, {'id': 'cfg-2'}]
    ws_by_config = {
        'cfg-1': [_wsp(2, False)],  # not read-only
        'cfg-2': [_wsp(3, True)],  # the match
    }

    async def _list_for_config(component_id: str, config_id: str) -> list[dict]:
        assert component_id == WorkspaceManager.MCP_WORKSPACE_COMPONENT_ID
        return ws_by_config[config_id]

    mock_storage_client.workspace_list_for_config.side_effect = _list_for_config

    manager = WorkspaceManager(mock_client)
    info = await manager._find_ws_in_branch()

    assert info is not None
    assert info.id == 3
    mock_storage_client.configuration_list.assert_awaited_once_with(WorkspaceManager.MCP_WORKSPACE_COMPONENT_ID)
    mock_storage_client.workspace_list.assert_not_called()
    mock_storage_client.branch_metadata_get.assert_not_called()


@pytest.mark.asyncio
async def test_find_ws_in_branch_returns_none_when_no_mcp_workspace():
    mock_client = Mock(spec=KeboolaClient)
    mock_storage_client = AsyncMock()
    mock_client.storage_client = mock_storage_client
    mock_storage_client.configuration_list.return_value = [{'id': 'cfg-1'}]
    mock_storage_client.workspace_list_for_config.return_value = [_wsp(2, False)]

    manager = WorkspaceManager(mock_client)
    assert await manager._find_ws_in_branch() is None
