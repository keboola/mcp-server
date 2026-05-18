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
