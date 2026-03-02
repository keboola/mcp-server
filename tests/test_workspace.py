from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlparse

import pytest
from httpx import HTTPStatusError, Request, Response

from keboola_mcp_server.clients.base import RawKeboolaClient
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.clients.storage import AsyncStorageClient
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
    """Test that configuration is cleaned up when workspace creation fails."""
    # Create mock raw client
    mock_raw_client = Mock(spec=RawKeboolaClient)

    # Create real instance with mocked raw client
    storage_client = AsyncStorageClient(raw_client=mock_raw_client, branch_id='default')

    # Mock configuration_create to return valid response
    storage_client.configuration_create = AsyncMock(return_value={'id': 'test-config-123', 'name': 'test'})

    # Mock post() to fail (simulating workspace creation failure)
    mock_response = Mock(spec=Response)
    mock_response.status_code = 500
    mock_response.text = 'Workspace creation failed'
    mock_request = Mock(spec=Request)
    mock_request.url = 'https://connection.keboola.com/v2/storage'
    storage_client.post = AsyncMock(
        side_effect=HTTPStatusError('Workspace creation failed', request=mock_request, response=mock_response)
    )

    # Mock cleanup method
    storage_client.configuration_delete = AsyncMock()

    # Attempt workspace creation (should fail and trigger cleanup)
    with pytest.raises(HTTPStatusError):
        await storage_client.workspace_create_for_config(
            component_id='keboola.mcp-server-tool',
            login_type='snowflake-person-sso',
            backend='snowflake',
            async_run=True,
            read_only_storage_access=True,
        )

    # Verify config creation was called
    storage_client.configuration_create.assert_called_once()

    # Verify cleanup was attempted
    storage_client.configuration_delete.assert_called_once_with('keboola.mcp-server-tool', 'test-config-123')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('metadata_value', 'should_skip'),
    [
        (None, True),  # None value should be skipped
        ('', True),  # Empty string should be skipped
        ('999', False),  # Valid integer as string should work
        (999, False),  # Valid integer should work
    ],
    ids=['none_value', 'empty_string', 'valid_string', 'valid_int'],
)
async def test_find_ws_in_branch_handles_invalid_metadata(metadata_value: str | int | None, should_skip: bool):
    """Test that invalid workspace_id values in metadata are handled gracefully."""
    # Create mock KeboolaClient
    mock_client = Mock(spec=KeboolaClient)
    mock_client.branch_id = None

    # Mock storage client with metadata containing test value
    mock_storage_client = AsyncMock()
    mock_storage_client.branch_metadata_get.return_value = [
        {'key': WorkspaceManager.MCP_META_KEY, 'value': metadata_value}
    ]

    # Mock workspace_detail to return valid workspace if called
    mock_storage_client.workspace_detail.return_value = {
        'id': 999,
        'connection': {'schema': 'test_schema', 'backend': 'snowflake'},
        'readOnlyStorageAccess': True,
    }

    mock_client.storage_client = mock_storage_client

    # Create workspace manager
    manager = WorkspaceManager(mock_client)

    # Call _find_ws_in_branch
    result = await manager._find_ws_in_branch()

    if should_skip:
        # Invalid values should be skipped, workspace_detail should not be called
        assert result is None
        mock_storage_client.workspace_detail.assert_not_called()
    else:
        # Valid values should result in workspace lookup
        assert result is not None
        mock_storage_client.workspace_detail.assert_called_once()


@pytest.mark.asyncio
async def test_workspace_create_for_config_validates_config_id():
    """Test that missing configuration ID raises ValueError."""
    # Create mock raw client
    mock_raw_client = Mock(spec=RawKeboolaClient)

    # Create real instance with mocked raw client
    storage_client = AsyncStorageClient(raw_client=mock_raw_client, branch_id='default')

    # Mock configuration_create to return response WITHOUT 'id' field
    storage_client.configuration_create = AsyncMock(
        return_value={'name': 'test', 'component': 'keboola.mcp-server-tool'}
    )

    # Attempt workspace creation (should fail with ValueError)
    with pytest.raises(ValueError, match="Configuration creation response missing 'id'"):
        await storage_client.workspace_create_for_config(
            component_id='keboola.mcp-server-tool',
            login_type='snowflake-person-sso',
            backend='snowflake',
            async_run=True,
            read_only_storage_access=True,
        )
