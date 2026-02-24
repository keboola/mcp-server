from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import urlparse

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.workspace import _SnowflakeWorkspace


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
