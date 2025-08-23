"""Tests for OAuth URL generation tools."""

from typing import Any, Mapping

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.oauth import create_oauth_url


@pytest.fixture
def mock_token_response() -> Mapping[str, Any]:
    """Mock valid response from the token creation endpoint."""
    return {
        'token': 'KBC_TOKEN_12345',
        'description': 'Short-lived token for OAuth URL - keboola.ex-google-analytics-v4/config-123',
        'expiresIn': 3600,
    }


@pytest.mark.asyncio
async def test_create_oauth_url_success(mcp_context_client: Context, mock_token_response: Mapping[str, Any]) -> None:
    """Test successful OAuth URL creation."""
    # Mock the storage client's token_create method to return the token response
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.token_create.return_value = mock_token_response
    keboola_client.storage_api_url = 'https://connection.test.keboola.com'

    component_id = 'keboola.ex-google-analytics-v4'
    config_id = 'config-123'

    result = await create_oauth_url(component_id=component_id, config_id=config_id, ctx=mcp_context_client)

    # Verify the storage client was called with correct parameters
    keboola_client.storage_client.token_create.assert_called_once_with(
        description=f'Short-lived token for OAuth URL - {component_id}/{config_id}',
        component_access=[component_id],
        expires_in=3600,
    )

    # Verify the response is the URL string
    assert isinstance(result, str)

    expected_url = (
        f'https://external.keboola.com/oauth/index.html'
        f'?token=KBC_TOKEN_12345'
        f'&sapiUrl=https%3A%2F%2Fconnection.test.keboola.com'
        f'#/{component_id}/{config_id}'
    )
    assert result == expected_url


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('component_id', 'config_id'),
    [
        ('keboola.ex-google-analytics-v4', 'my-config-123'),
        ('keboola.ex-gmail', 'gmail-config-456'),
        ('other.component', 'test-config'),
    ],
)
async def test_create_oauth_url_different_components(
    mcp_context_client: Context,
    mock_token_response: Mapping[str, Any],
    component_id: str,
    config_id: str,
) -> None:
    """Test OAuth URL creation for different components."""
    # Mock the storage client
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.token_create.return_value = mock_token_response

    result = await create_oauth_url(component_id=component_id, config_id=config_id, ctx=mcp_context_client)

    # Verify component-specific parameters were used
    assert isinstance(result, str)
    assert f'#/{component_id}/{config_id}' in result

    # Verify the API call included the correct component access
    call_args = keboola_client.storage_client.token_create.call_args
    assert call_args[1]['component_access'] == [component_id]
    assert component_id in call_args[1]['description']
    assert config_id in call_args[1]['description']


@pytest.mark.asyncio
async def test_create_oauth_url_token_creation_failure(
    mcp_context_client: Context,
) -> None:
    """Test OAuth URL creation when token creation fails."""
    # Mock the storage client to raise an exception
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.token_create.side_effect = Exception('Token creation failed')

    with pytest.raises(Exception, match='Token creation failed'):
        await create_oauth_url(
            component_id='keboola.ex-google-analytics-v4', config_id='config-123', ctx=mcp_context_client
        )


@pytest.mark.asyncio
async def test_create_oauth_url_missing_token_in_response(mcp_context_client: Context) -> None:
    """Test OAuth URL creation when token is missing from response."""
    # Mock response without token field
    invalid_response = {
        'description': 'Short-lived token for OAuth URL',
        'expiresIn': 3600,
    }
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.token_create.return_value = invalid_response

    with pytest.raises(KeyError):
        await create_oauth_url(
            component_id='keboola.ex-google-analytics-v4', config_id='config-123', ctx=mcp_context_client
        )
