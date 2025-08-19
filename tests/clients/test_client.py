import importlib.metadata
from typing import Any, Mapping
from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import httpx
import pytest

from keboola_mcp_server.clients.base import RawKeboolaClient
from keboola_mcp_server.clients.client import KeboolaClient


@pytest.fixture
def keboola_client() -> KeboolaClient:
    return KeboolaClient(storage_api_url='https://connection.nowhere', storage_api_token='test-token')


@pytest.fixture
def mock_http_request() -> httpx.Request:
    """Create a mock HTTP request."""
    request = Mock(spec=httpx.Request)
    request.url = 'https://api.example.com/test'
    request.method = 'GET'
    return request


@pytest.fixture
def mock_http_response_500(mock_http_request: httpx.Request) -> httpx.Response:
    """Create a mock HTTP response with 500 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 500
    response.reason_phrase = 'Internal Server Error'
    response.url = 'https://api.example.com/test'
    response.request = mock_http_request
    response.is_error = True
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        message=f"{response.reason_phrase} for url '{response.url}'", request=mock_http_request, response=response
    )
    return response


@pytest.fixture
def mock_http_response_404(mock_http_request: httpx.Request) -> httpx.Response:
    """Create a mock HTTP response with 404 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 404
    response.reason_phrase = 'Not Found'
    response.url = 'https://api.example.com/test'
    response.request = mock_http_request
    response.is_error = True
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        message=f"{response.reason_phrase} for url '{response.url}'", request=mock_http_request, response=response
    )
    return response


class TestRawKeboolaClient:
    """Test suite for enhanced HTTP client error handling."""

    @pytest.fixture
    def raw_client(self) -> RawKeboolaClient:
        """Create a RawKeboolaClient instance for testing."""
        return RawKeboolaClient(base_api_url='https://api.example.com', api_token='test-token')

    def test_raise_for_status_500_with_exception_id(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors are enhanced with exception ID when available."""

        # Mock response with valid JSON containing exception ID
        mock_http_response_500.json.return_value = {
            'exceptionId': 'exc-123-456',
            'message': 'Application error',
            'errorCode': 'DB_ERROR',
            'requestId': 'req-789',
        }

        match = (
            "Internal Server Error for url 'https://api.example.com/test'\n"
            'Exception ID: exc-123-456\n'
            'When contacting Keboola support please provide the exception ID.'
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_500_without_exception_id(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors without exception ID fall back gracefully."""

        # Mock response with JSON but no exception ID
        mock_http_response_500.json.return_value = {'message': 'Internal server error', 'errorCode': 'INTERNAL_ERROR'}

        with pytest.raises(httpx.HTTPStatusError, match="Internal Server Error for url 'https://api.example.com/test'"):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_500_with_malformed_json(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors with malformed JSON fall back to standard error handling."""

        # Mock response with invalid JSON
        type(mock_http_response_500).text = PropertyMock(return_value='Invalid JSON')
        mock_http_response_500.json.side_effect = ValueError('Invalid JSON')

        match = "Internal Server Error for url 'https://api.example.com/test'\n" 'API error: Invalid JSON'
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_404_uses_standard_exception(
        self, raw_client: RawKeboolaClient, mock_http_response_404: httpx.Response
    ):
        """Test that HTTP 404 errors use standard HTTPStatusError."""

        mock_http_response_404.json.return_value = {
            'exceptionId': 'exc-123-456',
            'error': 'The bucket "foo.bar.baz" was not found in the project "123"',
            'code': 'storage.buckets.notFound',
        }

        match = (
            "Not Found for url 'https://api.example.com/test'\n"
            'API error: The bucket "foo.bar.baz" was not found in the project "123"\n'
            'Exception ID: exc-123-456\n'
            'When contacting Keboola support please provide the exception ID.'
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_404)

    @pytest.mark.asyncio
    async def test_get_method_integration_with_enhanced_error_handling(
        self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that GET method integrates with enhanced error handling."""

        # Mock the HTTP client to return a 500 error
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.get.return_value = mock_http_response_500
            mock_http_response_500.json.return_value = {'exceptionId': 'test-exc-123', 'message': 'Test error message'}

            match = (
                "Internal Server Error for url 'https://api.example.com/test'\n"
                'Exception ID: test-exc-123\n'
                'When contacting Keboola support please provide the exception ID.'
            )
            with pytest.raises(httpx.HTTPStatusError, match=match):
                await raw_client.get('test-endpoint')


class TestAsyncStorageClient:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('message', 'component_id', 'configuration_id', 'event_type', 'params', 'results', 'duration', 'run_id'),
        [
            ('foo', 'bar', None, None, None, None, None, None),
            ('foo', 'bar', 'baz', 'error', {'param1': 'value1'}, {'result1': 'value1'}, 123, '987654321'),
        ],
    )
    async def test_trigger_event(
        self,
        message: str,
        component_id: str,
        configuration_id: str | None,
        event_type: str | None,
        params: Mapping[str, Any] | None,
        results: Mapping[str, Any],
        duration: int | None,
        run_id: str | None,
        keboola_client: KeboolaClient,
    ):
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.post.return_value = (response := Mock(spec=httpx.Response))
            response.status_code = 200
            response.json.return_value = {'id': '13008826', 'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3'}

            result = await keboola_client.storage_client.trigger_event(
                message=message,
                component_id=component_id,
                configuration_id=configuration_id,
                event_type=event_type,
                params=params,
                results=results,
                duration=duration,
                run_id=run_id,
            )

            assert result == {'id': '13008826', 'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3'}
            version = importlib.metadata.version('keboola-mcp-server')
            mock_client.post.assert_called_once_with(
                'https://connection.nowhere/v2/storage/events',
                params=None,
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Encoding': 'gzip',
                    'X-StorageAPI-Token': 'test-token',
                    'User-Agent': f'Keboola MCP Server/{version} app_env=local',
                },
                json={
                    key: value
                    for key, value in [
                        ('message', message),
                        ('component', component_id),
                        ('configurationId', configuration_id),
                        ('type', event_type),
                        ('params', params),
                        ('results', results),
                        ('duration', duration),
                        ('runId', run_id),
                    ]
                    if value
                },
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('description', 'component_access', 'expires_in', 'expected_data'),
        [
            # Basic token creation with just description
            ('Test token', None, None, {'description': 'Test token'}),
            # Token with component access
            (
                'OAuth token',
                ['keboola.ex-google-analytics-v4'],
                None,
                {'description': 'OAuth token', 'componentAccess': ['keboola.ex-google-analytics-v4']},
            ),
            # Token with expiration
            ('Short-lived token', None, 3600, {'description': 'Short-lived token', 'expiresIn': 3600}),
            # Token with all parameters
            (
                'Full token',
                ['keboola.ex-gmail', 'keboola.ex-google-analytics-v4'],
                7200,
                {
                    'description': 'Full token',
                    'componentAccess': ['keboola.ex-gmail', 'keboola.ex-google-analytics-v4'],
                    'expiresIn': 7200,
                },
            ),
        ],
    )
    async def test_token_create(
        self,
        description: str,
        component_access: list[str] | None,
        expires_in: int | None,
        expected_data: dict[str, Any],
        keboola_client: KeboolaClient,
    ):
        """Test token creation with various parameter combinations."""
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client_class.return_value.__aenter__.return_value = (mock_client := AsyncMock())
            mock_client.post.return_value = (response := Mock(spec=httpx.Response))
            response.status_code = 201
            response.json.return_value = {
                'id': '12345',
                'token': 'KBC_TOKEN_TEST_12345',
                'description': description,
                'created': '2023-01-01T00:00:00+00:00',
                'expiresIn': expires_in,
                'componentAccess': component_access or [],
            }

            result = await keboola_client.storage_client.token_create(
                description=description, component_access=component_access, expires_in=expires_in
            )

            # Verify the response
            assert result['token'] == 'KBC_TOKEN_TEST_12345'
            assert result['description'] == description

            # Verify the API call was made with correct parameters
            version = importlib.metadata.version('keboola-mcp-server')
            mock_client.post.assert_called_once_with(
                'https://connection.nowhere/v2/storage/tokens',
                params=None,
                headers={
                    'Content-Type': 'application/json',
                    'Accept-Encoding': 'gzip',
                    'X-StorageAPI-Token': 'test-token',
                    'User-Agent': f'Keboola MCP Server/{version} app_env=local',
                },
                json=expected_data,
            )
