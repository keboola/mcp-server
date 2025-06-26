from unittest.mock import AsyncMock, Mock, PropertyMock, patch

import httpx
import pytest

from keboola_mcp_server.client import RawKeboolaClient


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
        message=f"{response.reason_phrase} for url '{response.url}'",
        request=mock_http_request,
        response=response
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
        message=f"{response.reason_phrase} for url '{response.url}'",
        request=mock_http_request,
        response=response
    )
    return response


class TestRawKeboolaClient:
    """Test suite for enhanced HTTP client error handling."""

    @pytest.fixture
    def raw_client(self) -> RawKeboolaClient:
        """Create a RawKeboolaClient instance for testing."""
        return RawKeboolaClient(
            base_api_url='https://api.example.com',
            api_token='test-token'
        )

    def test_raise_for_status_500_with_exception_id(
            self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors are enhanced with exception ID when available."""

        # Mock response with valid JSON containing exception ID
        mock_http_response_500.json.return_value = {
            'exceptionId': 'exc-123-456',
            'message': 'Application error',
            'errorCode': 'DB_ERROR',
            'requestId': 'req-789'
        }

        match = ("Internal Server Error for url 'https://api.example.com/test'\n"
                 'Exception ID: exc-123-456\n'
                 'When contacting Keboola support please provide the exception ID.')
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_500_without_exception_id(
            self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors without exception ID fall back gracefully."""

        # Mock response with JSON but no exception ID
        mock_http_response_500.json.return_value = {
            'message': 'Internal server error',
            'errorCode': 'INTERNAL_ERROR'
        }

        with pytest.raises(httpx.HTTPStatusError, match="Internal Server Error for url 'https://api.example.com/test'"):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_500_with_malformed_json(
            self, raw_client: RawKeboolaClient, mock_http_response_500: httpx.Response
    ):
        """Test that HTTP 500 errors with malformed JSON fall back to standard error handling."""

        # Mock response with invalid JSON
        type(mock_http_response_500).text = PropertyMock(return_value='Invalid JSON')
        mock_http_response_500.json.side_effect = ValueError('Invalid JSON')

        match = ("Internal Server Error for url 'https://api.example.com/test'\n"
                 'API error: Invalid JSON')
        with pytest.raises(httpx.HTTPStatusError, match=match):
            raw_client._raise_for_status(mock_http_response_500)

    def test_raise_for_status_404_uses_standard_exception(
            self, raw_client: RawKeboolaClient, mock_http_response_404: httpx.Response
    ):
        """Test that HTTP 404 errors use standard HTTPStatusError."""

        mock_http_response_404.json.return_value = {
            'exceptionId': 'exc-123-456',
            'error': 'The bucket "foo.bar.baz" was not found in the project "123"',
            'code': 'storage.buckets.notFound'
        }

        match = ("Not Found for url 'https://api.example.com/test'\n"
                 'API error: The bucket "foo.bar.baz" was not found in the project "123"\n'
                 'Exception ID: exc-123-456\n'
                 'When contacting Keboola support please provide the exception ID.')
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
            mock_http_response_500.json.return_value = {
                'exceptionId': 'test-exc-123',
                'message': 'Test error message'
            }

            match = ("Internal Server Error for url 'https://api.example.com/test'\n"
                     'Exception ID: test-exc-123\n'
                     'When contacting Keboola support please provide the exception ID.')
            with pytest.raises(httpx.HTTPStatusError, match=match):
                await raw_client.get('test-endpoint')
