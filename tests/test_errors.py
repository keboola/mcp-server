import logging
import pytest
from unittest.mock import AsyncMock, Mock, patch

import httpx

from keboola_mcp_server.client import RawKeboolaClient
from keboola_mcp_server.errors import ToolException, tool_errors


@pytest.fixture
def mock_http_request() -> httpx.Request:
    """Create a mock HTTP request."""
    request = Mock(spec=httpx.Request)
    request.url = "https://api.example.com/test"
    request.method = "GET"
    return request


@pytest.fixture
def mock_http_response_500() -> httpx.Response:
    """Create a mock HTTP response with 500 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 500
    response.reason_phrase = "Internal Server Error"
    response.url = "https://api.example.com/test"
    response.is_error = True
    return response


@pytest.fixture
def mock_http_response_404() -> httpx.Response:
    """Create a mock HTTP response with 404 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 404
    response.reason_phrase = "Not Found"
    response.url = "https://api.example.com/test"
    response.is_error = True
    return response


@pytest.fixture
def mock_http_response_502() -> httpx.Response:
    """Create a mock HTTP response with 502 status."""
    response = Mock(spec=httpx.Response)
    response.status_code = 502
    response.reason_phrase = "Bad Gateway"
    response.url = "https://api.example.com/test"
    response.is_error = True
    return response


# --- Test Cases for Enhanced HTTP Client Error Handling ---


class TestRawKeboolaClientErrorHandling:
    """Test suite for enhanced HTTP client error handling."""

    @pytest.fixture
    def raw_client(self) -> RawKeboolaClient:
        """Create a RawKeboolaClient instance for testing."""
        return RawKeboolaClient(
            base_api_url="https://api.example.com",
            api_token="test-token"
        )

    def test_raise_for_status_500_with_exception_id(self, raw_client, mock_http_response_500, mock_http_request):
        """Test that HTTP 500 errors are enhanced with exception ID when available."""
        
        # Mock response with valid JSON containing exception ID
        mock_http_response_500.json.return_value = {
            "exceptionId": "exc-123-456",
            "message": "Application error",
            "errorCode": "DB_ERROR",
            "requestId": "req-789"
        }
        mock_http_response_500.request = mock_http_request
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._raise_for_status(mock_http_response_500)
        
        exception = exc_info.value
        error_message = str(exception)
        # Should contain the exception ID in user-friendly format
        assert "Please contact support and provide this exception ID: exc-123-456" in error_message
        # Should contain the specific URL from the response
        assert "Server error for url 'https://api.example.com/test'" in error_message

    def test_raise_for_status_500_without_exception_id(self, raw_client, mock_http_response_500, mock_http_request):
        """Test that HTTP 500 errors without exception ID fall back gracefully."""
        
        # Mock response with JSON but no exception ID
        mock_http_response_500.json.return_value = {
            "message": "Internal server error",
            "errorCode": "INTERNAL_ERROR"
        }
        mock_http_response_500.request = mock_http_request
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._raise_for_status(mock_http_response_500)
        
        exception = exc_info.value
        error_message = str(exception)
        # Should not contain exception ID when not available
        assert "provide this exception ID:" not in error_message
        # Should contain the specific URL from the response
        assert "Server error for url 'https://api.example.com/test'" in error_message

    def test_raise_for_status_500_with_malformed_json(self, raw_client, mock_http_response_500, mock_http_request):
        """Test that HTTP 500 errors with malformed JSON fall back to standard error handling."""
        
        # Mock response with invalid JSON
        mock_http_response_500.json.side_effect = ValueError("Invalid JSON")
        mock_http_response_500.request = mock_http_request
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._raise_for_status(mock_http_response_500)
        
        exception = exc_info.value
        error_message = str(exception)
        # Should fall back to generic message when JSON parsing fails and include the specific URL
        assert error_message == "Server error for url 'https://api.example.com/test'"

    def test_raise_for_status_404_uses_standard_exception(self, raw_client, mock_http_response_404, mock_http_request):
        """Test that HTTP 404 errors use standard HTTPStatusError."""
        mock_http_response_404.request = mock_http_request
        
        # Configure the mock to raise HTTPStatusError when raise_for_status is called
        mock_http_response_404.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Client error '404 Not Found' for url 'https://api.example.com/test'",
            request=mock_http_request,
            response=mock_http_response_404
        )
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._raise_for_status(mock_http_response_404)
        
        # Should be standard HTTPStatusError message
        error_message = str(exc_info.value)
        assert "Exception ID:" not in error_message
        assert "404 Not Found" in error_message

    def test_raise_for_status_502_uses_standard_exception(self, raw_client, mock_http_response_502, mock_http_request):
        """Test that other HTTP 5xx errors (non-500) use standard HTTPStatusError."""
        mock_http_response_502.request = mock_http_request
        
        # Configure the mock to raise HTTPStatusError when raise_for_status is called
        mock_http_response_502.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server error '502 Bad Gateway' for url 'https://api.example.com/test'",
            request=mock_http_request,
            response=mock_http_response_502
        )
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._raise_for_status(mock_http_response_502)
        
        # Should be standard HTTPStatusError message
        error_message = str(exc_info.value)
        assert "Exception ID:" not in error_message
        assert "502 Bad Gateway" in error_message

    @pytest.mark.asyncio
    async def test_get_method_integration_with_enhanced_error_handling(self, raw_client):
        """Test that GET method integrates with enhanced error handling."""
        
        # Mock the HTTP client to return a 500 error
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client_class.return_value.__aenter__.return_value = mock_client
            
            # Mock response with 500 error and exception ID
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.reason_phrase = "Internal Server Error"
            mock_response.url = "https://api.example.com/test"
            mock_response.is_error = True
            mock_response.json.return_value = {
                "exceptionId": "test-exc-123",
                "message": "Test error message"
            }
            mock_response.request = Mock()
            
            mock_client.get.return_value = mock_response
            
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                await raw_client.get("test-endpoint")
            
            exception = exc_info.value
            error_message = str(exception)
            # Should contain the exception ID in user-friendly format
            assert "Please contact support and provide this exception ID: test-exc-123" in error_message
            # Should contain the specific URL from the response
            assert "Server error for url 'https://api.example.com/test'" in error_message


# --- Test Cases for Enhanced Tool Error Decorator ---


class TestEnhancedToolErrorDecorator:
    """Test suite for enhanced tool_errors decorator handling HTTP 500 errors with exception IDs."""

    @pytest.fixture
    def function_that_raises_http_500_with_exception_id(self, mock_http_response_500, mock_http_request):
        """A function that raises HTTPStatusError with HTTP 500 and exception ID."""
        async def func():
            error_message = "Server error '500 Internal Server Error' for url 'https://api.example.com/test' (Exception ID: test-exc-500-123) - Database connection timeout"
            raise httpx.HTTPStatusError(error_message, request=mock_http_request, response=mock_http_response_500)
        return func

    @pytest.fixture
    def function_that_raises_http_500_without_exception_id(self, mock_http_response_500, mock_http_request):
        """A function that raises HTTPStatusError with HTTP 500 but no exception ID."""
        async def func():
            error_message = "Server error '500 Internal Server Error' for url 'https://api.example.com/test' - Internal server error"
            raise httpx.HTTPStatusError(error_message, request=mock_http_request, response=mock_http_response_500)
        return func

    @pytest.fixture
    def function_that_raises_http_404(self, mock_http_response_404, mock_http_request):
        """A function that raises HTTPStatusError with HTTP 404."""
        async def func():
            error_message = "Client error '404 Not Found' for url 'https://api.example.com/test' - Resource not found"
            raise httpx.HTTPStatusError(error_message, request=mock_http_request, response=mock_http_response_404)
        return func

    @pytest.mark.asyncio
    async def test_tool_errors_decorator_with_http_500_and_exception_id(self, function_that_raises_http_500_with_exception_id):
        """Test that tool_errors decorator includes exception ID in recovery message for HTTP 500 errors."""
        decorated_func = tool_errors(
            default_recovery="Please try again later."
        )(function_that_raises_http_500_with_exception_id)

        with pytest.raises(ToolException) as exc_info:
            await decorated_func()

        # Check that exception ID is included in the recovery message for HTTP 500
        error_message = str(exc_info.value)
        assert "test-exc-500-123" in error_message
        assert "Exception ID: test-exc-500-123" in error_message
        assert "Database connection timeout" in error_message

    @pytest.mark.asyncio
    async def test_tool_errors_decorator_with_http_500_without_exception_id(self, function_that_raises_http_500_without_exception_id):
        """Test that tool_errors decorator handles HTTP 500 errors without exception ID gracefully."""
        decorated_func = tool_errors(
            default_recovery="Please try again later."
        )(function_that_raises_http_500_without_exception_id)

        with pytest.raises(ToolException) as exc_info:
            await decorated_func()

        # Check that no exception ID is included when not available
        error_message = str(exc_info.value)
        assert "Exception ID:" not in error_message or "For support reference Exception ID:" not in error_message
        assert "Internal server error" in error_message
        assert "Please try again later." in error_message

    @pytest.mark.asyncio
    async def test_tool_errors_decorator_with_http_404_does_not_include_exception_id(self, function_that_raises_http_404):
        """Test that tool_errors decorator does NOT include exception ID for non-500 HTTP errors."""
        decorated_func = tool_errors(
            default_recovery="Please check your request and try again."
        )(function_that_raises_http_404)

        with pytest.raises(ToolException) as exc_info:
            await decorated_func()

        # Check that exception ID is NOT included for non-500 errors
        error_message = str(exc_info.value)
        assert "For support reference Exception ID:" not in error_message
        assert "Resource not found" in error_message
        assert "Please check your request and try again." in error_message


@pytest.fixture
def function_with_value_error():
    """A function that raises ValueError for testing general error handling."""
    async def func():
        raise ValueError("Simulated ValueError")
    return func


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('function_fixture', 'default_recovery', 'recovery_instructions', 'expected_recovery_message', 'exception_message'),
    [
        # Case with both default_recovery and recovery_instructions specified
        (
            'function_with_value_error',
            'General recovery message.',
            {ValueError: 'Check that data has valid types.'},
            'Check that data has valid types.',
            'Simulated ValueError',
        ),
        # Case where only default_recovery is provided
        (
            'function_with_value_error',
            'General recovery message.',
            {},
            'General recovery message.',
            'Simulated ValueError',
        ),
        # Case with only recovery_instructions provided
        (
            'function_with_value_error',
            None,
            {ValueError: 'Check that data has valid types.'},
            'Check that data has valid types.',
            'Simulated ValueError',
        ),
        # Case with no recovery instructions provided
        (
            'function_with_value_error',
            None,
            {},
            None,
            'Simulated ValueError',
        ),
    ],
)
async def test_tool_function_recovery_instructions(
    function_fixture,
    default_recovery,
    recovery_instructions,
    expected_recovery_message,
    exception_message,
    request,
):
    """Test tool_errors decorator with various recovery instruction configurations."""
    function = request.getfixturevalue(function_fixture)
    
    decorated_func = tool_errors(
        default_recovery=default_recovery,
        recovery_instructions=recovery_instructions,
    )(function)

    with pytest.raises(ToolException) as exc_info:
        await decorated_func()

    error_message = str(exc_info.value)
    assert exception_message in error_message

    if expected_recovery_message:
        assert expected_recovery_message in error_message
    else:
        # Should fall back to default message
        assert "Please try again later." in error_message


@pytest.mark.asyncio
async def test_logging_on_tool_exception(caplog, function_with_value_error):
    """Test that tool_errors decorator logs exceptions properly."""
    decorated_func = tool_errors()(function_with_value_error)

    with pytest.raises(ToolException):
        await decorated_func()

    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert "Failed to run tool func" in caplog.records[0].message
    assert "Simulated ValueError" in caplog.records[0].message
