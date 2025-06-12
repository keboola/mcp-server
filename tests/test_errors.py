import logging
from unittest.mock import Mock, AsyncMock, patch

import httpx
import pytest

from keboola_mcp_server.errors import ToolException, tool_errors


# --- New test fixtures for KeboolaHTTPException ---
@pytest.fixture
def mock_http_request():
    """Mock HTTP request object."""
    mock_request = Mock(spec=httpx.Request)
    mock_request.url = "https://api.example.com/test"
    mock_request.method = "GET"
    return mock_request


@pytest.fixture
def mock_http_response_500():
    """Mock HTTP response object with 500 status."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_response.reason_phrase = "Internal Server Error"
    mock_response.url = "https://api.example.com/test"
    return mock_response


@pytest.fixture
def mock_http_response_404():
    """Mock HTTP response object with 404 status."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 404
    mock_response.reason_phrase = "Not Found"
    mock_response.url = "https://api.example.com/test"
    return mock_response


@pytest.fixture
def mock_http_response_502():
    """Mock HTTP response object with 502 status."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 502
    mock_response.reason_phrase = "Bad Gateway"
    mock_response.url = "https://api.example.com/test"
    return mock_response


@pytest.fixture
def mock_httpstatus_error_500(mock_http_request, mock_http_response_500):
    """Mock HTTPStatusError for 500 status."""
    return httpx.HTTPStatusError(
        "Server error '500 Internal Server Error' for url 'https://api.example.com/test'",
        request=mock_http_request,
        response=mock_http_response_500
    )


@pytest.fixture
def mock_httpstatus_error_404(mock_http_request, mock_http_response_404):
    """Mock HTTPStatusError for 404 status."""
    return httpx.HTTPStatusError(
        "Client error '404 Not Found' for url 'https://api.example.com/test'",
        request=mock_http_request,
        response=mock_http_response_404
    )


# --- Test Cases for KeboolaHTTPException ---


class TestKeboolaHTTPException:
    """Test suite for KeboolaHTTPException class."""

    def test_create_with_exception_id_for_500_error(self, mock_httpstatus_error_500):
        """Test that KeboolaHTTPException includes exception ID for HTTP 500 errors."""
        from keboola_mcp_server.errors import KeboolaHTTPException

        exception_id = "abc123-def456-ghi789"
        error_details = {"message": "Internal server error occurred"}
        
        keboola_exception = KeboolaHTTPException(
            mock_httpstatus_error_500, 
            exception_id, 
            error_details
        )
        
        # Test that exception ID is included in the message for HTTP 500
        error_message = str(keboola_exception)
        assert exception_id in error_message
        assert "Exception ID: abc123-def456-ghi789" in error_message
        assert "Internal server error occurred" in error_message

    def test_create_without_exception_id_for_500_error(self, mock_httpstatus_error_500):
        """Test that KeboolaHTTPException handles missing exception ID gracefully for HTTP 500."""
        from keboola_mcp_server.errors import KeboolaHTTPException

        keboola_exception = KeboolaHTTPException(mock_httpstatus_error_500)
        
        # Test fallback behavior when exception ID is missing
        error_message = str(keboola_exception)
        assert "Exception ID:" not in error_message
        assert str(mock_httpstatus_error_500) in error_message

    def test_create_with_exception_id_for_non_500_error(self, mock_httpstatus_error_404):
        """Test that KeboolaHTTPException does NOT include exception ID for non-500 HTTP errors."""
        from keboola_mcp_server.errors import KeboolaHTTPException

        exception_id = "abc123-def456-ghi789"
        error_details = {"message": "Resource not found"}
        
        keboola_exception = KeboolaHTTPException(
            mock_httpstatus_error_404, 
            exception_id, 
            error_details
        )
        
        # Test that exception ID is NOT included for non-500 errors
        error_message = str(keboola_exception)
        assert "Exception ID:" not in error_message
        assert exception_id not in error_message
        # Should still include error details message
        assert "Resource not found" in error_message

    def test_error_details_filtering(self, mock_httpstatus_error_500):
        """Test that error details are properly filtered and included."""
        from keboola_mcp_server.errors import KeboolaHTTPException

        error_details = {
            "message": "Database connection failed",
            "errorCode": "DB_CONNECTION_ERROR",
            "requestId": "req-123",
            "sensitiveData": "secret-key-12345"  # Should not appear in message
        }
        
        keboola_exception = KeboolaHTTPException(
            mock_httpstatus_error_500, 
            "exc-123", 
            error_details
        )
        
        error_message = str(keboola_exception)
        assert "Database connection failed" in error_message
        # Sensitive data should not appear in the message
        assert "secret-key-12345" not in error_message

    def test_preserves_original_exception_properties(self, mock_httpstatus_error_500):
        """Test that KeboolaHTTPException preserves original exception properties."""
        from keboola_mcp_server.errors import KeboolaHTTPException

        keboola_exception = KeboolaHTTPException(mock_httpstatus_error_500, "exc-123")
        
        # Should preserve original exception properties
        assert keboola_exception.request == mock_httpstatus_error_500.request
        assert keboola_exception.response == mock_httpstatus_error_500.response
        assert keboola_exception.original_exception == mock_httpstatus_error_500

    def test_empty_error_details_handling(self, mock_httpstatus_error_500):
        """Test handling of empty or None error details."""
        from keboola_mcp_server.errors import KeboolaHTTPException

        # Test with None error details
        keboola_exception_none = KeboolaHTTPException(mock_httpstatus_error_500, "exc-123", None)
        assert str(keboola_exception_none)  # Should not raise exception
        
        # Test with empty dict error details
        keboola_exception_empty = KeboolaHTTPException(mock_httpstatus_error_500, "exc-123", {})
        assert str(keboola_exception_empty)  # Should not raise exception


# --- Test Cases for Enhanced HTTP Client Error Handling ---


class TestRawKeboolaClientErrorHandling:
    """Test suite for enhanced HTTP client error handling."""

    @pytest.fixture
    def raw_client(self):
        """Create a RawKeboolaClient instance for testing."""
        from keboola_mcp_server.client import RawKeboolaClient
        return RawKeboolaClient(
            base_api_url="https://api.example.com",
            api_token="test-token"
        )

    def test_handle_http_error_500_with_exception_id(self, raw_client, mock_http_response_500, mock_http_request):
        """Test that HTTP 500 errors are enhanced with exception ID when available."""
        from keboola_mcp_server.errors import KeboolaHTTPException
        
        # Mock response with valid JSON containing exception ID
        mock_http_response_500.json.return_value = {
            "exceptionId": "exc-123-456",
            "message": "Database connection failed",
            "errorCode": "DB_ERROR",
            "requestId": "req-789"
        }
        mock_http_response_500.request = mock_http_request
        
        with pytest.raises(KeboolaHTTPException) as exc_info:
            raw_client._handle_http_error(mock_http_response_500)
        
        exception = exc_info.value
        error_message = str(exception)
        assert "Exception ID: exc-123-456" in error_message
        assert "Database connection failed" in error_message
        assert exception.exception_id == "exc-123-456"

    def test_handle_http_error_500_without_exception_id(self, raw_client, mock_http_response_500, mock_http_request):
        """Test that HTTP 500 errors without exception ID fall back gracefully."""
        from keboola_mcp_server.errors import KeboolaHTTPException
        
        # Mock response with JSON but no exception ID
        mock_http_response_500.json.return_value = {
            "message": "Internal server error",
            "errorCode": "INTERNAL_ERROR"
        }
        mock_http_response_500.request = mock_http_request
        
        with pytest.raises(KeboolaHTTPException) as exc_info:
            raw_client._handle_http_error(mock_http_response_500)
        
        exception = exc_info.value
        error_message = str(exception)
        assert "Exception ID:" not in error_message
        assert "Internal server error" in error_message
        assert exception.exception_id is None

    def test_handle_http_error_500_with_malformed_json(self, raw_client, mock_http_response_500, mock_http_request):
        """Test that HTTP 500 errors with malformed JSON fall back to standard error handling."""
        from keboola_mcp_server.errors import KeboolaHTTPException
        
        # Mock response with invalid JSON
        mock_http_response_500.json.side_effect = ValueError("Invalid JSON")
        mock_http_response_500.request = mock_http_request
        
        with pytest.raises(KeboolaHTTPException) as exc_info:
            raw_client._handle_http_error(mock_http_response_500)
        
        exception = exc_info.value
        error_message = str(exception)
        assert "Exception ID:" not in error_message
        assert exception.exception_id is None
        assert exception.error_details == {}

    def test_handle_http_error_404_uses_standard_exception(self, raw_client, mock_http_response_404, mock_http_request):
        """Test that HTTP 404 errors use standard HTTPStatusError."""
        mock_http_response_404.request = mock_http_request
        
        # Configure the mock to raise HTTPStatusError when raise_for_status is called
        mock_http_response_404.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Client error '404 Not Found' for url 'https://api.example.com/test'",
            request=mock_http_request,
            response=mock_http_response_404
        )
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._handle_http_error(mock_http_response_404)
        
        # Should be standard HTTPStatusError, not KeboolaHTTPException
        assert not hasattr(exc_info.value, 'exception_id')
        assert not hasattr(exc_info.value, 'original_exception')

    def test_handle_http_error_502_uses_standard_exception(self, raw_client, mock_http_response_502, mock_http_request):
        """Test that other HTTP 5xx errors (non-500) use standard HTTPStatusError."""
        mock_http_response_502.request = mock_http_request
        
        # Configure the mock to raise HTTPStatusError when raise_for_status is called
        mock_http_response_502.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server error '502 Bad Gateway' for url 'https://api.example.com/test'",
            request=mock_http_request,
            response=mock_http_response_502
        )
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            raw_client._handle_http_error(mock_http_response_502)
        
        # Should be standard HTTPStatusError, not KeboolaHTTPException
        assert not hasattr(exc_info.value, 'exception_id')
        assert not hasattr(exc_info.value, 'original_exception')

    @pytest.mark.asyncio
    async def test_get_method_integration_with_enhanced_error_handling(self, raw_client):
        """Test that GET method integrates with enhanced error handling."""
        from keboola_mcp_server.errors import KeboolaHTTPException
        
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
            
            with pytest.raises(KeboolaHTTPException) as exc_info:
                await raw_client.get("test-endpoint")
            
            exception = exc_info.value
            assert "Exception ID: test-exc-123" in str(exception)
            assert "Test error message" in str(exception)

    @pytest.mark.asyncio
    async def test_post_method_integration_with_enhanced_error_handling(self, raw_client):
        """Test that POST method integrates with enhanced error handling."""
        from keboola_mcp_server.errors import KeboolaHTTPException
        
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
                "exceptionId": "test-exc-456",
                "message": "POST error message"
            }
            mock_response.request = Mock()
            
            mock_client.post.return_value = mock_response
            
            with pytest.raises(KeboolaHTTPException) as exc_info:
                await raw_client.post("test-endpoint", data={"key": "value"})
            
            exception = exc_info.value
            assert "Exception ID: test-exc-456" in str(exception)
            assert "POST error message" in str(exception)


# --- Fixtures ---
@pytest.fixture
def function_with_value_error():
    """A function that raises ValueError."""

    async def func():
        raise ValueError('Simulated ValueError')

    return func


# --- Test Cases ---


# --- Test tool_errors ---
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
    """
    Test that the appropriate recovery message is applied based on the exception type.
    Verifies that the tool_errors decorator handles various combinations of recovery parameters.
    """
    tool_func = request.getfixturevalue(function_fixture)
    decorated_func = tool_errors(default_recovery=default_recovery, recovery_instructions=recovery_instructions)(
        tool_func
    )

    if expected_recovery_message is None:
        with pytest.raises(ValueError, match=exception_message) as excinfo:
            await decorated_func()
    else:
        with pytest.raises(ToolException) as excinfo:
            await decorated_func()
        assert expected_recovery_message in str(excinfo.value)
    assert exception_message in str(excinfo.value)


# --- Test Logging ---
@pytest.mark.asyncio
async def test_logging_on_tool_exception(caplog, function_with_value_error):
    """Test if logging works correctly with the tool function."""
    decorated_func = tool_errors(default_recovery='General recovery message.')(function_with_value_error)

    with caplog.at_level(logging.ERROR):
        try:
            await decorated_func()
        except ToolException:
            pass

    # Capture and assert the correct logging output
    assert 'failed to run tool' in caplog.text.lower()
    assert 'simulated valueerror' in caplog.text.lower()
    assert 'raise valueerror' in caplog.text.lower()
