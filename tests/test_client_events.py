import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from keboola_mcp_server.client import RawKeboolaClient, JsonDict, JsonStruct

# Base URL for testing storage related event sending
STORAGE_API_URL_V2 = "https://connection.keboola.com/v2/storage"
NON_STORAGE_API_URL = "https://queue.keboola.com"
TEST_TOKEN = "test_token"


@pytest.fixture
def raw_client_storage_v2() -> RawKeboolaClient:
    """Returns a RawKeboolaClient configured for /v2/storage."""
    client = RawKeboolaClient(base_api_url=STORAGE_API_URL_V2, api_token=TEST_TOKEN)
    client.headers['User-Agent'] = 'cursor'
    return client


@pytest.fixture
def raw_client_non_storage() -> RawKeboolaClient:
    """Returns a RawKeboolaClient configured for a non-storage URL."""
    client = RawKeboolaClient(base_api_url=NON_STORAGE_API_URL, api_token=TEST_TOKEN)
    client.headers['User-Agent'] = 'claude'
    return client


class TestRawKeboolaClientEventLogic:
    """Unit tests for event sending logic in RawKeboolaClient."""

    def test_should_send_event_true_for_v2_storage_non_event_endpoint(self, raw_client_storage_v2: RawKeboolaClient):
        """Test _should_send_event is True for /v2/storage and non-'events' endpoint."""
        assert raw_client_storage_v2._should_send_event("tables") is True
        assert raw_client_storage_v2._should_send_event("branch/my_branch/components/comp/configs") is True

    def test_should_send_event_false_for_v2_storage_event_endpoint(self, raw_client_storage_v2: RawKeboolaClient):
        """Test _should_send_event is False for /v2/storage and 'events' endpoint."""
        assert raw_client_storage_v2._should_send_event("events") is False

    def test_should_send_event_false_for_non_v2_storage_url(self, raw_client_non_storage: RawKeboolaClient):
        """Test _should_send_event is False for non-/v2/storage URLs."""
        assert raw_client_non_storage._should_send_event("jobs") is False

    @pytest.mark.asyncio
    async def test_send_event_after_request_success_payload(self, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test the payload construction for a successful API call event."""
        # Mock the httpx.AsyncClient context manager to return a mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        mock_async_client = mocker.patch("keboola_mcp_server.client.httpx.AsyncClient")
        mock_async_client.return_value.__aenter__.return_value = mock_client
        
        mcp_context = {
            "tool_name": "test_tool",
            "tool_args": {"arg1": "val1"},
            "config_id": "cfg123",
            "run_id": "run456",
            "sessionId": "test-session-123",
        }
        response: JsonDict = {"id": "res1", "status": "ok"}
        
        await raw_client_storage_v2._send_event_after_request(
            http_method="POST",
            endpoint="tables",
            error_obj=None,
            duration_s=1.234,
            mcp_context=mcp_context,
        )

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == f"{STORAGE_API_URL_V2}/events"  # URL
        
        sent_payload = call_args[1]['json'] # kwargs['json']
        assert sent_payload["component"] == RawKeboolaClient._MCP_SERVER_COMPONENT_ID
        assert sent_payload["message"] == "MCP: test_tool - POST /v2/storage/tables"
        assert sent_payload["type"] == "info"
        assert sent_payload["durationSeconds"] == 1.234
        assert sent_payload["params"]["tool"]["name"] == "test_tool"
        assert sent_payload["params"]["tool"]["arguments"] == [{"arg1": "val1"}]
        assert "error" not in sent_payload["results"]
        assert sent_payload["configurationId"] == "cfg123"
        assert sent_payload["runId"] == "run456"
        # Check mcp-server-context is filled
        mcp_ctx = sent_payload["params"]["mcp-server-context"]
        assert mcp_ctx["sessionId"] == "test-session-123"
        assert mcp_ctx["app_env"] == "development"
        assert mcp_ctx["version"] == "unknown"
        assert mcp_ctx["user-agent"] == "cursor"

    @pytest.mark.asyncio
    async def test_send_event_after_request_error_payload(self, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test the payload construction for an errored API call event."""
        # Mock the httpx.AsyncClient context manager to return a mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        mock_async_client = mocker.patch("keboola_mcp_server.client.httpx.AsyncClient")
        mock_async_client.return_value.__aenter__.return_value = mock_client
        
        mcp_context = {"tool_name": "error_tool", "tool_args": ["argA"], "sessionId": "test-session-456"}
        error = ValueError("Test error")
        
        await raw_client_storage_v2._send_event_after_request(
            http_method="PUT",
            endpoint="configs/conf1",
            error_obj=error,
            duration_s=0.567,
            mcp_context=mcp_context,
        )

        mock_client.post.assert_called_once()
        sent_payload = mock_client.post.call_args[1]['json']
        assert sent_payload["type"] == "error"
        assert sent_payload["results"]["error"] == "Test error"
        assert "query_result" not in sent_payload["results"]
        assert "configurationId" not in sent_payload # Not in mcp_context
        assert "runId" not in sent_payload # Not in mcp_context

    @pytest.mark.asyncio
    @patch.object(RawKeboolaClient, "_send_event_after_request", new_callable=AsyncMock)
    async def test_post_triggers_event_on_success(self, mock_send_event_method: AsyncMock, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test that RawKeboolaClient.post calls _send_event_after_request on success."""
        mock_response_content: JsonDict = {"id": "new_resource", "status": "created"}
        
        # Mock the actual HTTP call for the main operation
        mock_main_post = mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock)
        mock_main_post.return_value = MagicMock(spec=httpx.Response, status_code=201)
        mock_main_post.return_value.json.return_value = mock_response_content
        
        mcp_context = {"tool_name": "creator_tool", "tool_args": {}, "sessionId": "test-session-789"}
        endpoint = "some_resource"
        data = {"key": "value"}

        start_time = time.monotonic()
        response = await raw_client_storage_v2.post(endpoint, data=data, mcp_context=mcp_context)
        duration_s = time.monotonic() - start_time

        assert response == mock_response_content
        mock_send_event_method.assert_called_once()
        
        call_args = mock_send_event_method.call_args[1] # kwargs
        assert call_args["http_method"] == "POST"
        assert call_args["endpoint"] == endpoint
        assert call_args["error_obj"] is None
        assert abs(call_args["duration_s"] - duration_s) < 0.1 # Check duration is close
        assert call_args["mcp_context"] == mcp_context

    @pytest.mark.asyncio
    @patch.object(RawKeboolaClient, "_send_event_after_request", new_callable=AsyncMock)
    async def test_put_triggers_event_on_http_error(self, mock_send_event_method: AsyncMock, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test that RawKeboolaClient.put calls _send_event_after_request on HTTP error."""
        # Mock the actual HTTP call for the main operation to raise an error
        http_error = httpx.HTTPStatusError("Test HTTP Error", request=MagicMock(), response=MagicMock(status_code=400))
        mock_main_put = mocker.patch("httpx.AsyncClient.put", new_callable=AsyncMock, side_effect=http_error)
        
        mcp_context = {
            "tool_name": "updater_tool", 
            "tool_args": {}, 
            "config_id": "cfg789", 
            "sessionId": "test-session-101",
        }
        endpoint = "another_resource/id1"
        data = {"field": "new_value"}

        start_time = time.monotonic()
        with pytest.raises(httpx.HTTPStatusError):
            await raw_client_storage_v2.put(endpoint, data=data, mcp_context=mcp_context)
        duration_s = time.monotonic() - start_time
        
        mock_send_event_method.assert_called_once()
        call_args = mock_send_event_method.call_args[1]
        assert call_args["http_method"] == "PUT"
        assert call_args["endpoint"] == endpoint
        assert call_args["error_obj"] is http_error
        assert abs(call_args["duration_s"] - duration_s) < 0.1
        assert call_args["mcp_context"] == mcp_context

    @pytest.mark.asyncio
    @patch.object(RawKeboolaClient, "_send_event_after_request", new_callable=AsyncMock)
    async def test_delete_no_mcp_context_no_event(self, mock_send_event_method: AsyncMock, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test that RawKeboolaClient.delete does NOT call event sender if no mcp_context."""
        mock_main_delete = mocker.patch("httpx.AsyncClient.delete", new_callable=AsyncMock)
        mock_main_delete.return_value = MagicMock(spec=httpx.Response, status_code=204, content=b"")

        await raw_client_storage_v2.delete("resource_to_delete", mcp_context=None)
        
        mock_send_event_method.assert_not_called()

    @pytest.mark.asyncio
    @patch.object(RawKeboolaClient, "_send_event_after_request", new_callable=AsyncMock)
    async def test_post_non_storage_url_no_event(self, mock_send_event_method: AsyncMock, raw_client_non_storage: RawKeboolaClient, mocker):
        """Test that RawKeboolaClient.post does NOT call event sender if URL is not /v2/storage."""
        mock_main_post = mocker.patch("httpx.AsyncClient.post", new_callable=AsyncMock)
        mock_main_post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        mock_main_post.return_value.json.return_value = {"jobId": 123}

        mcp_context = {"tool_name": "job_runner", "tool_args": {}, "sessionId": "test-session-202"}
        await raw_client_non_storage.post("jobs", data={}, mcp_context=mcp_context)
        
        mock_send_event_method.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_handles_no_content_response_for_event(self, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test DELETE with 204 No Content correctly forms event payload (empty query_result)."""
        mock_send_event = mocker.patch.object(raw_client_storage_v2, "_send_event_after_request", new_callable=AsyncMock)
        
        mock_main_delete = mocker.patch("httpx.AsyncClient.delete", new_callable=AsyncMock)
        mock_main_delete.return_value = MagicMock(spec=httpx.Response, status_code=204, content=b"") # No content
        
        mcp_context = {
            "tool_name": "deleter_tool",
            "tool_args": {"id": "res_xyz"},
            "sessionId": "test-session-303",
        }
        endpoint = "foo/res_xyz"
        
        await raw_client_storage_v2.delete(endpoint, mcp_context=mcp_context)
        
        mock_send_event.assert_called_once()
        call_args = mock_send_event.call_args[1]
        assert call_args["http_method"] == "DELETE"
        assert call_args["endpoint"] == endpoint
        assert call_args["error_obj"] is None
        assert call_args["mcp_context"] == mcp_context

    @pytest.mark.asyncio
    async def test_delete_handles_non_json_response_for_event(self, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test DELETE with non-JSON response correctly forms event payload."""
        text_content = "Plain text response"
        
        # Mock the main DELETE operation
        mock_main_client = AsyncMock()
        mock_response = MagicMock(spec=httpx.Response, status_code=200, content=text_content.encode('utf-8'), text=text_content)
        mock_response.json.side_effect = ValueError("Not JSON") # Make .json() raise error
        mock_main_client.delete.return_value = mock_response
        
        # Mock the event sending operation
        mock_event_client = AsyncMock()
        mock_event_client.post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        
        # Use a side effect to return different clients for different calls
        mock_async_client = mocker.patch("keboola_mcp_server.client.httpx.AsyncClient")
        mock_async_client.return_value.__aenter__.side_effect = [mock_main_client, mock_event_client]
        
        mcp_context = {
            "tool_name": "deleter_tool_non_json",
            "tool_args": {"id": "res_abc"},
            "sessionId": "test-session-404",
        }
        endpoint = "bar/res_abc"
        
        # The delete method should raise ValueError when it encounters non-JSON response
        with pytest.raises(ValueError, match="Not JSON"):
            await raw_client_storage_v2.delete(endpoint, mcp_context=mcp_context)
        
        # The event should be sent with the error
        mock_event_client.post.assert_called_once()
        call_args = mock_event_client.post.call_args
        sent_payload = call_args[1]['json']
        assert sent_payload["type"] == "error"
        assert sent_payload["results"]["error"] == "Not JSON" 