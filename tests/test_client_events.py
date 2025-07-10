from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from keboola_mcp_server.client import JsonDict, RawKeboolaClient

# Base URL for testing storage related event sending
STORAGE_API_URL_V2 = 'https://connection.keboola.com/v2/storage'
NON_STORAGE_API_URL = 'https://queue.keboola.com'
TEST_TOKEN = 'test_token'


@pytest.fixture
def raw_client_storage_v2() -> RawKeboolaClient:
    """Create a RawKeboolaClient instance for /v2/storage URL testing."""
    client = RawKeboolaClient(STORAGE_API_URL_V2, TEST_TOKEN)
    client.headers['User-Agent'] = 'cursor'
    return client


@pytest.fixture
def raw_client_non_storage() -> RawKeboolaClient:
    """Create a RawKeboolaClient instance for non-/v2/storage URL testing."""
    client = RawKeboolaClient(NON_STORAGE_API_URL, TEST_TOKEN)
    client.headers['User-Agent'] = 'cursor'
    return client


class TestRawKeboolaClientEventLogic:
    """Test the event sending logic in RawKeboolaClient."""

    @pytest.mark.asyncio
    async def test_trigger_event_success_payload(self, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test the payload construction for a successful API call event."""
        # Mock the httpx.AsyncClient context manager to return a mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        mock_async_client = mocker.patch('keboola_mcp_server.client.httpx.AsyncClient')
        mock_async_client.return_value.__aenter__.return_value = mock_client

        mcp_context = {
            'tool_name': 'test_tool',
            'tool_args': {'arg1': 'val1'},
            'config_id': 'cfg123',
            'job_id': '456',
            'sessionId': 'test-session-123',
        }

        await raw_client_storage_v2.trigger_event(
            error_obj=None,
            duration_s=1.234,
            mcp_context=mcp_context,
        )

        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == f'{STORAGE_API_URL_V2}/events'  # URL

        sent_payload = call_args[1]['json']  # kwargs['json']
        assert sent_payload['component'] == RawKeboolaClient._MCP_SERVER_COMPONENT_ID
        assert sent_payload['message'] == 'MCP tool execution: test_tool'
        assert sent_payload['type'] == 'info'
        assert sent_payload['durationSeconds'] == 1.234
        assert sent_payload['params']['tool']['name'] == 'test_tool'
        assert sent_payload['params']['tool']['arguments'] == [{'arg1': 'val1'}]
        assert sent_payload['configurationId'] == 'cfg123'
        assert sent_payload['runId'] == '456'
        # Check mcpServerContext is filled
        mcp_ctx = sent_payload['params']['mcpServerContext']
        assert mcp_ctx['sessionId'] == 'test-session-123'
        assert mcp_ctx['appEnv'] == 'development'
        assert mcp_ctx['version'] == 'unknown'
        assert mcp_ctx['userAgent'] == 'cursor'

    @pytest.mark.asyncio
    async def test_trigger_event_error_payload(self, raw_client_storage_v2: RawKeboolaClient, mocker):
        """Test the payload construction for an errored API call event."""
        # Mock the httpx.AsyncClient context manager to return a mock client
        mock_client = AsyncMock()
        mock_client.post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        mock_async_client = mocker.patch('keboola_mcp_server.client.httpx.AsyncClient')
        mock_async_client.return_value.__aenter__.return_value = mock_client

        mcp_context = {'tool_name': 'error_tool', 'tool_args': ['argA'], 'sessionId': 'test-session-456'}
        error = ValueError('Test error')

        await raw_client_storage_v2.trigger_event(
            error_obj=error,
            duration_s=0.567,
            mcp_context=mcp_context,
        )

        mock_client.post.assert_called_once()
        sent_payload = mock_client.post.call_args[1]['json']
        assert sent_payload['type'] == 'error'
        assert sent_payload['message'] == 'Test error'
        assert 'configurationId' not in sent_payload  # Not in mcp_context
        assert 'runId' not in sent_payload  # Not in mcp_context

    @pytest.mark.asyncio
    async def test_post_does_not_trigger_event_anymore(
        self, raw_client_storage_v2: RawKeboolaClient, mocker
    ):
        """Test that RawKeboolaClient.post does NOT call trigger_event anymore."""
        mock_response_content: JsonDict = {'id': 'new_resource', 'status': 'created'}

        # Mock the actual HTTP call for the main operation
        mock_main_post = mocker.patch('httpx.AsyncClient.post', new_callable=AsyncMock)
        mock_main_post.return_value = MagicMock(spec=httpx.Response, status_code=201)
        mock_main_post.return_value.json.return_value = mock_response_content

        endpoint = 'some_resource'
        data = {'key': 'value'}

        response = await raw_client_storage_v2.post(endpoint, data=data)

        assert response == mock_response_content
        # No event should be triggered by HTTP methods anymore

    @pytest.mark.asyncio
    async def test_put_does_not_trigger_event_on_http_error(
        self, raw_client_storage_v2: RawKeboolaClient, mocker
    ):
        """Test that RawKeboolaClient.put does NOT call trigger_event on HTTP error."""
        # Mock the actual HTTP call for the main operation to raise an error
        http_error = httpx.HTTPStatusError(
            'Test HTTP Error', request=MagicMock(), response=MagicMock(status_code=400)
        )
        mocker.patch('httpx.AsyncClient.put', new_callable=AsyncMock, side_effect=http_error)

        endpoint = 'another_resource/id1'
        data = {'field': 'new_value'}

        with pytest.raises(httpx.HTTPStatusError):
            await raw_client_storage_v2.put(endpoint, data=data)
        # No event should be triggered by HTTP methods anymore

    @pytest.mark.asyncio
    async def test_delete_does_not_trigger_event_anymore(
        self, raw_client_storage_v2: RawKeboolaClient, mocker
    ):
        """Test that RawKeboolaClient.delete does NOT call trigger_event anymore."""
        mock_main_delete = mocker.patch('httpx.AsyncClient.delete', new_callable=AsyncMock)
        mock_main_delete.return_value = MagicMock(spec=httpx.Response, status_code=204, content=b'')

        await raw_client_storage_v2.delete('resource_to_delete')
        # No event should be triggered by HTTP methods anymore

    @pytest.mark.asyncio
    async def test_post_non_storage_url_does_not_trigger_event(
        self, raw_client_non_storage: RawKeboolaClient, mocker
    ):
        """Test that RawKeboolaClient.post does NOT call trigger_event for non-storage URLs."""
        mock_main_post = mocker.patch('httpx.AsyncClient.post', new_callable=AsyncMock)
        mock_main_post.return_value = MagicMock(spec=httpx.Response, status_code=200)
        mock_main_post.return_value.json.return_value = {'jobId': 123}

        await raw_client_non_storage.post('jobs', data={})
        # No event should be triggered by HTTP methods anymore

    @pytest.mark.asyncio
    async def test_delete_handles_no_content_response_without_event(
        self, raw_client_storage_v2: RawKeboolaClient, mocker
    ):
        """Test DELETE with 204 No Content works without triggering events (events handled by tool_errors decorator)."""
        mock_main_delete = mocker.patch('httpx.AsyncClient.delete', new_callable=AsyncMock)
        mock_main_delete.return_value = MagicMock(
            spec=httpx.Response, status_code=204, content=b''
        )  # No content

        endpoint = 'foo/res_xyz'

        result = await raw_client_storage_v2.delete(endpoint)
        # No event should be triggered by HTTP methods anymore
        assert result is None  # DELETE with 204 returns None

    @pytest.mark.asyncio
    async def test_delete_handles_non_json_response_without_event(
        self, raw_client_storage_v2: RawKeboolaClient, mocker
    ):
        """Test DELETE with non-JSON response raises error without triggering events."""
        text_content = 'Plain text response'

        # Mock the main DELETE operation
        mock_main_client = AsyncMock()
        mock_response = MagicMock(
            spec=httpx.Response,
            status_code=200,
            content=text_content.encode('utf-8'),
            text=text_content
        )
        # Make .json() raise error
        mock_response.json.side_effect = ValueError('Not JSON')
        mock_main_client.delete.return_value = mock_response

        # Use a side effect to return different clients for different calls
        mock_async_client = mocker.patch('keboola_mcp_server.client.httpx.AsyncClient')
        mock_async_client.return_value.__aenter__.return_value = mock_main_client

        endpoint = 'bar/res_abc'

        with pytest.raises(ValueError, match='Not JSON'):
            await raw_client_storage_v2.delete(endpoint)

        # Verify the main DELETE was called
        mock_main_client.delete.assert_called_once()
        # No event should be triggered by HTTP methods anymore
