import asyncio
import json
import math
import re
import uuid
from importlib.metadata import distribution
from typing import Any, Mapping

import httpx
import pytest
from fastmcp import Context
from mcp.types import ClientCapabilities, Implementation, InitializeRequestParams

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import CONVERSATION_ID, AggregateError
from keboola_mcp_server.tools.doc import docs_query
from keboola_mcp_server.tools.jobs import get_jobs
from keboola_mcp_server.tools.sql import query_data
from keboola_mcp_server.tools.storage import GetBucketsOutput, get_buckets


class TestHttpErrors:
    """Test different HTTP error scenarios to ensure enhanced error handling works correctly."""

    @pytest.mark.asyncio
    async def test_storage_api_404_error_maintains_standard_behavior(self, mcp_context: Context):
        result = await get_buckets(mcp_context, ['non.existent.bucket'])
        assert 'non.existent.bucket' in result.buckets_not_found

    @pytest.mark.asyncio
    async def test_jobs_api_404_error_(self, mcp_context: Context):
        match = re.compile(
            r"Client error '404 Not Found' "
            r"for url 'https://queue.keboola.com/jobs/999999999'\n"
            r'For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404\n'
            r'API error: Job "999999999" not found\n'
            r'Exception ID: .+\n'
            r'When contacting Keboola support please provide the exception ID\.',
            re.IGNORECASE,
        )
        with pytest.raises(AggregateError) as exc_info:
            await get_jobs(ctx=mcp_context, job_ids=('999999999',))

        # Verify AggregateError contains the HTTPStatusError
        err = exc_info.value
        assert len(err.exceptions) == 1
        assert isinstance(err.exceptions[0], httpx.HTTPStatusError)
        assert match.search(str(err.exceptions[0])) is not None

    @pytest.mark.asyncio
    async def test_docs_api_empty_query_error(self, mcp_context: Context):
        """Test that docs_query raises 422 error for empty queries."""
        match = re.compile(
            r"Client error '422 Unprocessable Content' "
            r"for url 'https://ai.keboola.com/docs/question'\n"
            r'For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/422\n'
            r'API error: Request contents is not valid\n'
            r'Exception ID: .+\n'
            r'When contacting Keboola support please provide the exception ID\.',
            re.IGNORECASE,
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            await docs_query(ctx=mcp_context, query='')

    @pytest.mark.asyncio
    async def test_sql_api_invalid_query_error(self, mcp_context: Context):
        match = re.compile(
            r'Failed to run SQL query, error: SQL compilation error:\n'
            r"syntax error line 1 at position 0 unexpected 'INVALID'\.",
            re.IGNORECASE,
        )
        with pytest.raises(ValueError, match=match):
            await query_data('INVALID SQL SYNTAX HERE', 'Invalid SQL query.', mcp_context)

    @pytest.mark.asyncio
    async def test_concurrent_error_handling(self, mcp_context: Context):
        # Run multiple concurrent operations that will trigger 404 errors
        tasks = [get_buckets(mcp_context, [f'non.existent.bucket.{i}']) for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all errors are handled consistently
        pattern = re.compile(r'non\.existent\.bucket\.\d+', re.IGNORECASE)

        for result in results:
            assert isinstance(result, GetBucketsOutput)
            assert result.buckets_not_found
            assert len(result.buckets_not_found) == 1
            assert pattern.fullmatch(result.buckets_not_found[0])


class TestStorageEvents:
    @staticmethod
    @tool_errors()
    async def foo(unique: str, ctx: Context):
        """A fake MCP tool to test events emitting."""
        await asyncio.sleep(0.1)

    @staticmethod
    @tool_errors()
    async def bar(unique: str, ctx: Context):
        """A fake MCP tool that fails by raising an error to test events emitting."""
        await asyncio.sleep(0.1)
        raise ValueError('Intentional error in bar tool.')

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('tool_name', 'event_message', 'event_type'),
        [
            ('foo', 'MCP tool "foo" call succeeded.', 'success'),
            ('bar', 'MCP tool "bar" call failed. ValueError: Intentional error in bar tool.', 'error'),
        ],
    )
    async def test_event_emitted(self, tool_name: str, event_message: str, event_type: str, mcp_context: Context):
        mcp_context.session_id = 'deadbee'
        mcp_context.session.client_params = InitializeRequestParams(
            protocolVersion='1',
            capabilities=ClientCapabilities(),
            clientInfo=Implementation(name='integtest', version='1.2.3'),
        )
        mcp_context.session.state[CONVERSATION_ID] = '#987654321'
        unique = uuid.uuid4().hex
        tool_func = getattr(self, tool_name)
        try:
            await tool_func(unique, mcp_context)
        except ValueError:
            pass  # ignore
        await asyncio.sleep(1)  # give SAPI time to digest the event

        client = KeboolaClient.from_state(mcp_context.session.state)
        events = await client.storage_client.get(
            endpoint='events',
            params={
                'component': 'keboola.mcp-server-tool',
                'q': f'message:"MCP tool "{tool_name}" call*"',
                'limit': 10,
            },
        )
        emitted_event = self._find_event(events, tool_name=tool_name, param_name='unique', param_value=unique)
        assert emitted_event is not None
        assert emitted_event['message'] == event_message
        assert emitted_event['type'] == event_type
        # SAPI events don't support float durations, so the duration is rounded up to the nearest second.
        assert math.fabs(emitted_event['performance']['duration'] - 0.1) < 1
        assert emitted_event['params']['mcpServerContext'] == {
            'appEnv': 'DEV',
            'version': distribution('keboola_mcp_server').version,
            'userAgent': 'integtest/1.2.3',
            'sessionId': 'deadbee',
            'serverTransport': 'stdio',
            'conversationId': '#987654321',
        }

    @staticmethod
    def _find_event(
        events: list[Mapping[str, Any]], *, tool_name: str, param_name: str, param_value: str
    ) -> Mapping[str, Any] | None:
        for event in events:
            event_tool = event['params']['tool']
            if event_tool['name'] != tool_name:
                continue
            for argument in event_tool['arguments']:
                if argument['key'] == param_name and json.loads(argument['value']) == param_value:
                    return event
        return None
