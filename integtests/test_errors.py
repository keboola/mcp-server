import asyncio
import re
from unittest.mock import AsyncMock

import httpx
import pytest
from fastmcp import Context

from keboola_mcp_server.tools.doc import docs_query
from keboola_mcp_server.tools.jobs import get_job
from keboola_mcp_server.tools.sql import query_data
from keboola_mcp_server.tools.storage import get_bucket


class TestHttpErrors:
    """Test different HTTP error scenarios to ensure enhanced error handling works correctly."""

    @pytest.mark.asyncio
    async def test_storage_api_404_error_maintains_standard_behavior(self, mcp_context: Context):
        match = re.compile(
            r"Client error '404 Not Found' "
            r"for url 'https://connection.keboola.com/v2/storage/buckets/non.existent.bucket'\n"
            r'For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404\n'
            r'API error: The bucket "non.existent.bucket" was not found in the project "\d+"\n'
            r'Exception ID: .+\n'
            r'When contacting Keboola support please provide the exception ID\.',
            re.IGNORECASE
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            await get_bucket('non.existent.bucket', mcp_context)

    @pytest.mark.asyncio
    async def test_jobs_api_404_error_(self, mcp_context: Context):
        match = re.compile(
            r"Client error '404 Not Found' "
            r"for url 'https://queue.keboola.com/jobs/999999999'\n"
            r'For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404\n'
            r'API error: Job "999999999" not found\n'
            r'Exception ID: .+\n'
            r'When contacting Keboola support please provide the exception ID\.',
            re.IGNORECASE
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            await get_job('999999999', mcp_context)

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
            re.IGNORECASE
        )
        with pytest.raises(httpx.HTTPStatusError, match=match):
            await docs_query(ctx=mcp_context, query='')

    @pytest.mark.asyncio
    async def test_sql_api_invalid_query_error(self, mcp_context: Context):
        match = re.compile(
            r'Failed to run SQL query, error: An exception occurred while executing a query: SQL compilation error:\n'
            r"syntax error line 1 at position 0 unexpected 'INVALID'\.",
            re.IGNORECASE
        )
        with pytest.raises(ValueError, match=match):
            await query_data('INVALID SQL SYNTAX HERE', mcp_context)

    @pytest.mark.asyncio
    async def test_concurrent_error_handling(self, mcp_context: Context):
        # Run multiple concurrent operations that will trigger 404 errors
        tasks = [get_bucket(f'non.existent.bucket.{i}', mcp_context) for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all errors are handled consistently
        match = re.compile(
            r"Client error '404 Not Found' "
            r"for url 'https://connection.keboola.com/v2/storage/buckets/non.existent.bucket.\d'\n"
            r'For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404\n'
            r'API error: The bucket "non.existent.bucket.\d" was not found in the project "\d+"\n'
            r'Exception ID: .+\n'
            r'When contacting Keboola support please provide the exception ID\.',
            re.IGNORECASE
        )

        unexpected_errors: list[str] = []
        for result in results:
            assert isinstance(result, httpx.HTTPStatusError)
            error_message = str(result)
            if not match.fullmatch(error_message):
                unexpected_errors.append(error_message)

        assert unexpected_errors == []


class TestToolErrorsEventTriggering:
    """Test that the tool_errors decorator properly triggers events when errors occur."""

    @pytest.mark.asyncio
    async def test_storage_api_error_triggers_event(self, mcp_context: Context, mocker):
        """Test that get_bucket tool triggers an event when a 404 error occurs."""
        # Mock the trigger_event method to verify it's called
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger a 404 error
        with pytest.raises(httpx.HTTPStatusError):
            await get_bucket('non.existent.bucket', mcp_context)

        # Verify trigger_event was called with correct parameters
        mock_trigger_event.assert_called_once()
        call_args = mock_trigger_event.call_args

        # Check the error object
        error_obj = call_args[1]['error_obj']
        assert isinstance(error_obj, httpx.HTTPStatusError)
        assert '404 Not Found' in str(error_obj)

        # Check duration is captured
        duration_s = call_args[1]['duration_s']
        assert isinstance(duration_s, float)
        assert duration_s > 0

        # Check mcp_context contains expected fields
        mcp_context_arg = call_args[1]['mcp_context']
        assert mcp_context_arg['tool_name'] == 'get_bucket'
        assert mcp_context_arg['tool_args'] == {'bucket_id': 'non.existent.bucket'}
        assert 'sessionId' in mcp_context_arg
        # Verify ctx parameter is filtered out
        assert 'ctx' not in mcp_context_arg['tool_args']

    @pytest.mark.asyncio
    async def test_jobs_api_error_triggers_event(self, mcp_context: Context, mocker):
        """Test that get_job tool triggers an event when a 404 error occurs."""
        # Mock the trigger_event method to verify it's called
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger a 404 error
        with pytest.raises(httpx.HTTPStatusError):
            await get_job('999999999', mcp_context)

        # Verify trigger_event was called with correct parameters
        mock_trigger_event.assert_called_once()
        call_args = mock_trigger_event.call_args

        # Check the error object
        error_obj = call_args[1]['error_obj']
        assert isinstance(error_obj, httpx.HTTPStatusError)
        assert '404 Not Found' in str(error_obj)

        # Check mcp_context contains expected fields
        mcp_context_arg = call_args[1]['mcp_context']
        assert mcp_context_arg['tool_name'] == 'get_job'
        assert mcp_context_arg['tool_args'] == {'job_id': '999999999'}
        assert 'sessionId' in mcp_context_arg
        # Verify ctx parameter is filtered out
        assert 'ctx' not in mcp_context_arg['tool_args']

    @pytest.mark.asyncio
    async def test_sql_api_error_triggers_event(self, mcp_context: Context, mocker):
        """Test that query_data tool triggers an event when a SQL error occurs."""
        # Mock the trigger_event method to verify it's called
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger a SQL error
        with pytest.raises(ValueError, match='Failed to run SQL query'):
            await query_data('INVALID SQL SYNTAX HERE', mcp_context)

        # Verify trigger_event was called with correct parameters
        mock_trigger_event.assert_called_once()
        call_args = mock_trigger_event.call_args

        # Check the error object
        error_obj = call_args[1]['error_obj']
        assert isinstance(error_obj, ValueError)
        assert 'Failed to run SQL query' in str(error_obj)

        # Check mcp_context contains expected fields
        mcp_context_arg = call_args[1]['mcp_context']
        assert mcp_context_arg['tool_name'] == 'query_data'
        assert mcp_context_arg['tool_args'] == {'sql_query': 'INVALID SQL SYNTAX HERE'}
        assert 'sessionId' in mcp_context_arg
        # Verify ctx parameter is filtered out
        assert 'ctx' not in mcp_context_arg['tool_args']

    @pytest.mark.asyncio
    async def test_concurrent_errors_trigger_multiple_events(self, mcp_context: Context, mocker):
        """Test that concurrent errors trigger multiple events correctly."""
        # Mock the trigger_event method to verify it's called multiple times
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Run multiple concurrent operations that will trigger 404 errors
        tasks = [get_bucket(f'non.existent.bucket.{i}', mcp_context) for i in range(3)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Verify all operations failed
        for result in results:
            assert isinstance(result, httpx.HTTPStatusError)

        # Verify trigger_event was called for each error
        assert mock_trigger_event.call_count == 3

        # Verify each call had correct parameters
        # Collect all bucket_ids from the calls to check they match expected values
        expected_bucket_ids = {f'non.existent.bucket.{i}' for i in range(3)}
        actual_bucket_ids = set()

        for call in mock_trigger_event.call_args_list:
            mcp_context_arg = call[1]['mcp_context']
            assert mcp_context_arg['tool_name'] == 'get_bucket'
            assert 'bucket_id' in mcp_context_arg['tool_args']
            actual_bucket_ids.add(mcp_context_arg['tool_args']['bucket_id'])
            assert 'sessionId' in mcp_context_arg
            # Verify ctx parameter is filtered out
            assert 'ctx' not in mcp_context_arg['tool_args']

        # Verify all expected bucket IDs were present
        assert actual_bucket_ids == expected_bucket_ids

    @pytest.mark.asyncio
    async def test_event_triggering_failure_does_not_break_error_handling(self, mcp_context: Context, mocker):
        """Test that if trigger_event fails, the original error is still raised."""
        # Mock trigger_event to raise an exception
        mock_trigger_event = AsyncMock(side_effect=Exception('Event triggering failed'))
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # The original error should still be raised even if event triggering fails
        with pytest.raises(httpx.HTTPStatusError):
            await get_bucket('non.existent.bucket', mcp_context)

        # Verify trigger_event was attempted
        mock_trigger_event.assert_called_once()

    @pytest.mark.asyncio
    async def test_tool_args_extraction_includes_all_parameters(self, mcp_context: Context, mocker):
        """Test that tool arguments are correctly extracted and included in the event."""
        # Mock the trigger_event method
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger an error with a tool that has multiple parameters
        with pytest.raises(httpx.HTTPStatusError):
            await get_bucket('non.existent.bucket', mcp_context)

        # Verify the tool_args contain the expected parameters
        call_args = mock_trigger_event.call_args
        mcp_context_arg = call_args[1]['mcp_context']
        tool_args = mcp_context_arg['tool_args']

        # Check that bucket_id is included but ctx is not
        assert 'bucket_id' in tool_args
        assert tool_args['bucket_id'] == 'non.existent.bucket'
        assert 'ctx' not in tool_args  # Context should be filtered out

    @pytest.mark.asyncio
    async def test_mcp_context_structure_completeness(self, mcp_context: Context, mocker):
        """Test that mcp_context contains all required fields with correct structure."""
        # Mock the trigger_event method
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger an error
        with pytest.raises(httpx.HTTPStatusError):
            await get_bucket('test.bucket', mcp_context)

        # Verify mcp_context structure
        call_args = mock_trigger_event.call_args
        mcp_context_arg = call_args[1]['mcp_context']

        # Check required fields exist
        assert 'tool_name' in mcp_context_arg
        assert 'tool_args' in mcp_context_arg
        assert 'sessionId' in mcp_context_arg

        # Check field types and values
        assert isinstance(mcp_context_arg['tool_name'], str)
        assert mcp_context_arg['tool_name'] == 'get_bucket'

        assert isinstance(mcp_context_arg['tool_args'], dict)
        assert mcp_context_arg['tool_args'] == {'bucket_id': 'test.bucket'}

        assert isinstance(mcp_context_arg['sessionId'], str)
        assert len(mcp_context_arg['sessionId']) > 0

    @pytest.mark.asyncio
    async def test_tool_args_extraction_with_complex_parameters(self, mcp_context: Context, mocker):
        """Test that complex tool arguments are correctly extracted and serialized."""
        # Mock the trigger_event method
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Test with a complex SQL query that might contain special characters
        complex_query = "SELECT * FROM `my.table` WHERE column = 'test' AND id IN (1, 2, 3)"
        with pytest.raises(ValueError, match='Failed to run SQL query'):
            await query_data(complex_query, mcp_context)

        # Verify the tool_args contain the complex parameter correctly
        call_args = mock_trigger_event.call_args
        mcp_context_arg = call_args[1]['mcp_context']
        tool_args = mcp_context_arg['tool_args']

        assert 'sql_query' in tool_args
        assert tool_args['sql_query'] == complex_query
        assert 'ctx' not in tool_args

    @pytest.mark.asyncio
    async def test_session_id_extraction_from_context(self, mcp_context: Context, mocker):
        """Test that sessionId is correctly extracted from the Context object."""
        # Mock the trigger_event method
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger an error
        with pytest.raises(httpx.HTTPStatusError):
            await get_job('12345', mcp_context)

        # Verify sessionId is extracted and included
        call_args = mock_trigger_event.call_args
        mcp_context_arg = call_args[1]['mcp_context']

        assert 'sessionId' in mcp_context_arg
        session_id = mcp_context_arg['sessionId']
        assert isinstance(session_id, str)
        assert session_id != 'unknown-session'  # Should be a real session ID
        assert len(session_id) > 0

    @pytest.mark.asyncio
    async def test_error_duration_calculation(self, mcp_context: Context, mocker):
        """Test that error duration is correctly calculated and included in the event."""
        # Mock the trigger_event method
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Trigger an error
        with pytest.raises(httpx.HTTPStatusError):
            await get_bucket('test.bucket', mcp_context)

        # Verify duration is captured correctly
        call_args = mock_trigger_event.call_args
        duration_s = call_args[1]['duration_s']

        assert isinstance(duration_s, float)
        assert duration_s > 0
        assert duration_s < 10  # Should be reasonable for a test

    @pytest.mark.asyncio
    async def test_tool_name_extraction_accuracy(self, mcp_context: Context, mocker):
        """Test that tool names are correctly extracted for different tools."""
        # Mock the trigger_event method
        mock_trigger_event = AsyncMock()
        mocker.patch.object(
            mcp_context.session.state['sapi_client'].storage_client.raw_client,
            'trigger_event',
            mock_trigger_event
        )

        # Test different tools to ensure tool names are extracted correctly
        tools_to_test = [
            (get_bucket, 'get_bucket', {'bucket_id': 'test.bucket'}),
            (get_job, 'get_job', {'job_id': '12345'}),
            (query_data, 'query_data', {'sql_query': 'SELECT 1'}),
        ]

        for tool_func, expected_name, expected_args in tools_to_test:
            mock_trigger_event.reset_mock()

            try:
                await tool_func(*expected_args.values(), mcp_context)
            except (httpx.HTTPStatusError, ValueError):
                pass  # Expected to fail

            # Verify tool name is correct
            if mock_trigger_event.called:
                call_args = mock_trigger_event.call_args
                mcp_context_arg = call_args[1]['mcp_context']
                assert mcp_context_arg['tool_name'] == expected_name
                assert mcp_context_arg['tool_args'] == expected_args
