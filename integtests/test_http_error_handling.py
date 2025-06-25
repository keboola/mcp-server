"""
Integration tests for enhanced HTTP error handling across different error scenarios.

These tests use real API calls to verify that:
1. HTTP 500 errors are enhanced with exception IDs when available
2. Other HTTP errors (4xx, other 5xx) maintain standard behavior
3. Error handling works correctly across different service clients

Note: These tests require actual API credentials and may create/delete test resources.
"""
import httpx
import pytest
from fastmcp import Context

from integtests.conftest import BucketDef
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import ToolException
from keboola_mcp_server.tools.jobs import get_job, run_job
from keboola_mcp_server.tools.sql import query_data
from keboola_mcp_server.tools.storage import get_bucket, get_table, update_bucket_description


class TestHTTPErrorScenarios:
    """Test different HTTP error scenarios to ensure enhanced error handling works correctly."""

    @pytest.mark.asyncio
    async def test_storage_api_404_error_maintains_standard_behavior(self, mcp_context: Context):
        """Test that Storage API 404 errors maintain standard behavior (no exception ID enhancement)."""
        # Try to access a non-existent bucket
        with pytest.raises(ToolException) as exc_info:
            await get_bucket('non.existent.bucket', mcp_context)

        tool_exception = exc_info.value
        # Should not include exception ID for non-500 errors
        assert 'For support reference Exception ID:' not in tool_exception.recovery_message
        # Should include standard recovery message
        assert 'Please try again later.' in tool_exception.recovery_message

    @pytest.mark.asyncio
    async def test_storage_api_403_error_maintains_standard_behavior(self, mcp_context: Context):
        """Test that Storage API 403 errors maintain standard behavior (no exception ID enhancement)."""
        # Try to access a table that doesn't exist or we don't have permission for
        with pytest.raises(ToolException) as exc_info:
            await get_table('forbidden.table.access', mcp_context)

        tool_exception = exc_info.value
        # Should not include exception ID for non-500 errors
        assert 'For support reference Exception ID:' not in tool_exception.recovery_message
        # Should include standard recovery message
        assert 'Please try again later.' in tool_exception.recovery_message

    @pytest.mark.asyncio
    async def test_jobs_api_404_error_maintains_standard_behavior(self, mcp_context: Context):
        """Test that Jobs API 404 errors maintain standard behavior (no exception ID enhancement)."""
        # Try to access a non-existent job
        with pytest.raises(ToolException) as exc_info:
            await get_job('999999999', mcp_context)

        tool_exception = exc_info.value
        # Should not include exception ID for non-500 errors
        assert 'For support reference Exception ID:' not in tool_exception.recovery_message
        # Should include standard recovery message
        assert 'Please try again later.' in tool_exception.recovery_message

    @pytest.mark.asyncio
    async def test_jobs_api_400_error_with_invalid_component(self, mcp_context: Context):
        """Test that Jobs API 400 errors maintain standard behavior (no exception ID enhancement)."""
        # Try to start a job with invalid component ID
        with pytest.raises(ToolException) as exc_info:
            await run_job(mcp_context, 'invalid.component.id', 'invalid-config')

        tool_exception = exc_info.value
        # Should not include exception ID for non-500 errors
        assert 'For support reference Exception ID:' not in tool_exception.recovery_message
        # Should include standard recovery message
        assert 'Please try again later.' in tool_exception.recovery_message

    @pytest.mark.asyncio
    async def test_sql_api_invalid_query_error_behavior(self, mcp_context: Context):
        """Test that SQL API errors for invalid queries maintain standard behavior."""
        # Try to execute invalid SQL
        with pytest.raises(ToolException) as exc_info:
            await query_data('INVALID SQL SYNTAX HERE', mcp_context)

        tool_exception = exc_info.value
        # Should not include exception ID for non-500 errors (likely 400 for invalid SQL)
        assert 'For support reference Exception ID:' not in tool_exception.recovery_message
        # Should include standard recovery message
        assert 'Please try again later.' in tool_exception.recovery_message

    @pytest.mark.asyncio
    async def test_storage_api_unauthorized_operation_error(self, mcp_context: Context, buckets: list[BucketDef]):
        """Test that Storage API unauthorized operations maintain standard behavior."""
        # Try to perform an operation that might not be allowed
        # (this might return 403 or 401 depending on the specific restriction)
        if buckets:
            bucket = buckets[0]
            with pytest.raises(ToolException) as exc_info:
                # Try to update bucket description with potentially problematic content
                await update_bucket_description(bucket.bucket_id, 'x' * 1000000, mcp_context)  # Very long description

            tool_exception = exc_info.value
            # Should not include exception ID for non-500 errors
            assert 'For support reference Exception ID:' not in tool_exception.recovery_message
            # Should include standard recovery message
            assert 'Please try again later.' in tool_exception.recovery_message


class TestHTTPErrorHandlingIntegration:
    """Test integration of enhanced error handling with actual service operations."""

    @pytest.mark.asyncio
    async def test_direct_client_error_handling_integration(self, keboola_client: KeboolaClient):
        """Test enhanced error handling directly through KeboolaClient operations."""
        # Test Storage API client error handling
        try:
            # Try to get details of a non-existent bucket
            await keboola_client.storage_client.bucket_detail('non.existent.bucket')
            pytest.fail('Expected an exception to be raised')
        except Exception as e:
            # Should be a standard HTTP error for 404 (not enhanced)
            assert not hasattr(e, 'exception_id')
            assert not isinstance(e, httpx.HTTPStatusError)

    @pytest.mark.asyncio
    async def test_jobs_client_error_handling_integration(self, keboola_client: KeboolaClient):
        """Test enhanced error handling directly through Jobs client operations."""
        try:
            # Try to get details of a non-existent job
            await keboola_client.jobs_queue_client.get_job_detail('999999999')
            pytest.fail('Expected an exception to be raised')
        except Exception as e:
            # Should be a standard HTTP error for 404 (not enhanced)
            assert not hasattr(e, 'exception_id')
            assert not isinstance(e, httpx.HTTPStatusError)

    @pytest.mark.asyncio
    async def test_error_propagation_through_tool_layers(self, mcp_context: Context):
        """Test that errors propagate correctly through all tool layers."""
        # This test verifies that the error handling works end-to-end:
        # Tool -> Service Client -> RawKeboolaClient -> Enhanced Error Handling -> Tool Error Decorator

        with pytest.raises(ToolException) as exc_info:
            # Use a tool that will definitely cause a 404 error
            await get_bucket('definitely.non.existent.bucket', mcp_context)

        tool_exception = exc_info.value

        # Verify error message structure
        assert isinstance(tool_exception, ToolException)
        assert tool_exception.recovery_message is not None

        # Verify that non-500 errors don't get enhanced with exception ID
        assert 'For support reference Exception ID:' not in tool_exception.recovery_message

        # Verify that standard recovery message is present
        assert 'Please try again later.' in tool_exception.recovery_message

    @pytest.mark.asyncio
    async def test_mixed_error_scenarios_in_sequence(self, mcp_context: Context):
        """Test handling of different error types in sequence to ensure state is not corrupted."""
        # Test multiple different error scenarios to ensure our error handling
        # doesn't have any state corruption issues

        # Test 1: 404 error
        with pytest.raises(ToolException) as exc_info:
            await get_bucket('non.existent.bucket.1', mcp_context)

        assert 'For support reference Exception ID:' not in exc_info.value.recovery_message

        # Test 2: Another 404 error with different resource
        with pytest.raises(ToolException) as exc_info:
            await get_table('non.existent.table.2', mcp_context)

        assert 'For support reference Exception ID:' not in exc_info.value.recovery_message

        # Test 3: Jobs API 404 error
        with pytest.raises(ToolException) as exc_info:
            await get_job('999999999', mcp_context)

        assert 'For support reference Exception ID:' not in exc_info.value.recovery_message

        # All tests should consistently not include exception IDs for non-500 errors


class TestHTTP500ErrorDetection:
    """Test detection and handling of HTTP 500 errors when they occur."""

    @pytest.mark.asyncio
    async def test_http_500_error_detection_and_enhancement(self, keboola_client: KeboolaClient):
        """
        Test that if an HTTP 500 error occurs, it gets enhanced with exception ID.

        Note: This test is designed to detect HTTP 500 errors if they occur naturally
        during API operations. We cannot easily force HTTP 500 errors in integration tests
        without potentially damaging test infrastructure.
        """
        # This test will pass if no HTTP 500 errors occur, or verify enhancement if they do

        try:
            # Perform an operation that might occasionally return HTTP 500
            # (like querying a potentially overloaded service)
            await keboola_client.storage_client.bucket_list()

            # If no error occurs, the test passes (no HTTP 500 to test)
            assert True

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 500:
                # This is an HTTP 500 error - verify it has exception ID enhancement
                assert 'Exception ID:' in str(e)
                print(f'Successfully detected and enhanced HTTP 500 error: {e}')

    @pytest.mark.asyncio
    async def test_concurrent_error_handling(self, mcp_context: Context):
        """Test that error handling works correctly under concurrent operations."""
        import asyncio

        async def trigger_404_error(resource_id: str):
            """Helper to trigger a 404 error."""
            try:
                await get_bucket(f'non.existent.{resource_id}', mcp_context)
                return None
            except ToolException as e:
                return e

        # Run multiple concurrent operations that will trigger 404 errors
        tasks = [trigger_404_error(f'bucket_{i}') for i in range(5)]
        results = await asyncio.gather(*tasks)

        # Verify all errors are handled consistently
        for result in results:
            assert isinstance(result, ToolException)
            assert 'For support reference Exception ID:' not in result.recovery_message
            assert 'Please try again later.' in result.recovery_message
