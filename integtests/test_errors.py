import asyncio
import re

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
