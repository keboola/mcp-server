import pytest
from fastmcp import Context

from keboola_mcp_server.tools.doc import DocsAnswer, docs_query


@pytest.mark.asyncio
async def test_docs_query(mcp_context: Context) -> None:
    """Tests that `docs_query` returns a valid `DocsAnswer` with text and source URLs."""
    query = 'What is Keboola Connection?'

    result = await docs_query(ctx=mcp_context, query=query)

    assert isinstance(result, DocsAnswer)
    assert len(result.text) > 0, 'Answer text should not be empty'
    assert len(result.source_urls) > 0, 'Source URLs list should not be empty'
