import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.tools.storage import BucketDetail, retrieve_buckets


@pytest.mark.asyncio
async def test_retrieve_buckets(mcp_context_client: Context):
    """Tests that retrieve_buckets returns a list of BucketDetail instances."""
    result = await retrieve_buckets(mcp_context_client)

    assert isinstance(result, list)
    for item in result:
        assert isinstance(item, BucketDetail)
