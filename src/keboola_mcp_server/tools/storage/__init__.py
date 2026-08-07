from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.storage import tools as _tools
from keboola_mcp_server.tools.storage.shared_buckets import add_shared_bucket_tools
from keboola_mcp_server.tools.storage.tools import STORAGE_TOOLS_TAG

# shared_buckets.py imports STORAGE_TOOLS_TAG/BucketDetail from tools.py, so tools.py can't
# import shared_buckets.py back at module level without a circular import -- this package's
# __init__ is the composition point instead.


def add_storage_tools(mcp: KeboolaMcpServer) -> None:
    """Adds all storage tools (buckets, tables, shared buckets) to the MCP server."""
    _tools.add_storage_tools(mcp)
    add_shared_bucket_tools(mcp)


__all__ = ['STORAGE_TOOLS_TAG', 'add_storage_tools']
