from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.storage.shared_buckets import add_shared_bucket_tools
from keboola_mcp_server.tools.storage.tools import STORAGE_TOOLS_TAG
from keboola_mcp_server.tools.storage.tools import add_storage_tools as _add_storage_tools


def add_storage_tools(mcp: KeboolaMcpServer) -> None:
    """Adds all storage tools (buckets, tables, shared buckets) to the MCP server."""
    _add_storage_tools(mcp)
    add_shared_bucket_tools(mcp)


__all__ = ['STORAGE_TOOLS_TAG', 'add_storage_tools']
