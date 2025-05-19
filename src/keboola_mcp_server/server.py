"""MCP server implementation for Keboola Connection."""

import logging
from typing import Any, Callable, Literal, Optional

from fastmcp import FastMCP

from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.components import add_component_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.jobs import add_job_tools
from keboola_mcp_server.tools.sql import add_sql_tools
from keboola_mcp_server.tools.storage import add_storage_tools

LOG = logging.getLogger(__name__)

TransportType = Literal['stdio', 'sse', 'streamable-http']
RequestParameterSource = Literal['query_params', 'headers']
SessionParams = dict[str, str]
SessionState = dict[str, Any]
SessionStateFactory = Callable[[Optional[SessionParams]], SessionState]


def create_server(config: Optional[Config] = None) -> FastMCP:
    """Create and configure the MCP server.

    Args:
        config: Server configuration. If None, loads from environment.

    Returns:
        Configured FastMCP server instance
    """
    # Initialize FastMCP server with system instructions
    mcp = KeboolaMcpServer(name='Keboola Explorer')
    mcp.set_config(config)

    add_component_tools(mcp)
    add_doc_tools(mcp)
    add_job_tools(mcp)
    add_storage_tools(mcp)
    add_sql_tools(mcp)

    return mcp
