import logging

from mcp.server.fastmcp import FastMCP

from keboola_mcp_server.components.read_tools import add_component_read_tools
from keboola_mcp_server.components.modify_tools import add_component_write_tools

LOG = logging.getLogger(__name__)


############################## Add component tools to the MCP server #########################################


def add_component_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    add_component_read_tools(mcp)
    add_component_write_tools(mcp)
    LOG.info("Component tools initialized.")
