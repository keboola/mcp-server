"""Shared utility functions for the Keboola MCP Server.

This module contains utility functions used across multiple modules to avoid code duplication.
"""

from fastmcp.tools import Tool


def is_read_only_tool(tool: Tool) -> bool:
    """Check if a tool has readOnlyHint=True annotation.

    This helper is used by both ToolsFilteringMiddleware and ToolAuthorizationMiddleware
    to determine which tools are read-only. Tools with readOnlyHint=True are safe for
    guest and read-only users to access.

    :param tool: The Tool to check
    :return: True if the tool has readOnlyHint=True, False otherwise
    """
    if tool.annotations is None:
        return False
    return tool.annotations.readOnlyHint is True
