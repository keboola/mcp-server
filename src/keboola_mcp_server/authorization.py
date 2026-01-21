"""
Tool authorization middleware for granular access control.

This module provides middleware to filter tools based on client-specific permissions,
allowing administrators to restrict which tools specific clients (like Devin) can access.

Authorization is configured via HTTP headers:
- X-Allowed-Tools: Comma-separated list of allowed tool names
- X-Disallowed-Tools: Comma-separated list of tools to exclude (removed from allowed set)
- X-Read-Only-Mode: Set to "true" for read-only access (only tools with readOnlyHint=True)

Note: These headers are intended to be injected by infrastructure/proxy layers (e.g., API gateways,
reverse proxies) rather than set directly by end clients. For direct client access control,
use Storage API token permissions which provide the security layer.
"""

import logging

from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import Tool
from mcp import types as mt

from keboola_mcp_server.mcp import get_http_request_or_none

LOG = logging.getLogger(__name__)


class ToolAuthorizationMiddleware(fmw.Middleware):
    """
    Middleware that filters tools based on client-specific authorization.

    Authorization is configured via HTTP headers:
    - X-Allowed-Tools: Comma-separated list of allowed tool names
    - X-Disallowed-Tools: Comma-separated list of tools to exclude (removed from allowed set)
    - X-Read-Only-Mode: Set to "true" for read-only access (filters to tools with readOnlyHint=True)

    The middleware:
    - Filters the tools list in on_list_tools() to hide unauthorized tools
    - Blocks unauthorized tool calls in on_call_tool() with a ToolError
    """

    @staticmethod
    def _get_authorization_config() -> tuple[set[str] | None, set[str] | None, bool]:
        """
        Determines the authorization configuration for the current request based on HTTP headers.

        Returns a tuple of (allowed_tools, disallowed_tools, read_only_mode):
        - allowed_tools: Set of allowed tool names, or None if all tools are allowed
        - disallowed_tools: Set of tool names to exclude, or None if no tools are explicitly disallowed
        - read_only_mode: Whether X-Read-Only-Mode header is enabled
        """
        http_rq = get_http_request_or_none()
        if not http_rq:
            # No HTTP request means no authorization headers are present, so we do not apply any filters.
            return None, None, False

        allowed_tools: set[str] | None = None
        disallowed_tools: set[str] | None = None
        read_only_mode = False

        # Check X-Allowed-Tools header for explicit tool list
        if header_tools := http_rq.headers.get('X-Allowed-Tools'):
            parsed_tools = set(t.strip() for t in header_tools.split(',') if t.strip())
            if parsed_tools:
                allowed_tools = parsed_tools
                LOG.info(f'Tool authorization: X-Allowed-Tools={sorted(allowed_tools)}')

        # Check X-Read-Only-Mode header
        if http_rq.headers.get('X-Read-Only-Mode', '').lower() in ('true', '1', 'yes'):
            read_only_mode = True
            LOG.info('Tool authorization: X-Read-Only-Mode=true')

        # Check X-Disallowed-Tools header for tools to exclude
        if header_disallowed := http_rq.headers.get('X-Disallowed-Tools'):
            parsed_tools = set(t.strip() for t in header_disallowed.split(',') if t.strip())
            if parsed_tools:
                disallowed_tools = parsed_tools
                LOG.info(f'Tool authorization: X-Disallowed-Tools={sorted(disallowed_tools)}')

        return allowed_tools, disallowed_tools, read_only_mode

    @staticmethod
    def _is_read_only_tool(tool: Tool) -> bool:
        """Check if a tool has readOnlyHint=True annotation."""
        if tool.annotations is None:
            return False
        return tool.annotations.readOnlyHint is True

    @staticmethod
    def _is_tool_authorized(
        tool: Tool, allowed_tools: set[str] | None, disallowed_tools: set[str] | None, read_only_mode: bool
    ) -> bool:
        """Check if a tool is authorized based on allowed/disallowed sets and read-only mode."""
        # First check if tool is in disallowed list (if any disallow filter is configured)
        if disallowed_tools and tool.name in disallowed_tools:
            return False
        # Check read-only mode - only allow tools with readOnlyHint=True
        if read_only_mode and not ToolAuthorizationMiddleware._is_read_only_tool(tool):
            return False
        # Then check if tool is in allowed list (if specified)
        if allowed_tools is not None and tool.name not in allowed_tools:
            return False
        return True

    async def on_list_tools(
        self, context: MiddlewareContext[mt.ListToolsRequest], call_next: CallNext[mt.ListToolsRequest, list[Tool]]
    ) -> list[Tool]:
        """Filters the tools list to only include authorized tools."""
        tools = await call_next(context)

        allowed_tools, disallowed_tools, read_only_mode = self._get_authorization_config()
        if allowed_tools is None and not disallowed_tools and not read_only_mode:
            return tools

        filtered_tools = [
            t for t in tools if self._is_tool_authorized(t, allowed_tools, disallowed_tools, read_only_mode)
        ]
        LOG.debug(f'Tool authorization: filtered {len(tools)} tools to {len(filtered_tools)} allowed tools')
        return filtered_tools

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        """Blocks calls to unauthorized tools."""
        tool_name = context.message.name
        allowed_tools, disallowed_tools, read_only_mode = self._get_authorization_config()

        # For on_call_tool, we need to get the tool to check its annotations
        tool = await context.fastmcp_context.fastmcp.get_tool(tool_name)

        if not self._is_tool_authorized(tool, allowed_tools, disallowed_tools, read_only_mode):
            LOG.info(f'Tool authorization denied: {tool_name} not authorized')
            raise ToolError(
                f'Access denied: The tool "{tool_name}" is not authorized for this client. '
                f'Contact your administrator to request access.'
            )

        return await call_next(context)
