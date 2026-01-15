"""
Tool authorization middleware for granular access control.

This module provides middleware to filter tools based on client-specific permissions,
allowing administrators to restrict which tools specific clients (like Devin) can access.

Authorization is configured via HTTP headers:
- X-Allowed-Tools: Comma-separated list of allowed tool names
- X-Read-Only-Mode: Set to "true" for read-only access (only tools with readOnlyHint=True)
"""

import logging

from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import Tool
from mcp import types as mt

from keboola_mcp_server.mcp import get_http_request_or_none

LOG = logging.getLogger(__name__)

# Read-only tools that don't modify data (tools with readOnlyHint=True annotation)
# These are tools that only retrieve information without making any changes
READ_ONLY_TOOLS: frozenset[str] = frozenset(
    {
        # components
        'get_configs',
        'get_components',
        'get_config_examples',
        # flows
        'get_flows',
        'get_flow_examples',
        'get_flow_schema',
        # storage
        'get_buckets',
        'get_tables',
        # sql
        'query_data',
        # data_apps
        'get_data_apps',
        # jobs
        'get_jobs',
        # search
        'search',
        'find_component_id',
        # project
        'get_project_info',
        # docs
        'docs_query',
    }
)


class ToolAuthorizationMiddleware(fmw.Middleware):
    """
    Middleware that filters tools based on client-specific authorization.

    Authorization is configured via HTTP headers:
    - X-Allowed-Tools: Comma-separated list of allowed tool names
    - X-Read-Only-Mode: Set to "true" for read-only access

    The middleware:
    - Filters the tools list in on_list_tools() to hide unauthorized tools
    - Blocks unauthorized tool calls in on_call_tool() with a ToolError
    """

    @staticmethod
    def _get_allowed_tools(ctx: Context) -> set[str] | None:
        """
        Determines the set of allowed tools for the current request based on HTTP headers.

        Returns None if all tools are allowed (no restrictions).
        """
        http_rq = get_http_request_or_none()
        if not http_rq:
            return None

        allowed_tools: set[str] | None = None

        # Check X-Allowed-Tools header for explicit tool list
        if header_tools := http_rq.headers.get('X-Allowed-Tools'):
            allowed_tools = set(t.strip() for t in header_tools.split(',') if t.strip())
            LOG.debug(f'Tool authorization: X-Allowed-Tools header specifies {len(allowed_tools)} tools')

        # Check X-Read-Only-Mode header
        if http_rq.headers.get('X-Read-Only-Mode', '').lower() in ('true', '1', 'yes'):
            if allowed_tools is not None:
                # Intersect with read-only tools
                allowed_tools &= READ_ONLY_TOOLS
            else:
                allowed_tools = set(READ_ONLY_TOOLS)
            LOG.debug(f'Tool authorization: X-Read-Only-Mode enabled, {len(allowed_tools)} tools allowed')

        return allowed_tools

    async def on_list_tools(
        self, context: MiddlewareContext[mt.ListToolsRequest], call_next: CallNext[mt.ListToolsRequest, list[Tool]]
    ) -> list[Tool]:
        """Filters the tools list to only include authorized tools."""
        tools = await call_next(context)

        allowed_tools = self._get_allowed_tools(context.fastmcp_context)
        if allowed_tools is None:
            return tools

        filtered_tools = [t for t in tools if t.name in allowed_tools]
        LOG.debug(f'Tool authorization: filtered {len(tools)} tools to {len(filtered_tools)} allowed tools')
        return filtered_tools

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        """Blocks calls to unauthorized tools."""
        tool_name = context.message.name
        allowed_tools = self._get_allowed_tools(context.fastmcp_context)

        if allowed_tools is not None and tool_name not in allowed_tools:
            LOG.warning(f'Tool authorization denied: {tool_name} not in allowed tools')
            raise ToolError(
                f'Access denied: The tool "{tool_name}" is not authorized for this client. '
                f'Contact your administrator to request access.'
            )

        return await call_next(context)
