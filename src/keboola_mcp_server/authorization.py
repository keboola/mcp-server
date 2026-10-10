"""
Tool authorization middleware for granular access control.

This module provides middleware to filter tools based on client-specific permissions,
allowing administrators to restrict which tools specific clients (like Devin) can access.

Authorization is configured via HTTP headers:
- X-Allowed-Tools: Comma-separated list of allowed tool names
- X-Disallowed-Tools: Comma-separated list of tools to exclude (removed from allowed set)
- X-Read-Only-Mode: Set to "true" for read-only access (only tools with readOnlyHint=True)

Tool loading is shaped (not authorized) via:
- X-Deferred-Tools: Comma-separated list of tools listed with `_meta["anthropic/alwaysLoad"] = false`

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
from starlette.requests import Request

from keboola_mcp_server.mcp import get_http_request_or_none, is_read_only_tool

LOG = logging.getLogger(__name__)

ALWAYS_LOAD_META_KEY = 'anthropic/alwaysLoad'


def _parse_tool_names(header_value: str | None) -> set[str]:
    """Parses a comma-separated tool names header; blank entries are dropped."""
    return {t.strip() for t in (header_value or '').split(',') if t.strip()}


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
    def _get_authorization_config(
        http_rq: Request | None = None,
    ) -> tuple[set[str] | None, set[str] | None, bool]:
        """
        Determines the authorization configuration for the current request based on HTTP headers.

        Returns a tuple of (allowed_tools, disallowed_tools, read_only_mode):
        - allowed_tools: Set of allowed tool names, or None if all tools are allowed
        - disallowed_tools: Set of tool names to exclude, or None if no tools are explicitly disallowed
        - read_only_mode: Whether X-Read-Only-Mode header is enabled

        :param http_rq: Explicit request to read headers from. Falls back to the FastMCP request
            context when omitted. Raw Starlette routes (e.g. /preview/configuration) must pass it
            explicitly because the FastMCP request contextvar is not populated for them.
        """
        if http_rq is None:
            http_rq = get_http_request_or_none()
        if not http_rq:
            # No HTTP request means no authorization headers are present, so we do not apply any filters.
            return None, None, False

        allowed_tools: set[str] | None = None
        disallowed_tools: set[str] | None = None
        read_only_mode = False

        # Check X-Allowed-Tools header for explicit tool list
        if parsed_tools := _parse_tool_names(http_rq.headers.get('X-Allowed-Tools')):
            allowed_tools = parsed_tools
            LOG.info(f'Tool authorization: X-Allowed-Tools={sorted(allowed_tools)}')

        # Check X-Read-Only-Mode header
        if http_rq.headers.get('X-Read-Only-Mode', '').lower() in ('true', '1', 'yes'):
            read_only_mode = True
            LOG.info('Tool authorization: X-Read-Only-Mode=true')

        # Check X-Disallowed-Tools header for tools to exclude
        if parsed_tools := _parse_tool_names(http_rq.headers.get('X-Disallowed-Tools')):
            disallowed_tools = parsed_tools
            LOG.info(f'Tool authorization: X-Disallowed-Tools={sorted(disallowed_tools)}')

        return allowed_tools, disallowed_tools, read_only_mode

    @staticmethod
    def _is_tool_name_authorized(
        tool_name: str,
        is_read_only: bool,
        allowed_tools: set[str] | None,
        disallowed_tools: set[str] | None,
        read_only_mode: bool,
    ) -> bool:
        """
        Header-based (X-Allowed-Tools / X-Disallowed-Tools / X-Read-Only-Mode) authorization decision
        for a single tool identified by name.

        This is the single source of truth for the header-based gating. :meth:`_is_tool_authorized`
        uses it for the MCP middleware path; the raw ``/preview/configuration`` Starlette route reuses
        it (see ``preview.py``) so the preview path enforces exactly the same rules.
        """
        # First check if tool is in disallowed list (if any disallow filter is configured)
        if disallowed_tools and tool_name in disallowed_tools:
            return False
        # Check read-only mode - only allow tools with readOnlyHint=True
        if read_only_mode and not is_read_only:
            return False
        # Then check if tool is in allowed list (if specified)
        return not (allowed_tools is not None and tool_name not in allowed_tools)

    @staticmethod
    def _is_tool_authorized(
        tool: Tool, allowed_tools: set[str] | None, disallowed_tools: set[str] | None, read_only_mode: bool
    ) -> bool:
        """Check if a tool is authorized based on allowed/disallowed sets and read-only mode."""
        return ToolAuthorizationMiddleware._is_tool_name_authorized(
            tool.name, is_read_only_tool(tool), allowed_tools, disallowed_tools, read_only_mode
        )

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


class ToolDeferralMiddleware(fmw.Middleware):
    """
    Marks the tools named in the X-Deferred-Tools header as deferred in the tools list.

    - Each named tool is listed with `_meta["anthropic/alwaysLoad"] = false`; other `_meta` keys are kept.
    - Claude Code / Agent SDK clients that register the server with `alwaysLoad: true` then keep these
      tools behind tool search instead of loading their definitions into the context up front.
    - No header, an empty header or unknown tool names leave the tools list unchanged.
    - Tool calls are not affected; authorization stays with :class:`ToolAuthorizationMiddleware`.
    """

    async def on_list_tools(
        self, context: MiddlewareContext[mt.ListToolsRequest], call_next: CallNext[mt.ListToolsRequest, list[Tool]]
    ) -> list[Tool]:
        tools = await call_next(context)

        http_rq = get_http_request_or_none()
        deferred = _parse_tool_names(http_rq.headers.get('X-Deferred-Tools')) if http_rq else set()
        if not deferred:
            return tools

        LOG.debug(f'Tool deferral: X-Deferred-Tools={sorted(deferred)}')
        return [
            t.model_copy(update={'meta': {**(t.meta or {}), ALWAYS_LOAD_META_KEY: False}}) if t.name in deferred else t
            for t in tools
        ]
