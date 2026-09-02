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

On top of the headers, a server started with row-level-security rules (`--rls-rules-path`) is
restricted to read-only tools unconditionally -- see `ToolAuthorizationMiddleware.is_rls_mode()`.
"""

import logging

from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import Tool
from mcp import types as mt
from starlette.requests import Request

from keboola_mcp_server.mcp import ServerState, get_http_request_or_none, is_read_only_tool

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
    def is_rls_mode(server_state: 'ServerState | None') -> bool:
        """Whether the server was started with row-level-security rules.

        RLS mode is a server-wide, header-independent read-only mode. The whole point of RLS is that a
        query can only ever return the slice the rules allow; that guarantee is worth nothing if the
        same session can run a job, create a transformation or deploy a data app that copies the
        unfiltered table somewhere the rules do not reach. So RLS mode restricts the server to
        `readOnlyHint=True` tools no matter what the caller's headers or token role say.
        """
        return server_state is not None and server_state.rls_rules is not None

    @staticmethod
    def _server_state_or_none(context: MiddlewareContext) -> 'ServerState | None':
        """The `ServerState` behind the current MCP request, or None when it cannot be reached.

        The lifespan state is where RLS mode is recorded, and it is the only unforgeable input the
        middleware has (headers are caller-controlled). Every server built by `create_server()` has
        one; None here means there is no FastMCP request context at all (unit tests with mock
        contexts), in which case there are no RLS rules to enforce either.
        """
        ctx = getattr(context, 'fastmcp_context', None)
        if ctx is None:
            return None
        try:
            return ServerState.from_context(ctx)
        except Exception as e:
            LOG.debug(f'Tool authorization: no ServerState in the request context: {e}')
            return None

    @staticmethod
    def _get_authorization_config(
        http_rq: Request | None = None,
        server_state: 'ServerState | None' = None,
    ) -> tuple[set[str] | None, set[str] | None, bool]:
        """
        Determines the authorization configuration for the current request based on HTTP headers
        and on the server-wide RLS mode.

        Returns a tuple of (allowed_tools, disallowed_tools, read_only_mode):
        - allowed_tools: Set of allowed tool names, or None if all tools are allowed
        - disallowed_tools: Set of tool names to exclude, or None if no tools are explicitly disallowed
        - read_only_mode: Whether X-Read-Only-Mode header is enabled, or the server runs in RLS mode

        :param http_rq: Explicit request to read headers from. Falls back to the FastMCP request
            context when omitted. Raw Starlette routes (e.g. /preview/configuration) must pass it
            explicitly because the FastMCP request contextvar is not populated for them.
        :param server_state: The server's lifespan state, used to detect RLS mode. Callers that can
            reach it must pass it; omitting it only means RLS mode is not enforced on that path.
        """
        # RLS mode forces read-only regardless of headers, and it is decided before them so that a
        # request with no HTTP headers at all (stdio, in-memory client) is still restricted.
        rls_mode = ToolAuthorizationMiddleware.is_rls_mode(server_state)
        if rls_mode:
            LOG.debug('Tool authorization: RLS mode is on, restricting to read-only tools')

        if http_rq is None:
            http_rq = get_http_request_or_none()
        if not http_rq:
            # No HTTP request means no authorization headers are present, so we only apply RLS mode.
            return None, None, rls_mode

        allowed_tools: set[str] | None = None
        disallowed_tools: set[str] | None = None
        read_only_mode = rls_mode

        # Check X-Allowed-Tools header for explicit tool list
        if header_tools := http_rq.headers.get('X-Allowed-Tools'):
            parsed_tools = {t.strip() for t in header_tools.split(',') if t.strip()}
            if parsed_tools:
                allowed_tools = parsed_tools
                LOG.info(f'Tool authorization: X-Allowed-Tools={sorted(allowed_tools)}')

        # Check X-Read-Only-Mode header
        if http_rq.headers.get('X-Read-Only-Mode', '').lower() in ('true', '1', 'yes'):
            read_only_mode = True
            LOG.info('Tool authorization: X-Read-Only-Mode=true')

        # Check X-Disallowed-Tools header for tools to exclude
        if header_disallowed := http_rq.headers.get('X-Disallowed-Tools'):
            parsed_tools = {t.strip() for t in header_disallowed.split(',') if t.strip()}
            if parsed_tools:
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

        allowed_tools, disallowed_tools, read_only_mode = self._get_authorization_config(
            server_state=self._server_state_or_none(context)
        )
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
        server_state = self._server_state_or_none(context)
        allowed_tools, disallowed_tools, read_only_mode = self._get_authorization_config(server_state=server_state)

        # For on_call_tool, we need to get the tool to check its annotations
        tool = await context.fastmcp_context.fastmcp.get_tool(tool_name)

        if not self._is_tool_authorized(tool, allowed_tools, disallowed_tools, read_only_mode):
            if self.is_rls_mode(server_state) and not is_read_only_tool(tool):
                # Name the real reason: the tool is not hidden because of this client's headers, it
                # cannot be called on this server at all while RLS mode is on.
                LOG.info(f'Tool authorization denied: {tool_name} is not read-only and the server runs in RLS mode')
                raise ToolError(
                    f'Access denied: this server runs with row-level security (RLS) enabled, which allows '
                    f'read-only tools only. The tool "{tool_name}" modifies data and cannot be called.'
                )
            LOG.info(f'Tool authorization denied: {tool_name} not authorized')
            raise ToolError(
                f'Access denied: The tool "{tool_name}" is not authorized for this client. '
                f'Contact your administrator to request access.'
            )

        return await call_next(context)
