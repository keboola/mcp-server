"""
This module overrides FastMCP.add_tool() to improve conversion of tool function docstrings
into tool descriptions.
It also provides a decorator that MCP tool functions can use to inject session state into their Context parameter.
"""

import dataclasses
import logging
import textwrap
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import Tool
from mcp import types as mt
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from pydantic import BaseModel
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.oauth import ProxyAccessToken
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServerState:
    config: Config
    runtime_info: ServerRuntimeInfo

    @classmethod
    def from_context(cls, ctx: Context) -> 'ServerState':
        server_state = ctx.request_context.lifespan_context
        if not isinstance(server_state, ServerState):
            raise ValueError('ServerState is not available in the context.')
        return server_state


class ForwardSlashMiddleware:
    def __init__(self, app: ASGIApp):
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        LOG.debug(f'ForwardSlashMiddleware: scope={scope}')

        if scope['type'] == 'http':
            path = scope['path']
            if path in ['/sse', '/messages', '/mcp']:
                scope = dict(scope)
                scope['path'] = f'{path}/'

        await self._app(scope, receive, send)


class KeboolaMcpServer(FastMCP):
    def add_tool(self, tool: Tool) -> None:
        """Applies `textwrap.dedent()` function to the tool's docstring, if no explicit description is provided."""
        update = {}
        if tool.description:
            description = textwrap.dedent(tool.description).strip()
            if description != tool.description:
                update['description'] = description
        if not tool.serializer:
            update['serializer'] = _exclude_none_serializer

        if update:
            tool = tool.model_copy(update=update)

        super().add_tool(tool)


def get_http_request_or_none() -> Request | None:
    try:
        return get_http_request()
    except RuntimeError:
        return None


class SessionStateMiddleware(fmw.Middleware):
    """
    FastMCP middleware that manages session state in the Context parameter.

    This middleware sets up the session state containing instances of `KeboolaClient` and `WorkspaceManager`
    in the tool function's Context. These are initialized using the MCP server configuration, which is
    composed of the following parameter sources:

    * Initial configuration obtained from CLI parameters when starting the server
    * Environment variables
    * HTTP headers
    * URL query parameters

    Note: HTTP headers and URL query parameters are only used when the server runs on HTTP-based transport.
    """

    async def on_message(
        self,
        context: fmw.MiddlewareContext[Any],
        call_next: fmw.CallNext[Any, Any],
    ) -> Any:
        """
        Manages session state in the Context parameter. This middleware sets up the session state for all the other
        MCP functions down the chain. It is called for each tool, prompt, resource, etc. calls.

        :param context: Middleware context containing FastMCP context.
        :param call_next: Next middleware in the chain to call.
        :returns: Result from executing the middleware chain.
        """
        ctx = context.fastmcp_context
        assert isinstance(ctx, Context), f'Expecting Context, got {type(ctx)}.'

        if not isinstance(ctx.session, MagicMock):
            server_state = ServerState.from_context(ctx)
            config: Config = server_state.config
            runtime_info: ServerRuntimeInfo = server_state.runtime_info
            accept_secrets_in_url = config.accept_secrets_in_url

            # IMPORTANT: Be careful what functions you use for accessing the HTTP request when handling SSE traffic.
            # The SSE is asynchronous and it maintains two connections for each client.
            # A tool call is requested using 'POST /messages' endpoint, but the tool itself is called outside
            # the scope of this HTTP call and its result is returned as a message on the long-living connection
            # opened by the initial `POST /sse` call.
            #
            # The functions such as fastmcp.server.dependencies.get_http_request() return the HTTP request received
            # on the initial 'POST /sse' endpoint call.
            #
            # The Context.request_context.request is the HTTP request received by the 'POST /messages' endpoint
            # when the tool call was requested by a client.

            if http_rq := get_http_request_or_none():
                LOG.debug(f'Injecting headers: http_rq={http_rq}, headers={http_rq.headers}')
                config = config.replace_by(http_rq.headers)
                if accept_secrets_in_url:
                    LOG.debug(f'Injecting URL query params: http_rq={http_rq}, query_params={http_rq.query_params}')
                    config = config.replace_by(http_rq.query_params)

            if http_rq := ctx.request_context.request:
                if user := http_rq.scope.get('user'):
                    LOG.debug(f'Injecting bearer and SAPI tokens: user={user}, access_token={user.access_token}')
                    assert isinstance(user, AuthenticatedUser), f'Expecting AuthenticatedUser, got: {type(user)}'
                    assert isinstance(
                        user.access_token, ProxyAccessToken
                    ), f'Expecting ProxyAccessToken, got: {type(user.access_token)}'
                    config = dataclasses.replace(
                        config,
                        storage_token=user.access_token.sapi_token,
                        bearer_token=user.access_token.delegate.token,
                    )

            # TODO: We could probably get rid of the 'state' attribute set on ctx.session and just
            #  pass KeboolaClient and WorkspaceManager instances to a tool as extra parameters.
            state = self._create_session_state(config, runtime_info)
            ctx.session.state = state

        try:
            return await call_next(context)
        finally:
            # NOTE: This line is commented following a bug related to session state clearance in Claude client
            # ctx.session.state = {}
            pass

    @classmethod
    def _get_headers(cls, runtime_info: ServerRuntimeInfo) -> dict[str, Any]:
        """
        :param runtime_info: Runtime information
        :return: Additional headers for the requests used for tracing the MCP server
        """
        return {
            'User-Agent': (
                f'Keboola MCP Server/{runtime_info.server_version} app_env={runtime_info.app_env} '
                f'transport={runtime_info.transport}'
            ),
            'MCP-Server-Transport': runtime_info.transport or 'NA',
            'MCP-Server-Versions': (
                f'keboola-mcp-server/{runtime_info.server_version} mcp/{runtime_info.mcp_library_version} '
                f'fastmcp/{runtime_info.fastmcp_library_version}'
            ),
        }

    @classmethod
    def _create_session_state(cls, config: Config, runtime_info: ServerRuntimeInfo) -> dict[str, Any]:
        """Creates `KeboolaClient` and `WorkspaceManager` instances and returns them in the session state."""
        LOG.info(f'Creating SessionState from config: {config}.')

        state: dict[str, Any] = {}
        try:
            if not config.storage_token:
                raise ValueError('Storage API token is not provided.')
            if not config.storage_api_url:
                raise ValueError('Storage API URL is not provided.')
            client = KeboolaClient(
                storage_api_url=config.storage_api_url,
                storage_api_token=config.storage_token,
                bearer_token=config.bearer_token,
                branch_id=config.branch_id,
                headers=cls._get_headers(runtime_info),
            )
            state[KeboolaClient.STATE_KEY] = client
            LOG.info('Successfully initialized Storage API client.')
        except Exception as e:
            LOG.error(f'Failed to initialize Keboola client: {e}')
            raise

        try:
            workspace_manager = WorkspaceManager(client, config.workspace_schema)
            state[WorkspaceManager.STATE_KEY] = workspace_manager
            LOG.info('Successfully initialized Storage API Workspace manager.')
        except Exception as e:
            LOG.error(f'Failed to initialize Storage API Workspace manager: {e}')
            raise

        return state


class ToolsFilteringMiddleware(fmw.Middleware):
    """
    This middleware filters out tools that are not available in the current project. The filtering is based on the
    project features.

    The middleware intercepts the `on_list_tools()` call and removes the unavailable tools
    from the list. The AI assistants should not even see the tools that are not available in the current project.

    The middleware also intercepts the `on_call_tool()` call and raises an exception if a call is attempted to a tool
    that is not available in the current project.
    """

    @staticmethod
    async def get_project_features(ctx: Context) -> set[str]:
        assert isinstance(ctx, Context), f'Expecting Context, got {type(ctx)}.'
        client = KeboolaClient.from_state(ctx.session.state)
        token_info = await client.storage_client.verify_token()
        return set(filter(None, token_info.get('owner', {}).get('features', [])))

    async def on_list_tools(
        self, context: MiddlewareContext[mt.ListToolsRequest], call_next: CallNext[mt.ListToolsRequest, list[Tool]]
    ) -> list[Tool]:
        tools = await call_next(context)
        features = await self.get_project_features(context.fastmcp_context)

        from keboola_mcp_server.tools import search

        if 'global-search' not in features:
            tools = [t for t in tools if t.name != search.SEARCH_TOOL_NAME]

        # TODO: uncomment and adjust when WAII tools are implemented
        # if 'waii-integration' not in features:
        #     tools = [t for t in tools if t.name != 'text_to_sql']

        if 'hide-conditional-flows' in features:
            tools = [t for t in tools if t.name != 'create_conditional_flow']
        else:
            tools = [t for t in tools if t.name != 'create_flow']

        return tools

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        tool = await context.fastmcp_context.fastmcp.get_tool(context.message.name)
        features = await self.get_project_features(context.fastmcp_context)

        if 'global-search' not in features:
            if tool.name == 'search':
                raise ToolError(
                    'The "search" tool is not available in this project. '
                    'Please ask Keboola support to enable "Global Search" feature.'
                )

        # TODO: uncomment and adjust when WAII tools are implemented
        # if 'waii-integration' not in features:
        #     if tool.name == 'text_to_sql':
        #         raise ToolError('The "text_to_sql" tool is not available in this project. '
        #                         'Please ask Keboola support to enable "WAII Integration" feature.')

        # TODO: uncomment and adjust when the conditional flows support is added
        # if 'conditional-flows-disabled' in features:
        #     if tool.name == 'create_conditional_flow':
        #         raise ToolError('The "create_conditional_flow" tool is not available in this project. '
        #                         'Please ask Keboola support to enable "Conditional Flows" feature '
        #                         'or use "create_flow" tool instead.')
        # else:
        #     if tool.name == 'create_flow':
        #         raise ToolError('The "create_flow" tool is not available in this project. '
        #                         'This project uses "Conditional Flows", '
        #                         'please use"create_conditional_flow" tool instead.')

        return await call_next(context)


def _exclude_none_serializer(data: BaseModel) -> str:
    return data.model_dump_json(exclude_none=True, by_alias=False)
