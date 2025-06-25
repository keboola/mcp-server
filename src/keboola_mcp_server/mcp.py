"""
This module overrides FastMCP.add_tool() to improve conversion of tool function docstrings
into tool descriptions.
It also provides a decorator that MCP tool functions can use to inject session state into their Context parameter.
"""
import dataclasses
import inspect
import logging
import textwrap
from dataclasses import dataclass
from functools import wraps
from typing import Any
from unittest.mock import MagicMock

from fastmcp import Context, FastMCP
from fastmcp.server.dependencies import get_http_request
from fastmcp.tools import Tool
from fastmcp.utilities.types import find_kwarg_by_type
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.types import AnyFunction
from pydantic import BaseModel
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.oauth import ProxyAccessToken
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@dataclass
class ServerState:
    config: Config

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
        if tool.description:
            description = textwrap.dedent(tool.description).strip()
            if description != tool.description:
                tool = tool.model_copy(update={'description': description})

        super().add_tool(tool)


def _create_session_state(config: Config) -> dict[str, Any]:
    """Creates `KeboolaClient` and `WorkspaceManager` instances and returns them in the session state."""
    LOG.info(f'Creating SessionState from config: {config}.')

    state: dict[str, Any] = {}
    try:
        if not config.storage_token:
            raise ValueError('Storage API token is not provided.')
        if not config.storage_api_url:
            raise ValueError('Storage API URL is not provided.')
        client = KeboolaClient(config.storage_token, config.storage_api_url, bearer_token=config.bearer_token)
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


def _get_http_request() -> Request | None:
    try:
        return get_http_request()
    except RuntimeError:
        return None


def with_session_state() -> AnyFunction:
    """
    Decorator that injects the session state into the Context parameter of a tool function.

    This decorator dynamically inserts a session state object into the `Context` parameter of a tool function.
    The session state contains instances of `KeboolaClient` and `WorkspaceManager`. These are initialized using
    the MCP server configuration, which is composed of the following parameter sources:

    * Initial configuration obtained from CLI parameters when starting the server
    * Environment variables
    * HTTP headers
    * URL query parameters

    Note: HTTP headers and URL query parameters are only used when the server runs on HTTP-based transport.

    Usage example:
    ```python
    @with_session_state()
    def tool(ctx: Context, ...):
        client = KeboolaClient.from_state(ctx.session.state)
        manager = WorkspaceManager.from_state(ctx.session.state)
    ```
    """
    def _wrapper(fn: AnyFunction) -> AnyFunction:
        """
        :param fn: The tool function to decorate.
        """

        @wraps(fn)
        async def _inject_session_state(*args, **kwargs) -> Any:
            """
            Injects the session state into the Context parameter of the tool function. The injection is executed
            by the MCP server when the annotated tool function is called.
            :param args: Positional arguments of the tool function
            :param kwargs: Keyword arguments of the tool function
            :raises TypeError: If no Context argument is found in the function parameters
            :returns: Result of the tool function
            """
            # finds the Context type argument name in the function parameters
            ctx_kwarg = find_kwarg_by_type(fn, Context)
            if ctx_kwarg is None:
                raise TypeError(
                    'Context argument is required, add "ctx: Context" parameter to the function parameters.'
                )
            # convert positional args to kwargs using inspect.signature in case context is passed as positional arg
            updated_kwargs = inspect.signature(fn).bind(*args, **kwargs).arguments
            ctx = updated_kwargs.get(ctx_kwarg) if ctx_kwarg else None

            if not isinstance(ctx, Context):
                raise TypeError(f'The "ctx" argument must be of type Context, got {type(ctx)}.')

            # This is here to allow mocking the context.session.state in tests.
            if not isinstance(ctx.session, MagicMock):
                config = ServerState.from_context(ctx).config
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

                if http_rq := _get_http_request():
                    LOG.debug(f'Injecting headers: http_rq={http_rq}, headers={http_rq.headers}')
                    config = config.replace_by(http_rq.headers)
                    if accept_secrets_in_url:
                        LOG.debug(f'Injecting URL query params: http_rq={http_rq}, query_params={http_rq.query_params}')
                        config = config.replace_by(http_rq.query_params)

                if http_rq := ctx.request_context.request:
                    if user := http_rq.scope.get('user'):
                        LOG.debug(f'Injecting bearer and SAPI tokens: user={user}, access_token={user.access_token}')
                        assert isinstance(user, AuthenticatedUser), f'Expecting AuthenticatedUser, got: {type(user)}'
                        assert isinstance(user.access_token, ProxyAccessToken), \
                            f'Expecting ProxyAccessToken, got: {type(user.access_token)}'
                        config = dataclasses.replace(
                            config,
                            storage_token=user.access_token.sapi_token,
                            bearer_token=user.access_token.delegate.token
                        )

                # TODO: We could probably get rid of the 'state' attribute set on ctx.session and just
                #  pass KeboolaClient and WorkspaceManager instances to a tool as extra parameters.
                state = _create_session_state(config)
                ctx.session.state = state

            return await fn(*args, **kwargs)

        return _inject_session_state

    return _wrapper


def listing_output_serializer(data: BaseModel) -> str:
    return data.model_dump_json(exclude_none=True, indent=2, by_alias=False)
