"""
This module overrides FastMCP.add_tool() to improve conversion of tool function docstrings
into tool descriptions.
It also provides a decorator that MCP tool functions can use to inject session state into their Context parameter.
"""

import inspect
import logging
import os
import textwrap
from dataclasses import dataclass
from functools import wraps
from typing import Any

from fastmcp import Context, FastMCP
from fastmcp.server.dependencies import get_http_request
from fastmcp.utilities.types import find_kwarg_by_type
from mcp.types import AnyFunction, ToolAnnotations
from starlette.requests import Request

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.tools.workspace import WorkspaceManager

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


class KeboolaMcpServer(FastMCP):

    def add_tool(
        self,
        fn: AnyFunction,
        name: str | None = None,
        description: str | None = None,
        tags: set[str] | None = None,
        annotations: ToolAnnotations | dict[str, Any] | None = None,
    ) -> None:
        """Applies `textwrap.dedent()` function to the tool's docstring, if no explicit description is provided."""
        super().add_tool(
            fn=fn,
            name=name,
            description=description or textwrap.dedent(fn.__doc__ or '').strip(),
            tags=tags,
            annotations=annotations,
        )


def _create_session_state(config: Config) -> dict[str, Any]:
    """Creates `KeboolaClient` and `WorkspaceManager` instances and returns them in the session state."""
    LOG.info(f'Creating SessionState from config: {config}.')

    state: dict[str, Any] = {}
    try:
        if not config.storage_token:
            raise ValueError('Storage API token is not provided.')
        if not config.storage_api_url:
            raise ValueError('Storage API URL is not provided.')
        client = KeboolaClient(config.storage_token, config.storage_api_url)
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

            if not getattr(ctx.session, 'state', None):
                # This is here to allow mocking the context.session.state in tests.
                config = ServerState.from_context(ctx).config
                config = config.replace_by(os.environ)
                accept_secrets_in_url = config.accept_secrets_in_url

                if http_rq := _get_http_request():
                    config = config.replace_by(http_rq.headers)
                    if accept_secrets_in_url:
                        config = config.replace_by(http_rq.query_params)

                # TODO: We could probably get rid of the 'state' attribute set on ctx.session and just
                #  pass KeboolaClient and WorkspaceManager instances to a tool as extra parameters.
                state = _create_session_state(config)
                ctx.session.state = state

            return await fn(*args, **kwargs)

        return _inject_session_state

    return _wrapper
