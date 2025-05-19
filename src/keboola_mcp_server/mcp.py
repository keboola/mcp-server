"""
This module extends fastmcp.FastMCP to provide Keboola-specific functionality:

1. Keboola Configuration:
   - Adds a Config object to the MCP server for Keboola-specific settings

2. Session State Management:
   - Maintains state across multiple tool invocations using decorator-based injection
   - Stores Keboola clients (KeboolaClient, WorkspaceManager) in session state
   - Resolves parameters from multiple sources (HTTP headers, query params, environment)
   - Supports different transport types (stdio, HTTP, SSE)
   - The session state is created using the session_state_factory and attached to the Context's session.
Usage:
    @session_state('my_state', session_state_factory)
    def tool(ctx: Context, ...):
        keboola_client = ctx.session.my_state[KeboolaClient.STATE_KEY]
        # Use client to interact with Keboola

Issues:
  * The current implementation of FastMCP does not support using state in `Context` object.
"""

import inspect
import logging
import os
import textwrap
from functools import wraps
from typing import Any, Callable, Literal, Optional

from fastmcp import Context, FastMCP
from fastmcp.server.dependencies import get_http_request
from fastmcp.utilities.types import find_kwarg_by_type
from mcp.types import AnyFunction, ToolAnnotations
from starlette.requests import Request

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.tools.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

TransportType = Literal['stdio', 'sse', 'streamable-http']
SessionParams = dict[str, str]
SessionState = dict[str, Any]
SessionStateFactory = Callable[[Optional[SessionParams]], SessionState]


class KeboolaMcpServer(FastMCP):
    config: Config | None = None

    def add_tool(
        self,
        fn: AnyFunction,
        name: str | None = None,
        description: str | None = None,
        tags: set[str] | None = None,
        annotations: ToolAnnotations | dict[str, Any] | None = None,
    ) -> None:
        super().add_tool(
            fn=fn,
            name=name,
            description=description or textwrap.dedent(fn.__doc__ or '').strip(),
            tags=tags,
            annotations=annotations,
        )

    def set_config(self, config: Optional[Config] = None) -> None:
        self.config = config


def session_state_factory(params: SessionParams | None = None) -> SessionState:
    params = params or {}
    LOG.info(f'Creating SessionState for params: {params.keys() if params else "None"}.')

    cfg = Config.from_dict(params)

    LOG.info(f'Creating SessionState from config: {cfg}.')

    state: SessionState = {}
    # Create Keboola client instance
    try:
        if not cfg.storage_token:
            raise ValueError('Storage token is not provided.')
        client = KeboolaClient(cfg.storage_token, cfg.storage_api_url)
        state[KeboolaClient.STATE_KEY] = client
        LOG.info('Successfully initialized Storage API client.')
    except Exception as e:
        # TODO: When using the server remotelly (http transport), we need to handle the case when the storage token
        # is not provided. We do not want to fail the server when private session state is created.
        LOG.error(f'Failed to initialize Keboola client: {e}')
        raise
    try:
        if not cfg.workspace_schema:
            raise ValueError('Workspace schema is not provided.')
        workspace_manager = WorkspaceManager(client, cfg.workspace_schema)
        state[WorkspaceManager.STATE_KEY] = workspace_manager
        LOG.info('Successfully initialized Storage API Workspace manager.')
    except Exception as e:
        LOG.error(f'Failed to initialize Storage API Workspace manager: {e}')
        raise

    return state


def with_session_state(
    session_state_field_name: str = 'state',
    session_state_factory: SessionStateFactory = session_state_factory
):
    """
    Simplified decorator to inject the session state into the session of the Context parameter of a tool function using
    Keboola-specific session_state_factory in order not to have to specify the default decorator and
    session_state_factory parameters all the time.
    """
    return with_session_state_from(
        session_state_field_name=session_state_field_name,
        session_state_factory=session_state_factory,
        custom_decorator=None,
    )


def with_session_state_from(
    session_state_field_name: str = 'state',
    session_state_factory: SessionStateFactory | None = None,
    custom_decorator: Callable[[str, SessionStateFactory], AnyFunction] | None = None,
) -> Callable[[AnyFunction], AnyFunction]:
    """Decorator to inject the session state into the Context parameter of a tool function.

    This decorator dynamically inserts a session state object into the Context parameter of a tool function.
    The session state is created using the session_state_factory and attached to the Context's session.
    This allows tools to access and modify persistent state across multiple requests.

    :param session_state_field_name: The name of the field in the Context.session object that will hold the
    session state.
    :param session_state_factory: The factory function that creates the session state. If not provided, the
    default factory is used. The factory function can use the `get_context`, `get_http_request` or access the
    `environment` variables to create the session state.
    :param custom_decorator: A custom decorator function that will be used to insert the session state into the
    tool function. If not provided, the default decorator function will be used. To customize this,
    follow the default_decorator pattern bellow.

    example:
    ```python
    @session_state_from('my_state', session_state_factory)
    def tool(ctx: Context, ...):
        ...
        ... = ctx.session.my_state['my_key']  # my_key is inserted by the session_state_factory
    ```
    """
    if callable(session_state_field_name):
        raise TypeError(
            'The @session_state_from decorator was used incorrectly. '
            'Did you forget to call it? Use @session_state_from()'
        )

    session_state_factory = session_state_factory or _default_session_state_factory()

    def default_decorator(session_state_field_name: str, session_state_factory: SessionStateFactory) -> AnyFunction:
        """
        Serves for wrapping the session_state_field_name and session_state_factory parameters due to customization
        of the default decorator.
        """

        def _wrapper(fn: AnyFunction) -> AnyFunction:
            """
            :param fn: The tool function to decorate.
            """

            @wraps(fn)
            async def _inject_session_state(*args, **kwargs) -> Any:
                """
                Injects the session state into the Context parameter in the tool function, only if the function
                has a parameter of a type Context, otherwise it raises an error. If the context already has the
                session state, it is not overridden. It is executed by the MCP server when the annotated tool
                function is called.

                :param kwargs: The arguments of the tool function. Must include the Context type argument.
                :raises TypeError: If the Context argument is not found in the function parameters.
                                    If the session instance is not available in the context.
                :returns: The result of the tool function.
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

                if ctx is None or not hasattr(ctx, 'session'):
                    raise TypeError('The context is undefined or the session instance is not available in the context.')

                if not hasattr(ctx.session, session_state_field_name):
                    # get session params from the request or environment variables
                    params: SessionParams = _get_session_params()
                    # set the session state to the context session
                    state: SessionState = session_state_factory(params)
                    setattr(ctx.session, session_state_field_name, state)
                return await fn(*args, **kwargs)

            return _inject_session_state

        return _wrapper

    decorator = custom_decorator or default_decorator
    return decorator(session_state_field_name, session_state_factory)


def _default_session_state_factory() -> SessionStateFactory:
    def _(params: SessionParams | None = None) -> SessionState:
        return params or {}

    return _


def _safe_get_http_request() -> Optional[Request]:
    """
    Attempts to retrieve the current HTTP request. Returns None if not available.
    """
    try:
        return get_http_request()
    except RuntimeError:
        return None


def _infer_session_params(request: Optional[Request] = None) -> SessionParams:
    """
    Infers session parameters from the current HTTP request if available, or falls back to environment variables.
    :param request: The current HTTP request. If None, falls back to environment variables.
    """
    if request is None:
        # Fallback to environment variables for stdio-based communication because there is no HTTP request
        # So the server is running locally
        return dict(os.environ)
    if Config.contains_required_fields(request.query_params):
        return dict(request.query_params)
    elif Config.contains_required_fields(request.headers):
        return dict(request.headers)
    else:
        LOG.warning('No required fields found in the request. Using empty session params.')
        return {}


def _get_session_params(transport: Optional[TransportType] = None) -> SessionParams:
    """
    Gets or infers the session parameters based on the current HTTP request or environment variables from the given
    transport.
    :param transport: The transport type.
        - 'stdio': Use environment variables.
        - 'streamable-http': Use headers.
        - 'sse': Use headers.
        - None: Infer from the current HTTP request (query params or headers) or from environment variables.
    """
    if transport == 'stdio':
        return dict(os.environ)
    if transport == 'streamable-http':
        request = get_http_request()
        return dict(request.headers)
    if transport == 'sse':
        # SSE transport if explicitly specified, uses query params due to backward compatibility
        request = get_http_request()
        return dict(request.query_params)
    return _infer_session_params(_safe_get_http_request())
