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
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Any, Callable, Literal, Optional, cast

from fastmcp import Context, FastMCP
from fastmcp.server.dependencies import get_http_request
from fastmcp.utilities.types import find_kwarg_by_type
from mcp.types import AnyFunction, ToolAnnotations

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.tools.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

TransportType = Literal['stdio', 'sse', 'streamable-http']
SessionParams = dict[str, str]
SessionState = dict[str, Any]
SessionStateFactory = Callable[[Optional[SessionParams]], SessionState]
SessionParamsFactory = Callable[[Optional[Context]], SessionParams]
SessionStateInjector = Callable[[str, SessionStateFactory, SessionParamsFactory], AnyFunction]


@dataclass
class ServerState:
    config: Config

    @classmethod
    def from_context(cls, ctx: Context) -> 'ServerState':
        life_span = ctx.request_context.lifespan_context
        if not isinstance(life_span, ServerState):
            raise ValueError('ServerState is not available in the context.')
        return life_span


class KeboolaMcpServer(FastMCP):

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


def keboola_session_params_factory(ctx: Optional[Context] = None) -> SessionParams:
    """
    Creates session parameters for the Keboola MCP server, expecting a context with a lifespan context.
    :param ctx: The context of the tool function.
    :returns: The session parameters, if not provided, falls back to the server configuration.
    """
    if ctx:
        life_span = ServerState.from_context(ctx)
        LOG.info(f'Creating SessionParams from server config: {life_span.config}.')
        config = life_span.config
        session_params = _get_session_params(cast(TransportType, config.transport))
        return asdict(config.replace_by(session_params))
    else:
        LOG.info('Inferring SessionParams.')
        session_params = _get_session_params(None)
        return asdict(Config.from_dict(session_params))


def keboola_session_state_factory(params: SessionParams | None = None) -> SessionState:
    """
    Creates a session state for the Keboola MCP server, expecting session parameters.
    :param params: The session parameters.
    :returns: The session state.
    """
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
    session_state_factory: SessionStateFactory = keboola_session_state_factory,
    session_params_factory: SessionParamsFactory = keboola_session_params_factory,
) -> Any:
    """
    Simplified decorator to inject the session state into the session of the Context parameter of a tool function using
    Keboola-specific session_state_factory and session_params_factory in order not to have to specify the default
    values all the time.
    """
    return with_session_state_from(
        session_state_field_name=session_state_field_name,
        session_state_factory=session_state_factory,
        session_params_factory=session_params_factory,
        custom_session_state_injector=None,
    )


def with_session_state_from(
    session_state_field_name: str = 'state',
    session_state_factory: SessionStateFactory | None = None,
    session_params_factory: SessionParamsFactory | None = None,
    custom_session_state_injector: SessionStateInjector | None = None,
) -> AnyFunction:
    """Decorator to inject the session state into the Context parameter of a tool function.

    This decorator dynamically inserts a session state object into the Context parameter of a tool function. Session
    parameters are created using the session_params_factory and the session state is created using the
    session_state_factory. The session state is then attached to the Context's session.
    This allows tools to access and modify persistent state across multiple requests.

    :param session_state_field_name: The name of the field in the Context.session object that will hold the
    session state.
    :param session_state_factory: The factory function that creates the session state. If not provided, the
    default factory is used. The factory function can use the `get_context`, `get_http_request` or access the
    `environment` variables to create the session state.
    :param session_params_factory: The factory function that creates the session parameters. If not provided, the
    default factory is used. The factory function can use the `get_context` to create the session parameters.
    :param custom_session_state_injector: A custom injector function that will be used to insert the session state
    into the tool function. If not provided, the default injector function will be used. To customize this,
    follow the _default_session_state_injector pattern bellow.

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

    session_state_factory = session_state_factory or default_session_state_factory()
    session_params_factory = session_params_factory or default_session_params_factory()
    injector = custom_session_state_injector or default_session_state_injector()
    return injector(session_state_field_name, session_state_factory, session_params_factory)


def default_session_state_factory() -> SessionStateFactory:
    def _(params: SessionParams | None = None) -> SessionState:
        return params or {}

    return _


def default_session_params_factory() -> SessionParamsFactory:
    def _(ctx: Optional[Context] = None) -> SessionParams:
        return _get_session_params()

    return _


def default_session_state_injector() -> SessionStateInjector:
    def _default_session_state_injector(
        session_state_field_name: str,
        session_state_factory: SessionStateFactory,
        session_params_factory: SessionParamsFactory,
    ) -> AnyFunction:
        """
        Wraps the session_state_field_name, session_state_factory and session_params_factory parameters due to
        customization of the default decorator.
        :param session_state_field_name: The name of the field in the Context.session object that will hold the
        session state.
        :param session_state_factory: The factory function that creates the session state.
        :param session_params_factory: The factory function that creates the session parameters (from request...).
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
                :param args: The positional arguments of the tool function.
                :param kwargs: The keyword arguments of the tool function.
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
                    params: SessionParams = session_params_factory(ctx)
                    state: SessionState = session_state_factory(params)
                    setattr(ctx.session, session_state_field_name, state)
                return await fn(*args, **kwargs)

            return _inject_session_state

        return _wrapper

    return _default_session_state_injector


def _infer_session_params() -> SessionParams:
    """
    Infers session parameters. If the request is available, session parameters are inferred from the request headers and
    query params. Otherwise, it falls back to environment variables, since the server is running locally.
    """
    try:
        request = get_http_request()
        LOG.info('Inferring SessionParams from request.')
        return dict(request.headers) | dict(request.query_params)
    except RuntimeError:
        LOG.info('Inferring SessionParams from environment variables.')
        return dict(os.environ)


def _get_session_params(transport: Optional[TransportType] = None) -> SessionParams:
    """
    Gets the session parameters from the given transport, or infers them from the current HTTP request or environment
    variables if the transport is not specified.
    :param transport: The transport type, if none, it is inferred. values: stdio, streamable-http, sse, None
    """
    LOG.info(f'Retrieving SessionParams from transport: {transport}.')

    if not transport:
        return _infer_session_params()

    if transport == 'stdio':
        return dict(os.environ)
    if transport == 'streamable-http':
        request = get_http_request()
        return dict(request.headers) | dict(request.query_params)
    if transport == 'sse':
        # SSE transport if explicitly specified, uses query params due to backward compatibility
        request = get_http_request()
        return dict(request.query_params)
