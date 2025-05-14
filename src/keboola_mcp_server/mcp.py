"""
This is the extension of mcp.server.FastMCP and mcp.server.Server classes that allows to attach the "state"
to the SSE session. The state is created by the state factory function that can be plugged in to the MCP server,
and that creates a state which contains arbitrary objects keyed by string identifiers. The factory is given the
query parameters from the HTTP request that initiates the SSE connection.

Example:
def factory(params: HttpRequestParams) -> SessionState:
    return { 'sapi_client': KeboolaClient(params['storage_token']) }

mcp = KeboolaMcpServer(name='SAPI Connector', session_state_factory=factory)

@mcp.tool()
def list_all_buckets(ctx: Context):
    client = ctx.session.state['sapi_client']
    return client.storage_client.buckets.list()

mcp.run(transport='sse')

Issues:
  * The current implementation of FastMCP does not support sending `Context` to the registered
    resources' functions. The parameter is passed only to the registered tools.
"""

import logging
import textwrap
from contextlib import AsyncExitStack
from typing import Any, Callable, Optional

import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from fastmcp import FastMCP
from mcp import ServerSession
from mcp.server.lowlevel.server import Server
from mcp.server.models import InitializationOptions
from mcp.shared.message import SessionMessage
from mcp.types import AnyFunction, ToolAnnotations

LOG = logging.getLogger(__name__)

SessionParams = dict[str, str]
SessionState = dict[str, Any]
SessionStateFactory = Callable[[Optional[SessionParams]], SessionState]


class StatefulServerSession(ServerSession):
    def __init__(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
        write_stream: MemoryObjectSendStream[SessionMessage],
        init_options: InitializationOptions,
        stateless: bool = False,
        state: SessionState | None = None,
        **kwargs: Any,
    ) -> None:
        if stateless:
            LOG.warning(
                'Stateless mode is not supported for StatefulServerSession, stateless parameter will be ignored.'
            )
        super().__init__(read_stream, write_stream, init_options, stateless=False, **kwargs)
        self._state = state or {}

    @property
    def state(self) -> SessionState:
        return self._state


def _default_session_state_factory() -> SessionStateFactory:
    def _(params: SessionParams | None = None) -> SessionState:
        return params or {}

    return _


class _KeboolaServer(Server):

    def __init__(self, session_state_factory: SessionStateFactory | None = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._session_state_factory = session_state_factory or _default_session_state_factory()

    async def run(
        self,
        read_stream: MemoryObjectReceiveStream[SessionMessage | Exception],
        write_stream: MemoryObjectSendStream[SessionMessage],
        initialization_options: InitializationOptions,
        # When False, exceptions are returned as messages to the client.
        # When True, exceptions are raised, which will cause the server to shut down
        # but also make tracing exceptions much easier during testing and when using
        # in-process servers.
        raise_exceptions: bool = False,
        # When True, the server is stateless and
        # clients can perform initialization with any node. The client must still follow
        # the initialization lifecycle, but can do so with any available node
        # rather than requiring initialization for each connection.
        stateless: bool = False,
        **kwargs: Any,
    ):
        """
        This class is overridden to allow passing the session state factory to the session.
        Other approach would be use the session in appropriate place in the code (in tools, etc.), but this
        approach allows to have the session state factory in the server constructor and use it in the tools.
        """
        async with AsyncExitStack() as stack:
            lifespan_context = await stack.enter_async_context(self.lifespan(self))
            session = await stack.enter_async_context(
                StatefulServerSession(
                    read_stream,
                    write_stream,
                    initialization_options,
                    stateless=stateless,
                    state=self._session_state_factory(None),
                    **kwargs,
                )
            )

            async with anyio.create_task_group() as tg:
                async for message in session.incoming_messages:
                    LOG.debug(f'Received message: {message}')

                    tg.start_soon(
                        self._handle_message,
                        message,
                        session,
                        lifespan_context,
                        raise_exceptions,
                    )


class KeboolaMcpServer(FastMCP):
    def __init__(
        self,
        session_state_factory: SessionStateFactory | None = None,
        *args: Any,
        **settings: Any,
    ) -> None:
        super().__init__(*args, **settings)
        self._mcp_server = _KeboolaServer(
            name=self._mcp_server.name,
            instructions=self._mcp_server.instructions,
            lifespan=self._mcp_server.lifespan,
            session_state_factory=session_state_factory,
        )
        self._setup_handlers()

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
