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
import os
import textwrap
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from typing import Any, Callable

import anyio
import mcp.types as types
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from mcp import ServerSession, stdio_server
from mcp.server import FastMCP, Server
from mcp.server.lowlevel.server import LifespanResultT
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
from mcp.types import AnyFunction

LOG = logging.getLogger(__name__)

SessionParams = dict[str, str]
SessionState = dict[str, Any]
SessionStateFactory = Callable[[SessionParams], SessionState]


def _default_session_state_factory(params: SessionParams) -> SessionState:
    return params


class StatefulServerSession(ServerSession):
    def __init__(
        self,
        read_stream: MemoryObjectReceiveStream[types.JSONRPCMessage | Exception],
        write_stream: MemoryObjectSendStream[types.JSONRPCMessage],
        init_options: InitializationOptions,
        state: SessionState | None = None,
    ) -> None:
        super().__init__(read_stream, write_stream, init_options)
        self._state = state or {}

    @property
    def state(self) -> SessionState:
        return self._state


class _KeboolaServer(Server):
    def __init__(
        self,
        name: str,
        version: str | None = None,
        instructions: str | None = None,
        lifespan: Callable[['Server'], AbstractAsyncContextManager[LifespanResultT]] | None = None,
    ) -> None:
        super().__init__(name, version=version, instructions=instructions, lifespan=lifespan)

    async def run(
        self,
        read_stream: MemoryObjectReceiveStream[types.JSONRPCMessage | Exception],
        write_stream: MemoryObjectSendStream[types.JSONRPCMessage],
        initialization_options: InitializationOptions,
        # When False, exceptions are returned as messages to the client.
        # When True, exceptions are raised, which will cause the server to shut down
        # but also make tracing exceptions much easier during testing and when using
        # in-process servers.
        raise_exceptions: bool = False,
        state: SessionState | None = None,
    ):
        async with AsyncExitStack() as stack:
            lifespan_context = await stack.enter_async_context(self.lifespan(self))
            session = await stack.enter_async_context(
                StatefulServerSession(read_stream, write_stream, initialization_options, state)
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
        name: str | None = None,
        instructions: str | None = None,
        *,
        session_state_factory: SessionStateFactory | None = None,
        **settings: Any,
    ) -> None:
        super().__init__(name, instructions, **settings)
        self._mcp_server = _KeboolaServer(
            name=self._mcp_server.name,
            instructions=self._mcp_server.instructions,
            lifespan=self._mcp_server.lifespan,
        )
        self._setup_handlers()
        self._session_state_factory = session_state_factory or _default_session_state_factory

    async def run_stdio_async(self) -> None:
        """Run the server using stdio transport."""
        async with stdio_server() as (read_stream, write_stream):
            await self._mcp_server.run(
                read_stream,
                write_stream,
                initialization_options=self._mcp_server.create_initialization_options(),
                state=self._session_state_factory(dict(os.environ)),
            )

    async def run_sse_async(self) -> None:
        """Run the server using SSE transport."""
        import uvicorn
        from starlette.applications import Starlette
        from starlette.requests import Request
        from starlette.routing import Mount, Route

        sse = SseServerTransport('/messages/')

        async def handle_sse(request: Request):
            async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
                await self._mcp_server.run(
                    streams[0],
                    streams[1],
                    initialization_options=self._mcp_server.create_initialization_options(),
                    state=self._session_state_factory(dict(request.query_params)),
                )

        starlette_app = Starlette(
            debug=self.settings.debug,
            routes=[
                Route('/sse', endpoint=handle_sse),
                Mount('/messages/', app=sse.handle_post_message),
                # TODO: add endpoints for health-check and info
            ],
        )

        config = uvicorn.Config(
            starlette_app,
            host=self.settings.host,
            port=self.settings.port,
            log_level=self.settings.log_level.lower(),
        )
        server = uvicorn.Server(config)
        await server.serve()

    def add_tool(
        self,
        fn: AnyFunction,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        super().add_tool(
            fn=fn,
            name=name,
            description=description or textwrap.dedent(fn.__doc__ or '').strip(),
        )
