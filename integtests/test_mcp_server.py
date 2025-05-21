import asyncio
import threading
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import asdict
from typing import Any, AsyncGenerator, Callable, Generator, Literal, Optional

import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp import Tool
from mcp.server.auth.provider import OAuthAuthorizationServerProvider
from mcp.server.lowlevel.server import LifespanResultT

from integtests.conftest import BucketDef
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.storage import retrieve_buckets


@pytest.fixture(scope='session')
def live_server_sse(event_loop) -> Generator[tuple[str, FastMCP], Any, None]:
    """
    Spins up the Flask development server in a separate thread.
    Yields the URL of the running server.
    """
    port = 5001  # Use a different port to avoid conflicts
    url = f"http://127.0.0.1:{port}/sse"
    server = create_server(
        Config.from_dict(
            {
                'storage_api_url': 'https://connection.keboola.com',
                'storage_token': 'test',  # should not work
                'workspace_schema': 'test',
            }
        )
    )
    server_thread = threading.Thread(target=lambda: asyncio.run(server.run_async(transport='sse', port=port)))
    server_thread.daemon = True  # Allow the main program to exit even if thread is running
    server_thread.start()

    yield url, server


class TestMcpServer:

    def init_server(
        self,
        name: str | None = None,
        instructions: str | None = None,
        auth_server_provider: OAuthAuthorizationServerProvider[Any, Any, Any] | None = None,
        lifespan: (
            Callable[
                [FastMCP[LifespanResultT]],
                AbstractAsyncContextManager[LifespanResultT],
            ]
            | None
        ) = None,
        tags: set[str] | None = None,
        tool_serializer: Callable[[Any], str] | None = None,
        **settings: Any,
    ) -> KeboolaMcpServer:
        server = KeboolaMcpServer(
            name=name,
            instructions=instructions,
            auth_server_provider=auth_server_provider,
            lifespan=lifespan,
            tags=tags,
            tool_serializer=tool_serializer,
            **settings,
        )
        return server

    async def assert_basic_setup(self, server: FastMCP, client: Client):
        server_tools = await server.get_tools()
        server_prompts = await server.get_prompts()
        server_resources = await server.get_resources()

        client_tools = await client.list_tools()
        client_prompts = await client.list_prompts()
        client_resources = await client.list_resources()

        assert len(client_tools) == len(server_tools)
        assert len(client_prompts) == len(server_prompts)
        assert len(client_resources) == len(server_resources)
        assert all(expected == ret_tool.name for expected, ret_tool in zip(server_tools.keys(), client_tools))
        assert all(expected == ret_prompt.name for expected, ret_prompt in zip(server_prompts.keys(), client_prompts))
        assert all(
            expected == ret_resource.name for expected, ret_resource in zip(server_resources.keys(), client_resources)
        )

    @asynccontextmanager
    async def run_server_remote(
        self, fastmcp: FastMCP, transport: Literal['sse', 'streamable-http'], port: int = 8000
    ) -> AsyncGenerator[str, None]:
    
        server_thread = threading.Thread(target=lambda: asyncio.run(fastmcp.run_async(transport=transport, port=port)))
        server_thread.daemon = True # Allow the main program to exit even if thread is running
        server_thread.start()

        if transport == 'sse':
            url = f"http://127.0.0.1:{port}/sse"
        else:
            url = f"http://127.0.0.1:{port}/mcp"

        await asyncio.sleep(1.0) # wait for the server to start
        yield url
        server_thread.join(timeout=1.0)
        
        # End the server thread
        

    def test_init_server(self):
        server = self.init_server(name='Test MCP Server')
        assert server is not None

    @pytest.mark.asyncio
    async def test_sse_setup(self, storage_api_token: str, workspace_schema: str, storage_api_url: str):
        config = Config.from_dict(
            {
                'storage_api_url': storage_api_url,
                'storage_token': storage_api_token,
                'workspace_schema': workspace_schema,
            }
        )

        server = create_server(config)
        async with self.run_server_remote(server, 'sse') as url:
            sse_url = f"{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}"
            transport_explicit = SSETransport(url=sse_url)
            client_explicit = Client(transport_explicit)
            async with client_explicit:
                await self.assert_basic_setup(server, client_explicit)
            del client_explicit


    @pytest.mark.asyncio
    async def test_http_setup(self, storage_api_token: str, workspace_schema: str, storage_api_url: str):

        # Server is created with storage_api_url only, so we need to set the storage_token and workspace_schema
        # in the headers
        config = Config.from_dict(
            {
                'storage_api_url': storage_api_url,
            }
        )

        server = create_server(config)

        async def run_client(client: Client, server: FastMCP):

            async with client:
                await self.assert_basic_setup(server, client)

        
        async with self.run_server_remote(server, 'streamable-http', 8001) as url:
            print(url)
            headers = {"storage_token": storage_api_token,
                    "workspace_schema": workspace_schema}
            transport_explicit = StreamableHttpTransport(url=url, headers=headers)
            client_explicit = Client(transport_explicit)
            await run_client(client_explicit, server)
            del client_explicit
                
                

        #    async for (server_task, url) in self.run_server_remote(event_loop, server, 'sse', 5001):
        #     sse_url = f"{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}"
        #     transport_explicit = SSETransport(url=sse_url)
        #     client_explicit = Client(transport_explicit)
        #     async with client_explicit:
        #         await self.assert_basic_setup(server, client_explicit)

        # async with self.run_server_
        # remote(event_loop, server, 'sse', 5001) as (server_task, url):
        #     sse_url = f"{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}"
        # sse_url = f"{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}"
        # transport_explicit = SSETransport(url=sse_url)
        # client_explicit = Client(transport_explicit)

        # async with client_explicit:
        #     await self.assert_basic_setup(server, client_explicit)

        # # async with Client(sse_url) as client:
        # #     print("jere")
        # #     await self.assert_basic_setup(server, client)
        # print(sse_url)

        # try:
        #     await asyncio.wait_for(run_client(client_explicit, server), timeout=5.0)
        #     import requests
        #     server_task.cancel()
        #     print(requests.get('http://localhost:8001/sse/shutdown'))

        # except asyncio.TimeoutError:
        #     pytest.fail("Client task did not complete within 5 seconds. Probably error in the server or client setup.")

    # @pytest.mark.asyncio
    # async def test_stdio_setup(self,
    #     mocker,
    #     storage_api_url: str,
    #     storage_api_token: str,
    #     workspace_schema: str,
    #     # buckets: list[BucketDef],
    #     # keboola_project: str,
    #     ):

    #     config = Config.from_dict(
    #         {
    #             'storage_api_url': storage_api_url,
    #             'storage_token': storage_api_token,
    #             'workspace_schema': workspace_schema,
    #         }
    #     )

    #     server = create_server(config)
    #     tools = await server.get_tools()
    #     prompts = await server.get_prompts()
    #     resources = await server.get_resources()
    #     assert server is not None

    #     # following code creates server with stdio transport and connects via client to it
    #     async with Client(server) as client:
    #         await self.assert_basic_setup(server, client)
    #         # check tools:
    #         _returned_buckets = await client.call_tool(retrieve_buckets.__name__, {})
    #         assert len(_returned_buckets) == 1 # we expect only one call to the tool (encapsulated client result)
    #         assert isinstance(_returned_buckets[0], mcp.types.TextContent)
    #         returned_buckets = _returned_buckets[0].text
    #         # check if all expected bucket ids are in the returned buckets
    #         assert all(expected.bucket_id in returned_buckets for expected in buckets)
