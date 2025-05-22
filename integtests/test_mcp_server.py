import asyncio
import json
import logging
import random
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from multiprocessing import Process
from typing import AsyncGenerator, Awaitable, Callable, Literal

import pytest
from fastmcp import Client, Context, FastMCP
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp.types import TextContent

from integtests.conftest import ConfigDef
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components.model import ComponentConfiguration
from keboola_mcp_server.tools.workspace import WorkspaceManager

AsyncContextServerRemoteRunner = Callable[
    [FastMCP, Literal['sse', 'streamable-http']], _AsyncGeneratorContextManager[str]
]
AsyncContextClientRunner = Callable[
    [Literal['sse', 'streamable-http'], str, dict[str, str] | None], _AsyncGeneratorContextManager[Client]
]


LOG = logging.getLogger(__name__)


@pytest.fixture
def run_server_remote() -> AsyncContextServerRemoteRunner:
    """fixture returning a _run_server_remote function"""

    @asynccontextmanager
    async def _run_server_remote(
        server: FastMCP, transport: Literal['sse', 'streamable-http']
    ) -> AsyncGenerator[str, None]:
        """
        Run the server in a async context manager which will ensure that the server is properly closed after the test.
        The server is created with the given transport and port.
        :yield: The url of the server.
        """

        port = random.randint(8000, 9000)
        proc = Process(target=lambda: asyncio.run(server.run_async(transport=transport, port=port)))
        proc.start()

        if transport == 'sse':
            url = f'http://127.0.0.1:{port}/sse'
        else:
            url = f'http://127.0.0.1:{port}/mcp'

        LOG.info(f'Running server on {url} with transport {transport}')
        try:
            await asyncio.sleep(1.0)  # wait for the server to start
            yield url
        except Exception as e:
            proc.terminate()
            proc.join()
            raise e
        finally:
            proc.terminate()
            proc.join()

    return _run_server_remote


@pytest.fixture
def run_client() -> AsyncContextClientRunner:
    """
    Run the client in a async context manager which will ensure that the client is properly closed after the test.
    The client is created with the given transport and url of the remote server.
    """

    @asynccontextmanager
    async def _run_client(
        transport: Literal['sse', 'streamable-http'], url: str, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[Client, None]:

        if transport == 'sse':
            transport_explicit = SSETransport(url=url)
        else:
            transport_explicit = StreamableHttpTransport(url=url, headers=headers)

        client_explicit = Client(transport_explicit)
        exception_from_client = None

        LOG.info(f'Running client connecting to {url} expecting `{transport}` server transport.')
        async with client_explicit:
            try:
                yield client_explicit
            except Exception as e:
                LOG.error(f'Error in client TaskGroup: {e}')
                exception_from_client = e

        del client_explicit
        if exception_from_client:
            # we need to raise the exception from the client TaskGroup otherwise it will inform only
            # about task group error
            raise exception_from_client

    return _run_client


@pytest.fixture
def assert_basic_setup():
    """
    Assert that the basic setup of the server and client is correct.
    """

    async def _assert_basic_setup(server: FastMCP, client: Client):
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

    return _assert_basic_setup


@pytest.fixture(scope='session')
def assert_mcp_tool_call(configs: list[ConfigDef]):
    """
    Assert that the MCP tool call is correct.
    """
    _component_details_tool_name = 'get_component_details'

    async def _assert_mcp_tool_call(client: Client):
        for config in configs:
            assert config.configuration_id is not None

            tool_result = await client.call_tool(
                _component_details_tool_name,
                {'configuration_id': config.configuration_id, 'component_id': config.component_id},
            )
            assert tool_result is not None
            assert len(tool_result) == 1
            tool_result_content = tool_result[0]
            assert isinstance(tool_result_content, TextContent)  # only one tool call is executed
            component_str = tool_result_content.text
            component_json = json.loads(component_str)

            component_config = ComponentConfiguration.model_validate(component_json)
            assert isinstance(component_config, ComponentConfiguration)
            assert component_config.component_id == config.component_id
            assert component_config.configuration_id == config.configuration_id

            assert component_config.configuration is not None
            assert component_config.component is not None

            assert component_config.component.component_id == config.component_id
            assert component_config.component.component_type is not None
            assert component_config.component.component_name is not None

    return _assert_mcp_tool_call


@pytest.mark.asyncio
async def test_stdio_setup(
    mocker,
    assert_basic_setup: Callable[[FastMCP, Client], Awaitable[None]],
    assert_mcp_tool_call: Callable[[Client], Awaitable[None]],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):

    setup = {
        'storage_token': storage_api_token,
        'workspace_schema': workspace_schema,
        'storage_api_url': storage_api_url,
    }
    config = Config.from_dict(setup)

    mocker.patch('keboola_mcp_server.mcp.os.environ', setup)

    server = create_server(config)
    async with Client(server) as client:
        await assert_basic_setup(server, client)
        await assert_mcp_tool_call(client)


@pytest.mark.asyncio
async def test_sse_setup(
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    assert_basic_setup: Callable[[FastMCP, Client], Awaitable[None]],
    assert_mcp_tool_call: Callable[[Client], Awaitable[None]],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    config = Config.from_dict(
        {
            'storage_api_url': storage_api_url,
        }
    )

    server = create_server(config)
    async with run_server_remote(server, 'sse') as url:
        sse_url = f'{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}'
        async with run_client('sse', sse_url, None) as client:
            await assert_basic_setup(server, client)
            await assert_mcp_tool_call(client)


@pytest.mark.asyncio
@pytest.mark.parametrize('use_header', [True, False])
async def test_http_setup(
    use_header: bool,
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    assert_basic_setup: Callable[[FastMCP, Client], Awaitable[None]],
    assert_mcp_tool_call: Callable[[Client], Awaitable[None]],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):

    # test that storage api is set in the server init
    config = Config.from_dict(
        {
            'storage_api_url': storage_api_url,
        }
    )

    server = create_server(config)
    async with run_server_remote(server, 'streamable-http') as url:
        # if use_header is True, we use the headers to pass the storage_token and workspace_schema,
        if use_header:
            headers = {'storage_token': storage_api_token, 'workspace_schema': workspace_schema}
        else:
            headers = None
            url = f'{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}'

        async with run_client('streamable-http', url, headers) as client:
            await assert_basic_setup(server, client)
            await assert_mcp_tool_call(client)


@pytest.mark.asyncio
async def test_http_multiple_clients(
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    assert_basic_setup: Callable[[FastMCP, Client], Awaitable[None]],
    assert_mcp_tool_call: Callable[[Client], Awaitable[None]],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):

    # we pass empty dict and test if it is set from the headers
    config = Config.from_dict({})

    server = create_server(config)
    async with run_server_remote(server, 'streamable-http') as url:
        headers = {
            'storage_token': storage_api_token,
            'workspace_schema': workspace_schema,
            'storage_api_url': storage_api_url,
        }
        url = url
        async with (
            run_client('streamable-http', url, headers) as client_1,
            run_client('streamable-http', url, headers) as client_2,
            run_client('streamable-http', url, headers) as client_3,
        ):
            await assert_basic_setup(server, client_1)
            await assert_basic_setup(server, client_2)
            await assert_basic_setup(server, client_3)
            await assert_mcp_tool_call(client_1)
            await assert_mcp_tool_call(client_2)
            await assert_mcp_tool_call(client_3)


@pytest.mark.asyncio
async def test_http_multiple_clients_with_different_headers(
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    assert_basic_setup: Callable[[FastMCP, Client], Awaitable[None]],
    storage_api_url: str,
):
    """
    Test that the server can handle multiple clients with different headers and checks the values of the headers.
    """

    config = Config.from_dict({'storage_api_url': storage_api_url})

    headers = {
        'client_1': {'storage_token': 'client_1_storage_token', 'workspace_schema': 'client_1_workspace_schema'},
        'client_2': {'storage_token': 'client_2_storage_token', 'workspace_schema': 'client_2_workspace_schema'},
    }

    @with_session_state()
    async def assessed_function(ctx: Context, which_client: str) -> str:
        storage_token = KeboolaClient.from_state(ctx.session.state).token
        workspace_schema = WorkspaceManager.from_state(ctx.session.state)._workspace_schema
        assert which_client in headers.keys()
        assert storage_token == headers[which_client]['storage_token']
        assert workspace_schema == headers[which_client]['workspace_schema']
        return f'{which_client}'

    server = create_server(config)
    server.add_tool(assessed_function)
    async with run_server_remote(server, 'streamable-http') as url:
        async with (
            run_client('streamable-http', url, headers['client_1']) as client_1,
            run_client('streamable-http', url, headers['client_2']) as client_2,
        ):
            await assert_basic_setup(server, client_1)
            await assert_basic_setup(server, client_2)
            ret_1 = await client_1.call_tool('assessed_function', {'which_client': 'client_1'})
            ret_2 = await client_2.call_tool('assessed_function', {'which_client': 'client_2'})
            assert isinstance(ret_1[0], TextContent) and isinstance(ret_2[0], TextContent)
            assert ret_1[0].text == 'client_1'
            assert ret_2[0].text == 'client_2'


@pytest.mark.asyncio
async def test_http_server_header_and_query_params_client(
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    assert_basic_setup: Callable[[FastMCP, Client], Awaitable[None]],
    assert_mcp_tool_call: Callable[[Client], Awaitable[None]],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):

    config = Config.from_dict({'storage_api_url': storage_api_url})

    server = create_server(config)
    async with run_server_remote(server, 'streamable-http') as url:
        headers = {'storage_token': storage_api_token, 'workspace_schema': workspace_schema}
        url_params = f'{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}'
        async with (
            run_client('streamable-http', url, headers) as client_1,
            run_client('streamable-http', url_params, None) as client_2,
        ):
            await assert_basic_setup(server, client_1)
            await assert_basic_setup(server, client_2)
            await assert_mcp_tool_call(client_1)
            await assert_mcp_tool_call(client_2)
            assert client_1.session != client_2.session
