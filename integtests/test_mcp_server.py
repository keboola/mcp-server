import json

import pytest
from fastmcp import Client, Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import TextContent

from integtests.conftest import AsyncContextClientRunner, AsyncContextServerRemoteRunner, ConfigDef
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components.model import Configuration
from keboola_mcp_server.workspace import WorkspaceManager


@pytest.mark.asyncio
async def test_stdio_setup(
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    assert storage_api_token is not None
    assert workspace_schema is not None
    assert storage_api_url is not None

    config = Config(storage_api_url=storage_api_url)
    # We expect getting the credentials from environment variables.

    server = create_server(config)
    component_config = configs[0]
    async with Client(server) as client:
        await _assert_basic_setup(server, client)
        await _assert_get_component_details_tool_call(client, component_config)


@pytest.mark.asyncio
# if use_headers: we pass headers else we pass query params in the url.
@pytest.mark.parametrize('use_headers', [True, False])
async def test_sse_setup(
    mocker,
    use_headers: bool,
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    if use_headers:
        config = Config(storage_api_url=storage_api_url)
    else:
        config = Config(storage_api_url=storage_api_url, accept_secrets_in_url=True)

    # we delete env vars to ensure the server uses http request
    mocker.patch('keboola_mcp_server.server.os.environ', {})

    server = create_server(config)
    component_config = configs[0]

    async with run_server_remote(server, 'sse') as url:
        # test both cases: with headers and without headers using query params
        if use_headers:
            headers = {'storage_token': storage_api_token, 'workspace_schema': workspace_schema}
        else:
            headers = None
            url = f'{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}'

        async with run_client('sse', url, headers) as client:
            await _assert_basic_setup(server, client)
            await _assert_get_component_details_tool_call(client, component_config)


@pytest.mark.asyncio
# if use_headers: we pass headers else we pass query params in the url.
@pytest.mark.parametrize('use_headers', [True, False])
async def test_http_setup(
    mocker,
    use_headers: bool,
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    if use_headers:
        config = Config(storage_api_url=storage_api_url)
    else:
        config = Config(storage_api_url=storage_api_url, accept_secrets_in_url=True)

    # we delete env vars to ensure the server uses http request
    mocker.patch('keboola_mcp_server.server.os.environ', {})

    server = create_server(config)
    component_config = configs[0]
    async with run_server_remote(server, 'streamable-http') as url:
        # test both cases: with headers and without headers using query params
        if use_headers:
            headers = {'storage_token': storage_api_token, 'workspace_schema': workspace_schema}
        else:
            headers = None
            url = f'{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}'

        async with run_client('streamable-http', url, headers) as client:
            await _assert_basic_setup(server, client)
            await _assert_get_component_details_tool_call(client, component_config)


@pytest.mark.asyncio
async def test_http_multiple_clients(
    mocker,
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):

    # we pass empty config and test if it is set from the headers
    config = Config()
    # we delete env vars to ensure the server uses http request
    mocker.patch('keboola_mcp_server.server.os.environ', {})

    server = create_server(config)
    component_config = configs[0]
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
            await _assert_basic_setup(server, client_1)
            await _assert_basic_setup(server, client_2)
            await _assert_basic_setup(server, client_3)
            await _assert_get_component_details_tool_call(client_1, component_config)
            await _assert_get_component_details_tool_call(client_2, component_config)
            await _assert_get_component_details_tool_call(client_3, component_config)


@pytest.mark.asyncio
async def test_http_multiple_clients_with_different_headers(
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    storage_api_url: str,
    storage_api_token: str,
):
    """
    Test that the server can handle multiple clients with different headers and checks the values of the headers.
    """

    config = Config(storage_api_url=storage_api_url)
    # we do not delete env vars, we want the env vars to be overwritten by http request params

    headers = {
        'client_1': {'storage_token': storage_api_token, 'workspace_schema': 'client_1_workspace_schema'},
        'client_2': {'storage_token': storage_api_token, 'workspace_schema': 'client_2_workspace_schema'},
    }

    async def assessed_function(ctx: Context, which_client: str) -> str:
        storage_token = KeboolaClient.from_state(ctx.session.state).token
        workspace_schema = WorkspaceManager.from_state(ctx.session.state)._workspace_schema
        assert which_client in headers.keys()
        assert storage_token == headers[which_client]['storage_token']
        assert workspace_schema == headers[which_client]['workspace_schema']
        return f'{which_client}'

    server = create_server(config)
    server.add_tool(FunctionTool.from_function(assessed_function))

    async with run_server_remote(server, 'streamable-http') as url:
        async with (
            run_client('streamable-http', url, headers['client_1']) as client_1,
            run_client('streamable-http', url, headers['client_2']) as client_2,
        ):
            await _assert_basic_setup(server, client_1)
            await _assert_basic_setup(server, client_2)
            ret_1 = await client_1.call_tool('assessed_function', {'which_client': 'client_1'})
            ret_2 = await client_2.call_tool('assessed_function', {'which_client': 'client_2'})
            assert isinstance(ret_1.content[0], TextContent)
            assert isinstance(ret_2.content[0], TextContent)
            assert ret_1.content[0].text == 'client_1'
            assert ret_2.content[0].text == 'client_2'


@pytest.mark.asyncio
async def test_http_server_header_and_query_params_client(
    mocker,
    run_server_remote: AsyncContextServerRemoteRunner,
    run_client: AsyncContextClientRunner,
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    config = Config(storage_api_url=storage_api_url, accept_secrets_in_url=True)

    # we delete env vars to ensure the server uses http request
    mocker.patch('keboola_mcp_server.server.os.environ', {})

    server = create_server(config)
    component_config = configs[0]

    async with run_server_remote(server, 'streamable-http') as url:
        headers = {'storage_token': storage_api_token, 'workspace_schema': workspace_schema}
        url_params = f'{url}?storage_token={storage_api_token}&workspace_schema={workspace_schema}'
        async with (
            run_client('streamable-http', url, headers) as client_1,
            run_client('streamable-http', url_params, None) as client_2,
        ):
            await _assert_basic_setup(server, client_1)
            await _assert_basic_setup(server, client_2)
            await _assert_get_component_details_tool_call(client_1, component_config)
            await _assert_get_component_details_tool_call(client_2, component_config)


async def _assert_basic_setup(server: FastMCP, client: Client):
    server_tools = await server.get_tools()
    server_prompts = await server.get_prompts()
    server_resources = await server.get_resources()

    client_tools = await client.list_tools()
    client_prompts = await client.list_prompts()
    client_resources = await client.list_resources()

    # in our case we expect the server contains atleast 1 tool
    assert len(server_tools) > 0
    assert len(server_prompts) > 0

    # ignore 'search' tool which may or may not be exposed based on the testing project's features
    # ignore 'create_flow' and 'create_conditional_flow' since they are dependent on project settings
    client_tool_names = sorted(
        t.name for t in client_tools if t.name not in ['search', 'create_flow', 'create_conditional_flow']
    )

    server_tool_names = sorted(
        t for t in server_tools.keys() if t not in ['search', 'create_flow', 'create_conditional_flow']
    )
    assert client_tool_names == server_tool_names
    assert len(client_prompts) == len(server_prompts)
    assert all(expected == ret_prompt.name for expected, ret_prompt in zip(server_prompts.keys(), client_prompts))
    assert len(client_resources) == len(server_resources)
    assert all(
        expected == ret_resource.name for expected, ret_resource in zip(server_resources.keys(), client_resources)
    )


async def _assert_get_component_details_tool_call(client: Client, config: ConfigDef):
    assert config.configuration_id is not None

    tool_result = await client.call_tool(
        'get_config',
        {'configuration_id': config.configuration_id, 'component_id': config.component_id},
    )

    assert tool_result is not None
    assert len(tool_result.content) == 1
    tool_result_content = tool_result.content[0]
    assert isinstance(tool_result_content, TextContent)  # only one tool call is executed
    component_str = tool_result_content.text
    component_json = json.loads(component_str)

    component_config = Configuration.model_validate(component_json)
    assert isinstance(component_config, Configuration)
    assert component_config.component is not None
    assert component_config.component.component_id == config.component_id
    assert component_config.component.component_type is not None
    assert component_config.component.component_name is not None

    assert component_config.configuration_root is not None
    assert component_config.configuration_root.configuration_id == config.configuration_id

    assert component_config.configuration_rows is None
