import asyncio
import json
import logging
import os
import random
import subprocess
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Literal

import pytest
from fastmcp import Client
from fastmcp.client import SSETransport, StdioTransport, StreamableHttpTransport
from mcp.types import TextContent

from integtests.conftest import (
    DEV_STORAGE_API_URL_ENV_VAR,
    DEV_STORAGE_TOKEN_ENV_VAR,
    DEV_WORKSPACE_SCHEMA_ENV_VAR,
    ConfigDef,
)
from keboola_mcp_server.tools.components.model import Configuration
from keboola_mcp_server.tools.project import ProjectInfo

LOG = logging.getLogger(__name__)
HttpTransportStr = Literal['sse', 'streamable-http']


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

    transport = StdioTransport(
        command='python',
        args=[
            '-m',
            'keboola_mcp_server',
            '--api-url',
            storage_api_url,
            '--storage-token',
            storage_api_token,
            '--workspace-schema',
            workspace_schema,
        ],
        env={},  # make sure no env vars are passed from the test environment
    )
    component_config = configs[0]
    async with Client(transport) as client:
        await _assert_basic_setup(client)
        await _assert_get_component_details_tool_call(client, component_config)


@pytest.mark.asyncio
@pytest.mark.parametrize('transport', ['sse', 'streamable-http'])
async def test_remote_setup(
    transport: HttpTransportStr,
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    assert storage_api_token is not None
    assert workspace_schema is not None
    assert storage_api_url is not None

    component_config = configs[0]
    async with _run_server_remote(storage_api_url, transport) as url:
        # test both cases: with headers and without headers using query params
        headers = {'storage_token': storage_api_token, 'workspace_schema': workspace_schema}
        async with _run_client(transport, url, headers) as client:
            await _assert_basic_setup(client)
            await _assert_get_component_details_tool_call(client, component_config)


@pytest.mark.asyncio
async def test_http_multiple_clients(
    configs: list[ConfigDef],
    storage_api_token: str,
    workspace_schema: str,
    storage_api_url: str,
):
    transport: HttpTransportStr = 'streamable-http'
    component_config = configs[0]
    async with _run_server_remote(storage_api_url, transport) as url:
        headers = {
            'storage_token': storage_api_token,
            'workspace_schema': workspace_schema,
            'storage_api_url': storage_api_url,
        }
        async with (
            _run_client(transport, url, headers) as client_1,
            _run_client(transport, url, headers) as client_2,
            _run_client(transport, url, headers) as client_3,
        ):
            await _assert_basic_setup(client_1)
            await _assert_basic_setup(client_2)
            await _assert_basic_setup(client_3)
            await _assert_get_component_details_tool_call(client_1, component_config)
            await _assert_get_component_details_tool_call(client_2, component_config)
            await _assert_get_component_details_tool_call(client_3, component_config)


@pytest.mark.asyncio
async def test_http_multiple_clients_with_different_headers(
    storage_api_url: str,
    storage_api_token: str,
    workspace_schema: str,
    storage_api_token_2: str | None,
    workspace_schema_2: str | None,
):
    """
    Test that the server can handle multiple clients with different headers and checks the values of the headers.
    """
    if not storage_api_token_2 or not workspace_schema_2:
        pytest.skip('No SAPI token or workspace schema for the second client. Skipping test.')

    headers = {
        'client_1': {'storage_token': storage_api_token, 'workspace_schema': workspace_schema},
        'client_2': {'storage_token': storage_api_token_2, 'workspace_schema': workspace_schema_2},
    }

    transport: HttpTransportStr = 'streamable-http'
    async with _run_server_remote(storage_api_url, transport) as url:
        async with (
            _run_client(transport, url, headers['client_1']) as client_1,
            _run_client(transport, url, headers['client_2']) as client_2,
        ):
            await _assert_basic_setup(client_1)
            await _assert_basic_setup(client_2)

            response_1 = await client_1.call_tool('get_project_info')
            project_info_1 = ProjectInfo.model_validate(response_1.structured_content)
            project_info_1.project_id = storage_api_token.split(sep='-')[0]
            LOG.info(f'project_info_1={project_info_1}')

            response_2 = await client_2.call_tool('get_project_info')
            project_info_2 = ProjectInfo.model_validate(response_2.structured_content)
            project_info_2.project_id = storage_api_token_2.split(sep='-')[0]
            LOG.info(f'project_info_2={project_info_2}')


async def _assert_basic_setup(client: Client):
    tools = await client.list_tools()
    # the create_conditional_flow, create_flow and search tools may not be present based on the testing project
    expected_tools = {
        'add_config_row',
        # 'create_conditional_flow',
        'create_config',
        # 'create_flow',
        'create_oauth_url',
        'create_sql_transformation',
        'deploy_data_app',
        'docs_query',
        'find_component_id',
        'get_bucket',
        'get_component',
        'get_config',
        'get_config_examples',
        'get_data_apps',
        'get_flow',
        'get_flow_examples',
        'get_flow_schema',
        'get_job',
        'get_project_info',
        'get_table',
        'list_buckets',
        'list_configs',
        'list_flows',
        'list_jobs',
        'list_tables',
        'modify_data_app',
        'query_data',
        'run_job',
        # 'search',
        'update_config',
        'update_config_row',
        'update_descriptions',
        'update_flow',
        'update_sql_transformation',
    }
    actual_tools = {tool.name for tool in tools}
    missing_tools = expected_tools - actual_tools
    assert not missing_tools, f'Missing tools: {missing_tools}'

    prompts = await client.list_prompts()
    assert len(prompts) == 6

    # there are no resources exposed in the MCP server; just check that the call succeeds
    resources = await client.list_resources()
    assert len(resources) == 0


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


@asynccontextmanager
async def _run_server_remote(storage_api_url: str, transport: HttpTransportStr) -> AsyncGenerator[str, None]:
    """
    Run the server in a subprocess.
    :param storage_api_url: The Storage API URL to use.
    :param transport: The transport to use.
    :return: The url of the remote server.
    """

    port = random.randint(8000, 9000)
    p = subprocess.Popen(
        [
            'python',
            '-m',
            'keboola_mcp_server',
            '--transport',
            transport,
            '--api-url',
            storage_api_url,
            '--port',
            str(port),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={
            name: val
            for name, val in os.environ.items()
            if name not in [DEV_STORAGE_API_URL_ENV_VAR, DEV_STORAGE_TOKEN_ENV_VAR, DEV_WORKSPACE_SCHEMA_ENV_VAR]
        },
    )
    try:
        if transport == 'sse':
            url = f'http://127.0.0.1:{port}/sse'
        elif transport == 'streamable-http':
            url = f'http://127.0.0.1:{port}/mcp'
        else:
            raise ValueError(f'Unknown transport: {transport}')

        LOG.info(f'Running MCP server in subprocess listening on {url} with {transport} transport.')
        await asyncio.sleep(5)  # wait for the server to start
        yield url
    finally:
        LOG.info('Terminating MCP server subprocess.')
        p.terminate()
        stdout, stderr = p.communicate()
        LOG.info(f'-- MCP server stdout --\n{stdout}\n-- end stdout --')
        LOG.info(f'-- MCP server stderr --\n{stderr}\n-- end stderr --')


@asynccontextmanager
async def _run_client(
    transport: HttpTransportStr, url: str, headers: dict[str, str] | None = None
) -> AsyncGenerator[Client, None]:
    """
    Run the client in an async context manager which will ensure that the client is properly closed after the test.
    The client is created with the given transport and connected to the url of the remote server with which it
    communicates.
    :param transport: The transport of the server to which the client will be connected.
    :param url: The url of the remote server to which the client will be connected.
    :param headers: The headers to use for the client.
    :return: The Client connected to the remote server.
    """
    if transport == 'sse':
        transport_explicit = SSETransport(url=url, headers=headers)
    elif transport == 'streamable-http':
        transport_explicit = StreamableHttpTransport(url=url, headers=headers)
    else:
        raise ValueError(f'Unknown transport: {transport}')

    client_explicit = Client(transport_explicit)
    exception_from_client = None

    LOG.info(f'Running MCP client connecting to {url} and expecting `{transport}` server transport.')
    try:
        async with client_explicit:
            try:
                yield client_explicit
            except Exception as e:
                LOG.error(f'Error in client TaskGroup: {e}')
                exception_from_client = e
                # we need to keep an exception from the client TaskGroup and raise it
                # outside the context manager, otherwise it will inform only about task group error
    finally:
        del client_explicit
        if isinstance(exception_from_client, Exception):
            raise exception_from_client
