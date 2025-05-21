import asyncio
import json
import logging
import random
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from multiprocessing import Process
from typing import AsyncGenerator, Awaitable, Callable, Literal

import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.transports import SSETransport, StreamableHttpTransport
from mcp.types import TextContent

from integtests.conftest import ConfigDef
from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components.model import ComponentConfiguration



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
        'transport': 'stdio',
    }
    config = Config.from_dict(setup)

    mocker.patch('keboola_mcp_server.mcp.os.environ', setup)

    server = create_server(config)
    async with Client(server) as client:
        await assert_basic_setup(server, client)
        await assert_mcp_tool_call(client)

