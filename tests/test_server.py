from dataclasses import asdict
from typing import Annotated, Any

import pytest
from fastmcp import Client, Context
from mcp.types import TextContent
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import ServerState, with_session_state
from keboola_mcp_server.server import create_server
from keboola_mcp_server.workspace import WorkspaceManager


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server(Config())
        tools = await server.get_tools()
        assert sorted(tool.name for tool in tools.values()) == [
            'add_config_row',
            'create_config',
            'create_flow',
            'create_sql_transformation',
            'docs_query',
            'find_component_id',
            'get_bucket',
            'get_component',
            'get_config',
            'get_config_examples',
            'get_flow',
            'get_flow_schema',
            'get_job',
            'get_project_info',
            'get_sql_dialect',
            'get_table',
            'list_buckets',
            'list_configs',
            'list_flows',
            'list_jobs',
            'list_tables',
            'list_transformations',
            'query_data',
            'run_job',
            'update_bucket_description',
            'update_column_description',
            'update_config',
            'update_config_row',
            'update_flow',
            'update_sql_transformation',
            'update_table_description',
        ]

    @pytest.mark.asyncio
    async def test_tools_have_descriptions(self):
        server = create_server(Config())
        tools = await server.get_tools()

        missing_descriptions: list[str] = []
        for tool in tools.values():
            if not tool.description:
                missing_descriptions.append(tool.name)

        missing_descriptions.sort()
        assert not missing_descriptions, f'These tools have no description: {missing_descriptions}'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('config', 'envs'),
    [
        (  # config params in Config class
            Config(
                storage_token='SAPI_1234', storage_api_url='http://connection.sapi', workspace_schema='WORKSPACE_1234'
            ),
            {},
        ),
        (  # config params in the OS environment
            Config(),
            {
                'KBC_STORAGE_TOKEN': 'SAPI_1234',
                'KBC_STORAGE_API_URL': 'http://connection.sapi',
                'KBC_WORKSPACE_SCHEMA': 'WORKSPACE_1234',
            },
        ),
        (  # config params mixed up in both the Config class and the OS environment
            Config(storage_api_url='http://connection.sapi'),
            {'KBC_STORAGE_TOKEN': 'SAPI_1234', 'KBC_WORKSPACE_SCHEMA': 'WORKSPACE_1234'},
        ),
        (  # the OS environment overrides the initial Config class
            Config(storage_token='foo-bar', storage_api_url='http://connection.sapi', workspace_schema='xyz_123'),
            {'KBC_STORAGE_TOKEN': 'SAPI_1234', 'KBC_WORKSPACE_SCHEMA': 'WORKSPACE_1234'},
        ),
        # TODO: Also test values obtained from an HTTP request.
    ],
)
async def test_with_session_state(config: Config, envs: dict[str, Any], mocker):
    expected_param_description = 'Parameter 1 description'

    @with_session_state()
    async def assessed_function(
        ctx: Context, param: Annotated[str, Field(description=expected_param_description)]
    ) -> str:
        """custom text"""
        assert hasattr(ctx.session, 'state')

        keboola_client = KeboolaClient.from_state(ctx.session.state)
        assert keboola_client is not None
        assert keboola_client.token == 'SAPI_1234'

        workspace_manager = WorkspaceManager.from_state(ctx.session.state)
        assert workspace_manager is not None
        assert workspace_manager._workspace_schema == 'WORKSPACE_1234'

        return param

    # mock the environment variables
    os_mock = mocker.patch('keboola_mcp_server.server.os')
    os_mock.environ = envs

    # create MCP server with the initial Config
    mcp = create_server(config)
    tools_count = len(await mcp.get_tools())
    mcp.add_tool(assessed_function, name='assessed-function')

    # running the server as stdio transport through client
    async with Client(mcp) as client:
        tools = await client.list_tools()
        assert len(tools) == tools_count + 1  # plus the one we've added in this test
        assert tools[-1].name == 'assessed-function'
        assert tools[-1].description == 'custom text'
        # check if the inputSchema contains the expected param description
        assert expected_param_description in str(tools[-1].inputSchema)
        result = await client.call_tool('assessed-function', {'param': 'value'})
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('os_environ_params', 'expected_params'),
    [
        # no params in os.environ, tokens as in the config
        ({}, {'storage_token': 'test-storage-token', 'workspace_schema': 'test-workspace-schema'}),
        # params in os.environ, tokens configured from os.environ, missing from the config
        (
            {'storage_token': 'test-storage-token-2'},
            {'storage_token': 'test-storage-token-2', 'workspace_schema': 'test-workspace-schema'},
        ),
    ],
)
async def test_keboola_injection_and_lifespan(
    mocker, os_environ_params: dict[str, str], expected_params: dict[str, str]
):
    """
    Test that the KeboolaClient and WorkspaceManager are injected into the context and that the lifespan of the client
    is managed by the server.
    Test that the ServerState is properly initialized and that the client and workspace are properly disposed of.
    """
    cfg_dict = {
        'storage_token': 'test-storage-token',
        'workspace_schema': 'test-workspace-schema',
        'storage_api_url': 'https://connection.keboola.com',
        'transport': 'stdio',
    }
    config = Config.from_dict(cfg_dict)

    mocker.patch('keboola_mcp_server.server.os.environ', os_environ_params)

    server = create_server(config)

    @with_session_state()
    async def assessed_function(ctx: Context, param: str) -> str:
        assert hasattr(ctx.session, 'state')
        client = KeboolaClient.from_state(ctx.session.state)
        assert isinstance(client, KeboolaClient)
        workspace = WorkspaceManager.from_state(ctx.session.state)
        assert isinstance(workspace, WorkspaceManager)

        # check that the server state config contains the initial params + the environment params
        server_state = ServerState.from_context(ctx)
        assert asdict(server_state.config) == asdict(config) | os_environ_params

        assert client.token == expected_params['storage_token']
        assert workspace._workspace_schema == expected_params['workspace_schema']

        return param

    server.add_tool(assessed_function, name='assessed_function')

    async with Client(server) as client:
        result = await client.call_tool('assessed_function', {'param': 'value'})
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'
