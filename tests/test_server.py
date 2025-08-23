import json
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any

import pytest
from fastmcp import Client, Context
from fastmcp.tools import FunctionTool
from mcp.types import TextContent
from pydantic import Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components.tools import COMPONENT_TOOLS_TAG
from keboola_mcp_server.tools.doc import DOC_TOOLS_TAG
from keboola_mcp_server.tools.flow.tools import FLOW_TOOLS_TAG
from keboola_mcp_server.tools.jobs import JOB_TOOLS_TAG
from keboola_mcp_server.tools.oauth import OAUTH_TOOLS_TAG
from keboola_mcp_server.tools.project import PROJECT_TOOLS_TAG
from keboola_mcp_server.tools.search import SEARCH_TOOLS_TAG
from keboola_mcp_server.tools.sql import SQL_TOOLS_TAG
from keboola_mcp_server.tools.storage import STORAGE_TOOLS_TAG
from keboola_mcp_server.workspace import WorkspaceManager


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server(Config())
        tools = await server.get_tools()
        assert sorted(tool.name for tool in tools.values()) == [
            'add_config_row',
            'create_conditional_flow',
            'create_config',
            'create_flow',
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
            'list_transformations',
            'modify_data_app',
            'query_data',
            'run_job',
            'search',
            'update_config',
            'update_config_row',
            'update_description',
            'update_flow',
            'update_sql_transformation',
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
    async def test_tools_input_schema(self):
        server = create_server(Config())
        tools = await server.get_tools()

        missing_properties: list[str] = []
        missing_type: list[str] = []
        missing_default: list[str] = []
        for tool in tools.values():
            properties = tool.parameters['properties']
            if not properties:
                missing_properties.append(tool.name)
                continue

            required = tool.parameters.get('required') or []
            for prop_name, prop_def in properties.items():
                if 'type' not in prop_def:
                    missing_type.append(f'{tool.name}.{prop_name}')
                if prop_name not in required and 'default' not in prop_def:
                    missing_default.append(f'{tool.name}.{prop_name}')

        missing_properties.sort()
        assert missing_properties == ['get_project_info', 'list_buckets']
        missing_type.sort()
        assert not missing_type, f'These tool params have no "type" info: {missing_type}'
        missing_default.sort()
        assert not missing_default, f'These tool params are optional, but have no default value: {missing_default}'


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

    mocker.patch(
        'keboola_mcp_server.clients.client.AsyncStorageClient.verify_token',
        return_value={'owner': {'features': ['global-search', 'waii-integration', 'hide-conditional-flows']}},
    )

    # create MCP server with the initial Config
    mcp = create_server(config)
    tools_count = len(await mcp.get_tools())
    mcp.add_tool(FunctionTool.from_function(assessed_function, name='assessed-function'))

    # running the server as stdio transport through client
    async with Client(mcp) as client:
        tools = await client.list_tools()
        # plus the one we've added in this test minus one for filtering create_flow()
        assert len(tools) == tools_count + 1 - 1
        assert tools[-1].name == 'assessed-function'
        assert tools[-1].description == 'custom text'
        # check if the inputSchema contains the expected param description
        assert expected_param_description in str(tools[-1].inputSchema)
        result = await client.call_tool('assessed-function', {'param': 'value'})
        assert isinstance(result.content[0], TextContent)
        assert result.content[0].text == 'value'


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
    mocker.patch(
        'keboola_mcp_server.clients.client.AsyncStorageClient.verify_token',
        return_value={'owner': {'features': ['global-search', 'waii-integration', 'conditional-flows']}},
    )

    server = create_server(config)

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

    server.add_tool(FunctionTool.from_function(assessed_function, name='assessed_function'))

    async with Client(server) as client:
        result = await client.call_tool('assessed_function', {'param': 'value'})
        assert isinstance(result.content[0], TextContent)
        assert result.content[0].text == 'value'


@pytest.mark.asyncio
async def test_tool_annotations_and_tags():
    """
    Test that the tool annotations are properly set.
    """
    server = create_server(Config())
    tools = await server.get_tools()
    for tool in tools.values():
        assert tool.tags is not None, f'{tool.name} has no tags'
        if tool.annotations is not None:
            if tool.annotations.readOnlyHint:
                assert tool.annotations.destructiveHint is None, f'{tool.name} has destructiveHint'
                assert tool.annotations.idempotentHint is None, f'{tool.name} has idempotentHint'
            elif tool.annotations.destructiveHint:
                assert tool.annotations.readOnlyHint is None, f'{tool.name} has readOnlyHint'
            elif tool.annotations.destructiveHint is False:
                assert tool.annotations.idempotentHint is None, f'{tool.name} has idempotentHint'
            if tool.annotations.idempotentHint:
                assert tool.annotations.readOnlyHint is None, f'{tool.name} has readOnlyHint'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('tool_name', 'expected_readonly', 'expected_destructive', 'expected_idempotent', 'tags'),
    [
        # components
        ('get_component', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('get_config', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('list_configs', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('get_config_examples', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('create_config', None, False, None, {COMPONENT_TOOLS_TAG}),
        ('update_config', None, True, None, {COMPONENT_TOOLS_TAG}),
        ('add_config_row', None, False, None, {COMPONENT_TOOLS_TAG}),
        ('update_config_row', None, True, None, {COMPONENT_TOOLS_TAG}),
        ('list_transformations', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('create_sql_transformation', None, False, None, {COMPONENT_TOOLS_TAG}),
        ('update_sql_transformation', None, True, None, {COMPONENT_TOOLS_TAG}),
        # storage
        ('get_bucket', True, None, None, {STORAGE_TOOLS_TAG}),
        ('list_buckets', True, None, None, {STORAGE_TOOLS_TAG}),
        ('get_table', True, None, None, {STORAGE_TOOLS_TAG}),
        ('list_tables', True, None, None, {STORAGE_TOOLS_TAG}),
        ('update_description', None, True, None, {STORAGE_TOOLS_TAG}),
        # flows
        ('create_flow', None, False, None, {FLOW_TOOLS_TAG}),
        ('create_conditional_flow', None, False, None, {FLOW_TOOLS_TAG}),
        ('list_flows', True, None, None, {FLOW_TOOLS_TAG}),
        ('update_flow', None, True, None, {FLOW_TOOLS_TAG}),
        ('get_flow', True, None, None, {FLOW_TOOLS_TAG}),
        ('get_flow_examples', True, None, None, {FLOW_TOOLS_TAG}),
        ('get_flow_schema', True, None, None, {FLOW_TOOLS_TAG}),
        # sql
        ('query_data', True, None, None, {SQL_TOOLS_TAG}),
        # jobs
        ('get_job', True, None, None, {JOB_TOOLS_TAG}),
        ('list_jobs', True, None, None, {JOB_TOOLS_TAG}),
        ('run_job', None, True, None, {JOB_TOOLS_TAG}),
        # project/doc/search
        ('get_project_info', True, None, None, {PROJECT_TOOLS_TAG}),
        ('docs_query', True, None, None, {DOC_TOOLS_TAG}),
        ('search', True, None, None, {SEARCH_TOOLS_TAG}),
        ('find_component_id', True, None, None, {SEARCH_TOOLS_TAG}),
        # oauth
        ('create_oauth_url', None, True, None, {OAUTH_TOOLS_TAG}),
    ],
)
async def test_tool_annotations_tags_values(
    tool_name: str,
    expected_readonly: bool | None,
    expected_destructive: bool | None,
    expected_idempotent: bool | None,
    tags: set[str],
) -> None:
    """
    Test that the tool annotations are having the expected values.
    """
    server = create_server(Config())
    tools = await server.get_tools()

    # check tool registration
    assert tool_name in tools, f'Missing tool registered: {tool_name}'

    # check annotations
    tool = tools[tool_name]
    if all(exp_val is None for exp_val in (expected_readonly, expected_destructive, expected_idempotent)):
        assert tool.annotations is None, f'{tool_name} has annotations'
    else:
        assert tool.annotations is not None, f'{tool_name} has no annotations'
        assert tool.annotations.readOnlyHint is expected_readonly, f'{tool_name}.readOnlyHint mismatch'
        assert tool.annotations.destructiveHint is expected_destructive, f'{tool_name}.destructiveHint mismatch'
        assert tool.annotations.idempotentHint is expected_idempotent, f'{tool_name}.idempotentHint mismatch'

    # check tags
    assert tool.tags == tags, f'{tool_name} tags mismatch'


def test_json_logging(mocker):
    log_config_file = Path(__file__).parent.parent / 'logging-json.conf'
    assert log_config_file.is_file(), f'No logging config file found at {log_config_file.absolute()}'

    # start the MCP server process with json logging
    p = subprocess.Popen(
        ['python', '-m', 'keboola_mcp_server', '--transport', 'sse', '--log-config', log_config_file.absolute()],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    # give the server time to fully start
    time.sleep(5)

    # kill the server and capture streams
    p.terminate()
    stdout, stderr = p.communicate()

    # there is only one handler (the root one) in logging-json.conf which sends messages to stdout
    assert stderr == ''

    # all messages should be JSON-formatted, including those logged by FastMCP loggers
    top_names: set[str] = set()
    fastmcp_startup_message: dict[str, Any] | None = None
    for line in stdout.splitlines():
        message = json.loads(line)
        name = message['name']
        if message['message'].startswith('Starting MCP server') and name.startswith('FastMCP.fastmcp'):
            fastmcp_startup_message = message
        top_names.add(name.split('.')[0])

    assert sorted(top_names) == ['FastMCP', 'keboola_mcp_server', 'uvicorn']
    assert fastmcp_startup_message is not None
