import asyncio
import base64
import json
import subprocess
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any
from unittest.mock import MagicMock

import httpx
import pytest
from fastmcp import Client, Context, FastMCP
from fastmcp.client import StreamableHttpTransport
from fastmcp.tools import FunctionTool
from mcp.types import TextContent
from pydantic import Field
from starlette.exceptions import HTTPException
from starlette.requests import Request

from keboola_mcp_server import cli
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import (
    ServerState,
    SessionStateMiddleware,
    _exclude_none_serializer,
    toon_serializer,
    toon_serializer_compact,
)
from keboola_mcp_server.server import CustomRoutes, create_server
from keboola_mcp_server.tools.components.tools import COMPONENT_TOOLS_TAG
from keboola_mcp_server.tools.constants import CONFIG_DIFF_PREVIEW_TAG
from keboola_mcp_server.tools.data_apps import DATA_APP_TOOLS_TAG
from keboola_mcp_server.tools.doc import DOC_TOOLS_TAG
from keboola_mcp_server.tools.flow.tools import FLOW_TOOLS_TAG
from keboola_mcp_server.tools.jobs import JOB_TOOLS_TAG
from keboola_mcp_server.tools.oauth import OAUTH_TOOLS_TAG
from keboola_mcp_server.tools.project import PROJECT_TOOLS_TAG
from keboola_mcp_server.tools.search import SEARCH_TOOLS_TAG
from keboola_mcp_server.tools.semantic import SEMANTIC_TOOLS_TAG
from keboola_mcp_server.tools.sql import SQL_TOOLS_TAG
from keboola_mcp_server.tools.storage.tools import STORAGE_TOOLS_TAG
from keboola_mcp_server.workspace import WorkspaceManager


class TestServer:
    def test_create_server_fails_loudly_on_malformed_env_workspace_id(self, monkeypatch) -> None:
        """`create_server()` merges `os.environ` into the config via `Config.replace_by()`
        (KBC_WORKSPACE_ID is trusted operator input, not an untrusted per-request header) -- a
        malformed value here must crash startup, the same way a malformed `--workspace-id` CLI
        flag already does, instead of silently starting the server unpinned."""
        monkeypatch.setenv('KBC_WORKSPACE_ID', 'not-a-valid-id')
        with pytest.raises(ValueError, match='Invalid workspace_id'):
            create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))

    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        tools = await server.list_tools(run_middleware=False)
        assert sorted(tool.name for tool in tools) == [
            'add_config_row',
            'create_conditional_flow',
            'create_config',
            'create_flow',
            'create_oauth_url',
            'create_python_js_data_app_git_credential',
            'create_sql_transformation',
            'delete_python_js_data_app_draft',
            'deploy_data_app',
            'docs_query',
            'find_component_id',
            'get_accessible_projects',
            'get_buckets',
            'get_components',
            'get_config_examples',
            'get_configs',
            'get_data_apps',
            'get_flow_examples',
            'get_flow_schema',
            'get_flows',
            'get_jobs',
            'get_project_info',
            'get_semantic_context',
            'get_semantic_schema',
            'get_shared_buckets',
            'get_tables',
            'link_shared_bucket',
            'modify_flow',
            'modify_python_js_data_app',
            'modify_streamlit_data_app',
            'query_data',
            'run_job',
            'run_sync_action',
            'search',
            'search_semantic_context',
            'set_project_scope',
            'update_config',
            'update_config_row',
            'update_descriptions',
            'update_flow',
            'update_project_description',
            'update_sql_transformation',
            'validate_semantic_query',
        ]

    @pytest.mark.asyncio
    async def test_tools_have_descriptions(self):
        server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        tools = await server.list_tools(run_middleware=False)

        missing_descriptions: list[str] = []
        for tool in tools:
            if not tool.description:
                missing_descriptions.append(tool.name)

        missing_descriptions.sort()
        assert not missing_descriptions, f'These tools have no description: {missing_descriptions}'

    @pytest.mark.asyncio
    async def test_tools_have_serializer(self):
        server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        tools = await server.list_tools(run_middleware=False)

        missing_serializer: list[str] = []
        for tool in tools:
            if not tool.serializer:
                missing_serializer.append(tool.name)
            if tool.serializer not in (_exclude_none_serializer, toon_serializer, toon_serializer_compact):
                missing_serializer.append(tool.name)

        missing_serializer.sort()
        assert not missing_serializer, f'These tools have no serializer: {missing_serializer}'

    @pytest.mark.asyncio
    async def test_tools_input_schema(self):
        server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        tools = await server.list_tools(run_middleware=False)

        missing_properties: list[str] = []
        missing_type: list[str] = []
        missing_default: list[str] = []
        for tool in tools:
            properties = tool.parameters['properties']
            if not properties:
                missing_properties.append(tool.name)
                continue

            required = tool.parameters.get('required') or []
            for prop_name, prop_def in properties.items():
                if all(
                    [
                        'type' not in prop_def,
                        ('anyOf' not in prop_def or any('type' not in t for t in prop_def['anyOf'])),
                    ]
                ):
                    missing_type.append(f'{tool.name}.{prop_name}')
                if prop_name not in required and 'default' not in prop_def:
                    missing_default.append(f'{tool.name}.{prop_name}')

        missing_properties.sort()
        assert missing_properties == []
        missing_type.sort()
        assert not missing_type, f'These tool params have no "type" info: {missing_type}'
        missing_default.sort()
        assert not missing_default, f'These tool params are optional, but have no default value: {missing_default}'


@pytest.mark.asyncio
async def test_own_stack_from_cli_parameter_only(tmp_path, monkeypatch):
    """
    A server started with '--api-url' and no environment variables must recognize that stack as its
    own in both places where the check is made: the per-request Storage API URL is pinned to it, and
    the Kubernetes ServiceAccount step-up header is attached for it.

    Regression test: the step-up check used to read the process environment on its own, so it saw no
    stack at all here and stopped attaching the header for the whole life of the process.
    """
    monkeypatch.delenv('KBC_STORAGE_API_URL', raising=False)
    monkeypatch.delenv('HOSTNAME_SUFFIX', raising=False)
    token_file = tmp_path / 'token'
    token_file.write_text('sa-jwt')

    # This is what cli.run_server() builds from the command line.
    parsed_args = cli.parse_args(['--transport', 'streamable-http', '--api-url', 'https://connection.keboola.com'])
    config = Config(storage_api_url=parsed_args.api_url, storage_token='user-token', workspace_schema='WORKSPACE_1234')
    created = create_server(
        config,
        runtime_info=ServerRuntimeInfo(transport='http-compat/streamable-http'),
        custom_routes_handling='return',
    )
    assert isinstance(created, tuple)
    server_state = created[1].server_state
    assert server_state.own_stack_storage_api_url == 'https://connection.keboola.com'

    # A request asking for another stack is served from the server's own stack ...
    http_rq = MagicMock(spec=Request)
    http_rq.headers = {'X-Storage-Api-Url': 'https://connection.attacker.example'}
    http_rq.scope = {}
    request_config = SessionStateMiddleware.apply_request_config(
        http_rq, server_state.config, own_stack_storage_api_url=server_state.own_stack_storage_api_url
    )
    assert request_config.storage_api_url == 'https://connection.keboola.com'

    # ... and the step-up header is attached, because that stack is ours.
    state = await SessionStateMiddleware.create_session_state(
        request_config, server_state.runtime_info, own_stack_storage_api_url=server_state.own_stack_storage_api_url
    )
    stepped = KeboolaClient.from_state(state).step_up_storage_client(str(token_file))
    assert stepped.raw_client.headers['X-Kubernetes-Authorization'] == 'Bearer sa-jwt'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('config', 'envs'),
    [
        (  # config params in Config class
            Config(
                storage_token='SAPI_1234',
                storage_api_url='http://connection.test.keboola.com',
                workspace_schema='WORKSPACE_1234',
            ),
            {},
        ),
        (  # config params in the OS environment
            Config(),
            {
                'KBC_STORAGE_TOKEN': 'SAPI_1234',
                'KBC_STORAGE_API_URL': 'http://connection.test.keboola.com',
                'KBC_WORKSPACE_SCHEMA': 'WORKSPACE_1234',
            },
        ),
        (  # config params mixed up in both the Config class and the OS environment
            Config(storage_api_url='http://connection.test.keboola.com'),
            {'KBC_STORAGE_TOKEN': 'SAPI_1234', 'KBC_WORKSPACE_SCHEMA': 'WORKSPACE_1234'},
        ),
        (  # the OS environment overrides the initial Config class
            Config(
                storage_token='foo-bar',
                storage_api_url='http://connection.test.keboola.com',
                workspace_schema='xyz_123',
            ),
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
        return_value={
            'owner': {'features': ['global-search', 'waii-integration', 'hide-conditional-flows']},
            'admin': {'role': 'admin'},
        },
    )

    # create MCP server with the initial Config
    mcp = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(mcp, FastMCP)
    tools_count = len(await mcp.list_tools(run_middleware=False))
    mcp.add_tool(FunctionTool.from_function(assessed_function, name='assessed-function'))

    # running the server as stdio transport through client
    async with Client(mcp) as client:
        tools = await client.list_tools()
        # plus the one we've added in this test minus two filtered tools
        # create_flow() and update_flow(), and four semantic tools (feature not enabled in mock)
        assert len(tools) == tools_count + 1 - 2 - 4
        assert tools[-1].name == 'assessed-function'
        assert tools[-1].description == 'custom text'
        # check if the inputSchema contains the expected param description
        assert expected_param_description in str(tools[-1].inputSchema)
        result = await client.call_tool('assessed-function', {'param': 'value'})
        assert isinstance(result.content[0], TextContent)
        assert result.content[0].text == 'value'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('admin_info', 'expected_included', 'expected_excluded'),
    [
        ({'role': 'admin'}, 'modify_flow', 'update_flow'),
        ({'role': None}, 'update_flow', 'modify_flow'),
        ({}, 'update_flow', 'modify_flow'),
    ],
)
async def test_with_session_state_admin_role_tools(mocker, admin_info, expected_included, expected_excluded):

    os_mock = mocker.patch('keboola_mcp_server.server.os')
    os_mock.environ = {
        'KBC_STORAGE_TOKEN': 'SAPI_1234',
        'KBC_STORAGE_API_URL': 'http://connection.test.keboola.com',
        'KBC_WORKSPACE_SCHEMA': 'WORKSPACE_1234',
    }

    mocker.patch(
        'keboola_mcp_server.clients.client.AsyncStorageClient.verify_token',
        return_value={
            'owner': {'features': ['global-search', 'waii-integration', 'hide-conditional-flows']},
            'admin': admin_info,
        },
    )

    mcp = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(mcp, FastMCP)

    async with Client(mcp) as client:
        tools = await client.list_tools()
        tool_names = {tool.name for tool in tools}
        assert expected_included in tool_names
        assert expected_excluded not in tool_names


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

    server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(server, FastMCP)

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
    server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(server, FastMCP)
    tools = await server.list_tools(run_middleware=False)
    for tool in tools:
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
        ('get_components', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('get_configs', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('get_config_examples', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('create_config', None, False, None, {COMPONENT_TOOLS_TAG}),
        ('update_config', None, True, None, {COMPONENT_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG}),
        ('add_config_row', None, False, None, {COMPONENT_TOOLS_TAG}),
        ('update_config_row', None, True, None, {COMPONENT_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG}),
        ('run_sync_action', True, None, None, {COMPONENT_TOOLS_TAG}),
        ('create_sql_transformation', None, False, None, {COMPONENT_TOOLS_TAG}),
        ('update_sql_transformation', None, True, None, {COMPONENT_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG}),
        # storage
        ('get_buckets', True, None, None, {STORAGE_TOOLS_TAG}),
        ('get_tables', True, None, None, {STORAGE_TOOLS_TAG}),
        ('update_descriptions', None, True, None, {STORAGE_TOOLS_TAG}),
        # flows
        ('create_flow', None, False, None, {FLOW_TOOLS_TAG}),
        ('create_conditional_flow', None, False, None, {FLOW_TOOLS_TAG}),
        ('get_flows', True, None, None, {FLOW_TOOLS_TAG}),
        ('update_flow', None, True, None, {FLOW_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG}),
        ('modify_flow', None, True, None, {FLOW_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG}),
        ('get_flow_examples', True, None, None, {FLOW_TOOLS_TAG}),
        ('get_flow_schema', True, None, None, {FLOW_TOOLS_TAG}),
        # sql
        ('query_data', True, None, None, {SQL_TOOLS_TAG}),
        # jobs
        ('get_jobs', True, None, None, {JOB_TOOLS_TAG}),
        ('run_job', None, True, None, {JOB_TOOLS_TAG}),
        # project/doc/search
        ('get_project_info', True, None, None, {PROJECT_TOOLS_TAG}),
        ('update_project_description', None, True, None, {PROJECT_TOOLS_TAG}),
        ('docs_query', True, None, None, {DOC_TOOLS_TAG}),
        ('find_component_id', True, None, None, {SEARCH_TOOLS_TAG}),
        # semantic
        ('search_semantic_context', True, None, None, {SEMANTIC_TOOLS_TAG}),
        ('get_semantic_context', True, None, None, {SEMANTIC_TOOLS_TAG}),
        ('get_semantic_schema', True, None, None, {SEMANTIC_TOOLS_TAG}),
        ('validate_semantic_query', True, None, None, {SEMANTIC_TOOLS_TAG}),
        # oauth
        ('create_oauth_url', None, True, None, {OAUTH_TOOLS_TAG}),
        # data apps
        ('modify_streamlit_data_app', None, True, None, {DATA_APP_TOOLS_TAG, CONFIG_DIFF_PREVIEW_TAG}),
        ('modify_python_js_data_app', None, True, None, {DATA_APP_TOOLS_TAG}),
        ('create_python_js_data_app_git_credential', None, False, None, {DATA_APP_TOOLS_TAG}),
        ('get_data_apps', True, None, None, {DATA_APP_TOOLS_TAG}),
        ('deploy_data_app', None, False, None, {DATA_APP_TOOLS_TAG}),
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
    server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
    assert isinstance(server, FastMCP)
    tools = {t.name: t for t in await server.list_tools(run_middleware=False)}

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


@pytest.mark.asyncio
async def test_json_logging():
    with tempfile.TemporaryDirectory() as tmp_dir:
        log_config_file = Path(__file__).parent.parent / 'logging-json.conf'
        assert log_config_file.is_file(), f'No logging config file found at {log_config_file.absolute()}'

        tmp_log_config_file = Path(tmp_dir) / 'logging-json.conf'
        tmp_log_config_file.write_text(log_config_file.read_text().replace('level=INFO', 'level=DEBUG'))

        # start the MCP server process with json logging
        p = subprocess.Popen(
            [
                'python',
                '-m',
                'keboola_mcp_server',
                '--transport',
                'streamable-http',
                '--api-url',
                'http://connection.test.keboola.com',
                '--storage-token',
                'foo',
                '--log-config',
                tmp_log_config_file.absolute(),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Read output streams in background to prevent buffer blocking
        stdout_lines = []
        stderr_lines = []

        async def read_stream(stream, lines_list):
            """Read from stream in a non-blocking way"""
            loop = asyncio.get_event_loop()
            while True:
                line = await loop.run_in_executor(None, stream.readline)
                if not line:
                    break
                lines_list.append(line)

        stdout_task = asyncio.create_task(read_stream(p.stdout, stdout_lines))
        stderr_task = asyncio.create_task(read_stream(p.stderr, stderr_lines))

        try:
            # Poll until the server is ready (up to 30s) instead of a fixed sleep.
            # A fixed sleep is fragile: slow CI runners may need more than 5s to start.
            for attempt in range(60):
                await asyncio.sleep(0.5)
                if p.poll() is not None:
                    raise RuntimeError(f'MCP server process exited early (rc={p.returncode})')
                try:
                    async with httpx.AsyncClient() as hc:
                        await hc.get('http://localhost:8000/mcp', timeout=1.0)
                    break
                except (httpx.ConnectError, httpx.TimeoutException):
                    if attempt == 59:
                        raise RuntimeError('MCP server did not become ready within 30s')

            # connect to the server and list prompts to force 'fastmcp' logger to get used
            # the listing of the prompts does not require SAPI connection
            async with Client(StreamableHttpTransport('http://localhost:8000/mcp')) as client:
                prompts = await client.list_prompts()
                assert len(prompts) > 1

        finally:
            # kill the server and wait for output tasks
            p.terminate()
            p.wait()

            # Cancel background tasks and collect remaining output
            stdout_task.cancel()
            stderr_task.cancel()

            stdout = ''.join(stdout_lines)
            stderr = ''.join(stderr_lines)

    # Filter out known deprecation warnings (these bypass logging config)
    # These warnings come from uvicorn's dependencies or fastmcp and are not actual logging errors
    stderr_lines = [
        line
        for line in stderr.splitlines()
        if not any(
            pattern in line
            for pattern in [
                'websockets/legacy/__init__.py',
                'websockets.legacy is deprecated',
                'websockets_impl.py',
                'WebSocketServerProtocol is deprecated',
                'warnings.warn',
                'from websockets.server import WebSocketServerProtocol',
                'FastMCPDeprecationWarning',
                'serializer` parameter is deprecated',
                'FunctionTool.from_function(',
            ]
        )
    ]
    filtered_stderr = '\n'.join(stderr_lines)

    # there is only one handler (the root one) in logging-json.conf which sends messages to stdout
    assert filtered_stderr == '', f'Unexpected stderr: {filtered_stderr}'

    # all messages should be JSON-formatted, including those logged by FastMCP loggers
    top_names: set[str] = set()
    for line in stdout.splitlines():
        message = json.loads(line)
        name = message['name']
        top_names.add(name.split('.')[0])

    missing_top_names = {'fastmcp', 'keboola_mcp_server', 'uvicorn'} - top_names
    assert not missing_top_names, f'Missing logger names: {missing_top_names}'


@pytest.mark.asyncio
async def test_oauth_callback_handler_propagates_http_exception(mocker) -> None:
    # handle_oauth_callback() raises starlette.exceptions.HTTPException; oauth_callback_handler must
    # re-raise it as-is (so Starlette renders the real status/detail) rather than falling through to
    # the generic except-Exception branch, which would mask it as an opaque 500.
    server_state = ServerState(config=Config(), runtime_info=ServerRuntimeInfo(transport='streamable-http'))
    oauth_provider = mocker.Mock()
    oauth_provider.handle_oauth_callback = mocker.AsyncMock(side_effect=HTTPException(400, 'Invalid state parameter'))
    routes = CustomRoutes(server_state=server_state, oauth_provider=oauth_provider)

    request = Request({'type': 'http', 'headers': [], 'query_string': b'code=abc&state=xyz'})
    with pytest.raises(HTTPException) as exc:
        await routes.oauth_callback_handler(request)
    assert exc.value.status_code == 400
    assert exc.value.detail == 'Invalid state parameter'


class TestCreateServerOAuthSessionStore:
    """OAuth sessions live in Postgres (oauth_session_persistence RFC) -- create_server() must
    refuse to enable OAuth without a DSN rather than silently falling back to something unrevoked."""

    _TEST_ENCRYPTION_KEY = base64.b64encode(b'0' * 32).decode()

    @staticmethod
    def _oauth_config(**overrides) -> Config:
        return Config(
            storage_api_url='https://connection.keboola.com',
            oauth_client_id='client-id',
            oauth_client_secret='client-secret',
            oauth_server_url='https://connection.keboola.com',
            mcp_server_url='https://mcp.keboola.com',
            **overrides,
        )

    def test_raises_without_postgres_dsn(self) -> None:
        with pytest.raises(RuntimeError, match='MCP_DB_URL'):
            create_server(
                self._oauth_config(session_encryption_key=self._TEST_ENCRYPTION_KEY),
                runtime_info=ServerRuntimeInfo(transport='streamable-http'),
            )

    def test_raises_without_session_encryption_key(self) -> None:
        # A silent fallback to a process-local key would make persisted OAuth sessions
        # undecryptable after every restart -- refuse to start instead, same as the DSN check.
        with pytest.raises(RuntimeError, match='KBC_SESSION_ENCRYPTION_KEY'):
            create_server(
                self._oauth_config(postgres_dsn='postgresql://u:p@host/db'),
                runtime_info=ServerRuntimeInfo(transport='streamable-http'),
            )

    def test_constructs_session_store_when_dsn_is_set(self) -> None:
        from keboola_mcp_server.session_store.repository import PostgresSessionStore

        server = create_server(
            self._oauth_config(
                postgres_dsn='postgresql://u:p@host/db', session_encryption_key=self._TEST_ENCRYPTION_KEY
            ),
            runtime_info=ServerRuntimeInfo(transport='streamable-http'),
        )
        assert isinstance(server, FastMCP)
        assert isinstance(server.auth._session_store, PostgresSessionStore)

    def test_no_oauth_configured_needs_no_postgres_dsn(self) -> None:
        # The vast majority of create_server() call sites (local stdio, header/PAT-token sessions)
        # have no OAuth at all -- this must keep working with zero Postgres setup.
        server = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        assert server.auth is None
