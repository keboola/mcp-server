from dataclasses import asdict
from functools import wraps
from typing import Annotated, Optional

import pytest
from fastmcp import Client, Context
from mcp.types import AnyFunction, TextContent
from pydantic import Field
from starlette.requests import Request

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import (
    KeboolaMcpServer,
    ServerState,
    SessionParamsFactory,
    SessionState,
    SessionStateFactory,
    TransportType,
    _get_session_params,
    with_session_state,
    with_session_state_from,
)
from keboola_mcp_server.server import (
    create_server,
)
from keboola_mcp_server.tools.components import (
    GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
    RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME,
    RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
)
from keboola_mcp_server.tools.workspace import WorkspaceManager


class TestServer:
    @pytest.mark.asyncio
    async def test_list_tools(self):
        server = create_server()
        tools = await server.get_tools()
        assert sorted(tool.name for tool in tools.values()) == [
            'create_sql_transformation',
            'docs_query',
            'get_bucket_detail',
            GET_COMPONENT_CONFIGURATION_DETAILS_TOOL_NAME,
            'get_job_detail',
            'get_sql_dialect',
            'get_table_detail',
            'query_table',
            'retrieve_bucket_tables',
            'retrieve_buckets',
            RETRIEVE_COMPONENTS_CONFIGURATIONS_TOOL_NAME,
            'retrieve_jobs',
            RETRIEVE_TRANSFORMATIONS_CONFIGURATIONS_TOOL_NAME,
            'start_job',
            'update_bucket_description',
            'update_column_description',
            'update_sql_transformation_configuration',
            'update_table_description',
        ]

    @pytest.mark.asyncio
    async def test_tools_have_descriptions(self):
        server = create_server()
        tools = await server.get_tools()

        missing_descriptions: list[str] = []
        for tool in tools.values():
            if not tool.description:
                missing_descriptions.append(tool.name)

        missing_descriptions.sort()
        assert not missing_descriptions, f'These tools have no description: {missing_descriptions}'


@pytest.mark.parametrize(
    ('current_transport', 'request_param_source'),
    [
        ('streamable-http', 'headers'),
        ('streamable-http', 'query_params'),
        ('sse', 'headers'),
        ('sse', 'query_params'),
        ('stdio', None),
    ],
)
def test_infer_session_params_request(mocker, current_transport: str, request_param_source: str):
    # Create a mock request with query parameters based on the request_param_source
    mock_request = None
    expected_params = {'storage_token': 'test-storage-token', 'workspace_schema': 'test-workspace-schema'}
    os_env_parameters = {}
    if current_transport in ('streamable-http', 'sse'):
        mock_request = mocker.MagicMock(spec=Request)
        if request_param_source == 'headers':
            mock_request.headers = expected_params
            mock_request.query_params = {}
        if request_param_source == 'query_params':
            mock_request.query_params = expected_params
            mock_request.headers = {}
    elif current_transport == 'stdio':
        mock_request = None
        os_env_parameters = expected_params

    if current_transport != 'stdio':
        # Patch the _safe_get_http_request function to return our mock request
        mocker.patch('keboola_mcp_server.mcp.get_http_request', return_value=mock_request)
    mocker.patch('keboola_mcp_server.mcp.os.environ', os_env_parameters)
    params = _get_session_params(None)
    assert params == expected_params


@pytest.mark.parametrize(
    ('current_transport'),
    [
        ('streamable-http'),
        ('sse'),
        ('stdio'),
    ],
)
def test_get_session_params(mocker, current_transport: Optional[TransportType]):

    # Create a mock request with query parameters based on the request_param_source
    mock_request = None
    expected_params = {'storage_token': 'test-storage-token', 'workspace_schema': 'test-workspace-schema'}
    os_env_parameters = {}
    if current_transport in ('streamable-http', 'sse'):
        mock_request = mocker.MagicMock(spec=Request)
        if current_transport == 'streamable-http':
            # Streamable HTTP transport we prefer headers
            mock_request.headers = expected_params
            mock_request.query_params = {}
        if current_transport == 'sse':
            # SSE transport expects query params due to backwards compatibility
            mock_request.query_params = expected_params
            mock_request.headers = {}
    elif current_transport == 'stdio':
        mock_request = None
        os_env_parameters = expected_params

    # Patch the _safe_get_http_request function to return our mock request
    mocker.patch('keboola_mcp_server.mcp.get_http_request', return_value=mock_request)
    mocker.patch('keboola_mcp_server.mcp.os.environ', os_env_parameters)
    params = _get_session_params(current_transport)
    assert params == expected_params


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('state_name', 'session_params_factory', 'session_state_factory', 'use_custom_decorator', 'expected_state'),
    [
        # whether to use the state factory and custom decorator
        ('state1', None, None, False, {}),
        # Testing the session state factory
        (
            'state2',
            None,
            lambda _: {'vaule1': 'value1', 'value2': 'value2'},
            False,
            {'vaule1': 'value1', 'value2': 'value2'},
        ),
        # Testing the session state factory with a custom decorator, decorator passes the params
        # and the state factory returns the params
        ('state3', None, lambda x: {'vaule1': 'value1', **x}, True, {'vaule1': 'value1', 'value2': 'value2'}),
        # Testing the session params factory, state factory passes the params
        ('state4', lambda x: {'vaule1': 'value1'}, lambda x: {**x}, False, {'vaule1': 'value1'}),
    ],
)
async def test_with_session_state_from(
    mocker,
    state_name: str,
    session_params_factory: Optional[SessionParamsFactory],
    session_state_factory: Optional[SessionStateFactory],
    use_custom_decorator: bool,
    expected_state: SessionState,
):

    input_params_function = {'param': 'value'}
    expected_param_description = 'Parameter 1 description'
    custom_decorator = None
    if use_custom_decorator:

        def decorator(
            state_name: str, session_state_factory: SessionStateFactory, session_params_factory: SessionParamsFactory
        ) -> AnyFunction:

            def _wrap(func: AnyFunction) -> AnyFunction:

                @wraps(func)
                def _inject(*args, **kwargs):
                    # we know that the context is named ctx
                    assert 'ctx' in kwargs
                    assert 'param' in kwargs
                    ctx = kwargs['ctx']
                    assert isinstance(ctx, Context)
                    if not hasattr(ctx.session, state_name):
                        setattr(ctx.session, state_name, session_state_factory({'value2': 'value2'}))
                    else:
                        assert getattr(ctx.session, state_name) == expected_state
                    return func(*args, **kwargs)

                return _inject

            return _wrap

        custom_decorator = decorator
    else:
        custom_decorator = None

    @with_session_state_from(state_name, session_state_factory, session_params_factory, custom_decorator)
    async def assessed_function(
        ctx: Context, param: Annotated[str, Field(description=expected_param_description)]
    ) -> str:
        """custom text"""
        assert hasattr(ctx.session, state_name)
        assert getattr(ctx.session, state_name) == expected_state
        return param

    mcp = KeboolaMcpServer()
    mcp.add_tool(assessed_function, name='assessed-function')
    # When calling the os.environ, we want to return the expected state to get params, default factory passes
    # all params
    mocker.patch('keboola_mcp_server.mcp.os.environ', return_value=expected_state)
    # running the server as stdio transport through client
    async with Client(mcp) as client:
        tools = await client.list_tools()
        assert len(tools) == 1
        assert tools[0].name == 'assessed-function'
        assert tools[0].description == 'custom text'
        # check if the inputSchema contains the expected param description
        assert expected_param_description in str(tools[0].inputSchema)
        result = await client.call_tool('assessed-function', input_params_function)
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'


@pytest.mark.asyncio
async def test_with_session_state_from_initialized_once(mocker):
    expected_state = {'value1': 'value1', 'value2': 'value2'}
    mock_func = mocker.patch('keboola_mcp_server.mcp._get_session_params', return_value=expected_state)

    @with_session_state_from('state', None, None, None)
    async def assessed_function(ctx: Context, param: str) -> str:
        assert hasattr(ctx.session, 'state')
        assert ctx.session.state == {'value1': 'value1', 'value2': 'value2'}
        return param

    mcp = KeboolaMcpServer()
    mcp.add_tool(assessed_function, name='assessed-function')
    async with Client(mcp) as client:
        result = await client.call_tool('assessed-function', {'param': 'value'})
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'
        result = await client.call_tool('assessed-function', {'param': 'value'})
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'
        result = await client.call_tool('assessed-function', {'param': 'value'})
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'

    # we expect the function to be called only once (for initialization) for one connection we reuse the same session
    assert mock_func.call_count == 1


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
    Test that the KeboolaClient and WorkspaceManager are injected into the context and that the lifespan of the client is managed
    by the server.
    Test that the ServerState is properly initialized and that the client and workspace are properly disposed of.
    """
    cfg_dict = {
        'storage_token': 'test-storage-token',
        'workspace_schema': 'test-workspace-schema',
        'storage_api_url': 'https://connection.keboola.com',
        'transport': 'stdio',
    }
    config = Config.from_dict(cfg_dict)

    mocker.patch('keboola_mcp_server.mcp.os.environ', os_environ_params)

    server = create_server(config)

    @with_session_state()
    async def assessed_function(ctx: Context, param: str) -> str:
        assert hasattr(ctx.session, 'state')
        client = KeboolaClient.from_state(ctx.session.state)
        assert isinstance(client, KeboolaClient)
        workspace = WorkspaceManager.from_state(ctx.session.state)
        assert isinstance(workspace, WorkspaceManager)

        # check the server state life_span
        server_state = ServerState.from_context(ctx)
        assert asdict(server_state.config) == asdict(config)

        assert client.token == expected_params['storage_token']
        assert workspace._workspace_schema == expected_params['workspace_schema']

        return param

    server.add_tool(assessed_function, name='assessed_function')

    async with Client(server) as client:
        result = await client.call_tool('assessed_function', {'param': 'value'})
        assert isinstance(result[0], TextContent)
        assert result[0].text == 'value'
