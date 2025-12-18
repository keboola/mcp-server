import logging
import uuid
from importlib.metadata import distribution
from unittest.mock import ANY

import pytest
import yaml
from fastmcp import Client, Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import FunctionTool
from mcp.shared.context import RequestContext
from mcp.types import ClientCapabilities, Implementation, InitializeRequestParams

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.storage import TableColumnInfo


@pytest.fixture
def function_with_value_error():
    """A function that raises ValueError for testing general error handling."""

    async def func(_ctx: Context):
        raise ValueError('Simulated ValueError')

    return func


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('function_fixture', 'default_recovery', 'recovery_instructions', 'expected_recovery_message', 'exception_message'),
    [
        # Case with both default_recovery and recovery_instructions specified
        (
            'function_with_value_error',
            'General recovery message.',
            {ValueError: 'Check that data has valid types.'},
            'Check that data has valid types.',
            'Simulated ValueError',
        ),
        # Case where only default_recovery is provided
        (
            'function_with_value_error',
            'General recovery message.',
            {},
            'General recovery message.',
            'Simulated ValueError',
        ),
        # Case with only recovery_instructions provided
        (
            'function_with_value_error',
            None,
            {ValueError: 'Check that data has valid types.'},
            'Check that data has valid types.',
            'Simulated ValueError',
        ),
        # Case with no recovery instructions provided
        (
            'function_with_value_error',
            None,
            {},
            None,
            'Simulated ValueError',
        ),
    ],
)
async def test_tool_errors(
    function_fixture,
    default_recovery,
    recovery_instructions,
    expected_recovery_message,
    exception_message,
    request,
    mcp_context_client: Context,
):
    """
    Test that the appropriate recovery message is applied based on the exception type.
    Verifies that the tool_errors decorator handles various combinations of recovery parameters.
    """
    tool_func = request.getfixturevalue(function_fixture)
    decorated_func = tool_errors(default_recovery=default_recovery, recovery_instructions=recovery_instructions)(
        tool_func
    )

    if expected_recovery_message is None:
        with pytest.raises(ValueError, match=exception_message) as excinfo:
            await decorated_func(mcp_context_client)
    else:
        with pytest.raises(ToolError) as excinfo:
            await decorated_func(mcp_context_client)
        assert expected_recovery_message in str(excinfo.value)
    assert exception_message in str(excinfo.value)


@pytest.mark.asyncio
async def test_logging_on_tool_exception(caplog, function_with_value_error, mcp_context_client: Context):
    """Test that the tool_errors decorator logs exceptions properly."""
    decorated_func = tool_errors()(function_with_value_error)

    with pytest.raises(ValueError, match='Simulated ValueError'):
        await decorated_func(mcp_context_client)

    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert 'MCP tool "func" call failed. ValueError: Simulated ValueError' in caplog.records[0].message
    assert 'Simulated ValueError' in caplog.records[0].message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('transport', 'client_info', 'component_id'),
    [
        ('http', None, 'keboola.mcp-server-tool'),
        ('stdio', Implementation(name='read-only-chat', version='1.2.3'), 'keboola.ai-chat'),
        ('stdio', Implementation(name='in-platform-chat', version='x.y.z'), 'keboola.kai-assistant'),
    ],
)
async def test_get_session_id(
    transport: str, client_info: Implementation | None, component_id: str, mcp_context_client: Context, mocker
):
    @tool_errors()
    async def foo(_ctx: Context):
        pass

    session_id = uuid.uuid4().hex
    if transport == 'stdio':
        mcp_context_client.session_id = None
        mcp_context_client.request_context = mocker.MagicMock(RequestContext)
        mcp_context_client.request_context.lifespan_context = ServerState(
            config=Config(), runtime_info=ServerRuntimeInfo(transport='stdio', server_id=session_id)
        )
    elif transport == 'http':
        mcp_context_client.session_id = session_id
        mcp_context_client.request_context.lifespan_context = ServerState(
            config=Config(), runtime_info=ServerRuntimeInfo(transport='http', server_id=session_id)
        )
    else:
        pytest.fail(f'Unknown transport: {transport}')

    if client_info:
        mcp_context_client.session.client_params = InitializeRequestParams(
            protocolVersion='1.0',
            clientInfo=client_info,
            capabilities=ClientCapabilities(),
        )

    await foo(mcp_context_client)
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.storage_client.trigger_event.assert_called_once_with(
        message='MCP tool "foo" call succeeded.',
        component_id=component_id,
        event_type='success',
        params={
            'mcpServerContext': {
                'appEnv': 'DEV',
                'version': distribution('keboola_mcp_server').version,
                'userAgent': f'{client_info.name}/{client_info.version}' if client_info else '',
                'sessionId': session_id,
                'serverTransport': transport,
                'conversationId': 'convo-1234',
            },
            'tool': {
                'name': 'foo',
                'arguments': [],
            },
        },
        duration=ANY,
    )


class TestPydanticValidationErrors:
    @pytest.fixture
    def mcp_server(self) -> FastMCP:
        cfg_dict = {
            'storage_token': '123-test-storage-token',
            'storage_api_url': 'https://connection.keboola.com',
            'transport': 'stdio',
        }
        config = Config.from_dict(cfg_dict)
        server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)
        return server

    @pytest.mark.asyncio
    async def test_error_in_tool_call_params(self, mocker, mcp_server: FastMCP):
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.get',
            return_value={'owner': {'id': '123'}},
        )

        async with Client(mcp_server) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool('query_data', arguments={'foo': 'bar'})

            assert isinstance(excinfo.value, ToolError)
            lines = str(excinfo.value).splitlines()
            assert len(lines) > 0, 'Empty error message'
            assert lines[0] == 'Found 3 validation error(s) for call[query_data]'
            formatted = '\n'.join(lines[1:])
            error_details = yaml.safe_load(formatted)
            assert error_details == {
                'errors': [
                    {
                        'field': 'sql_query',
                        'message': 'Missing required argument',
                        'extra': {
                            'type': 'missing_argument',
                            'input': "{'foo': 'bar'}",
                            'url': 'https://errors.pydantic.dev/2.12/v/missing_argument',
                        },
                    },
                    {
                        'field': 'query_name',
                        'message': 'Missing required argument',
                        'extra': {
                            'type': 'missing_argument',
                            'input': "{'foo': 'bar'}",
                            'url': 'https://errors.pydantic.dev/2.12/v/missing_argument',
                        },
                    },
                    {
                        'field': 'foo',
                        'message': 'Unexpected keyword argument',
                        'extra': {
                            'type': 'unexpected_keyword_argument',
                            'input': 'bar',
                            'url': 'https://errors.pydantic.dev/2.12/v/unexpected_keyword_argument',
                        },
                    },
                ]
            }

    @staticmethod
    @tool_errors()
    async def foo(_ctx: Context):
        # raises PydanticValidationError for missing quoted_name field
        TableColumnInfo.model_validate({'name': 'bar', 'database_native_type': 'text', 'nullable': False})

    @pytest.mark.asyncio
    async def test_error_inside_tool_call(self, caplog, mocker, mcp_server: FastMCP):
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.get',
            return_value={'owner': {'id': '123'}},  # response from GET /v2/storage/tokens/verify
        )
        post_mock = mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.post',
            return_value={  # response from POST /v2/storage/events
                'id': '13008826',
                'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3',
            },
        )

        mcp_server.add_tool(FunctionTool.from_function(self.foo))

        async with Client(mcp_server) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool('foo')

            expected_error_details = {
                'errors': [
                    {
                        'field': 'quotedName',
                        'message': 'Field required',
                        'extra': {
                            'type': 'missing',
                            'input': "{'name': 'bar', 'database_native_type': 'text', 'nullable': False}",
                            'url': 'https://errors.pydantic.dev/2.12/v/missing',
                        },
                    },
                ]
            }

            # check the message in the ToolError exception
            assert isinstance(excinfo.value, ToolError)
            lines = str(excinfo.value).splitlines()
            assert len(lines) > 0, 'Empty error message'
            assert lines[0] == 'Found 1 validation error(s) for TableColumnInfo'
            assert expected_error_details == yaml.safe_load('\n'.join(lines[1:]))

            # check the message in the LOG from 'keboola_mcp_server.errors' logger
            log_records = [r for r in caplog.records if r.name == 'keboola_mcp_server.errors']
            assert log_records, 'No log records from keboola_mcp_server.errors'
            lines = log_records[0].message.splitlines()
            assert len(lines) > 0, 'Empty log message'
            assert lines[0] == 'MCP tool "foo" call failed. ToolError: Found 1 validation error(s) for TableColumnInfo'
            assert expected_error_details == yaml.safe_load('\n'.join(lines[1:]))

            # check the message in the submitted SAPI event
            post_mock.assert_called_once()
            _, kwargs = post_mock.call_args
            lines = str(kwargs.get('data', {}).get('message') or '').splitlines()
            assert len(lines) > 0, 'Empty error message'
            assert lines[0] == 'MCP tool "foo" call failed. ToolError: Found 1 validation error(s) for TableColumnInfo'
            assert expected_error_details == yaml.safe_load('\n'.join(lines[1:]))
