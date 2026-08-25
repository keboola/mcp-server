import json
import logging
import uuid
from importlib.metadata import distribution
from unittest.mock import ANY

import httpx
import jsonschema
import pydantic
import pytest
import yaml
from fastmcp import Client, Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.tools import FunctionTool
from mcp.shared.context import RequestContext
from mcp.types import ClientCapabilities, Implementation, InitializeRequestParams

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import AsyncStorageClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.errors import MAX_ARG_VALUE_LEN, tool_errors
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.storage.tools import TableColumnInfo
from keboola_mcp_server.tools.validation import RecoverableValidationError, ValidationContext

PYDANTIC_DOCS_VERSION = '.'.join(pydantic.__version__.split('.')[:2])


@pytest.fixture
def function_with_value_error():
    """A function that raises ValueError for testing general error handling."""

    async def func(_ctx: Context):
        raise ValueError('Simulated ValueError')

    return func


@pytest.fixture
def function_with_jsonschema_validation_error():
    """A function that raises jsonschema.ValidationError for testing validation wrapping."""

    async def func(_ctx: Context):
        raise jsonschema.ValidationError('Simulated jsonschema validation error')

    return func


@pytest.fixture
def function_with_recoverable_jsonschema_validation_error():
    """A function that raises RecoverableValidationError to test rich __str__ propagation."""

    async def func(_ctx: Context):
        try:
            jsonschema.validate({'embedding_settings': {'provider_type': 'gpt-9000'}}, {'type': 'string'})
        except jsonschema.ValidationError as e:
            raise RecoverableValidationError.create_from_values(
                e,
                initial_message='The "parameters" field is not valid.',
                validation_context=ValidationContext(
                    component_id='keboola.wr-pinecone-embeddings',
                    configuration_id='cfg-1',
                    scope='parameters',
                ),
            )

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
    assert 'MCP tool "func" call failed.' in caplog.records[0].message
    assert 'Simulated ValueError' in caplog.text


@pytest.mark.asyncio
async def test_jsonschema_validation_error_wrapped(
    function_with_jsonschema_validation_error, mcp_context_client: Context
):
    decorated_func = tool_errors()(function_with_jsonschema_validation_error)

    with pytest.raises(ToolError) as excinfo:
        await decorated_func(mcp_context_client)

    message = str(excinfo.value)
    assert 'Simulated jsonschema validation error' in message


@pytest.mark.asyncio
async def test_recoverable_jsonschema_validation_error_uses_original_str(
    function_with_recoverable_jsonschema_validation_error, mcp_context_client: Context
):
    decorated_func = tool_errors()(function_with_recoverable_jsonschema_validation_error)

    with pytest.raises(ToolError) as excinfo:
        await decorated_func(mcp_context_client)

    message = str(excinfo.value)
    assert 'Failed validating' in message
    assert 'The "parameters" field is not valid.' in message
    assert 'Validation component context: component_id=keboola.wr-pinecone-embeddings' in message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('transport', 'client_info', 'component_id'),
    [
        ('http', None, 'keboola.mcp-server-tool'),
        ('stdio', Implementation(name='read-only-chat', version='1.2.3'), 'keboola.ai-chat'),
        ('stdio', Implementation(name='kai-assistant', version='x.y.z'), 'keboola.kai-assistant'),
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
                            'url': f'https://errors.pydantic.dev/{PYDANTIC_DOCS_VERSION}/v/missing_argument',
                        },
                    },
                    {
                        'field': 'query_name',
                        'message': 'Missing required argument',
                        'extra': {
                            'type': 'missing_argument',
                            'input': "{'foo': 'bar'}",
                            'url': f'https://errors.pydantic.dev/{PYDANTIC_DOCS_VERSION}/v/missing_argument',
                        },
                    },
                    {
                        'field': 'foo',
                        'message': 'Unexpected keyword argument',
                        'extra': {
                            'type': 'unexpected_keyword_argument',
                            'input': 'bar',
                            'url': (
                                f'https://errors.pydantic.dev/{PYDANTIC_DOCS_VERSION}/v/unexpected_keyword_argument'
                            ),
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
                            'url': f'https://errors.pydantic.dev/{PYDANTIC_DOCS_VERSION}/v/missing',
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
            assert lines[0] == 'MCP tool "foo" call failed.'

            # check the message in the submitted SAPI event
            post_mock.assert_called_once()
            _, kwargs = post_mock.call_args
            lines = str(kwargs.get('data', {}).get('message') or '').splitlines()
            assert len(lines) > 0, 'Empty error message'
            assert lines[0] == 'MCP tool "foo" call failed. ToolError: Found 1 validation error(s) for TableColumnInfo'
            assert expected_error_details == yaml.safe_load('\n'.join(lines[1:]))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'event_error',
    [
        httpx.HTTPStatusError(
            '400 Request too large',
            request=httpx.Request('POST', 'https://example.com/events'),
            response=httpx.Response(400),
        ),
        httpx.HTTPStatusError(
            '403 Forbidden',
            request=httpx.Request('POST', 'https://example.com/events'),
            response=httpx.Response(403),
        ),
        ConnectionError('Network failure'),
    ],
)
async def test_event_logging_failure_does_not_fail_tool(caplog, event_error, mcp_context_client: Context):
    """Event logging errors must never propagate as tool failures — the tool result is already determined."""

    @tool_errors()
    async def successful_tool(_ctx: Context) -> str:
        return 'ok'

    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.storage_client.trigger_event.side_effect = event_error

    # Tool must succeed despite event logging failure
    result = await successful_tool(mcp_context_client)
    assert result == 'ok'

    # Event failure must be logged as a warning, not re-raised
    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any('Failed to trigger tool event' in r.message for r in warning_records)


@pytest.mark.asyncio
async def test_large_argument_value_is_truncated_in_event(mcp_context_client: Context):
    """Argument values exceeding MAX_ARG_VALUE_LEN must be replaced with a truncation notice."""

    large_value = 'x' * (MAX_ARG_VALUE_LEN + 1)

    @tool_errors()
    async def tool_with_large_arg(_ctx: Context, big_param: str) -> str:
        return 'done'

    client = KeboolaClient.from_state(mcp_context_client.session.state)
    await tool_with_large_arg(mcp_context_client, big_param=large_value)

    client.storage_client.trigger_event.assert_called_once()
    _, kwargs = client.storage_client.trigger_event.call_args
    arguments = kwargs['params']['tool']['arguments']
    big_param_entry = next(a for a in arguments if a['key'] == 'big_param')
    decoded = json.loads(big_param_entry['value'])
    assert 'truncated' in decoded
    expected_length = len(json.dumps(json.dumps(large_value, ensure_ascii=False), ensure_ascii=False).encode('utf-8'))
    assert str(expected_length) in decoded


@pytest.mark.asyncio
async def test_event_uses_step_up_client_when_k8s_token_configured(
    tmp_path, monkeypatch, mocker, mcp_context_client: Context
):
    """On the deployed server (KBC_KUBERNETES_TOKEN_PATH set) the storage event must be emitted
    through the Kubernetes step-up client so a read-only user's token is not denied (403)."""
    token_file = tmp_path / 'token'
    token_file.write_text('sa-jwt')
    monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', str(token_file))

    client = KeboolaClient.from_state(mcp_context_client.session.state)
    step_up_client = mocker.AsyncMock()
    client.step_up_storage_client = mocker.Mock(return_value=step_up_client)

    @tool_errors()
    async def foo(_ctx: Context):
        pass

    await foo(mcp_context_client)

    client.step_up_storage_client.assert_called_once_with(str(token_file))
    step_up_client.trigger_event.assert_awaited_once()
    client.storage_client.trigger_event.assert_not_called()


@pytest.mark.asyncio
async def test_event_uses_plain_client_without_k8s_token(monkeypatch, mcp_context_client: Context):
    """Without KBC_KUBERNETES_TOKEN_PATH (local / normal use) events keep using the user's own client."""
    monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)

    @tool_errors()
    async def foo(_ctx: Context):
        pass

    await foo(mcp_context_client)

    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.storage_client.trigger_event.assert_called_once()
    client.step_up_storage_client.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('session_storage_api_url', 'expect_step_up_header'),
    [
        ('https://connection.keboola.com', True),
        ('https://connection.north-europe.azure.keboola.com', False),
    ],
    ids=['own_stack', 'other_stack'],
)
async def test_event_step_up_header_only_for_own_stack(
    tmp_path, monkeypatch, mocker, empty_context: Context, session_storage_api_url: str, expect_step_up_header: bool
):
    """The event is emitted in a `finally:` block that swallows its errors, so the ServiceAccount
    JWT must not be sent when the session talks to a stack other than this server's own."""
    token_file = tmp_path / 'token'
    token_file.write_text('sa-jwt')
    monkeypatch.setenv('KBC_KUBERNETES_TOKEN_PATH', str(token_file))

    # A real client, so the real step-up logic decides whether the JWT is attached. The server's own
    # stack is the one it was configured with when it started (see `ServerState.own_stack_storage_api_url`).
    client = KeboolaClient(
        storage_api_url=session_storage_api_url,
        legacy_storage_token='user-token',
        own_stack_storage_api_url='https://connection.keboola.com',
    )
    empty_context.session.state[KeboolaClient.STATE_KEY] = client
    trigger_event = mocker.patch.object(AsyncStorageClient, 'trigger_event', autospec=True)

    @tool_errors()
    async def foo(_ctx: Context):
        pass

    await foo(empty_context)

    trigger_event.assert_awaited_once()
    storage_client = trigger_event.await_args.args[0]
    assert ('X-Kubernetes-Authorization' in storage_client.raw_client.headers) is expect_step_up_header
