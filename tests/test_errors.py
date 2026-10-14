import copy
import json
import logging
import time
import uuid
from collections.abc import Sequence
from importlib.metadata import distribution
from typing import Any
from unittest.mock import ANY

import httpx
import jsonschema
import pydantic
import pytest
import yaml
from fastmcp import Client, Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.context import FastMCPRequestContext
from fastmcp.tools import FunctionTool
from mcp.types import ClientCapabilities, Implementation, InitializeRequestParams

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import AsyncStorageClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.errors import _SECRET_MARKER, MAX_ARG_VALUE_LEN, _redact_arguments, tool_errors
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.components.model import ConfigParamReplace, ConfigParamSet
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


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request('GET', 'https://connection.test.keboola.com/v1/auth/token/introspect')
    return httpx.HTTPStatusError(f'HTTP {status}', request=request, response=httpx.Response(status, request=request))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('exception', 'expected_level', 'expected_category'),
    [
        # the caller's own fault (expired token, missing object, bad input) is routine -> WARNING, no traceback
        (_http_status_error(401), logging.WARNING, 'client'),
        (_http_status_error(404), logging.WARNING, 'client'),
        (jsonschema.ValidationError('bad input'), logging.WARNING, 'client'),
        # upstream/server faults stay ERROR with the traceback (429/408 are throttling/timeouts, not caller mistakes)
        (_http_status_error(500), logging.ERROR, 'internal'),
        (_http_status_error(429), logging.ERROR, 'internal'),
        (ValueError('boom'), logging.ERROR, 'internal'),
    ],
)
async def test_tool_exception_log_level_and_fields(
    exception, expected_level, expected_category, caplog, mcp_context_client: Context
):
    async def func(_ctx: Context):
        raise exception

    with pytest.raises((type(exception), ToolError)):  # jsonschema errors are re-raised wrapped in ToolError
        await tool_errors()(func)(mcp_context_client)

    records = [r for r in caplog.records if r.name == 'keboola_mcp_server.errors']
    assert len(records) == 1
    assert records[0].levelno == expected_level
    assert bool(records[0].exc_info) is (expected_level == logging.ERROR)
    assert records[0].tool_name == 'func'
    assert records[0].error_category == expected_category


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
        mcp_context_client.request_context = mocker.MagicMock(FastMCPRequestContext)
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

            # check the message in the submitted SAPI event: a summary without the echoed input
            post_mock.assert_called_once()
            _, kwargs = post_mock.call_args
            assert kwargs.get('data', {}).get('message') == (
                'MCP tool "foo" call failed. ToolError: '
                'TableColumnInfo validation failed: Field required (missing) at quotedName'
            )

    @staticmethod
    @tool_errors()
    async def echo_ids(_ctx: Context, ids: Sequence[str] = ()) -> list[str]:
        return list(ids)

    @pytest.mark.asyncio
    async def test_empty_string_array_argument_is_coerced_to_empty_list(self, mocker, mcp_server: FastMCP):
        # Some tool-calling clients send `""` instead of omitting/`[]` an unset array-typed
        # argument (observed live against `get_configs`'s `component_types`/`component_ids`),
        # which pydantic-core always rejects for a `Sequence[...]` field. The declared `array`
        # schema type means `""` is never a legitimate value, so it's coerced to `[]` before the
        # tool ever sees it, rather than surfacing a validation error at all.
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.get',
            return_value={'owner': {'id': '123'}},
        )
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.post',
            return_value={'id': '13008826', 'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3'},
        )
        mcp_server.add_tool(FunctionTool.from_function(self.echo_ids))

        async with Client(mcp_server) as client:
            result = await client.call_tool('echo_ids', arguments={'ids': ''})

        assert result.structured_content == {'result': []}

    @pytest.mark.asyncio
    async def test_non_empty_array_argument_is_untouched(self, mocker, mcp_server: FastMCP):
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.get',
            return_value={'owner': {'id': '123'}},
        )
        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.post',
            return_value={'id': '13008826', 'uuid': '01958f48-b1fc-7f05-b9b9-8a4a7b385bc3'},
        )
        mcp_server.add_tool(FunctionTool.from_function(self.echo_ids))

        async with Client(mcp_server) as client:
            result = await client.call_tool('echo_ids', arguments={'ids': ['keboola.ex-db-mysql']})

        assert result.structured_content == {'result': ['keboola.ex-db-mysql']}


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
async def test_bootstrap_session_skips_the_event_without_warning(caplog, empty_context: Context):
    """A bootstrap session (agent_provisioning RFC) has no credential and no project, so there is
    nowhere to write a Storage event to. The tool must still run, and the missing client must not
    surface as a warning on every `create_project` call."""

    @tool_errors()
    async def bootstrap_tool(_ctx: Context) -> str:
        return 'ok'

    with caplog.at_level(logging.DEBUG):
        assert await bootstrap_tool(empty_context) == 'ok'

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


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


CANARY = 'canary-s3cr3t-ai4029'
FAKE_CIPHER = 'KBC::ProjectSecure::fake-cipher-value'
SHORT_CANARY = 'p4ss'
ENUM_CANARY = 'enum-canary-choice'
NUMERIC_CANARY = '987654321'
DELIMITED_TAIL = 'b2:c3,d4'
MIXED_QUOTES_SECRET = f'a\'b"{CANARY}'


def _raise(exc: Exception):
    raise exc


def _originating_failure():
    raise RuntimeError(f'Rejected credential {CANARY}')


def _raise_pydantic_error():
    pydantic.TypeAdapter(dict[str, int]).validate_python({'#password': CANARY})


def _raise_jsonschema_error():
    jsonschema.validate({'#password': CANARY}, {'properties': {'#password': {'type': 'integer'}}})


def _raise_jsonschema_secret_enum_error():
    jsonschema.validate({'#password': 'bad'}, {'properties': {'#password': {'enum': [ENUM_CANARY, 'other']}}})


@pytest.fixture
def storage_event_post(monkeypatch, mocker, empty_context: Context):
    """A real storage client with a mocked HTTP POST, so the `[trigger_event]` log line is emitted as well."""
    monkeypatch.delenv('KBC_KUBERNETES_TOKEN_PATH', raising=False)
    empty_context.session.state[KeboolaClient.STATE_KEY] = KeboolaClient(
        storage_api_url='https://connection.keboola.com', legacy_storage_token='fake-token'
    )
    return mocker.patch.object(AsyncStorageClient, 'post', autospec=True, return_value={'id': '1'})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('parameters', 'parameter_updates', 'failure', 'preserved'),
    [
        pytest.param(
            {
                'db': {'host': 'db.example.com', '#password': CANARY, 'hosts': [{'#token': CANARY}]},
                '#api_key': FAKE_CIPHER,
            },
            None,
            None,
            ['db.example.com', '[REDACTED]'],
            id='create_config_nested_secrets',
        ),
        pytest.param(
            json.dumps({'user': 'alice', 'db': {'#password': CANARY, '#token': FAKE_CIPHER}}),
            None,
            None,
            ['alice', '[REDACTED]'],
            id='create_config_json_string',
        ),
        pytest.param(
            str({'user': 'alice', '#password': CANARY, 'db': {"#token": CANARY}}),
            None,
            None,
            ['alice', '[REDACTED]'],
            id='string_python_literal',
        ),
        pytest.param(
            f'{{"user": "alice", "#password": "{CANARY}",}}',
            None,
            None,
            ['alice', '[REDACTED]'],
            id='string_json_trailing_comma',
        ),
        pytest.param(
            f'```json\n{{"user": "alice", "#password": "{CANARY}"}}\n```',
            None,
            None,
            ['alice', '[REDACTED]'],
            id='string_code_fenced_json',
        ),
        pytest.param(
            f'user: alice\ndb:\n  "#password": {CANARY}\n  #token: \'{CANARY}\'\n',
            None,
            None,
            ['[REDACTED]'],
            id='string_yaml',
        ),
        pytest.param(
            f'USER=alice\n#PASSWORD={CANARY}\n',
            None,
            None,
            ['[REDACTED]'],
            id='string_env',
        ),
        pytest.param(
            None,
            [
                ConfigParamSet(op='set', path='db.#token', value=CANARY),
                ConfigParamSet(op='set', path='#password', value={'nested': CANARY}),
                ConfigParamSet(op='set', path='db', value={'#password': CANARY, 'host': 'db.example.com'}),
                ConfigParamSet(op='set', path='#api_key', value=FAKE_CIPHER),
                ConfigParamSet(op='set', path='db.user', value='alice'),
            ],
            None,
            ['db.example.com', 'alice', 'db.#token', '[REDACTED]'],
            id='update_config_set',
        ),
        pytest.param(
            {'#password': f'KBC::{CANARY}'},
            None,
            None,
            ['[REDACTED]'],
            id='plaintext_with_cipher_prefix',
        ),
        pytest.param(
            None,
            [
                ConfigParamSet(op='set', path='"#password"', value=CANARY),
                ConfigParamReplace(op='str_replace', path="'#token'", search_for=CANARY, replace_with='new'),
            ],
            None,
            ['[REDACTED]'],
            id='update_config_quoted_root_path',
        ),
        pytest.param(
            '```json\n{"user": "alice", "#password": "x\\"' + CANARY + '"}\n```',
            None,
            None,
            ['alice', '[REDACTED]'],
            id='string_code_fenced_json_escaped_quote',
        ),
        pytest.param(
            f'user: alice\n#password: |\n  {CANARY}\n',
            None,
            None,
            ['[REDACTED]'],
            id='string_yaml_block_scalar',
        ),
        pytest.param(
            f"{{'user': 'alice', '#db': {{'user': 'u', 'pw': ['{CANARY}']}}, 'port': 1}}",
            None,
            None,
            ['alice', '[REDACTED]', 'port'],
            id='string_python_literal_secret_container',
        ),
        pytest.param(
            json.dumps({'#password': 'a"b' + CANARY}),
            None,
            lambda: _raise(RuntimeError(f'Bad value {json.dumps("a" + chr(34) + "b" + CANARY)} for #password')),
            ['RuntimeError: Bad value "[REDACTED]" for #password'],
            id='failure_echoing_json_escaped_secret',
        ),
        pytest.param(
            {'user': 'alice', '#pin': int(NUMERIC_CANARY), '#enabled': True},
            None,
            lambda: _raise(RuntimeError(f'Rejected credential "{NUMERIC_CANARY}" with flag "True"')),
            ['alice', 'RuntimeError: Rejected credential "[REDACTED]" with flag "[REDACTED]"'],
            id='failure_echoing_numeric_and_boolean_secrets',
        ),
        pytest.param(
            f'user: alice\n"#password": prefix {CANARY}-suffix\nhost: db.example.com\n',
            None,
            None,
            ['[REDACTED]'],
            id='string_yaml_multiword_scalar',
        ),
        pytest.param(
            '```json\n{"#credentials": {"password": "' + CANARY + '"}}\n```',
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using {CANARY}')),
            ['RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_leaf_of_json_container',
        ),
        pytest.param(
            '```json\n' + json.dumps({'user': 'alice', '#password': f'café-{CANARY}'}) + '\n```',
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using café-{CANARY}')),
            ['alice', 'RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_unicode_escaped_scalar',
        ),
        pytest.param(
            str({'user': 'alice', '#api key': CANARY}),
            None,
            lambda: _raise(RuntimeError(f'Rejected {CANARY}')),
            ['alice', 'RuntimeError: Rejected [REDACTED]'],
            id='failure_echoing_secret_under_spaced_key',
        ),
        pytest.param(
            f"curl -d '#api key': '{CANARY}'",
            None,
            None,
            ['[REDACTED]'],
            id='string_unparsable_spaced_key',
        ),
        pytest.param(
            {'#credentials': json.dumps({'password': CANARY})},
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using {CANARY}')),
            ['RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_leaf_of_json_string_under_secret_key',
        ),
        pytest.param(
            f'#PASSWORD=prefix,{CANARY}',
            None,
            lambda: _raise(RuntimeError(f'Rejected {CANARY}')),
            ['RuntimeError: Rejected [REDACTED]'],
            id='string_env_value_with_comma',
        ),
        pytest.param(
            f'"#password": prefix,{CANARY}',
            None,
            None,
            ['[REDACTED]'],
            id='string_yaml_value_with_comma',
        ),
        pytest.param(
            f'"#password": >\n  first line\n  {CANARY}\n',
            None,
            None,
            ['[REDACTED]'],
            id='string_yaml_folded_continuation',
        ),
        pytest.param(
            f"\"#password\": 'prefix''{CANARY}'",
            None,
            None,
            ['[REDACTED]'],
            id='string_yaml_doubled_apostrophe',
        ),
        pytest.param(
            '```json\n{"user": "alice", "\\u0023password": "' + CANARY + '"}\n```',
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using {CANARY}')),
            ['alice', 'RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_secret_under_escaped_key_in_fenced_json',
        ),
        pytest.param(
            f'user: alice\n"\\u0023password": {CANARY}\n',
            None,
            None,
            ['[REDACTED]'],
            id='string_unparsable_escaped_key',
        ),
        pytest.param(
            f'{{"user": "alice", "#password": "first-{CANARY}", "#password": "second"}}',
            None,
            lambda: _raise(RuntimeError(f'Rejected first-{CANARY}')),
            ['alice', 'RuntimeError: Rejected [REDACTED]'],
            id='failure_echoing_secret_of_duplicate_json_key',
        ),
        pytest.param(
            None,
            [
                ConfigParamSet(op='set', path="db['user', '#password']", value=CANARY),
                ConfigParamReplace(op='str_replace', path="db['host', '#token']", search_for=CANARY, replace_with='x'),
            ],
            None,
            ['[REDACTED]'],
            id='update_config_union_path_selector',
        ),
        pytest.param(
            '#PASSWORD=password',
            None,
            lambda: _raise(RuntimeError('Rejected credential "password"')),
            ['RuntimeError: Rejected credential "[REDACTED]"'],
            id='failure_echoing_env_credential',
        ),
        pytest.param(
            '"#password": pass',
            None,
            lambda: _raise(RuntimeError('Rejected credential "pass"')),
            ['RuntimeError: Rejected credential "[REDACTED]"'],
            id='failure_echoing_short_alphabetic_yaml_credential',
        ),
        pytest.param(
            str({'user': 'alice', 'items': ({'#password': CANARY},)}),
            None,
            None,
            ['alice', '[REDACTED]'],
            id='string_python_literal_tuple_container',
        ),
        pytest.param(
            str({'#credentials': (CANARY, 'other')}),
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using {CANARY}')),
            ['RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_leaf_of_tuple_under_secret_key',
        ),
        pytest.param(
            {'#password': CANARY, 'dsn': f'postgresql://alice:{CANARY}@db/app'},
            None,
            None,
            ['postgresql://alice:[REDACTED]@db/app'],
            id='secret_repeated_in_sibling_field',
        ),
        pytest.param(
            {'dsn': f'postgresql://alice:{CANARY}@db/app', '#password': CANARY},
            None,
            None,
            ['postgresql://alice:[REDACTED]@db/app'],
            id='secret_repeated_in_sibling_field_before_the_secret',
        ),
        pytest.param(
            {'#password': CANARY},
            [ConfigParamSet(op='set', path='db.dsn', value=f'postgresql://alice:{CANARY}@db/app')],
            None,
            ['postgresql://alice:[REDACTED]@db/app'],
            id='secret_repeated_in_another_argument',
        ),
        pytest.param(
            f"{{'#password': 'first-{CANARY}', '#password': 'second'}}",
            None,
            lambda: _raise(RuntimeError(f'Rejected first-{CANARY}')),
            ['RuntimeError: Rejected [REDACTED]'],
            id='failure_echoing_secret_of_duplicate_literal_key',
        ),
        pytest.param(
            {'#credentials': json.dumps({'token': CANARY, 'padding': 'x' * (MAX_ARG_VALUE_LEN + 1)})},
            None,
            lambda: _raise(RuntimeError(f'Rejected {CANARY}')),
            ['RuntimeError: [details omitted: argument redaction failed]'],
            id='failure_with_oversized_secret_container',
        ),
        pytest.param(
            '#password: "caf\\u00e9-' + CANARY + '"',
            None,
            lambda: _raise(RuntimeError(f'Rejected café-{CANARY}')),
            ['RuntimeError: Rejected [REDACTED]'],
            id='failure_echoing_escaped_yaml_credential',
        ),
        pytest.param(
            {'#pin': int(NUMERIC_CANARY), 'pin': int(NUMERIC_CANARY), '#code': SHORT_CANARY, 'code': SHORT_CANARY},
            None,
            None,
            ['[REDACTED]'],
            id='secret_scalars_repeated_exactly',
        ),
        pytest.param(
            {'#password': MIXED_QUOTES_SECRET},
            None,
            lambda: _raise(RuntimeError(f'Rejected credential {MIXED_QUOTES_SECRET!r}')),
            ['RuntimeError: Rejected credential'],
            id='failure_echoing_python_repr_of_mixed_quote_secret',
        ),
        pytest.param(
            {'#code': 'ab', '#pin': 1234},
            None,
            lambda: _raise(RuntimeError('Rejected credential ab; Rejected pin 1234 for config 7777')),
            ['Rejected credential [REDACTED]', 'Rejected pin [REDACTED]', 'config 7777'],
            id='failure_echoing_unquoted_short_credentials',
        ),
        pytest.param(
            {'#password': CANARY, 'lookup': {CANARY: True}},
            None,
            None,
            ['lookup'],
            id='secret_used_as_dictionary_key',
        ),
        pytest.param(
            {'#credentials': {CANARY: True}, 'echo': CANARY},
            None,
            None,
            ['echo'],
            id='secret_used_as_key_inside_secret_object',
        ),
        pytest.param(
            {'#credentials': f'#PASSWORD={CANARY}', 'dsn': f'postgresql://alice:{CANARY}@db/app'},
            None,
            lambda: _raise(RuntimeError(f'Rejected {CANARY}')),
            ['postgresql://alice:[REDACTED]@db/app', 'RuntimeError: Rejected [REDACTED]'],
            id='opaque_text_under_secret_key',
        ),
        pytest.param(
            f"{{u'#password': 'first-{CANARY}', '#password': 'second'}}",
            None,
            lambda: _raise(RuntimeError(f'Rejected first-{CANARY}')),
            ['RuntimeError: Rejected [REDACTED]'],
            id='failure_echoing_secret_of_equivalent_literal_keys',
        ),
        pytest.param(
            {'#pin': int(NUMERIC_CANARY), 'echo': json.dumps({'again': int(NUMERIC_CANARY)})},
            None,
            None,
            ['echo'],
            id='secret_number_repeated_in_serialized_container',
        ),
        pytest.param(
            {
                '#password': MIXED_QUOTES_SECRET,
                'echo': json.dumps({'nested': json.dumps({'password': MIXED_QUOTES_SECRET})}),
            },
            None,
            None,
            ['echo'],
            id='secret_repeated_in_nested_serialized_containers',
        ),
        pytest.param(
            None,
            None,
            lambda: _raise(RuntimeError(f'Upstream rejected #PASSWORD=prefix,{CANARY}')),
            ['RuntimeError: [details omitted: the message shows a secret key]'],
            id='failure_exception_only_env_fragment',
        ),
        pytest.param(
            None,
            None,
            lambda: _raise(RuntimeError(f'Upstream rejected "#api key": "{CANARY}"')),
            ['RuntimeError: [details omitted: the message shows a secret key]'],
            id='failure_exception_only_spaced_key',
        ),
        pytest.param(
            f'GET /callback?id=1#access_token={CANARY}',
            None,
            None,
            ['[REDACTED]'],
            id='string_secret_in_url_fragment',
        ),
        pytest.param(
            None,
            None,
            lambda: _raise(RuntimeError(f'Redirected to /callback?id=1#access_token={CANARY}')),
            ['RuntimeError: [details omitted: the message shows a secret key]'],
            id='failure_exception_only_url_fragment',
        ),
        pytest.param(
            None,
            [
                ConfigParamSet(op='set', path='db.*', value=CANARY),
                ConfigParamReplace(op='str_replace', path='db."*"', search_for=CANARY, replace_with='x'),
            ],
            None,
            ['[REDACTED]'],
            id='update_config_wildcard_selector',
        ),
        pytest.param(
            '#password: "caf\\xE9-' + CANARY + '"',
            None,
            lambda: _raise(RuntimeError(f'Rejected café-{CANARY}')),
            ['RuntimeError: [details omitted: argument redaction failed]'],
            id='failure_with_undecodable_escape_in_yaml_credential',
        ),
        pytest.param(
            {'#x': f'#PASSWORD=a\\b-{CANARY}', 'dsn': f'a\\\\b-{CANARY}'},
            None,
            None,
            ['[REDACTED]'],
            id='opaque_token_repeated_in_python_repr_form',
        ),
        pytest.param(
            '#PASSWORD=input-credential',
            None,
            lambda: _raise(RuntimeError(f'Upstream #PASSWORD={CANARY}')),
            ['RuntimeError: [details omitted: the message shows a secret key]'],
            id='failure_exception_only_credential_differing_from_argument',
        ),
        pytest.param(
            f'"#vendor\'s_api_key": {CANARY}',
            None,
            None,
            ['[REDACTED]'],
            id='string_quoted_key_containing_an_apostrophe',
        ),
        pytest.param(
            '"#key\\"x": ' + CANARY,
            None,
            None,
            ['[REDACTED]'],
            id='string_quoted_key_containing_an_escaped_quote',
        ),
        pytest.param(
            {'env': f'#PASSWORD={CANARY}={DELIMITED_TAIL}', 'echo': f'{CANARY}={DELIMITED_TAIL}'},
            None,
            lambda: _raise(RuntimeError(f'Rejected {CANARY}={DELIMITED_TAIL}')),
            ['RuntimeError: Rejected [REDACTED]'],
            id='opaque_credential_with_delimiters_repeated',
        ),
        pytest.param(
            f"{{'#credentials': {{'password': '{CANARY}'}}}}",
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using {CANARY}')),
            ['RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_leaf_of_python_container',
        ),
        pytest.param(
            f'#creds: {{password: {CANARY}',
            None,
            lambda: _raise(RuntimeError(f'Authentication failed using {CANARY}')),
            ['RuntimeError: Authentication failed using [REDACTED]'],
            id='failure_echoing_leaf_of_unparsable_container',
        ),
        pytest.param(
            None,
            [ConfigParamReplace(op='str_replace', path='#password', search_for=SHORT_CANARY, replace_with='new')],
            lambda: _raise(ValueError(f'Search string "{SHORT_CANARY}" not found in path "#password"')),
            ['ValueError: Search string "[REDACTED]" not found'],
            id='failure_echoing_short_secret_value',
        ),
        pytest.param(
            None,
            [
                ConfigParamReplace(op='str_replace', path="db['#token']", search_for=CANARY, replace_with='new'),
                ConfigParamReplace(op='str_replace', path='db["#token"]', search_for='old', replace_with=CANARY),
                ConfigParamReplace(op='str_replace', path='db.query', search_for='alice', replace_with='bob'),
            ],
            None,
            ['alice', 'bob', '[REDACTED]'],
            id='update_config_str_replace',
        ),
        pytest.param(
            None,
            [ConfigParamReplace(op='str_replace', path='#password', search_for=CANARY, replace_with='new')],
            lambda: _raise(ValueError(f'Search string "{CANARY}" not found in path "#password"')),
            ['ValueError: Search string "[REDACTED]"'],
            id='failure_echoing_secret_value',
        ),
        pytest.param(
            {'user': 'alice', '#password': CANARY},
            None,
            lambda: _raise(RuntimeError(f"Connection refused for {{'#pwd': '{CANARY}x'}} and #token={CANARY}y")),
            ['alice', 'RuntimeError: [details omitted: the message shows a secret key]'],
            id='failure_echoing_secret_fragment',
        ),
        pytest.param(
            {'#pin': 401, '#code': 'ab', '#password': CANARY},
            None,
            lambda: _raise(RuntimeError(f'HTTP 401 for config ab-401 using {CANARY}')),
            ['RuntimeError: HTTP 401 for config ab-401 using [REDACTED]'],
            id='failure_short_secrets_not_replaced_globally',
        ),
        pytest.param(
            None,
            None,
            _raise_pydantic_error,
            ['parse string as an integer (int_parsing) at #password'],
            id='failure_pydantic_validation',
        ),
        pytest.param(
            None,
            None,
            _raise_jsonschema_error,
            ['"type" constraint violated (schema: "integer")'],
            id='failure_jsonschema_validation',
        ),
        pytest.param(
            None,
            None,
            _raise_jsonschema_secret_enum_error,
            ['"enum" constraint violated'],
            id='failure_jsonschema_secret_enum_not_reported',
        ),
    ],
)
async def test_secrets_are_redacted_in_event(
    parameters, parameter_updates, failure, preserved, storage_event_post, caplog, empty_context: Context
):
    @tool_errors()
    async def config_tool(_ctx: Context, parameters: Any = None, parameter_updates: Any = None) -> str:
        if failure:
            failure()
        return 'ok'

    original = copy.deepcopy((parameters, parameter_updates))
    with caplog.at_level(logging.INFO, logger='keboola_mcp_server.clients.storage'):
        if failure:
            with pytest.raises((ToolError, ValueError, RuntimeError)):
                await config_tool(empty_context, parameters=parameters, parameter_updates=parameter_updates)
        else:
            await config_tool(empty_context, parameters=parameters, parameter_updates=parameter_updates)

    assert (parameters, parameter_updates) == original
    storage_event_post.assert_awaited_once()
    payload = json.dumps(storage_event_post.await_args.kwargs['data'], ensure_ascii=False)
    log_lines = [r.getMessage() for r in caplog.records if '[trigger_event]' in r.getMessage()]
    assert len(log_lines) == 1
    for secret in (CANARY, SHORT_CANARY, FAKE_CIPHER, ENUM_CANARY, NUMERIC_CANARY, DELIMITED_TAIL):
        assert secret not in payload
        assert secret not in log_lines[0]
    for value in preserved:
        assert value in log_lines[0]


@pytest.mark.asyncio
async def test_redaction_failure_reports_argument_names_only(
    mocker, storage_event_post, caplog, empty_context: Context
):
    mocker.patch('keboola_mcp_server.errors._redact_value', side_effect=RuntimeError('boom'))

    @tool_errors()
    async def config_tool(_ctx: Context, parameters: Any = None) -> str:
        return 'ok'

    with caplog.at_level(logging.WARNING, logger='keboola_mcp_server.errors'):
        await config_tool(empty_context, parameters={'#password': CANARY})

    warning = next(r.getMessage() for r in caplog.records if 'Failed to redact' in r.getMessage())
    assert 'RuntimeError' in warning
    assert '_redact_arguments' in warning
    assert 'boom' not in warning
    data = storage_event_post.await_args.kwargs['data']
    assert data['params']['tool']['arguments'] == [{'key': 'parameters', 'value': '"[redaction failed]"'}]
    assert CANARY not in json.dumps(data)


@pytest.mark.asyncio
async def test_redaction_failure_omits_exception_text(mocker, storage_event_post, empty_context: Context):
    mocker.patch('keboola_mcp_server.errors._redact_value', side_effect=RuntimeError('boom'))

    @tool_errors()
    async def config_tool(_ctx: Context, parameters: Any = None) -> str:
        raise RuntimeError(f'Rejected credential {CANARY}')

    with pytest.raises(RuntimeError):
        await config_tool(empty_context, parameters={'#password': CANARY})

    data = storage_event_post.await_args.kwargs['data']
    assert data['message'] == (
        'MCP tool "config_tool" call failed. RuntimeError: [details omitted: argument redaction failed]'
    )
    assert CANARY not in json.dumps(data)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('failure', 'expected_level', 'recovery'),
    [
        (_raise_jsonschema_error, logging.WARNING, None),
        (lambda: _raise(RuntimeError(f'Rejected credential {CANARY}')), logging.ERROR, None),
        (_originating_failure, logging.ERROR, 'Retry later.'),
    ],
    ids=['caller_error_warning', 'internal_error_traceback', 'wrapped_internal_error_keeps_originating_frame'],
)
async def test_failure_log_lines_do_not_contain_secrets(
    failure, expected_level, recovery, storage_event_post, caplog, empty_context: Context
):
    @tool_errors(default_recovery=recovery)
    async def config_tool(_ctx: Context, parameters: Any = None) -> str:
        failure()
        return 'ok'

    with pytest.raises((ToolError, RuntimeError)):
        await config_tool(empty_context, parameters={'#password': CANARY})

    records = [r for r in caplog.records if r.name == 'keboola_mcp_server.errors']
    assert [r.levelno for r in records] == [expected_level]
    formatter = logging.Formatter()
    assert CANARY not in formatter.format(records[0])
    assert (records[0].exc_info is not None) is (expected_level == logging.ERROR)
    if records[0].exc_info:
        assert 'RuntimeError' in formatter.format(records[0])
        assert 'config_tool' in formatter.format(records[0])
    if failure is _originating_failure:
        assert 'in _originating_failure' in formatter.format(records[0])


def test_short_secret_number_repeated_in_serialized_container_is_masked():
    redacted = _redact_arguments({'parameters': json.dumps({'#pin': 1234, 'echo': 1234, 'port': 80})})

    value = redacted.arguments[0]['value']
    assert '1234' not in value
    assert '"port\\": 80' in value


@pytest.mark.parametrize('text', ['#' * 20_000, '#' * 20_000 + ':', '"#' * 10_000, "'#" * 10_000, '#a' * 10_000])
def test_secret_marker_scan_is_linear(text: str):
    started = time.perf_counter()
    _SECRET_MARKER.search(text)

    assert time.perf_counter() - started < 0.5


def _words(count: int) -> str:
    return ' '.join(''.join(chr(97 + (index // 26**place) % 26) for place in range(4)) for index in range(count))


def test_redaction_of_many_known_secrets_is_bounded():
    arguments = {'opaque': '#PASSWORD=x\n' + _words(900)} | {f'arg{i}': _words(150) for i in range(40)}
    started = time.perf_counter()
    redacted = _redact_arguments(arguments)

    assert time.perf_counter() - started < 2
    assert redacted.complete


def test_redaction_gives_up_on_too_many_known_secrets():
    arguments = {'opaque': '#PASSWORD=x\n' + _words(3000)} | {f'arg{i}': _words(150) for i in range(40)}
    started = time.perf_counter()
    redacted = _redact_arguments(arguments)

    assert time.perf_counter() - started < 2
    assert not redacted.complete
    assert {argument['value'] for argument in redacted.arguments} == {'"[redaction failed]"'}
