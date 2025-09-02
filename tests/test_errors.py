import logging
import uuid
from importlib.metadata import distribution
from unittest.mock import ANY

import pytest
from fastmcp import Context
from mcp.shared.context import RequestContext

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.errors import ToolException, tool_errors
from keboola_mcp_server.mcp import ServerState


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
        with pytest.raises(ToolException) as excinfo:
            await decorated_func(mcp_context_client)
        assert expected_recovery_message in str(excinfo.value)
    assert exception_message in str(excinfo.value)


@pytest.mark.asyncio
async def test_logging_on_tool_exception(caplog, function_with_value_error, mcp_context_client: Context):
    """Test that tool_errors decorator logs exceptions properly."""
    decorated_func = tool_errors()(function_with_value_error)

    with pytest.raises(ValueError, match='Simulated ValueError'):
        await decorated_func(mcp_context_client)

    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert 'Failed to run tool func' in caplog.records[0].message
    assert 'Simulated ValueError' in caplog.records[0].message


@pytest.mark.asyncio
@pytest.mark.parametrize('transport', ['http', 'stdio'])
async def test_get_session_id(transport: str, mcp_context_client: Context, mocker):
    @tool_errors()
    async def foo(_ctx: Context):
        pass

    session_id = uuid.uuid4().hex
    if transport == 'stdio':
        mcp_context_client.session_id = None
        mcp_context_client.request_context = mocker.MagicMock(RequestContext)
        mcp_context_client.request_context.lifespan_context = ServerState(config=Config(), server_id=session_id)
    elif transport == 'http':
        mcp_context_client.session_id = session_id
    else:
        pytest.fail(f'Unknown transport: {transport}')

    await foo(mcp_context_client)
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.storage_client.trigger_event.assert_called_once_with(
        message='MCP tool "foo" call succeeded.',
        component_id='keboola.mcp-server-tool',
        event_type='success',
        params={
            'mcpServerContext': {
                'appEnv': 'DEV',
                'version': distribution('keboola_mcp_server').version,
                'userAgent': '',
                'sessionId': session_id,
            },
            'tool': {
                'name': 'foo',
                'arguments': [],
            },
        },
        duration=ANY,
    )
