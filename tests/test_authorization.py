"""Tests for the tool authorization middleware.

Uses parameterized tests to reduce boilerplate while maintaining comprehensive coverage.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools import Tool
from mcp.types import ToolAnnotations

from keboola_mcp_server.authorization import ToolAuthorizationMiddleware
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.rls import RlsRules

# Sample tools: 3 read-only (get_configs, get_buckets, query_data), 2 write (create_config, update_descriptions)
ALL_TOOLS = {'get_configs', 'create_config', 'get_buckets', 'update_descriptions', 'query_data'}
READ_ONLY_TOOLS = {'get_configs', 'get_buckets', 'query_data'}


def create_mock_tool(name: str, read_only: bool = False) -> MagicMock:
    """Create a mock Tool with the given name and read-only annotation."""
    tool = MagicMock(spec=Tool)
    tool.name = name
    tool.annotations = MagicMock(spec=ToolAnnotations)
    tool.annotations.readOnlyHint = read_only
    return tool


@pytest.fixture
def middleware():
    return ToolAuthorizationMiddleware()


@pytest.fixture
def mock_middleware_context():
    ctx = MagicMock(spec=Context)
    middleware_ctx = MagicMock(spec=MiddlewareContext)
    middleware_ctx.fastmcp_context = ctx
    return middleware_ctx


def server_state(*, rls: bool) -> ServerState:
    """A lifespan state with or without RLS rules loaded."""
    return ServerState(
        config=Config(),
        runtime_info=ServerRuntimeInfo(transport='stdio'),
        rls_rules=RlsRules(tables={'invoices': {'petr': 'TRUE'}}) if rls else None,
    )


@pytest.fixture
def rls_middleware_context(mock_middleware_context):
    """A middleware context whose lifespan state carries RLS rules (i.e. the server runs in RLS mode)."""
    mock_middleware_context.fastmcp_context.request_context.lifespan_context = server_state(rls=True)
    return mock_middleware_context


@pytest.fixture
def sample_tools():
    """Create sample tools with proper read-only annotations."""
    return [
        create_mock_tool('get_configs', read_only=True),
        create_mock_tool('create_config', read_only=False),
        create_mock_tool('get_buckets', read_only=True),
        create_mock_tool('update_descriptions', read_only=False),
        create_mock_tool('query_data', read_only=True),
    ]


# Parameterized test for on_list_tools with various header combinations
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('headers', 'expected_tools'),
    [
        # No headers - all tools returned
        (None, ALL_TOOLS),
        ({}, ALL_TOOLS),
        # X-Allowed-Tools only
        ({'X-Allowed-Tools': 'get_configs, get_buckets'}, {'get_configs', 'get_buckets'}),
        # X-Read-Only-Mode only
        ({'X-Read-Only-Mode': 'true'}, READ_ONLY_TOOLS),
        # X-Disallowed-Tools only
        ({'X-Disallowed-Tools': 'create_config, update_descriptions'}, READ_ONLY_TOOLS),
        # X-Allowed-Tools + X-Read-Only-Mode (intersection)
        (
            {'X-Allowed-Tools': 'get_configs, create_config, get_buckets', 'X-Read-Only-Mode': 'true'},
            {'get_configs', 'get_buckets'},
        ),
        # X-Allowed-Tools + X-Disallowed-Tools (disallowed takes precedence)
        (
            {'X-Allowed-Tools': 'get_configs, create_config, get_buckets', 'X-Disallowed-Tools': 'create_config'},
            {'get_configs', 'get_buckets'},
        ),
        # X-Read-Only-Mode + X-Disallowed-Tools
        ({'X-Read-Only-Mode': 'true', 'X-Disallowed-Tools': 'get_configs'}, {'get_buckets', 'query_data'}),
        # All three headers
        (
            {
                'X-Allowed-Tools': 'get_configs, get_buckets, query_data, create_config',
                'X-Read-Only-Mode': 'true',
                'X-Disallowed-Tools': 'query_data',
            },
            {'get_configs', 'get_buckets'},
        ),
        # Empty/whitespace headers - treated as no restriction
        ({'X-Allowed-Tools': ''}, ALL_TOOLS),
        ({'X-Allowed-Tools': '  ,  ,  '}, ALL_TOOLS),
        ({'X-Disallowed-Tools': ''}, ALL_TOOLS),
        # Whitespace handling
        ({'X-Allowed-Tools': '  get_configs  ,  get_buckets  ,  '}, {'get_configs', 'get_buckets'}),
        ({'X-Disallowed-Tools': '  create_config  ,  update_descriptions  ,  '}, READ_ONLY_TOOLS),
    ],
    ids=[
        'no_headers_none',
        'no_headers_empty_dict',
        'allowed_tools_only',
        'read_only_mode_only',
        'disallowed_tools_only',
        'allowed_and_read_only',
        'allowed_and_disallowed',
        'read_only_and_disallowed',
        'all_three_headers',
        'empty_allowed_tools',
        'whitespace_only_allowed_tools',
        'empty_disallowed_tools',
        'allowed_tools_with_whitespace',
        'disallowed_tools_with_whitespace',
    ],
)
async def test_on_list_tools(middleware, mock_middleware_context, sample_tools, headers, expected_tools):
    """Test on_list_tools with various header combinations."""
    call_next = AsyncMock(return_value=sample_tools)
    mock_request = MagicMock()
    mock_request.headers = headers if headers else {}
    http_request = mock_request if headers is not None else None

    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=http_request):
        result = await middleware.on_list_tools(mock_middleware_context, call_next)

    assert {t.name for t in result} == expected_tools


# Parameterized test for X-Read-Only-Mode truthy/falsy values
@pytest.mark.asyncio
@pytest.mark.parametrize('header_value', ['true', 'True', 'TRUE', '1', 'yes', 'Yes', 'YES'])
async def test_read_only_mode_truthy_values(middleware, mock_middleware_context, sample_tools, header_value):
    """Test that various truthy values enable read-only mode."""
    call_next = AsyncMock(return_value=sample_tools)
    mock_request = MagicMock()
    mock_request.headers = {'X-Read-Only-Mode': header_value}

    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
        result = await middleware.on_list_tools(mock_middleware_context, call_next)

    assert {t.name for t in result} == READ_ONLY_TOOLS


@pytest.mark.asyncio
@pytest.mark.parametrize('header_value', ['false', 'False', '0', 'no', '', 'random'])
async def test_read_only_mode_falsy_values(middleware, mock_middleware_context, sample_tools, header_value):
    """Test that various falsy values do not enable read-only mode."""
    call_next = AsyncMock(return_value=sample_tools)
    mock_request = MagicMock()
    mock_request.headers = {'X-Read-Only-Mode': header_value}

    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
        result = await middleware.on_list_tools(mock_middleware_context, call_next)

    assert result == sample_tools


# Parameterized test for on_call_tool with various header combinations
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('tool_name', 'tool_read_only', 'headers', 'should_allow'),
    [
        # No headers - all tools allowed
        ('create_config', False, None, True),
        ('get_configs', True, None, True),
        # X-Allowed-Tools - tool in list
        ('get_configs', True, {'X-Allowed-Tools': 'get_configs, get_buckets'}, True),
        # X-Allowed-Tools - tool not in list
        ('create_config', False, {'X-Allowed-Tools': 'get_configs, get_buckets'}, False),
        # X-Read-Only-Mode - read-only tool
        ('get_configs', True, {'X-Read-Only-Mode': 'true'}, True),
        # X-Read-Only-Mode - write tool
        ('create_config', False, {'X-Read-Only-Mode': 'true'}, False),
        # X-Disallowed-Tools - tool in list
        ('create_config', False, {'X-Disallowed-Tools': 'create_config, update_descriptions'}, False),
        # X-Disallowed-Tools - tool not in list
        ('get_configs', True, {'X-Disallowed-Tools': 'create_config, update_descriptions'}, True),
        # X-Allowed-Tools + X-Read-Only-Mode - tool in allowed but not read-only
        ('create_config', False, {'X-Allowed-Tools': 'get_configs, create_config', 'X-Read-Only-Mode': 'true'}, False),
        # X-Allowed-Tools + X-Disallowed-Tools - tool in both (disallowed wins)
        (
            'get_configs',
            True,
            {'X-Allowed-Tools': 'get_configs, get_buckets', 'X-Disallowed-Tools': 'get_configs'},
            False,
        ),
    ],
    ids=[
        'no_headers_write_tool',
        'no_headers_read_tool',
        'allowed_tool_in_list',
        'allowed_tool_not_in_list',
        'read_only_mode_read_tool',
        'read_only_mode_write_tool',
        'disallowed_tool_in_list',
        'disallowed_tool_not_in_list',
        'allowed_and_read_only_write_tool',
        'allowed_and_disallowed_same_tool',
    ],
)
async def test_on_call_tool(middleware, mock_middleware_context, tool_name, tool_read_only, headers, should_allow):
    """Test on_call_tool with various header combinations."""
    mock_middleware_context.message = MagicMock()
    mock_middleware_context.message.name = tool_name

    mock_tool = create_mock_tool(tool_name, read_only=tool_read_only)
    mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

    mock_request = MagicMock()
    mock_request.headers = headers if headers else {}
    http_request = mock_request if headers is not None else None

    call_next = AsyncMock(return_value=MagicMock())

    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=http_request):
        if should_allow:
            await middleware.on_call_tool(mock_middleware_context, call_next)
            call_next.assert_called_once_with(mock_middleware_context)
        else:
            with pytest.raises(ToolError) as exc_info:
                await middleware.on_call_tool(mock_middleware_context, call_next)
            assert tool_name in str(exc_info.value)
            assert 'not authorized' in str(exc_info.value)
            call_next.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'headers',
    [None, {}, {'X-Read-Only-Mode': 'false'}, {'X-Allowed-Tools': 'get_configs, create_config'}],
    ids=['no_http_request', 'no_headers', 'read_only_mode_off', 'allowed_tools_include_a_write_tool'],
)
async def test_rls_mode_lists_read_only_tools_regardless_of_headers(
    middleware, rls_middleware_context, sample_tools, headers
):
    """RLS mode is not a header: no combination of caller-supplied headers (including none at all,
    as on stdio) can put a write tool back on the list."""
    call_next = AsyncMock(return_value=sample_tools)
    mock_request = MagicMock()
    mock_request.headers = headers if headers else {}
    http_request = mock_request if headers is not None else None

    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=http_request):
        result = await middleware.on_list_tools(rls_middleware_context, call_next)

    assert {t.name for t in result} <= READ_ONLY_TOOLS


@pytest.mark.asyncio
async def test_rls_mode_blocks_calls_to_write_tools(middleware, rls_middleware_context):
    """Hiding a tool from tools/list is not enough -- a client that knows the name must be refused
    too, with a message that names RLS as the reason rather than blaming the client's headers."""
    rls_middleware_context.message = MagicMock()
    rls_middleware_context.message.name = 'create_config'
    rls_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(
        return_value=create_mock_tool('create_config', read_only=False)
    )
    call_next = AsyncMock(return_value=MagicMock())

    with (
        patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=None),
        pytest.raises(ToolError) as exc_info,
    ):
        await middleware.on_call_tool(rls_middleware_context, call_next)

    assert 'read-only' in str(exc_info.value)
    assert 'RLS' in str(exc_info.value)
    call_next.assert_not_called()


@pytest.mark.asyncio
async def test_rls_mode_allows_calls_to_read_only_tools(middleware, rls_middleware_context):
    rls_middleware_context.message = MagicMock()
    rls_middleware_context.message.name = 'query_data_rls'
    rls_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(
        return_value=create_mock_tool('query_data_rls', read_only=True)
    )
    call_next = AsyncMock(return_value=MagicMock())

    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=None):
        await middleware.on_call_tool(rls_middleware_context, call_next)

    call_next.assert_called_once_with(rls_middleware_context)


@pytest.mark.parametrize(('rls', 'expected_read_only'), [(True, True), (False, False)])
def test_authorization_config_reports_rls_as_read_only(rls, expected_read_only):
    """The raw `/preview/configuration` route reuses `_get_authorization_config` with the state it
    reads from the Starlette app, so this is also what keeps that route from becoming a back door
    around RLS mode."""
    mock_request = MagicMock()
    mock_request.headers = {}

    allowed, disallowed, read_only_mode = ToolAuthorizationMiddleware._get_authorization_config(
        mock_request, server_state=server_state(rls=rls)
    )

    assert (allowed, disallowed) == (None, None)
    assert read_only_mode is expected_read_only


def test_authorization_config_without_server_state_is_unrestricted():
    """Call sites that cannot reach the lifespan state must keep behaving exactly as before."""
    with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=None):
        assert ToolAuthorizationMiddleware._get_authorization_config() == (None, None, False)
