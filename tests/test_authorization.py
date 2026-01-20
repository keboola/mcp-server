"""Tests for the tool authorization middleware."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools import Tool
from mcp.types import ToolAnnotations

from keboola_mcp_server.authorization import ToolAuthorizationMiddleware


def create_mock_tool(name: str, read_only: bool = False) -> MagicMock:
    """Create a mock Tool with the given name and read-only annotation."""
    tool = MagicMock(spec=Tool)
    tool.name = name
    if read_only:
        tool.annotations = MagicMock(spec=ToolAnnotations)
        tool.annotations.readOnlyHint = True
    else:
        tool.annotations = MagicMock(spec=ToolAnnotations)
        tool.annotations.readOnlyHint = False
    return tool


class TestToolAuthorizationMiddleware:
    """Tests for ToolAuthorizationMiddleware."""

    @pytest.fixture
    def middleware(self):
        return ToolAuthorizationMiddleware()

    @pytest.fixture
    def mock_context(self):
        ctx = MagicMock(spec=Context)
        return ctx

    @pytest.fixture
    def mock_middleware_context(self, mock_context):
        middleware_ctx = MagicMock(spec=MiddlewareContext)
        middleware_ctx.fastmcp_context = mock_context
        return middleware_ctx

    @pytest.fixture
    def sample_tools(self):
        """Create sample tools with proper read-only annotations.

        Read-only tools (readOnlyHint=True): get_configs, get_buckets, query_data
        Write tools (readOnlyHint=False): create_config, update_descriptions
        """
        return [
            create_mock_tool('get_configs', read_only=True),
            create_mock_tool('create_config', read_only=False),
            create_mock_tool('get_buckets', read_only=True),
            create_mock_tool('update_descriptions', read_only=False),
            create_mock_tool('query_data', read_only=True),
        ]

    @pytest.mark.asyncio
    async def test_on_list_tools_no_headers_returns_all_tools(self, middleware, mock_middleware_context, sample_tools):
        """When no authorization headers are present, all tools should be returned."""
        call_next = AsyncMock(return_value=sample_tools)

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=None):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        assert result == sample_tools
        call_next.assert_called_once_with(mock_middleware_context)

    @pytest.mark.asyncio
    async def test_on_list_tools_with_allowed_tools_header(self, middleware, mock_middleware_context, sample_tools):
        """When X-Allowed-Tools header is present, only those tools should be returned."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Allowed-Tools': 'get_configs, get_buckets'}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        assert len(result) == 2
        assert {t.name for t in result} == {'get_configs', 'get_buckets'}

    @pytest.mark.asyncio
    async def test_on_list_tools_with_read_only_mode(self, middleware, mock_middleware_context, sample_tools):
        """When X-Read-Only-Mode header is true, only read-only tools should be returned."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Read-Only-Mode': 'true'}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Only read-only tools should be returned
        result_names = {t.name for t in result}
        assert result_names == {'get_configs', 'get_buckets', 'query_data'}
        assert 'create_config' not in result_names
        assert 'update_descriptions' not in result_names

    @pytest.mark.asyncio
    async def test_on_list_tools_with_both_headers(self, middleware, mock_middleware_context, sample_tools):
        """When both headers are present, intersection should be returned."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {
            'X-Allowed-Tools': 'get_configs, create_config, get_buckets',
            'X-Read-Only-Mode': 'true',
        }

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Only tools that are both in allowed list AND read-only
        result_names = {t.name for t in result}
        assert result_names == {'get_configs', 'get_buckets'}
        assert 'create_config' not in result_names  # Not read-only

    @pytest.mark.asyncio
    @pytest.mark.parametrize('header_value', ['true', 'True', 'TRUE', '1', 'yes', 'Yes', 'YES'])
    async def test_on_list_tools_read_only_mode_truthy_values(
        self, middleware, mock_middleware_context, sample_tools, header_value
    ):
        """Test various truthy values for X-Read-Only-Mode header."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Read-Only-Mode': header_value}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Only read-only tools should be returned
        result_names = {t.name for t in result}
        assert 'create_config' not in result_names
        assert 'update_descriptions' not in result_names

    @pytest.mark.asyncio
    @pytest.mark.parametrize('header_value', ['false', 'False', '0', 'no', '', 'random'])
    async def test_on_list_tools_read_only_mode_falsy_values(
        self, middleware, mock_middleware_context, sample_tools, header_value
    ):
        """Test various falsy values for X-Read-Only-Mode header."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Read-Only-Mode': header_value}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # All tools should be returned
        assert result == sample_tools

    @pytest.mark.asyncio
    async def test_on_call_tool_no_headers_allows_all(self, middleware, mock_middleware_context):
        """When no authorization headers are present, all tool calls should be allowed."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'create_config'

        # Mock get_tool to return a tool with annotations
        mock_tool = create_mock_tool('create_config', read_only=False)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        call_next = AsyncMock(return_value=MagicMock())

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=None):
            await middleware.on_call_tool(mock_middleware_context, call_next)

        call_next.assert_called_once_with(mock_middleware_context)

    @pytest.mark.asyncio
    async def test_on_call_tool_allowed_tool(self, middleware, mock_middleware_context):
        """When tool is in allowed list, call should proceed."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'get_configs'

        # Mock get_tool to return a tool with annotations
        mock_tool = create_mock_tool('get_configs', read_only=True)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {'X-Allowed-Tools': 'get_configs, get_buckets'}

        call_next = AsyncMock(return_value=MagicMock())

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            await middleware.on_call_tool(mock_middleware_context, call_next)

        call_next.assert_called_once_with(mock_middleware_context)

    @pytest.mark.asyncio
    async def test_on_call_tool_denied_tool(self, middleware, mock_middleware_context):
        """When tool is not in allowed list, ToolError should be raised."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'create_config'

        # Mock get_tool to return a tool with annotations
        mock_tool = create_mock_tool('create_config', read_only=False)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {'X-Allowed-Tools': 'get_configs, get_buckets'}

        call_next = AsyncMock()

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            with pytest.raises(ToolError) as exc_info:
                await middleware.on_call_tool(mock_middleware_context, call_next)

        assert 'create_config' in str(exc_info.value)
        assert 'not authorized' in str(exc_info.value)
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_call_tool_read_only_mode_allows_read_only_tool(self, middleware, mock_middleware_context):
        """When read-only mode is enabled, read-only tools should be allowed."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'get_configs'

        # Mock get_tool to return a read-only tool
        mock_tool = create_mock_tool('get_configs', read_only=True)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {'X-Read-Only-Mode': 'true'}

        call_next = AsyncMock(return_value=MagicMock())

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            await middleware.on_call_tool(mock_middleware_context, call_next)

        call_next.assert_called_once_with(mock_middleware_context)

    @pytest.mark.asyncio
    async def test_on_call_tool_read_only_mode_denies_write_tool(self, middleware, mock_middleware_context):
        """When read-only mode is enabled, write tools should be denied."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'create_config'

        # Mock get_tool to return a write tool (not read-only)
        mock_tool = create_mock_tool('create_config', read_only=False)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {'X-Read-Only-Mode': 'true'}

        call_next = AsyncMock()

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            with pytest.raises(ToolError) as exc_info:
                await middleware.on_call_tool(mock_middleware_context, call_next)

        assert 'create_config' in str(exc_info.value)
        assert 'not authorized' in str(exc_info.value)
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_call_tool_combined_headers_intersection(self, middleware, mock_middleware_context):
        """When both headers are present, tool must be in both allowed list AND read-only."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'create_config'

        # Mock get_tool to return a write tool (not read-only)
        mock_tool = create_mock_tool('create_config', read_only=False)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {
            'X-Allowed-Tools': 'get_configs, create_config',
            'X-Read-Only-Mode': 'true',
        }

        call_next = AsyncMock()

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            with pytest.raises(ToolError) as exc_info:
                await middleware.on_call_tool(mock_middleware_context, call_next)

        # create_config is in allowed list but not read-only, so should be denied
        assert 'create_config' in str(exc_info.value)
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_allowed_tools_header_with_whitespace(self, middleware, mock_middleware_context, sample_tools):
        """Test that whitespace in X-Allowed-Tools header is handled correctly."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Allowed-Tools': '  get_configs  ,  get_buckets  ,  '}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        assert len(result) == 2
        assert {t.name for t in result} == {'get_configs', 'get_buckets'}

    @pytest.mark.asyncio
    async def test_empty_allowed_tools_header(self, middleware, mock_middleware_context, sample_tools):
        """Test that empty X-Allowed-Tools header is treated as no restriction."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Allowed-Tools': ''}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Empty header is treated as no restriction (same as no header)
        assert result == sample_tools

    # Tests for X-Disallowed-Tools header

    @pytest.mark.asyncio
    async def test_on_list_tools_with_disallowed_tools_header(self, middleware, mock_middleware_context, sample_tools):
        """When X-Disallowed-Tools header is present, those tools should be excluded."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Disallowed-Tools': 'create_config, update_descriptions'}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        assert len(result) == 3
        result_names = {t.name for t in result}
        assert result_names == {'get_configs', 'get_buckets', 'query_data'}
        assert 'create_config' not in result_names
        assert 'update_descriptions' not in result_names

    @pytest.mark.asyncio
    async def test_on_list_tools_with_allowed_and_disallowed_tools(
        self, middleware, mock_middleware_context, sample_tools
    ):
        """When both X-Allowed-Tools and X-Disallowed-Tools are present, disallowed takes precedence."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {
            'X-Allowed-Tools': 'get_configs, create_config, get_buckets',
            'X-Disallowed-Tools': 'create_config',
        }

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # create_config is in allowed but also in disallowed, so should be excluded
        result_names = {t.name for t in result}
        assert result_names == {'get_configs', 'get_buckets'}
        assert 'create_config' not in result_names

    @pytest.mark.asyncio
    async def test_on_list_tools_with_read_only_and_disallowed_tools(
        self, middleware, mock_middleware_context, sample_tools
    ):
        """When both X-Read-Only-Mode and X-Disallowed-Tools are present, both filters apply."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {
            'X-Read-Only-Mode': 'true',
            'X-Disallowed-Tools': 'get_configs',
        }

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Only read-only tools minus disallowed
        result_names = {t.name for t in result}
        assert result_names == {'get_buckets', 'query_data'}
        assert 'get_configs' not in result_names  # Disallowed
        assert 'create_config' not in result_names  # Not read-only
        assert 'update_descriptions' not in result_names  # Not read-only

    @pytest.mark.asyncio
    async def test_on_list_tools_with_all_three_headers(self, middleware, mock_middleware_context, sample_tools):
        """When all three headers are present, all filters apply in order."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {
            'X-Allowed-Tools': 'get_configs, get_buckets, query_data, create_config',
            'X-Read-Only-Mode': 'true',
            'X-Disallowed-Tools': 'query_data',
        }

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Allowed & read-only - disallowed
        result_names = {t.name for t in result}
        assert result_names == {'get_configs', 'get_buckets'}
        assert 'query_data' not in result_names  # Disallowed
        assert 'create_config' not in result_names  # Not read-only

    @pytest.mark.asyncio
    async def test_on_call_tool_disallowed_tool(self, middleware, mock_middleware_context):
        """When tool is in disallowed list, ToolError should be raised."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'create_config'

        # Mock get_tool to return a tool with annotations
        mock_tool = create_mock_tool('create_config', read_only=False)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {'X-Disallowed-Tools': 'create_config, update_descriptions'}

        call_next = AsyncMock()

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            with pytest.raises(ToolError) as exc_info:
                await middleware.on_call_tool(mock_middleware_context, call_next)

        assert 'create_config' in str(exc_info.value)
        assert 'not authorized' in str(exc_info.value)
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_call_tool_not_in_disallowed_list(self, middleware, mock_middleware_context):
        """When tool is not in disallowed list, call should proceed."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'get_configs'

        # Mock get_tool to return a tool with annotations
        mock_tool = create_mock_tool('get_configs', read_only=True)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {'X-Disallowed-Tools': 'create_config, update_descriptions'}

        call_next = AsyncMock(return_value=MagicMock())

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            await middleware.on_call_tool(mock_middleware_context, call_next)

        call_next.assert_called_once_with(mock_middleware_context)

    @pytest.mark.asyncio
    async def test_on_call_tool_in_allowed_but_also_disallowed(self, middleware, mock_middleware_context):
        """When tool is in both allowed and disallowed lists, disallowed takes precedence."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'get_configs'

        # Mock get_tool to return a tool with annotations
        mock_tool = create_mock_tool('get_configs', read_only=True)
        mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

        mock_request = MagicMock()
        mock_request.headers = {
            'X-Allowed-Tools': 'get_configs, get_buckets',
            'X-Disallowed-Tools': 'get_configs',
        }

        call_next = AsyncMock()

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            with pytest.raises(ToolError) as exc_info:
                await middleware.on_call_tool(mock_middleware_context, call_next)

        assert 'get_configs' in str(exc_info.value)
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_disallowed_tools_header_with_whitespace(self, middleware, mock_middleware_context, sample_tools):
        """Test that whitespace in X-Disallowed-Tools header is handled correctly."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Disallowed-Tools': '  create_config  ,  update_descriptions  ,  '}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        assert len(result) == 3
        result_names = {t.name for t in result}
        assert 'create_config' not in result_names
        assert 'update_descriptions' not in result_names

    @pytest.mark.asyncio
    async def test_empty_disallowed_tools_header(self, middleware, mock_middleware_context, sample_tools):
        """Test that empty X-Disallowed-Tools header is treated as no exclusion."""
        call_next = AsyncMock(return_value=sample_tools)

        mock_request = MagicMock()
        mock_request.headers = {'X-Disallowed-Tools': ''}

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=mock_request):
            result = await middleware.on_list_tools(mock_middleware_context, call_next)

        # Empty header is treated as no exclusion (same as no header)
        assert result == sample_tools
