"""Tests for the tool authorization middleware."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools import Tool

from keboola_mcp_server.authorization import READ_ONLY_TOOLS, ToolAuthorizationMiddleware


class TestReadOnlyTools:
    """Test that READ_ONLY_TOOLS contains the expected tools."""

    def test_read_only_tools_contains_expected_tools(self):
        expected_tools = {
            'get_configs',
            'get_components',
            'get_config_examples',
            'get_flows',
            'get_flow_examples',
            'get_flow_schema',
            'get_buckets',
            'get_tables',
            'query_data',
            'get_data_apps',
            'get_jobs',
            'search',
            'find_component_id',
            'get_project_info',
            'docs_query',
        }
        assert READ_ONLY_TOOLS == expected_tools

    def test_read_only_tools_is_frozenset(self):
        assert isinstance(READ_ONLY_TOOLS, frozenset)


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
        tools = []
        for name in ['get_configs', 'create_config', 'get_buckets', 'update_descriptions', 'query_data']:
            tool = MagicMock(spec=Tool)
            tool.name = name
            tools.append(tool)
        return tools

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

        call_next = AsyncMock(return_value=MagicMock())

        with patch('keboola_mcp_server.authorization.get_http_request_or_none', return_value=None):
            await middleware.on_call_tool(mock_middleware_context, call_next)

        call_next.assert_called_once_with(mock_middleware_context)

    @pytest.mark.asyncio
    async def test_on_call_tool_allowed_tool(self, middleware, mock_middleware_context):
        """When tool is in allowed list, call should proceed."""
        mock_middleware_context.message = MagicMock()
        mock_middleware_context.message.name = 'get_configs'

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
