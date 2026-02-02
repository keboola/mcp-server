"""Tests for role-based tool authorization in ToolsFilteringMiddleware.

Uses parameterized tests to reduce boilerplate while maintaining comprehensive coverage.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from fastmcp.server.middleware import MiddlewareContext
from fastmcp.tools import Tool
from mcp.types import ToolAnnotations

from keboola_mcp_server.mcp import ToolsFilteringMiddleware

# Sample tools: read-only and write tools
ALL_TOOLS = {
    'get_jobs',
    'get_buckets',
    'get_tables',
    'get_project_info',
    'docs_query',
    'query_data',
    'search',
    'find_component_id',
    'get_data_apps',
    'get_components',
    'get_configs',
    'get_config_examples',
    'get_flows',
    'get_flow_examples',
    'get_flow_schema',
    'create_config',
    'update_descriptions',
    'delete_bucket',
    'create_flow',
}

READ_ONLY_TOOLS = {
    'get_jobs',
    'get_buckets',
    'get_tables',
    'get_project_info',
    'docs_query',
    'query_data',
    'search',
    'find_component_id',
    'get_data_apps',
    'get_components',
    'get_configs',
    'get_config_examples',
    'get_flows',
    'get_flow_examples',
    'get_flow_schema',
}


def create_mock_tool(name: str, read_only: bool = False) -> MagicMock:
    """Create a mock Tool with the given name and read-only annotation."""
    tool = MagicMock(spec=Tool)
    tool.name = name
    tool.annotations = MagicMock(spec=ToolAnnotations)
    tool.annotations.readOnlyHint = read_only
    return tool


@pytest.fixture
def middleware():
    return ToolsFilteringMiddleware()


@pytest.fixture
def mock_middleware_context():
    ctx = MagicMock(spec=Context)
    middleware_ctx = MagicMock(spec=MiddlewareContext)
    middleware_ctx.fastmcp_context = ctx
    return middleware_ctx


@pytest.fixture
def sample_tools():
    """Create sample tools with proper read-only annotations."""
    tools = []
    # Add read-only tools
    for name in READ_ONLY_TOOLS:
        tools.append(create_mock_tool(name, read_only=True))
    # Add write tools
    for name in ALL_TOOLS - READ_ONLY_TOOLS:
        tools.append(create_mock_tool(name, read_only=False))
    return tools


# Test role detection helpers
@pytest.mark.parametrize(
    ('token_info', 'expected_guest', 'expected_read', 'expected_requires_ro'),
    [
        # Guest role
        ({'admin': {'role': 'guest'}}, True, False, True),
        ({'admin': {'role': 'Guest'}}, True, False, True),
        ({'admin': {'role': 'GUEST'}}, True, False, True),
        # Read role
        ({'admin': {'role': 'read'}}, False, True, True),
        ({'admin': {'role': 'Read'}}, False, True, True),
        ({'admin': {'role': 'READ'}}, False, True, True),
        # Admin role
        ({'admin': {'role': 'admin'}}, False, False, False),
        ({'admin': {'role': 'Admin'}}, False, False, False),
        # Developer role
        ({'admin': {'role': 'developer'}}, False, False, False),
        # Missing role
        ({'admin': {'role': None}}, False, False, False),
        ({'admin': {}}, False, False, False),
        ({}, False, False, False),
        # Empty admin dict
        ({'admin': None}, False, False, False),
    ],
    ids=[
        'guest_role_lowercase',
        'guest_role_titlecase',
        'guest_role_uppercase',
        'read_role_lowercase',
        'read_role_titlecase',
        'read_role_uppercase',
        'admin_role_lowercase',
        'admin_role_titlecase',
        'developer_role',
        'missing_role_none',
        'missing_role_empty_dict',
        'missing_admin_key',
        'admin_none',
    ],
)
def test_role_detection(token_info, expected_guest, expected_read, expected_requires_ro):
    """Test role detection helper methods with various token info configurations."""
    assert ToolsFilteringMiddleware.is_guest_role(token_info) == expected_guest
    assert ToolsFilteringMiddleware.is_read_only_role(token_info) == expected_read
    assert ToolsFilteringMiddleware.requires_read_only_access(token_info) == expected_requires_ro


# Test on_list_tools() filtering
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('token_info', 'expected_tool_count', 'expected_has_write'),
    [
        # Guest role - should only get read-only tools
        ({'admin': {'role': 'guest'}}, len(READ_ONLY_TOOLS), False),
        ({'admin': {'role': 'Guest'}}, len(READ_ONLY_TOOLS), False),
        # Read role - should only get read-only tools
        ({'admin': {'role': 'read'}}, len(READ_ONLY_TOOLS), False),
        ({'admin': {'role': 'READ'}}, len(READ_ONLY_TOOLS), False),
        # Admin role - should get all tools (after other filters)
        ({'admin': {'role': 'admin'}}, len(ALL_TOOLS), True),
        # Developer role - should get all tools
        ({'admin': {'role': 'developer'}}, len(ALL_TOOLS), True),
        # Missing role - should get all tools
        ({'admin': {}}, len(ALL_TOOLS), True),
    ],
    ids=[
        'guest_role_lowercase',
        'guest_role_titlecase',
        'read_role_lowercase',
        'read_role_uppercase',
        'admin_role',
        'developer_role',
        'missing_role',
    ],
)
async def test_on_list_tools_role_filtering(
    middleware, mock_middleware_context, sample_tools, token_info, expected_tool_count, expected_has_write
):
    """Test on_list_tools() with various roles."""
    # Mock the call chain
    call_next = AsyncMock(return_value=sample_tools)

    # Mock get_token_info to return our test token_info
    with patch.object(middleware, 'get_token_info', AsyncMock(return_value=token_info)):
        # Mock is_client_using_main_branch to return True (so branch filtering doesn't interfere)
        with patch.object(middleware, 'is_client_using_main_branch', return_value=True):
            # Mock get_project_features to return hide-conditional-flows so create_flow is allowed
            with patch.object(middleware, 'get_project_features', return_value={'hide-conditional-flows'}):
                result = await middleware.on_list_tools(mock_middleware_context, call_next)

                # Check expected number of tools
                assert len(result) == expected_tool_count

                # Check if write tools are present/absent
                result_names = {t.name for t in result}
                write_tools = ALL_TOOLS - READ_ONLY_TOOLS
                has_write = bool(result_names & write_tools)
                assert has_write == expected_has_write

                # For guest/read roles, verify only read-only tools are present
                if not expected_has_write:
                    assert result_names == READ_ONLY_TOOLS


# Test on_call_tool() blocking
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('token_info', 'tool_name', 'tool_read_only', 'should_allow'),
    [
        # Guest role - allow read-only, block write
        ({'admin': {'role': 'guest'}}, 'get_buckets', True, True),
        ({'admin': {'role': 'guest'}}, 'create_config', False, False),
        ({'admin': {'role': 'Guest'}}, 'query_data', True, True),
        ({'admin': {'role': 'GUEST'}}, 'delete_bucket', False, False),
        # Read role - allow read-only, block write
        ({'admin': {'role': 'read'}}, 'get_flows', True, True),
        ({'admin': {'role': 'read'}}, 'update_descriptions', False, False),
        ({'admin': {'role': 'READ'}}, 'docs_query', True, True),
        ({'admin': {'role': 'Read'}}, 'create_flow', False, False),
        # Admin role - allow all
        ({'admin': {'role': 'admin'}}, 'get_buckets', True, True),
        ({'admin': {'role': 'admin'}}, 'create_config', False, True),
        # Developer role - allow all
        ({'admin': {'role': 'developer'}}, 'query_data', True, True),
        ({'admin': {'role': 'developer'}}, 'delete_bucket', False, True),
        # Missing role - allow all
        ({'admin': {}}, 'get_flows', True, True),
        ({'admin': {}}, 'create_flow', False, True),
    ],
    ids=[
        'guest_read_tool_allowed',
        'guest_write_tool_blocked',
        'guest_titlecase_read_allowed',
        'guest_uppercase_write_blocked',
        'read_role_read_allowed',
        'read_role_write_blocked',
        'read_uppercase_read_allowed',
        'read_titlecase_write_blocked',
        'admin_read_allowed',
        'admin_write_allowed',
        'developer_read_allowed',
        'developer_write_allowed',
        'missing_role_read_allowed',
        'missing_role_write_allowed',
    ],
)
async def test_on_call_tool_role_blocking(
    middleware, mock_middleware_context, token_info, tool_name, tool_read_only, should_allow
):
    """Test on_call_tool() blocks write operations for guest/read roles."""
    # Setup mock message
    mock_middleware_context.message = MagicMock()
    mock_middleware_context.message.name = tool_name

    # Create mock tool
    mock_tool = create_mock_tool(tool_name, read_only=tool_read_only)
    mock_middleware_context.fastmcp_context.fastmcp.get_tool = AsyncMock(return_value=mock_tool)

    # Mock get_token_info
    call_next = AsyncMock(return_value=MagicMock())

    with patch.object(middleware, 'get_token_info', AsyncMock(return_value=token_info)):
        # Mock is_client_using_main_branch to return True
        with patch.object(middleware, 'is_client_using_main_branch', return_value=True):
            # Mock get_project_features to return hide-conditional-flows so create_flow is allowed
            with patch.object(middleware, 'get_project_features', return_value={'hide-conditional-flows'}):
                if should_allow:
                    # Should not raise an error
                    await middleware.on_call_tool(mock_middleware_context, call_next)
                    call_next.assert_called_once_with(mock_middleware_context)
                else:
                    # Should raise ToolError
                    with pytest.raises(ToolError) as exc_info:
                        await middleware.on_call_tool(mock_middleware_context, call_next)
                    # Verify error message contains tool name and role info
                    error_msg = str(exc_info.value)
                    assert tool_name in error_msg
                    assert 'write permissions' in error_msg
                    # Extract role from token_info
                    role = token_info.get('admin', {}).get('role', '').lower()
                    assert role in error_msg
                    call_next.assert_not_called()


# Test edge cases
@pytest.mark.asyncio
async def test_read_only_tool_detection():
    """Test _is_read_only_tool() helper method."""
    # Tool with readOnlyHint=True
    read_only_tool = create_mock_tool('test_read', read_only=True)
    assert ToolsFilteringMiddleware._is_read_only_tool(read_only_tool) is True

    # Tool with readOnlyHint=False
    write_tool = create_mock_tool('test_write', read_only=False)
    assert ToolsFilteringMiddleware._is_read_only_tool(write_tool) is False

    # Tool with no annotations
    tool_no_annotations = MagicMock(spec=Tool)
    tool_no_annotations.name = 'test_no_annotations'
    tool_no_annotations.annotations = None
    assert ToolsFilteringMiddleware._is_read_only_tool(tool_no_annotations) is False
