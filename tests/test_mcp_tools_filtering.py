from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.exceptions import ToolError

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.mcp import ToolsFilteringMiddleware


def _tool(name: str) -> MagicMock:
    tool = MagicMock()
    tool.name = name
    return tool


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'dev_branch_detail', 'expect_filtered', 'expect_dev_branch_call'),
    [
        ('1234', {'id': 1234, 'isDefault': False}, True, True),
        ('default', None, False, False),
    ],
)
async def test_list_tools_filters_data_apps_by_branch(
    mcp_context_client,
    branch_id: str,
    dev_branch_detail: dict | None,
    expect_filtered: bool,
    expect_dev_branch_call: bool,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.verify_token = AsyncMock(return_value={'owner': {'features': []}, 'admin': {}})
    keboola_client.storage_client.dev_branch_detail = AsyncMock(return_value=dev_branch_detail)

    tools = [_tool('modify_data_app'), _tool('get_data_apps'), _tool('deploy_data_app'), _tool('other_tool')]

    async def call_next(_):
        return tools

    middleware = ToolsFilteringMiddleware()
    context = SimpleNamespace(fastmcp_context=mcp_context_client)
    result = await middleware.on_list_tools(context, call_next)

    result_names = {t.name for t in result}
    if expect_filtered:
        assert 'modify_data_app' not in result_names
        assert 'get_data_apps' not in result_names
        assert 'deploy_data_app' not in result_names
    else:
        assert 'modify_data_app' in result_names
        assert 'get_data_apps' in result_names
        assert 'deploy_data_app' in result_names
    assert 'other_tool' in result_names

    if expect_dev_branch_call:
        keboola_client.storage_client.dev_branch_detail.assert_called_once_with(branch_id)
    else:
        keboola_client.storage_client.dev_branch_detail.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'dev_branch_detail', 'expect_error', 'expect_dev_branch_call'),
    [
        ('5678', {'id': 5678, 'isDefault': False}, True, True),
        (None, None, False, False),
    ],
)
async def test_call_tool_blocks_data_apps_by_branch(
    mcp_context_client,
    branch_id: str | None,
    dev_branch_detail: dict | None,
    expect_error: bool,
    expect_dev_branch_call: bool,
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.verify_token = AsyncMock(return_value={'owner': {'features': []}, 'admin': {}})
    keboola_client.storage_client.dev_branch_detail = AsyncMock(return_value=dev_branch_detail)

    tool = _tool('modify_data_app')
    mcp_context_client.fastmcp = SimpleNamespace(get_tool=AsyncMock(return_value=tool))
    context = SimpleNamespace(fastmcp_context=mcp_context_client, message=SimpleNamespace(name='modify_data_app'))

    expected = MagicMock()

    async def call_next(_):
        return expected

    middleware = ToolsFilteringMiddleware()
    if expect_error:
        with pytest.raises(ToolError, match='Data apps are supported only in the main production branch'):
            await middleware.on_call_tool(context, call_next)
    else:
        result = await middleware.on_call_tool(context, call_next)
        assert result is expected

    if expect_dev_branch_call:
        keboola_client.storage_client.dev_branch_detail.assert_called_once_with(str(branch_id))
    else:
        keboola_client.storage_client.dev_branch_detail.assert_not_called()
