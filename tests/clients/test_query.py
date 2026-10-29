from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.query import QueryServiceClient


@pytest.mark.asyncio
async def test_get_rls_scope_posts_the_table_keys_to_the_workspace_scope_endpoint() -> None:
    client = QueryServiceClient.create(root_url='https://query.keboola.com', branch_id='1234', token='token')
    answer = {'readable': [{'table': 'in.c-crm.orders', 'columns': None}]}
    client.post = AsyncMock(return_value=answer)  # type: ignore[method-assign]

    result = await client.get_rls_scope(workspace_id='5678', tables=['in.c-crm.orders', 'in.c-crm.users'])

    assert result == answer
    client.post.assert_awaited_once_with(
        endpoint='branches/1234/workspaces/5678/rls/scope', data={'tables': ['in.c-crm.orders', 'in.c-crm.users']}
    )
