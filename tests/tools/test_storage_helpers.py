import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.storage_helpers import branch_table_list, prod_storage_id


@pytest.mark.parametrize(
    ('storage_id', 'branch_id', 'expected'),
    [
        # a bucket and a table of the development branch
        ('in.c-123-crm', '123', 'in.c-crm'),
        ('in.c-123-crm.orders', '123', 'in.c-crm.orders'),
        ('out.c-123-crm.orders', '123', 'out.c-crm.orders'),
        # the production ID is unchanged
        ('in.c-crm.orders', '123', 'in.c-crm.orders'),
        # no development branch
        ('in.c-123-crm.orders', None, 'in.c-123-crm.orders'),
        # the table name or the bucket name only contains the pattern
        ('in.c-crm.c-123-orders', '123', 'in.c-crm.c-123-orders'),
        ('in.c-crm.c-123-orders', None, 'in.c-crm.c-123-orders'),
        ('in.c-crm-c-123-x.orders', '123', 'in.c-crm-c-123-x.orders'),
        ('in.c-123-crm.c-123-orders', '123', 'in.c-crm.c-123-orders'),
        # a bucket of another branch is a bucket of its own
        ('in.c-456-crm.orders', '123', 'in.c-456-crm.orders'),
        ('in.c-1234-crm.orders', '123', 'in.c-1234-crm.orders'),
        # a bucket that is not a `c-` bucket
        ('in.sys-123-crm.orders', '123', 'in.sys-123-crm.orders'),
        ('in.123-crm.orders', '123', 'in.123-crm.orders'),
        # not a Storage stage
        ('xin.c-123-crm.orders', '123', 'xin.c-123-crm.orders'),
    ],
)
def test_prod_storage_id(storage_id: str, branch_id: str | None, expected: str) -> None:
    assert prod_storage_id(storage_id, branch_id) == expected


@pytest.mark.parametrize(
    ('branch_id', 'storage_branches', 'expected_branch_id'),
    [
        # a storage-branches development branch is listed over production by the server
        ('123', True, '123'),
        # a legacy development branch and production are listed from the default branch
        ('123', False, 'default'),
        (None, False, 'default'),
    ],
)
@pytest.mark.asyncio
async def test_branch_table_list(
    mocker: MockerFixture, branch_id: str | None, storage_branches: bool, expected_branch_id: str
) -> None:
    client = mocker.Mock(KeboolaClient)
    client.branch_id = branch_id
    client.has_feature = mocker.AsyncMock(return_value=storage_branches)
    client.storage_client.table_list = mocker.AsyncMock(return_value=[{'id': 'in.c-crm.orders'}])

    result = await branch_table_list(client, include=['columns'])

    assert result == [{'id': 'in.c-crm.orders'}]
    client.storage_client.table_list.assert_awaited_once_with(branch_id=expected_branch_id, include=['columns'])
