"""The metadata tools of a project that enforces row-level security show only what Query Service lets the reader read."""

from typing import Any, cast

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import GlobalSearchResponse
from keboola_mcp_server.tools import search as search_module
from keboola_mcp_server.tools.search import search
from keboola_mcp_server.tools.search_models import SearchItemType, SearchOutput
from keboola_mcp_server.tools.storage.tools import (
    GetBucketsOutput,
    GetTablesOutput,
    TableColumnInfo,
    TableDetail,
    TableSummary,
    get_buckets,
    get_tables,
)
from keboola_mcp_server.workspace import RlsScope, RlsScopeUnavailableError, WorkspaceManager
from tests.tools.storage.test_tools import (
    _bucket_detail_side_effect,
    _bucket_table_list_side_effect,
    _table_detail_side_effect,
)

_UNAVAILABLE = 'Table metadata of this project is temporarily unavailable. Please try again later.'

_USERS = 'in.c-foo.users'  # the primary key of the table is `user_id`
_EMAILS = 'in.c-foo.emails'  # the primary key of the table is `email_id`


@pytest.fixture
def keboola_client_in_branch_free_project(mocker: MockerFixture, mcp_context_client: Context) -> KeboolaClient:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = None
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(side_effect=_bucket_table_list_side_effect)
    return keboola_client


def _workspace_manager(ctx: Context) -> Any:
    return WorkspaceManager.from_state(ctx.session.state)


def _detail(table_id: str, columns: list[str], primary_key: list[str]) -> TableDetail:
    return TableDetail(
        id=table_id,
        name=table_id.rsplit('.', 1)[1],
        display_name=table_id.rsplit('.', 1)[1],
        created='2025-08-17T07:39:18+0200',
        primary_key=primary_key,
        rows_count=42,
        data_size_bytes=4096,
        columns=[
            TableColumnInfo(name=c, quoted_name=f'"{c}"', database_native_type='VARCHAR', nullable=True)
            for c in columns
        ],
    )


@pytest.mark.asyncio
async def test_get_tables_listing_shows_only_the_readable_tables_and_their_key_columns(
    mcp_context_client: Context, keboola_client_in_branch_free_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)
    manager.get_rls_scope.return_value = RlsScope(readable={_USERS: ['name'], _EMAILS: None})

    result = await get_tables(mcp_context_client, ['in.c-foo'])

    manager.get_rls_scope.assert_awaited_once_with([_USERS, _EMAILS])
    assert [t.id for t in result.tables] == [_USERS, _EMAILS]
    users, emails = result.tables
    # the primary key names a column that the column policy hides
    assert users.primary_key is None
    assert emails.primary_key == ['email_id']
    # the counts include the rows that a row policy hides
    assert [(t.rows_count, t.data_size_bytes) for t in result.tables] == [(None, None), (None, None)]
    assert result.tables_not_found is None


@pytest.mark.asyncio
async def test_get_tables_listing_hides_a_table_that_the_reader_may_not_read(
    mcp_context_client: Context, keboola_client_in_branch_free_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)
    manager.get_rls_scope.return_value = RlsScope(readable={_EMAILS: None})

    result = await get_tables(mcp_context_client, ['in.c-foo'])

    assert [t.id for t in result.tables] == [_EMAILS]
    assert result.tables_not_found is None, 'a hidden table of a listing is simply not there'


@pytest.mark.asyncio
async def test_get_tables_by_id_reports_a_hidden_table_as_not_found_and_limits_the_columns(
    mocker: MockerFixture, mcp_context_client: Context, keboola_client_in_branch_free_project: KeboolaClient
) -> None:
    details = {
        _USERS: _detail(_USERS, ['user_id', 'name', 'salary'], ['user_id']),
        _EMAILS: _detail(_EMAILS, ['email_id', 'address'], ['email_id']),
    }
    mocker.patch(
        'keboola_mcp_server.tools.storage.tools._get_table',
        mocker.AsyncMock(side_effect=lambda table_id, *args, **kwargs: details[table_id]),
    )
    manager = _workspace_manager(mcp_context_client)
    manager.get_workspace_id.return_value = 1
    manager.get_rls_scope.return_value = RlsScope(readable={_USERS: ['user_id', 'name']})

    result = await get_tables(mcp_context_client, table_ids=[_USERS, _EMAILS])

    assert [t.id for t in result.tables] == [_USERS]
    users = cast(TableDetail, result.tables[0])
    assert [c.name for c in users.columns or []] == ['user_id', 'name']
    assert users.primary_key == ['user_id']
    assert (users.rows_count, users.data_size_bytes) == (None, None), 'the counts include the hidden rows'
    assert result.tables_not_found == [_EMAILS], 'a hidden table looks like a table that does not exist'


@pytest.mark.asyncio
async def test_get_tables_of_an_ungoverned_project_is_unchanged(
    mcp_context_client: Context, keboola_client_in_branch_free_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)
    manager.get_rls_scope.return_value = None

    result = await get_tables(mcp_context_client, ['in.c-foo'])

    assert [t.id for t in result.tables] == [_USERS, _EMAILS]
    assert all(isinstance(t, TableSummary) for t in result.tables)
    assert result.tables[0].primary_key == ['user_id']
    assert [(t.rows_count, t.data_size_bytes) for t in result.tables] == [(10, 10240), (33, 332211)]


@pytest.mark.asyncio
async def test_get_tables_asks_nothing_when_there_is_no_table(
    mcp_context_client: Context, keboola_client_in_branch_free_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)

    result = await get_tables(mcp_context_client)

    assert result == GetTablesOutput(tables=[], links=result.links)
    manager.get_rls_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_tables_shows_nothing_when_the_scope_is_unavailable(
    mcp_context_client: Context, keboola_client_in_branch_free_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)
    manager.get_rls_scope.side_effect = RlsScopeUnavailableError()

    with pytest.raises(ToolError, match=_UNAVAILABLE):
        await get_tables(mcp_context_client, ['in.c-foo'])


@pytest.mark.parametrize('governed', [True, False])
@pytest.mark.asyncio
async def test_get_buckets_hides_the_totals_that_count_the_hidden_tables(
    mocker: MockerFixture, mcp_context_client: Context, governed: bool
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = None
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.bucket_list = mocker.AsyncMock(
        return_value=[
            _bucket_detail_side_effect('in.c-foo') | {'tablesCount': 2, 'dataSizeBytes': 1234},
        ]
    )
    _workspace_manager(mcp_context_client).is_governed.return_value = governed

    result = await get_buckets(mcp_context_client)

    assert isinstance(result, GetBucketsOutput)
    assert [b.id for b in result.buckets] == ['in.c-foo'], 'the bucket itself stays listed'
    bucket = result.buckets[0]
    if governed:
        assert bucket.tables_count is None and bucket.data_size_bytes is None
    else:
        assert bucket.tables_count == 2 and bucket.data_size_bytes == 1234


@pytest.mark.asyncio
async def test_get_buckets_fails_when_the_governed_state_is_unknown(
    mocker: MockerFixture, mcp_context_client: Context
) -> None:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = None
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)
    keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=[])
    _workspace_manager(mcp_context_client).is_governed.side_effect = RlsScopeUnavailableError()

    with pytest.raises(ToolError, match=_UNAVAILABLE):
        await get_buckets(mcp_context_client)


def _search_tables() -> list[dict[str, Any]]:
    return [
        {
            'id': 'in.c-foo.users',
            'name': 'users',
            'created': '2024-01-01T00:00:00Z',
            'columns': ['user_id', 'salary'],
            'columnMetadata': {
                'salary': [{'key': 'KBC.description', 'value': 'The salary of a user', 'provider': 'user'}]
            },
        },
        {
            'id': 'in.c-foo.hidden_users',
            'name': 'hidden_users',
            'created': '2024-01-01T00:00:00Z',
            'columns': ['salary'],
        },
    ]


@pytest.fixture
def searchable_project(mocker: MockerFixture, mcp_context_client: Context) -> KeboolaClient:
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = None
    keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)  # the global search is on
    keboola_client.storage_client.bucket_list = mocker.AsyncMock(
        return_value=[{'id': 'in.c-foo', 'name': 'foo', 'created': '2024-01-01T00:00:00Z'}]
    )
    keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=_search_tables())
    keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
    keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])
    return keboola_client


async def _search_tables_by(ctx: Context, *patterns: str) -> SearchOutput:
    return await search(ctx, patterns=list(patterns), item_types=(cast(SearchItemType, 'table'),), limit=20, offset=0)


@pytest.mark.asyncio
async def test_search_of_a_governed_project_hides_tables_and_columns_and_skips_the_global_search(
    mocker: MockerFixture, mcp_context_client: Context, searchable_project: KeboolaClient
) -> None:
    global_search = mocker.patch.object(search_module, '_global_textual_search', mocker.AsyncMock())
    manager = _workspace_manager(mcp_context_client)
    manager.is_governed.return_value = True
    manager.get_rls_scope.return_value = RlsScope(readable={'in.c-foo.users': ['user_id']})

    # `hidden_users` is not readable and `salary` is a column that the column policy hides, so a pattern for it
    # finds neither the column of `users` nor its description
    by_name = await _search_tables_by(mcp_context_client, 'users')
    by_hidden_column = await _search_tables_by(mcp_context_client, 'salary')

    global_search.assert_not_called()
    assert [(h.table_id, h.item_type) for h in by_name.hits] == [('in.c-foo.users', 'table')]
    assert by_name.total == 1 and by_name.by_type == {'table': 1}, 'the totals do not count the hidden tables'
    assert by_hidden_column.hits == [] and by_hidden_column.total == 0


@pytest.mark.asyncio
async def test_search_of_a_governed_project_finds_a_readable_column(
    mcp_context_client: Context, searchable_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)
    manager.is_governed.return_value = True
    manager.get_rls_scope.return_value = RlsScope(readable={'in.c-foo.users': ['user_id']})

    result = await _search_tables_by(mcp_context_client, 'user_id')

    assert [h.table_id for h in result.hits] == ['in.c-foo.users']


@pytest.mark.asyncio
async def test_search_of_an_ungoverned_project_uses_the_global_search_and_hides_nothing(
    mocker: MockerFixture, mcp_context_client: Context, searchable_project: KeboolaClient
) -> None:
    expected = SearchOutput(hits=[], total=0, by_type={}, branch_scope='current-branch')
    global_search = mocker.patch.object(
        search_module, '_global_textual_search', mocker.AsyncMock(return_value=expected)
    )
    manager = _workspace_manager(mcp_context_client)

    await _search_tables_by(mcp_context_client, 'users')

    manager.is_governed.assert_awaited_once()
    global_search.assert_awaited_once()
    manager.get_rls_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_search_of_a_governed_project_fails_when_the_scope_is_unavailable(
    mcp_context_client: Context, searchable_project: KeboolaClient
) -> None:
    manager = _workspace_manager(mcp_context_client)
    manager.is_governed.return_value = True
    manager.get_rls_scope.side_effect = RlsScopeUnavailableError()

    with pytest.raises(ToolError, match=_UNAVAILABLE):
        await _search_tables_by(mcp_context_client, 'users')


@pytest.mark.asyncio
async def test_get_tables_on_a_development_branch_asks_for_the_production_ids(
    mocker: MockerFixture, mcp_context_client: Context
) -> None:
    """The policies are keyed by the Storage id of the table on the production branch, whatever branch the session
    works on: a table of a development branch bucket (`in.c-<branch>-foo`) is asked for as `in.c-foo.<table>`."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = '1246948'
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(side_effect=_bucket_table_list_side_effect)
    manager = _workspace_manager(mcp_context_client)
    manager.get_rls_scope.return_value = RlsScope(readable={})

    await get_tables(mcp_context_client, ['in.c-foo'])

    asked = manager.get_rls_scope.await_args.args[0]
    assert asked, 'the tables of the bucket are asked for'
    assert all(table_id.startswith('in.c-foo.') for table_id in asked)
    assert not any('1246948' in table_id for table_id in asked)


@pytest.mark.parametrize(
    ('table_id', 'prod_id'),
    [
        ('in.c-foo.emails', 'in.c-foo.emails'),
        ('in.c-1246948-foo.emails', 'in.c-foo.emails'),
        ('in.c-1246948-foo.assets', 'in.c-foo.assets'),
    ],
)
@pytest.mark.asyncio
async def test_get_tables_by_id_on_a_development_branch_asks_for_the_production_id(
    mocker: MockerFixture, mcp_context_client: Context, table_id: str, prod_id: str
) -> None:
    """A table of a development branch is found under the id of its production bucket or of its branch bucket, and
    the scope asks for the production id that its policies are keyed by."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = '1246948'
    keboola_client.has_feature = mocker.AsyncMock(return_value=False)
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.table_detail = mocker.AsyncMock(side_effect=_table_detail_side_effect)
    manager = _workspace_manager(mcp_context_client)
    manager.get_table_info.return_value = None
    manager.get_quoted_name.side_effect = lambda name: f'"{name}"'
    manager.get_sql_dialect.return_value = 'Snowflake'
    manager.get_rls_scope.return_value = RlsScope(readable={prod_id: None})

    result = await get_tables(mcp_context_client, table_ids=[table_id])

    manager.get_rls_scope.assert_awaited_once_with([prod_id])
    assert [t.id for t in result.tables] == [prod_id]
    assert result.tables_not_found is None


@pytest.mark.asyncio
async def test_search_on_a_development_branch_asks_for_the_production_ids(
    mcp_context_client: Context, searchable_project: KeboolaClient
) -> None:
    searchable_project.branch_id = '1246948'
    searchable_project.storage_client.bucket_list.return_value = [
        {'id': 'in.c-1246948-foo', 'name': 'foo', 'created': '2024-01-01T00:00:00Z'}
    ]
    searchable_project.storage_client.bucket_table_list.return_value = [
        {'id': 'in.c-1246948-foo.users', 'name': 'users', 'created': '2024-01-01T00:00:00Z', 'columns': ['user_id']}
    ]
    manager = _workspace_manager(mcp_context_client)
    manager.is_governed.return_value = True
    manager.get_rls_scope.return_value = RlsScope(readable={'in.c-foo.users': None})

    result = await _search_tables_by(mcp_context_client, 'users')

    manager.get_rls_scope.assert_awaited_once_with(['in.c-foo.users'])
    # the hit keeps the id that the branch reports, the scope is looked up by the production id
    assert [h.table_id for h in result.hits] == ['in.c-1246948-foo.users']


@pytest.mark.parametrize(
    ('item_types', 'search_type', 'tables_possible'),
    [
        (('flow',), 'textual', False),
        (('flow', 'bucket'), 'textual', False),
        (('configuration', 'transformation'), 'config-based', False),
        (('component',), 'textual', False),
        (('table',), 'textual', True),
        (('table', 'flow'), 'textual', True),
        ((), 'textual', True),
        ((), 'config-based', True),
    ],
)
@pytest.mark.asyncio
async def test_search_looks_up_the_governed_state_only_when_a_table_can_be_in_the_result(
    mocker: MockerFixture,
    mcp_context_client: Context,
    searchable_project: KeboolaClient,
    item_types: tuple[str, ...],
    search_type: str,
    tables_possible: bool,
) -> None:
    searchable_project.storage_client.is_enabled = mocker.AsyncMock(return_value=False)
    manager = _workspace_manager(mcp_context_client)
    # a workspace that cannot be resolved fails a search that reads tables, and must not fail any other search
    manager.is_governed.side_effect = RlsScopeUnavailableError()

    call = search(
        mcp_context_client,
        patterns=['users'],
        item_types=cast(Any, item_types),
        search_type=cast(Any, search_type),
        limit=20,
        offset=0,
    )

    if tables_possible:
        with pytest.raises(ToolError, match=_UNAVAILABLE):
            await call
        manager.is_governed.assert_awaited_once()
    else:
        assert isinstance(await call, SearchOutput)
        manager.is_governed.assert_not_awaited()
        manager.get_rls_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_global_search_that_does_not_ask_for_tables_carries_no_table_count(
    mocker: MockerFixture, mcp_context_client: Context, searchable_project: KeboolaClient
) -> None:
    """The counts of the global search come straight from its answer, which counts only the types that the request
    asks for (on a dev stack a flow-only search for a name that tables carry answers `total: 0`, `by_type: {}`). The
    search, which does not look up whether the project is governed for it, therefore leaks no number of hidden
    tables, as long as it never asks the endpoint for the `table` type."""
    flow = {
        'id': 'flow-1',
        'name': 'orders flow',
        'type': 'configuration',
        'fullPath': {},
        'componentId': 'keboola.flow',
        'organizationId': 1,
        'projectId': 69420,
        'projectName': 'Test Project',
        'created': '2024-01-01T00:00:00Z',
    }
    global_search = mocker.AsyncMock(
        return_value=GlobalSearchResponse.model_validate(
            {'all': 1, 'items': [flow], 'byType': {'configuration': 1}, 'byProject': {}}
        )
    )
    searchable_project.storage_client.global_search = global_search
    manager = _workspace_manager(mcp_context_client)
    manager.is_governed.side_effect = RlsScopeUnavailableError()

    result = await search(
        mcp_context_client, patterns=['orders'], item_types=(cast(SearchItemType, 'flow'),), limit=20, offset=0
    )

    assert global_search.await_count >= 1
    assert all('table' not in call.kwargs['types'] for call in global_search.await_args_list)
    assert (result.total, result.by_type) == (1, {'configuration': 1})
    assert [h.item_type for h in result.hits] == ['flow']
    manager.is_governed.assert_not_awaited()
