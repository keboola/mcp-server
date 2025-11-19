import json
from typing import Any
from unittest.mock import call

import pytest
from mcp.server.fastmcp import Context
from pydantic import TypeAdapter

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.query import QueryServiceClient
from keboola_mcp_server.tools.sql import QueryDataOutput, query_data
from keboola_mcp_server.workspace import (
    QueryResult,
    SqlSelectData,
    TableFqn,
    WorkspaceManager,
    _SnowflakeWorkspace,
)


def _truncate_data(qr: QueryResult, max_rows: int | None, max_chars: int | None) -> QueryResult:
    rows = []
    total_chars = 0
    for row in qr.data.rows[: (max_rows or len(qr.data.rows))]:
        chars = sum(len(str(v)) for v in row.values() if v is not None)
        if max_chars is not None and total_chars + chars > max_chars:
            break
        total_chars += chars
        rows.append(row)

    return QueryResult(
        status=qr.status,
        data=SqlSelectData(columns=qr.data.columns, rows=rows),
        message=_SnowflakeWorkspace._SELECTED_ROWS_MSG.format(rows=len(rows), total=len(qr.data.rows)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('query', 'query_name', 'result', 'expected_csv'),
    [
        (
            'select 1;',
            'Simple Count Query',
            QueryResult(status='ok', data=SqlSelectData(columns=['a'], rows=[{'a': 1}]), message=None),
            'a\r\n1\r\n',  # CSV
        ),
        (
            'select id, name, email from user;',
            'User Details List',
            QueryResult(
                status='ok',
                data=SqlSelectData(
                    columns=['id', 'name', 'email'],
                    rows=[
                        {'id': 1, 'name': 'John', 'email': 'john@foo.com'},
                        {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},
                    ],
                ),
                message=None,
            ),
            'id,name,email\r\n1,John,john@foo.com\r\n2,Joe,joe@bar.com\r\n',  # CSV
        ),
        (
            'create table foo (id integer, name varchar);',
            'Create Table Operation',
            QueryResult(status='ok', data=None, message='1 table created'),
            'message\r\n1 table created\r\n',  # CSV
        ),
    ],
)
async def test_query_data(
    query: str, query_name: str, result: QueryResult, expected_csv: str, mcp_context_client: Context, mocker
):
    manager = mocker.AsyncMock(WorkspaceManager)
    manager.execute_query.return_value = result
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = manager

    result = await query_data(query, query_name, mcp_context_client)
    assert isinstance(result, QueryDataOutput)
    assert result.query_name == query_name
    assert result.csv_data == expected_csv


class TestWorkspaceManagerSnowflake:

    @pytest.fixture
    def context(self, keboola_client: KeboolaClient, empty_context: Context, mocker) -> Context:
        keboola_client.storage_client.workspace_list.return_value = [
            {
                'id': 1234,
                'connection': {
                    'schema': 'workspace_1234',
                    'backend': 'snowflake',
                    'user': 'user_1234',
                },
                'readOnlyStorageAccess': True,
            }
        ]

        empty_context.session.state[KeboolaClient.STATE_KEY] = keboola_client
        empty_context.session.state[WorkspaceManager.STATE_KEY] = WorkspaceManager(
            client=keboola_client, workspace_schema='workspace_1234'
        )

        return empty_context

    @pytest.mark.asyncio
    async def test_get_sql_dialect(self, context: Context):
        m = WorkspaceManager.from_state(context.session.state)
        assert await m.get_sql_dialect() == 'Snowflake'

    @pytest.mark.asyncio
    async def test_get_quoted_name(self, context: Context):
        m = WorkspaceManager.from_state(context.session.state)
        assert await m.get_quoted_name('foo') == '"foo"'

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('table', 'sapi_result', 'expected'),
        [
            (
                # table in.c-foo.bar in its own project
                {'id': 'in.c-foo.bar', 'name': 'bar'},
                {'current_database': 'db_xyz'},
                TableFqn(db_name='db_xyz', schema_name='in.c-foo', table_name='bar', quote_char='"'),
            ),
            (
                # temporary table not in a project, but in the writable schema of the workspace
                {'id': 'bar', 'name': 'bar'},
                {'current_database': 'db_xyz'},
                TableFqn(db_name='db_xyz', schema_name='workspace_1234', table_name='bar', quote_char='"'),
            ),
            (
                # table out.c-baz.bam exported from project 1234
                # and imported as table in.c-foo.bar in some other project
                {
                    'id': 'in.c-foo.bar',
                    'name': 'bar',
                    'sourceTable': {'project': {'id': '1234'}, 'id': 'out.c-baz.bam'},
                },
                {'DATABASE_NAME': 'sapi_1234'},
                TableFqn(db_name='sapi_1234', schema_name='out.c-baz', table_name='bam', quote_char='"'),
            ),
        ],
    )
    async def test_get_table_fqn(
        self,
        table: dict[str, Any],
        sapi_result,
        expected: TableFqn,
        keboola_client: KeboolaClient,
        context: Context,
        mocker,
    ):
        keboola_client.storage_client.branches_list.return_value = [{'id': 1234, 'isDefault': True}]

        qsclient = mocker.AsyncMock(QueryServiceClient)
        qsclient.submit_job.return_value = 'qs-job-1234'
        qsclient.get_job_status.return_value = {
            'status': 'completed',
            'statements': [{'id': 'qs-job-statement-1234', 'status': 'completed'}],
        }
        qsclient.get_job_results.side_effect = [
            {
                'status': 'completed',
                'data': [[value for value in sapi_result.values()]],
                'columns': [{'name': key} for key in sapi_result.keys()],
                'message': '',
            },
            {
                'status': 'completed',
                'data': [],
                'columns': [{'name': 'COLUMN_NAME'}, {'name': 'DATA_TYPE'}, {'name': 'IS_NULLABLE'}],
                'message': '',
            },
        ]
        mocker.patch.object(QueryServiceClient, 'create', return_value=qsclient)

        m = WorkspaceManager.from_state(context.session.state)
        info = await m.get_table_info(table)
        assert info is not None
        assert info.fqn == expected

        keboola_client.storage_client.branches_list.assert_called_once()
        qsclient.submit_job.assert_called()
        qsclient.get_job_status.assert_called()
        qsclient.get_job_results.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('query', 'db_data', 'max_rows', 'max_chars'),
        [
            (
                'select id, name, email from user;',
                QueryResult(
                    status='ok',
                    data=SqlSelectData(
                        columns=['id', 'name', 'email'],
                        rows=[
                            {'id': 1, 'name': 'John', 'email': 'john@foo.com'},
                            {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},
                        ],
                    ),
                ),
                None,
                None,
            ),
            (
                'select id, name, email from user;',
                QueryResult(
                    status='ok',
                    data=SqlSelectData(
                        columns=['id', 'name', 'email'],
                        rows=[
                            {'id': 1, 'name': 'John', 'email': 'john@foo.com'},
                            {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},
                        ],
                    ),
                ),
                1,
                None,
            ),
            (
                'select id, name, email from user;',
                QueryResult(
                    status='ok',
                    data=SqlSelectData(
                        columns=['id', 'name', 'email'],
                        rows=[
                            {'id': 1, 'name': 'John', 'email': 'john@foo.com'},  # 17 characters
                            {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},  # 16 characters
                        ],
                    ),
                ),
                None,
                20,
            ),
            (
                'create table foo (id integer, name varchar);',
                QueryResult(status='ok', message='1 table created'),
                None,
                None,
            ),
            ('bla bla bla', QueryResult(status='error', message='Invalid SQL...'), None, None),
        ],
    )
    async def test_execute_query(
        self,
        query: str,
        db_data: QueryResult,
        max_rows: int | None,
        max_chars: int | None,
        keboola_client: KeboolaClient,
        context: Context,
        mocker,
    ):
        keboola_client.storage_client.branches_list.return_value = [{'id': 1234, 'isDefault': True}]

        qsclient = mocker.AsyncMock(QueryServiceClient)
        qsclient.submit_job.return_value = 'qs-job-1234'
        qsclient.get_job_status.return_value = {
            'status': 'completed',
            'statements': [{'id': 'qs-job-statement-1234', 'status': 'completed'}],
        }
        qsclient.get_job_results.return_value = {
            'status': 'completed' if db_data.is_ok else 'failed',
            'data': [list(row.values()) for row in db_data.data.rows] if db_data.data else [],
            'columns': [{'name': col_name} for col_name in db_data.data.columns] if db_data.data else [],
            'message': db_data.message,
            'numberOfRows': len(db_data.data.rows) if db_data.data else None,
        }
        mocker.patch.object(QueryServiceClient, 'create', return_value=qsclient)

        if db_data.data is not None:
            expected = _truncate_data(db_data, max_rows, max_chars)
        else:
            expected = db_data

        m = WorkspaceManager.from_state(context.session.state)
        actual = await m.execute_query(query, max_rows=max_rows, max_chars=max_chars)

        assert actual == expected

        keboola_client.storage_client.branches_list.assert_called_once()
        qsclient.submit_job.assert_called_once()
        qsclient.get_job_status.assert_called_once_with('qs-job-1234')
        qsclient.get_job_results.assert_called_once_with(
            'qs-job-1234', 'qs-job-statement-1234', offset=0, limit=1000 if max_rows is None else max(max_rows, 100)
        )

    @pytest.mark.asyncio
    async def test_execute_query_pagination(self, keboola_client: KeboolaClient, context: Context, mocker):
        keboola_client.storage_client.branches_list.return_value = [{'id': 1234, 'isDefault': True}]

        qsclient = mocker.AsyncMock(QueryServiceClient)
        qsclient.submit_job.return_value = 'qs-job-1234'
        qsclient.get_job_status.return_value = {
            'status': 'completed',
            'statements': [{'id': 'qs-job-statement-1234', 'status': 'completed'}],
        }
        qsclient.get_job_results.side_effect = [
            {
                'status': 'completed',
                'data': [
                    [1, 'John', 'john@foo.com'],
                    [2, 'Joe', 'joe@foo.com'],
                    [3, 'Jack', 'jack@foo.com'],
                    [4, 'Jerry', 'jerry@foo.com'],
                ],
                'columns': [{'name': 'id'}, {'name': 'name'}, {'name': 'email'}],
                'message': None,
                'numberOfRows': 10,
            },
            {
                'status': 'completed',
                'data': [
                    [5, 'James', 'james@foo.com'],
                    [6, 'Julian', 'julian@foo.com'],
                    [7, 'Jordan', 'jordan@foo.com'],
                    [8, 'Jacob', 'jacob@foo.com'],
                ],
                'columns': [{'name': 'id'}, {'name': 'name'}, {'name': 'email'}],
                'message': None,
                'numberOfRows': 10,
            },
            {
                'status': 'completed',
                'data': [
                    [9, 'Jagger', 'jagger@foo.com'],
                    [10, 'Jackson', 'jackson@foo.com'],
                ],
                'columns': [{'name': 'id'}, {'name': 'name'}, {'name': 'email'}],
                'message': None,
                'numberOfRows': 10,
            },
        ]
        mocker.patch.object(QueryServiceClient, 'create', return_value=qsclient)
        mocker.patch.object(_SnowflakeWorkspace, '_PAGE_SIZE', 4)

        m = WorkspaceManager.from_state(context.session.state)
        actual = await m.execute_query('select id, name, email from user;')
        assert actual == QueryResult(
            status='ok',
            data=SqlSelectData(
                columns=['id', 'name', 'email'],
                rows=[
                    {'id': 1, 'name': 'John', 'email': 'john@foo.com'},
                    {'id': 2, 'name': 'Joe', 'email': 'joe@foo.com'},
                    {'id': 3, 'name': 'Jack', 'email': 'jack@foo.com'},
                    {'id': 4, 'name': 'Jerry', 'email': 'jerry@foo.com'},
                    {'id': 5, 'name': 'James', 'email': 'james@foo.com'},
                    {'id': 6, 'name': 'Julian', 'email': 'julian@foo.com'},
                    {'id': 7, 'name': 'Jordan', 'email': 'jordan@foo.com'},
                    {'id': 8, 'name': 'Jacob', 'email': 'jacob@foo.com'},
                    {'id': 9, 'name': 'Jagger', 'email': 'jagger@foo.com'},
                    {'id': 10, 'name': 'Jackson', 'email': 'jackson@foo.com'},
                ],
            ),
            message='Returning 10 of 10 selected rows.',
        )

        keboola_client.storage_client.branches_list.assert_called_once()
        qsclient.submit_job.assert_called_once()
        qsclient.get_job_status.assert_called_once_with('qs-job-1234')
        qsclient.get_job_results.assert_has_calls(
            [
                call('qs-job-1234', 'qs-job-statement-1234', offset=0, limit=100),
                call('qs-job-1234', 'qs-job-statement-1234', offset=4, limit=100),
                call('qs-job-1234', 'qs-job-statement-1234', offset=8, limit=100),
            ]
        )


class TestWorkspaceManagerBigQuery:
    @pytest.fixture
    def context(self, keboola_client: KeboolaClient, empty_context: Context, mocker) -> Context:
        keboola_client.storage_client.workspace_list.return_value = [
            {
                'id': 1234,
                'connection': {
                    'schema': 'workspace_1234',
                    'backend': 'bigquery',
                    'user': json.dumps({'project_id': 'project_1234'}),
                },
                'readOnlyStorageAccess': True,
            }
        ]

        empty_context.session.state[KeboolaClient.STATE_KEY] = keboola_client
        empty_context.session.state[WorkspaceManager.STATE_KEY] = WorkspaceManager(
            client=keboola_client, workspace_schema='workspace_1234'
        )

        return empty_context

    @pytest.mark.asyncio
    async def test_get_sql_dialect(self, context: Context):
        m = WorkspaceManager.from_state(context.session.state)
        assert await m.get_sql_dialect() == 'BigQuery'

    @pytest.mark.asyncio
    async def test_get_quoted_name(self, context: Context):
        m = WorkspaceManager.from_state(context.session.state)
        assert await m.get_quoted_name('foo') == '`foo`'

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('table', 'expected'),
        [
            (
                # table in.c-foo.bar in its own project or a tables shared from other project
                {'id': 'in.c-foo.bar', 'name': 'bar'},
                TableFqn(db_name='project_1234', schema_name='in_c_foo', table_name='bar', quote_char='`'),
            ),
            (
                # temporary table not in a project, but in the writable schema of the workspace
                {'id': 'bar', 'name': 'bar'},
                TableFqn(
                    db_name='project_1234',
                    schema_name='workspace_1234',
                    table_name='bar',
                    quote_char='`',
                ),
            ),
        ],
    )
    async def test_get_table_fqn(
        self, table: dict[str, Any], expected: TableFqn, keboola_client: KeboolaClient, context: Context
    ):
        keboola_client.storage_client.workspace_query.return_value = QueryResult(
            status='ok',
            data=SqlSelectData(columns=['column_name', 'data_type', 'is_nullable'], rows=[]),
        )
        m = WorkspaceManager.from_state(context.session.state)
        info = await m.get_table_info(table)
        assert info is not None
        assert info.fqn == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('query', 'db_data', 'max_rows', 'max_chars'),
        [
            (
                'select id, name, email from user;',
                QueryResult(
                    status='ok',
                    data=SqlSelectData(
                        columns=['id', 'name', 'email'],
                        rows=[
                            {'id': 1, 'name': 'John', 'email': 'john@foo.com'},
                            {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},
                        ],
                    ),
                ),
                None,
                None,
            ),
            (
                'select id, name, email from user;',
                QueryResult(
                    status='ok',
                    data=SqlSelectData(
                        columns=['id', 'name', 'email'],
                        rows=[
                            {'id': 1, 'name': 'John', 'email': 'john@foo.com'},
                            {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},
                        ],
                    ),
                ),
                1,
                None,
            ),
            (
                'select id, name, email from user;',
                QueryResult(
                    status='ok',
                    data=SqlSelectData(
                        columns=['id', 'name', 'email'],
                        rows=[
                            {'id': 1, 'name': 'John', 'email': 'john@foo.com'},  # 17 characters
                            {'id': 2, 'name': 'Joe', 'email': 'joe@bar.com'},  # 16 characters
                        ],
                    ),
                ),
                None,
                20,
            ),
            (
                'CREATE TABLE `foo` (id INT64, name STRING);',
                QueryResult(status='ok', data=None, message='1 table created'),
                None,
                None,
            ),
            ('bla bla bla', QueryResult(status='error', data=None, message='400 Invalid SQL...'), None, None),
        ],
    )
    async def test_execute_query(
        self,
        query: str,
        db_data: QueryResult,
        max_rows: int | None,
        max_chars: int | None,
        keboola_client: KeboolaClient,
        context: Context,
    ):
        keboola_client.storage_client.workspace_query.return_value = TypeAdapter(QueryResult).dump_python(db_data)

        if db_data.data is not None:
            expected = _truncate_data(db_data, max_rows, max_chars)
        else:
            expected = db_data

        m = WorkspaceManager.from_state(context.session.state)
        actual = await m.execute_query(query, max_rows=max_rows, max_chars=max_chars)

        assert actual == expected
