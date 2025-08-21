import json
from typing import Any

import pytest
from mcp.server.fastmcp import Context
from pydantic import TypeAdapter

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.sql import QueryDataOutput, query_data
from keboola_mcp_server.workspace import (
    QueryResult,
    SqlSelectData,
    TableFqn,
    WorkspaceManager,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('query', 'query_name', 'result', 'expected_csv'),
    [
        (
            'select 1;',
            'Simple Count Query',
            QueryResult(status='ok', data=SqlSelectData(columns=['a'], rows=[{'a': 1}])),
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
            ),
            'id,name,email\r\n1,John,john@foo.com\r\n2,Joe,joe@bar.com\r\n',  # CSV
        ),
        (
            'create table foo (id integer, name varchar);',
            'Create Table Operation',
            QueryResult(status='ok', message='1 table created'),
            'message\r\n1 table created\r\n',  # CSV
        ),
    ],
)
async def test_query_data(
    query: str, query_name: str, result: QueryResult, expected_csv: str, mcp_context_client: Context, mocker
):
    workspace_manager = mocker.AsyncMock(WorkspaceManager)
    workspace_manager.execute_query.return_value = result
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = workspace_manager

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
    ):
        keboola_client.storage_client.workspace_query.return_value = QueryResult(
            status='ok',
            data=SqlSelectData(columns=list(sapi_result.keys()), rows=[sapi_result]),
        )
        m = WorkspaceManager.from_state(context.session.state)
        fqn = await m.get_table_fqn(table)
        assert fqn == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('query', 'expected'),
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
            ),
            (
                'create table foo (id integer, name varchar);',
                QueryResult(status='ok', data=None, message='1 table created'),
            ),
            (
                'bla bla bla',
                QueryResult(status='error', message='Invalid SQL...'),
            ),
        ],
    )
    async def test_execute_query(
        self, query: str, expected: QueryResult, keboola_client: KeboolaClient, context: Context
    ):
        keboola_client.storage_client.workspace_query.return_value = TypeAdapter(QueryResult).dump_python(expected)
        m = WorkspaceManager.from_state(context.session.state)
        result = await m.execute_query(query)
        assert result == expected


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
    async def test_get_table_fqn(self, table: dict[str, Any], expected: TableFqn, context: Context):
        m = WorkspaceManager.from_state(context.session.state)
        fqn = await m.get_table_fqn(table)
        assert fqn == expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('query', 'expected'),
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
            ),
            (
                'CREATE TABLE `foo` (id INT64, name STRING);',
                QueryResult(status='ok', data=SqlSelectData(columns=[], rows=[])),
            ),
            (
                'bla bla bla',
                QueryResult(status='error', data=None, message='400 Invalid SQL...'),
            ),
        ],
    )
    async def test_execute_query(
        self, query: str, expected: QueryResult, keboola_client: KeboolaClient, context: Context
    ):
        keboola_client.storage_client.workspace_query.return_value = TypeAdapter(QueryResult).dump_python(expected)
        m = WorkspaceManager.from_state(context.session.state)
        result = await m.execute_query(query)
        assert result == expected
