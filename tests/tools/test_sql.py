import json
from typing import Any

import pytest
from mcp.server.fastmcp import Context
from pydantic import TypeAdapter

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.sql import AnomalyDetectionOutput, QueryDataOutput, detect_anomalies, get_sql_dialect, query_data
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


@pytest.mark.asyncio
@pytest.mark.parametrize('dialect', ['snowflake', 'biq-query', 'foo'])
async def test_get_sql_dialect(dialect: str, mcp_context_client: Context, mocker):
    workspace_manager = mocker.AsyncMock(WorkspaceManager)
    workspace_manager.get_sql_dialect.return_value = dialect
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = workspace_manager

    result = await get_sql_dialect(mcp_context_client)
    assert result == dialect


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
# ============================================================================
# ANOMALY DETECTION TESTS
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('table_name', 'numeric_column', 'date_column', 'time_window', 'anomaly_threshold', 'dialect', 'mock_result', 'expected_summary_contains'),
    [
        (
            '"DB"."SCHEMA"."TABLE"',
            '"value"',
            '"created_at"',
            'DAY',
            2.5,
            'Snowflake',
            QueryResult(
                status='ok',
                data=SqlSelectData(
                    columns=['time_period', 'record_count', 'total_value', 'avg_value', 'min_value', 'max_value', 'mean_value', 'stddev_value', 'z_score', 'abs_z_score', 'anomaly_status', 'anomaly_direction'],
                    rows=[
                        {'time_period': '2024-01-03', 'record_count': 10, 'total_value': 1000, 'avg_value': 100, 'min_value': 90, 'max_value': 110, 'mean_value': 950, 'stddev_value': 150, 'z_score': 3.333, 'abs_z_score': 3.333, 'anomaly_status': 'ANOMALY', 'anomaly_direction': 'HIGH_ANOMALY'},
                        {'time_period': '2024-01-02', 'record_count': 10, 'total_value': 950, 'avg_value': 95, 'min_value': 85, 'max_value': 105, 'mean_value': 950, 'stddev_value': 150, 'z_score': 0.000, 'abs_z_score': 0.000, 'anomaly_status': 'NORMAL', 'anomaly_direction': 'NORMAL'},
                        {'time_period': '2024-01-01', 'record_count': 10, 'total_value': 800, 'avg_value': 80, 'min_value': 70, 'max_value': 90, 'mean_value': 950, 'stddev_value': 150, 'z_score': -1.000, 'abs_z_score': 1.000, 'anomaly_status': 'NORMAL', 'anomaly_direction': 'NORMAL'},
                    ]
                )
            ),
            ['Total day periods analyzed: 3', 'Anomaly periods: 1', 'Normal periods: 2']
        ),
        (
            '`project`.`dataset`.`table`',
            '`amount`',
            '`timestamp`',
            'WEEK',
            3.0,
            'BigQuery',
            QueryResult(
                status='ok',
                data=SqlSelectData(
                    columns=['time_period', 'record_count', 'total_value', 'avg_value', 'min_value', 'max_value', 'mean_value', 'stddev_value', 'z_score', 'abs_z_score', 'anomaly_status', 'anomaly_direction'],
                    rows=[
                        {'time_period': '2024-01-08', 'record_count': 50, 'total_value': 5000, 'avg_value': 100, 'min_value': 80, 'max_value': 120, 'mean_value': 4500, 'stddev_value': 400, 'z_score': 1.250, 'abs_z_score': 1.250, 'anomaly_status': 'NORMAL', 'anomaly_direction': 'NORMAL'},
                        {'time_period': '2024-01-01', 'record_count': 50, 'total_value': 4500, 'avg_value': 90, 'min_value': 70, 'max_value': 110, 'mean_value': 4500, 'stddev_value': 400, 'z_score': 0.000, 'abs_z_score': 0.000, 'anomaly_status': 'NORMAL', 'anomaly_direction': 'NORMAL'},
                    ]
                )
            ),
            ['Total week periods analyzed: 2', 'Anomaly periods: 0', 'Normal periods: 2']
        )
    ]
)
async def test_detect_anomalies(
    table_name: str,
    numeric_column: str, 
    date_column: str,
    time_window: str,
    anomaly_threshold: float,
    dialect: str,
    mock_result: QueryResult,
    expected_summary_contains: list[str],
    mcp_context_client: Context,
    mocker
):
    """Test the detect_anomalies function with various parameters and dialects."""
    workspace_manager = mocker.AsyncMock(WorkspaceManager)
    workspace_manager.get_sql_dialect.return_value = dialect
    workspace_manager.execute_query.return_value = mock_result
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = workspace_manager

    result = await detect_anomalies(table_name, numeric_column, date_column, time_window, mcp_context_client, anomaly_threshold)
    
    assert isinstance(result, AnomalyDetectionOutput)
    assert result.query_name == f'Anomaly Detection: {numeric_column} by {time_window} (threshold={anomaly_threshold})'
    assert len(result.csv_data) > 0
    assert result.summary
    
    # Check that expected content appears in summary
    for expected_text in expected_summary_contains:
        assert expected_text in result.summary
    
    # Verify the SQL query was constructed with proper dialect functions
    workspace_manager.execute_query.assert_called_once()
    executed_query = workspace_manager.execute_query.call_args[0][0]
    
    if dialect.lower() == 'snowflake':
        assert 'TRY_TO_TIMESTAMP' in executed_query
        assert '::NUMBER' in executed_query
    elif dialect.lower() == 'bigquery':
        assert 'SAFE.PARSE_TIMESTAMP' in executed_query
        assert '::NUMERIC' in executed_query


@pytest.mark.asyncio
async def test_detect_anomalies_error_handling(mcp_context_client: Context, mocker):
    """Test that detect_anomalies handles SQL execution errors properly."""
    workspace_manager = mocker.AsyncMock(WorkspaceManager)
    workspace_manager.get_sql_dialect.return_value = 'Snowflake'
    workspace_manager.execute_query.return_value = QueryResult(status='error', message='Table not found')
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = workspace_manager

    with pytest.raises(ValueError, match='Failed to run anomaly detection query, error: Table not found'):
        await detect_anomalies('"DB"."SCHEMA"."TABLE"', '"value"', '"date"', 'DAY', mcp_context_client)


@pytest.mark.asyncio
async def test_detect_anomalies_empty_data(mcp_context_client: Context, mocker):
    """Test detect_anomalies with empty result set."""
    workspace_manager = mocker.AsyncMock(WorkspaceManager)
    workspace_manager.get_sql_dialect.return_value = 'Snowflake'
    workspace_manager.execute_query.return_value = QueryResult(
        status='ok',
        data=SqlSelectData(columns=['time_period'], rows=[])
    )
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = workspace_manager

    result = await detect_anomalies('"DB"."SCHEMA"."TABLE"', '"value"', '"date"', 'DAY', mcp_context_client)
    
    assert isinstance(result, AnomalyDetectionOutput)
    assert result.summary == 'No data found for anomaly analysis.'


@pytest.mark.asyncio
async def test_detect_anomalies_default_parameters(mcp_context_client: Context, mocker):
    """Test detect_anomalies uses default anomaly threshold when not specified."""
    workspace_manager = mocker.AsyncMock(WorkspaceManager)
    workspace_manager.get_sql_dialect.return_value = 'Snowflake'
    workspace_manager.execute_query.return_value = QueryResult(
        status='ok',
        data=SqlSelectData(
            columns=['time_period', 'anomaly_status'],
            rows=[{'time_period': '2024-01-01', 'anomaly_status': 'NORMAL'}]
        )
    )
    mcp_context_client.session.state[WorkspaceManager.STATE_KEY] = workspace_manager

    # Call without specifying anomaly_threshold (should default to 2.5)
    result = await detect_anomalies('"DB"."SCHEMA"."TABLE"', '"value"', '"date"', 'DAY', mcp_context_client)
    
    assert isinstance(result, AnomalyDetectionOutput)
    assert '(threshold=2.5)' in result.query_name
    
    # Verify the SQL query contains the default threshold value 2.5
    executed_query = workspace_manager.execute_query.call_args[0][0]
    assert '2.5' in executed_query
