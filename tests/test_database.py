from typing import Any

import pytest

from keboola_mcp_server.config import Config
from keboola_mcp_server.database import DatabasePathManager, TableFqn


class TestDatabasePathManager:

    @pytest.mark.parametrize('table, snowflake_result, expected', [
        (
            # table in.c-foo.bar in its own project
            {'id': 'in.c-foo.bar', 'name': 'bar'},
            {'current_database': 'db_xyz', 'current_schema': 'workspace_123'},
            TableFqn(db_name='db_xyz', schema_name='in.c-foo', table_name='bar')
        ),
        (
            # temporary table not in a project, but in the writable schema of the workspace
            {'id': 'bar', 'name': 'bar'},
            {'current_database': 'db_xyz', 'current_schema': 'workspace_123'},
            TableFqn(db_name='db_xyz', schema_name='workspace_123', table_name='bar')
        ),
        (
            # table out.c-baz.bam exported from project 1234 and imported as table in.c-foo.bar in some other project
            {'id': 'in.c-foo.bar', 'name': 'bar', 'sourceTable': {'project': {'id': '1234'}, 'id': 'out.c-baz.bam'}},
            {'name': 'sapi_1234'},
            TableFqn(db_name='sapi_1234', schema_name='out.c-baz', table_name='bam')
        ),
    ])
    def test_get_table_fqn(
            self, table: dict[str, Any], snowflake_result: dict[str, Any], expected: TableFqn, mocker
    ):
        conn = mocker.MagicMock()
        conn.__enter__.return_value = conn
        conn.connect.return_value = conn
        conn.cursor.return_value = (cursor := mocker.MagicMock())
        cursor.execute.return_value = (result := mocker.MagicMock())
        result.fetchone.return_value = snowflake_result

        cm = mocker.patch('keboola_mcp_server.database.ConnectionManager')
        cm.create_snowflake_connection.return_value = conn

        dpm = DatabasePathManager(config=Config(), connection_manager=cm)
        fqn = dpm.get_table_fqn(table)
        assert fqn == expected
