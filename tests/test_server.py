"""Tests for server functionality."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from typing import AsyncGenerator, Dict, List

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server, TableDetail, TableColumnInfo


@pytest.fixture
def test_config():
    return Config(
        storage_token="test-token",
        storage_api_url="https://connection.test.keboola.com",
        log_level="INFO",
        snowflake_account="test-account",
        snowflake_user="test-user",
        snowflake_password="test-password",
        snowflake_warehouse="test-warehouse",
        snowflake_database="test-database",
        snowflake_role="test-role",
    )


@pytest.fixture
def mock_table_detail() -> TableDetail:
    """Create a mock table detail."""
    return {
        "id": "in.c-test.test-table",
        "name": "test-table",
        "primary_key": ["id"],
        "created": "2024-01-01T00:00:00Z",
        "row_count": 100,
        "data_size_bytes": 1000,
        "columns": ["id", "name", "value"],
        "column_identifiers": [
            TableColumnInfo(name="id", db_identifier='"id"'),
            TableColumnInfo(name="name", db_identifier='"name"'),
            TableColumnInfo(name="value", db_identifier='"value"'),
        ],
        "db_identifier": '"KEBOOLA_test"."in.c-test"."test-table"',
    }


@pytest.mark.asyncio
async def test_table_detail_resource(test_config):
    # Mock table data
    mock_table = {
        "id": "in.c-test.test-table",
        "name": "test-table",
        "primaryKey": ["id"],
        "created": "2024-01-01",
        "rowsCount": 100,
        "dataSizeBytes": 1000,
        "columns": ["id", "name", "value"],
    }

    # Create server with mocked dependencies
    with patch("keboola_mcp_server.server.KeboolaClient") as mock_client:
        # Setup mock storage client
        mock_storage = MagicMock()
        mock_storage.tables.detail.return_value = mock_table
        mock_client.return_value.storage_client = mock_storage

        server = await create_server(test_config)
        result = await server.get_table_detail("in.c-test.test-table")

        assert result["id"] == "in.c-test.test-table"
        assert result["name"] == "test-table"
        assert result["columns"] == ["id", "name", "value"]
        assert len(result["column_identifiers"]) == 3
        assert result["column_identifiers"][0]["name"] == "id"


@pytest.mark.asyncio
async def test_query_table_data_tool(test_config):
    mock_table_info = {
        "db_identifier": 'SAPI_10025."in.c-test"."test_table"',
        "column_identifiers": [
            {"name": "id", "db_identifier": '"id"'},
            {"name": "name", "db_identifier": '"name"'},
        ],
    }

    mock_data = [("id1", "name1"), ("id2", "name2")]
    mock_description = [("id",), ("name",)]

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = mock_data
    mock_cursor.description = mock_description

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with (
        patch("keboola_mcp_server.server.create_snowflake_connection") as mock_create_conn,
        patch("keboola_mcp_server.server.get_table_detail") as mock_get_detail,
    ):

        mock_create_conn.return_value = mock_conn
        mock_get_detail.return_value = mock_table_info

        server = await create_server(test_config)

        result = await server.query_table_data("in.c-test.test_table")
        assert isinstance(result, str)
        assert "id,name" in result
        assert "id1,name1" in result
        assert "id2,name2" in result

        result = await server.query_table_data(
            "in.c-test.test_table", columns=["id"], where="name = test", limit=10
        )

        mock_cursor.execute.assert_called_with(
            'SELECT "id" FROM SAPI_10025."in.c-test"."test_table" WHERE name = \'test\' LIMIT 10'
        )


@pytest.mark.asyncio
async def test_query_table_tool(test_config):
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, "test", 100)]
    mock_cursor.description = [("id",), ("name",), ("value",)]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("keboola_mcp_server.server.create_snowflake_connection") as mock_create_conn:
        mock_create_conn.return_value = mock_conn

        server = await create_server(test_config)
        result = await server.query_table('SELECT * FROM SAPI_10025."test_table"')

        assert isinstance(result, str)
        assert "id,name,value" in result
        assert "1,test,100" in result

        # Verify proper cleanup
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


@pytest.mark.asyncio
async def test_query_table_error_handling(test_config):
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Test Snowflake programming error
    mock_cursor.execute.side_effect = snowflake.connector.errors.ProgrammingError("Invalid query")

    with patch("keboola_mcp_server.server.create_snowflake_connection") as mock_create_conn:
        mock_create_conn.return_value = mock_conn

        server = await create_server(test_config)

        with pytest.raises(ValueError) as exc_info:
            await server.query_table("SELECT * FROM invalid_table")

        assert "Snowflake query error" in str(exc_info.value)

        # Verify cleanup happened even with error
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
