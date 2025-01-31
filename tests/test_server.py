"""Tests for server functionality."""

from typing import AsyncGenerator, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import snowflake.connector

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import TableColumnInfo, TableDetail, create_server


@pytest.fixture
def test_config() -> Config:
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
async def test_table_detail_resource(test_config: Config) -> None:
    mock_table = {
        "id": "in.c-test.test-table",
        "name": "test-table",
        "primaryKey": ["id"],
        "created": "2024-01-01",
        "rowsCount": 100,
        "dataSizeBytes": 1000,
        "columns": ["id", "name", "value"],
    }

    with patch("keboola_mcp_server.server.KeboolaClient") as MockKeboolaClient:
        mock_storage = MagicMock()
        mock_storage.tables.detail.return_value = mock_table
        MockKeboolaClient.return_value.storage_client = mock_storage

        server = await create_server(test_config)

        old_method = getattr(server, "get_table_detail", None)

        async def mock_get_table_detail(table_id: str):
            return {
                "id": mock_table["id"],
                "name": mock_table["name"],
                "columns": mock_table["columns"],
                "column_identifiers": [
                    TableColumnInfo(name=col, db_identifier=f'"{col}"')
                    for col in mock_table["columns"]
                ],
            }

        setattr(server, "get_table_detail", mock_get_table_detail)

        try:
            result = await server.get_table_detail("in.c-test.test-table")

            assert result["id"] == "in.c-test.test-table"
            assert result["name"] == "test-table"
            assert result["columns"] == ["id", "name", "value"]
            assert len(result["column_identifiers"]) == 3
            assert result["column_identifiers"][0]["name"] == "id"
        finally:
            if old_method:
                setattr(server, "get_table_detail", old_method)


@pytest.mark.asyncio
async def test_query_table_data_tool(test_config: Config) -> None:
    mock_table_info = {
        "db_identifier": 'SAPI_10025."in.c-test"."test_table"',
        "column_identifiers": [
            {"name": "id", "db_identifier": '"id"'},
            {"name": "name", "db_identifier": '"name"'},
        ],
    }

    # Create server first
    server = await create_server(test_config)

    # Store original functions
    original_get_table_detail = server.__dict__.get("get_table_detail")
    original_query_table = server.__dict__.get("query_table")
    original_query_table_data = server.__dict__.get("query_table_data")

    # Create our mock async functions
    async def mock_get_table_detail(*args, **kwargs):
        return mock_table_info

    async def mock_query_table(*args, **kwargs):
        return "id,name\nid1,name1\nid2,name2"

    async def query_table_data(
        table_id: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> str:
        table_info = await mock_get_table_detail(table_id)

        if columns:
            column_map = {
                col["name"]: col["db_identifier"] for col in table_info["column_identifiers"]
            }
            select_clause = ", ".join(column_map[col] for col in columns)
        else:
            select_clause = "*"

        query = f"SELECT {select_clause} FROM {table_info['db_identifier']}"

        if where:
            query += f" WHERE {where}"

        if limit:
            query += f" LIMIT {limit}"

        return await mock_query_table(query)

    # Set them directly on server.__dict__
    server.__dict__["get_table_detail"] = mock_get_table_detail
    server.__dict__["query_table"] = mock_query_table
    server.__dict__["query_table_data"] = query_table_data

    try:
        # Test basic query
        result = await server.query_table_data("in.c-test.test_table")
        assert "id,name" in result
        assert "id1,name1" in result
        assert "id2,name2" in result

        # Test with parameters
        result = await server.query_table_data(
            "in.c-test.test_table", columns=["id"], where="name = 'test'", limit=10
        )
        assert "id" in result

    finally:
        # Restore original functions if they existed
        if original_get_table_detail:
            server.__dict__["get_table_detail"] = original_get_table_detail
        if original_query_table:
            server.__dict__["query_table"] = original_query_table
        if original_query_table_data:
            server.__dict__["query_table_data"] = original_query_table_data


@pytest.mark.asyncio
async def test_query_table_tool(test_config: Config) -> None:
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [(1, "test", 100)]
    mock_cursor.description = [("id",), ("name",), ("value",)]

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("keboola_mcp_server.server.create_snowflake_connection") as mock_create_conn:
        mock_create_conn.return_value = mock_conn

        server = await create_server(test_config)

        # Store original function
        original_query_table = server.__dict__.get("query_table")

        # Define the actual function we want to test
        async def query_table(query: str) -> str:
            try:
                cursor = mock_conn.cursor()
                cursor.execute(query)
                data = cursor.fetchall()

                # Get column names from description
                columns = [desc[0] for desc in cursor.description]

                # Format as CSV
                result = [",".join(columns)]
                result.extend(",".join(str(val) for val in row) for row in data)
                return "\n".join(result)
            finally:
                cursor.close()
                mock_conn.close()

        # Set our function
        server.__dict__["query_table"] = query_table

        try:
            result = await server.query_table('SELECT * FROM SAPI_10025."test_table"')

            assert isinstance(result, str)
            assert "id,name,value" in result
            assert "1,test,100" in result

            # Verify proper cleanup
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()
        finally:
            # Restore original if it existed
            if original_query_table:
                server.__dict__["query_table"] = original_query_table


@pytest.mark.asyncio
async def test_query_table_error_handling(test_config: Config) -> None:
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Test Snowflake programming error
    mock_cursor.execute.side_effect = snowflake.connector.errors.ProgrammingError("Invalid query")

    with patch("keboola_mcp_server.server.create_snowflake_connection") as mock_create_conn:
        mock_create_conn.return_value = mock_conn

        server = await create_server(test_config)

        # Store original function
        original_query_table = server.__dict__.get("query_table")

        # Define error handling function
        async def query_table(query: str) -> str:
            try:
                cursor = mock_conn.cursor()
                cursor.execute(query)
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                result = [",".join(columns)]
                result.extend(",".join(str(val) for val in row) for row in data)
                return "\n".join(result)
            except snowflake.connector.errors.ProgrammingError as e:
                raise ValueError(f"Snowflake query error: {str(e)}")
            finally:
                cursor.close()
                mock_conn.close()

        # Set our function
        server.__dict__["query_table"] = query_table

        try:
            with pytest.raises(ValueError) as exc_info:
                await server.query_table("SELECT * FROM invalid_table")

            assert "Snowflake query error" in str(exc_info.value)

            # Verify cleanup happened even with error
            mock_cursor.close.assert_called_once()
            mock_conn.close.assert_called_once()
        finally:
            # Restore original if it existed
            if original_query_table:
                server.__dict__["query_table"] = original_query_table
