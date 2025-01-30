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
        snowflake_role="test-role"
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
            TableColumnInfo(name="value", db_identifier='"value"')
        ],
        "db_identifier": '"KEBOOLA_test"."in.c-test"."test-table"'
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
        "columns": ["id", "name", "value"]
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
    mock_data = [("id1", "name1"), ("id2", "name2")]
    mock_description = [("id",), ("name",)]

    # Create mock cursor
    mock_cursor = AsyncMock()
    mock_cursor.fetchall.return_value = mock_data
    mock_cursor.description = mock_description
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None

    # Create mock connection
    mock_conn = AsyncMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__aenter__.return_value = mock_conn
    mock_conn.__aexit__.return_value = None

    # Create server with mocked dependencies
    with patch("keboola_mcp_server.server.snowflake_connection") as mock_sf:
        mock_sf.return_value = mock_conn
        
        server = await create_server(test_config)
        result = await server.query_table("SELECT * FROM test_table")

        assert isinstance(result, str)
        assert "id,name" in result
        assert "id1,name1" in result
        assert "id2,name2" in result


@pytest.mark.asyncio
async def test_query_table_with_invalid_config():
    # Create server with invalid config
    invalid_config = Config(storage_token="test-token")
    server = await create_server(invalid_config)
    
    with pytest.raises(ValueError) as exc_info:
        await server.query_table("SELECT * FROM test")
    
    assert "Snowflake credentials not fully configured" in str(exc_info.value)


@pytest.mark.asyncio
async def test_query_table_tool(test_config) -> None:
    """Test query_table tool."""
    server = create_server(test_config)
    
    # Mock cursor and connection
    mock_cursor = AsyncMock()
    mock_cursor.description = [("id",), ("name",), ("value",)]
    mock_cursor.fetchall.return_value = [(1, "test", 100)]
    mock_cursor.__aenter__.return_value = mock_cursor
    mock_cursor.__aexit__.return_value = None
    
    mock_conn = AsyncMock()
    mock_conn.cursor = AsyncMock(return_value=mock_cursor)
    mock_conn.__aenter__.return_value = mock_conn
    mock_conn.__aexit__.return_value = None
    
    with patch('snowflake.connector.connect', return_value=mock_conn):
        result = await server.call_tool('query_table', {"sql_query": 'SELECT * FROM "test_table"'})
        assert isinstance(result, str)
        assert "test" in result 