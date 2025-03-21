"""Tests for server functionality."""

from typing import Any, Dict, List, Optional

import pytest

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server, TableColumnInfo, TableDetail, BucketInfo


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

@pytest.fixture
def mock_buckets() -> List[Dict[str, Any]]:
    """Fixture for mock bucket data."""
    return [
        {
            "id": "bucket1",
            "name": "Test Bucket 1",
            "description": "A test bucket",
            "stage": "production",
            "created": "2024-01-01T00:00:00Z",
            "tables_count": 5,
            "data_size_bytes": 1024,
        },
        {
            "id": "bucket2",
            "name": "Test Bucket 2",
            "description": "Another test bucket",
            "tables_count": 3,
            "data_size_bytes": 2048,
        },
    ]

@pytest.mark.asyncio
async def test_query_table_data_tool(test_config: Config) -> None:
    # TODO -- make this test to actually test something; in its current shape it only calls the mock function
    #   Testing the MCP tools is tricky. Ideally, we would need a test client similar to
    #   `starlette.testclient.TestClient`, but for the statefull MCP.

    mock_table_info = {
        "db_identifier": 'SAPI_10025."in.c-test"."test_table"',
        "column_identifiers": [
            {"name": "id", "db_identifier": '"id"'},
            {"name": "name", "db_identifier": '"name"'},
        ],
    }

    # Create server first
    server = create_server(test_config)

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
async def test_list_all_buckets(test_config: Config, mock_buckets: List[Dict[str, Any]]) -> None:
    """Test the list_all_buckets tool."""
    # Create server first
    server = create_server(test_config)

    # Mock the storage client
    original_list_buckets = server.__dict__.get("list_all_buckets")

    async def mock_list_all_buckets(ctx):
        return [BucketInfo(**bucket) for bucket in mock_buckets]

    server.__dict__["list_all_buckets"] = mock_list_all_buckets

    try:
        # Call the tool
        result = await server.list_all_buckets(None)

        # Assert the result is a list of BucketInfo
        assert isinstance(result, list)
        assert all(isinstance(bucket, BucketInfo) for bucket in result)

        # Check the content of the first bucket
        assert result[0].id == "bucket1"
        assert result[0].name == "Test Bucket 1"
        assert result[0].description == "A test bucket"
        assert result[0].stage == "production"
        assert result[0].created == "2024-01-01T00:00:00Z"
        assert result[0].tables_count == 5
        assert result[0].data_size_bytes == 1024

    finally:
        # Restore original function if it existed
        if original_list_buckets:
            server.__dict__["list_all_buckets"] = original_list_buckets
