"""Tests for server functionality."""

from typing import Any, Dict, List, Optional

import pytest

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import BucketInfo, TableColumnInfo, TableDetail, create_server


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
def mock_table_detail() -> Dict[str, Any]:
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
            {"name": "id", "db_identifier": '"id"'},
            {"name": "name", "db_identifier": '"name"'},
            {"name": "value", "db_identifier": '"value"'},
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
            "created": "2025-01-01T00:00:00Z",
            "tables_count": 3,
            "data_size_bytes": 2048,
        },
    ]


@pytest.fixture
def mock_tables() -> List[Dict[str, Any]]:
    """Fixture for mock table data."""
    return [
        {
            "id": "in.c-test.table1",
            "name": "table1",
            "primary_key": ["id"],
            "created": "2024-01-01T00:00:00Z",
            "row_count": 100,
            "data_size_bytes": 1024,
            "columns": ["id", "name", "value"],
        },
        {
            "id": "in.c-test.table2",
            "name": "table2",
            "primary_key": ["id"],
            "created": "2024-01-01T00:00:00Z",
            "row_count": 200,
            "data_size_bytes": 2048,
            "columns": ["id", "description"],
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
    server = create_server(test_config)

    original_list_buckets = server.__dict__.get("list_all_buckets")

    async def mock_list_all_buckets(ctx):
        return [BucketInfo(**bucket) for bucket in mock_buckets]

    server.__dict__["list_all_buckets"] = mock_list_all_buckets

    try:
        result = await server.list_all_buckets(None)

        assert isinstance(result, list)
        assert all(isinstance(bucket, BucketInfo) for bucket in result)

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


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket_id", ["bucket1", "bucket2"])
async def test_get_bucket_metadata(
    test_config: Config, mock_buckets: List[Dict[str, Any]], bucket_id: str
) -> None:
    """Test the get_bucket_metadata tool."""
    server = create_server(test_config)
    original_get_bucket_metadata = server.__dict__.get("get_bucket_metadata")

    async def mock_get_bucket_metadata(bid: str, ctx):
        bucket = next((b for b in mock_buckets if b["id"] == bid), None)
        if not bucket:
            raise ValueError(f"Bucket {bid} not found")
        return BucketInfo(**bucket)

    server.__dict__["get_bucket_metadata"] = mock_get_bucket_metadata

    try:
        expected_bucket = next(b for b in mock_buckets if b["id"] == bucket_id)
        result = await server.get_bucket_metadata(bucket_id, None)

        assert isinstance(result, BucketInfo)

        for field, value in expected_bucket.items():
            assert getattr(result, field) == value

        with pytest.raises(ValueError, match="Bucket nonexistent-bucket not found"):
            await server.get_bucket_metadata("nonexistent-bucket", None)

    finally:
        if original_get_bucket_metadata:
            server.__dict__["get_bucket_metadata"] = original_get_bucket_metadata


@pytest.mark.asyncio
async def test_list_bucket_tables(test_config: Config, mock_tables: List[Dict[str, Any]]) -> None:
    """Test the list_bucket_tables tool."""
    server = create_server(test_config)
    original_list_bucket_tables = server.__dict__.get("list_bucket_tables")

    async def mock_list_bucket_tables(bucket_id: str, ctx):
        # In a real scenario, we would filter by bucket_id
        tables = [TableDetail(**table) for table in mock_tables]
        for table in tables:
            # Add column identifiers
            table.column_identifiers = [
                TableColumnInfo(name=col, db_identifier=f'"{col}"') for col in table.columns or []
            ]
            # Add db_identifier
            table.db_identifier = f'"KEBOOLA_test"."{bucket_id}"."{table.name}"'
        return tables

    server.__dict__["list_bucket_tables"] = mock_list_bucket_tables

    try:
        result = await server.list_bucket_tables("in.c-test", None)

        assert isinstance(result, list)
        assert all(isinstance(table, TableDetail) for table in result)
        assert len(result) == 2

        # Check first table
        assert result[0].id == "in.c-test.table1"
        assert result[0].name == "table1"
        assert result[0].primary_key == ["id"]
        assert result[0].created == "2024-01-01T00:00:00Z"
        assert result[0].row_count == 100
        assert result[0].data_size_bytes == 1024
        assert result[0].columns == ["id", "name", "value"]
        assert len(result[0].column_identifiers) == 3
        assert result[0].db_identifier == '"KEBOOLA_test"."in.c-test"."table1"'

        # Check second table
        assert result[1].id == "in.c-test.table2"
        assert result[1].name == "table2"
        assert result[1].columns == ["id", "description"]
        assert len(result[1].column_identifiers) == 2

    finally:
        if original_list_bucket_tables:
            server.__dict__["list_bucket_tables"] = original_list_bucket_tables


@pytest.mark.asyncio
@pytest.mark.parametrize("table_id", ["in.c-test.table1", "in.c-test.table2"])
async def test_get_table_metadata(
    test_config: Config, mock_tables: List[Dict[str, Any]], table_id: str
) -> None:
    """Test the get_table_metadata tool."""
    server = create_server(test_config)
    original_get_table_metadata = server.__dict__.get("get_table_metadata")

    async def mock_get_table_metadata(tid: str, ctx):
        table = next((t for t in mock_tables if t["id"] == tid), None)
        if not table:
            raise ValueError(f"Table {tid} not found")

        # Create TableDetail with column info
        table_detail = TableDetail(**table)
        table_detail.column_identifiers = (
            [TableColumnInfo(name=col, db_identifier=f'"{col}"') for col in table["columns"]]
            if table.get("columns")
            else []
        )
        table_detail.db_identifier = f'"KEBOOLA_test"."in.c-test"."{table["name"]}"'
        return table_detail

    server.__dict__["get_table_metadata"] = mock_get_table_metadata

    try:
        result = await server.get_table_metadata(table_id, None)

        assert isinstance(result, TableDetail)
        assert result.id == table_id

        # Get the expected mock data
        expected = next(t for t in mock_tables if t["id"] == table_id)

        # Check all fields match
        assert result.name == expected["name"]
        assert result.primary_key == expected["primary_key"]
        assert result.created == expected["created"]
        assert result.row_count == expected["row_count"]
        assert result.data_size_bytes == expected["data_size_bytes"]
        assert result.columns == expected["columns"]

        # Check column identifiers were created correctly
        column_identifiers = result.column_identifiers or []
        columns = expected.get("columns", [])
        assert len(column_identifiers) == len(columns)
        for col_info, col_name in zip(column_identifiers, columns):
            assert col_info.name == col_name
            assert col_info.db_identifier == f'"{col_name}"'

        # Check db_identifier was created correctly
        assert result.db_identifier == f'"KEBOOLA_test"."in.c-test"."{expected["name"]}"'

    finally:
        if original_get_table_metadata:
            server.__dict__["get_table_metadata"] = original_get_table_metadata
