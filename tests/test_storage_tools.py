from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.server import create_server
from keboola_mcp_server.storage_tools import (
    BucketInfo,
    TableColumnInfo,
    TableDetail,
    get_bucket_metadata,
    get_table_metadata,
    list_bucket_info,
)


@pytest.fixture
def test_config() -> Config:
    """Create a test configuration."""
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
            "table_count": 5,
            "data_size_bytes": 1024,
        },
        {
            "id": "bucket2",
            "name": "Test Bucket 2",
            "description": "Another test bucket",
            "created": "2025-01-01T00:00:00Z",
            "table_count": 3,
            "data_size_bytes": 2048,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("bucket_id", ["bucket1", "bucket2"])
async def test_get_bucket_metadata(
    mcp_context_client, mock_buckets: List[Dict[str, Any]], bucket_id: str
):
    """Test get_bucket_metadata tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.buckets = MagicMock()

    expected_bucket = next(b for b in mock_buckets if b["id"] == bucket_id)
    keboola_client.storage_client.buckets.detail = MagicMock(return_value=expected_bucket)

    result = await get_bucket_metadata(bucket_id, mcp_context_client)

    assert isinstance(result, BucketInfo)
    assert result.id == expected_bucket["id"]
    assert result.name == expected_bucket["name"]

    # Check optional fields only if they are present in the expected bucket
    if "description" in expected_bucket:
        assert result.description == expected_bucket["description"]
    if "stage" in expected_bucket:
        assert result.stage == expected_bucket["stage"]
    if "created" in expected_bucket:
        assert result.created == expected_bucket["created"]
    if "tables_count" in expected_bucket:
        assert result.tables_count == expected_bucket["tables_count"]
    if "data_size_bytes" in expected_bucket:
        assert result.data_size_bytes == expected_bucket["data_size_bytes"]


@pytest.mark.asyncio
async def test_list_bucket_info(mcp_context_client, mock_buckets: List[Dict[str, Any]]) -> None:
    """Test the list_bucket_info tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.buckets = MagicMock()

    # Mock the list method to return the mock_buckets data
    keboola_client.storage_client.buckets.list = MagicMock(return_value=mock_buckets)

    result = await list_bucket_info(mcp_context_client)

    assert isinstance(result, list)
    assert len(result) == len(mock_buckets)
    assert all(isinstance(bucket, BucketInfo) for bucket in result)

    # Assert that the returned BucketInfo objects match the mock data
    for expected_bucket, result_bucket in zip(mock_buckets, result):
        assert result_bucket.id == expected_bucket["id"]
        assert result_bucket.name == expected_bucket["name"]
        if "description" in expected_bucket:
            assert result_bucket.description == expected_bucket["description"]
        if "stage" in expected_bucket:
            assert result_bucket.stage == expected_bucket["stage"]
        if "created" in expected_bucket:
            assert result_bucket.created == expected_bucket["created"]
        if "tables_count" in expected_bucket:
            assert result_bucket.tables_count == expected_bucket["tables_count"]
        if "data_size_bytes" in expected_bucket:
            assert result_bucket.data_size_bytes == expected_bucket["data_size_bytes"]

    keboola_client.storage_client.buckets.list.assert_called_once()


@pytest.mark.asyncio
async def test_get_table_metadata(mcp_context_client, mock_table_detail) -> None:
    """Test get_table_metadata tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.tables = MagicMock()
    keboola_client.storage_client.tables.detail = MagicMock(return_value=mock_table_detail)

    result = await get_table_metadata(mock_table_detail["id"], mcp_context_client)

    assert isinstance(result, TableDetail)
    assert result.id == mock_table_detail["id"]
    assert result.name == mock_table_detail["name"]
    assert result.primary_key == mock_table_detail["primary_key"]
    assert result.row_count == mock_table_detail["row_count"]
    assert result.data_size_bytes == mock_table_detail["data_size_bytes"]
    assert result.columns == mock_table_detail["columns"]

    # # Assert that the column identifiers are correctly set
    # expected_column_info = [
    #     TableColumnInfo(name=col, db_identifier=f'"{col}"') for col in mock_table_detail["columns"]
    # ]
    # assert result.column_identifiers == expected_column_info

    # # Assert that the detail method was called once
    # keboola_client.storage_client.tables.detail.assert_called_once_with(mock_table_detail["id"])
