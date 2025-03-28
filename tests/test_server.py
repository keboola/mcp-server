"""Tests for server functionality."""

from typing import Any, Dict, List

import pytest

from keboola_mcp_server.config import Config
from keboola_mcp_server.server import BucketInfo, create_server


@pytest.fixture
def test_config() -> Config:
    return Config(
        storage_token="test-token",
        storage_api_url="https://connection.test.keboola.com",
        workspace_user="test-user",
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
        assert result[0].table_count == 5
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
