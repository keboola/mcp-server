from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config, MetadataField
from keboola_mcp_server.storage_tools import (
    BucketInfo,
    TableDetail,
    UpdateBucketDescriptionResponse,
    UpdateTableDescriptionResponse,
    get_bucket_metadata,
    get_table_metadata,
    list_bucket_info,
    update_bucket_description,
    update_table_description,
)


@pytest.fixture
def test_config() -> Config:
    """Create a test configuration."""
    return Config(
        storage_token="test-token",
        storage_api_url="https://connection.test.keboola.com",
    )


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


@pytest.fixture
def mock_table_data() -> Dict[str, Any]:
    """Create a combined fixture for table testing.

    Contains both the raw API response data and the expected transformed data.
    """
    raw_table_data = {
        "id": "in.c-test.test-table",
        "name": "test-table",
        "primary_key": ["id"],
        "created": "2024-01-01T00:00:00Z",
        "rows_count": 100,
        "data_size_bytes": 1000,
        "columns": ["id", "name", "value"],
    }

    return {
        "raw_table_data": raw_table_data,  # What the client returns
        "additional_data": {
            "snowflake_fqn": '"SAPI_TEST"."in.c-test"."test-table"',  # What workspace_manager should return
            "column_identifiers": [  # Expected transformed column identifiers
                {"name": "id", "db_identifier": '"id"'},
                {"name": "name", "db_identifier": '"name"'},
                {"name": "value", "db_identifier": '"value"'},
            ],
        },
    }


@pytest.fixture
def mock_update_table_description_response() -> Dict[str, Any]:
    """Mock valid response from the Keboola API for table description update."""
    return {
        "metadata": [
            {
                "id": "1724427984",
                "key": MetadataField.DESCRIPTION.value,
                "value": "Updated test description",
                "provider": "user",
                "timestamp": "2025-04-07T16:47:18+0200",
            }
        ]
    }


@pytest.fixture
def mock_update_bucket_description_response() -> List[Dict[str, Any]]:
    """Mock valid response list for updating a bucket description."""
    return [
        {
            "id": "999",
            "key": MetadataField.DESCRIPTION.value,
            "value": "Updated bucket description",
            "provider": "user",
            "timestamp": "2025-04-07T17:47:18+0200",
        }
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

    keboola_client.storage_client.buckets.detail.assert_called_once_with(bucket_id)


@pytest.mark.asyncio
async def test_list_bucket_info(mcp_context_client, mock_buckets: List[Dict[str, Any]]) -> None:
    """Test the list_bucket_info tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.buckets = MagicMock()
    keboola_client.storage_client.buckets.list = MagicMock(return_value=mock_buckets)

    result = await list_bucket_info(mcp_context_client)

    assert isinstance(result, list)
    assert len(result) == len(mock_buckets)
    assert all(isinstance(bucket, BucketInfo) for bucket in result)

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
async def test_get_table_metadata(mcp_context_client, mock_table_data) -> None:
    """Test get_table_metadata tool."""

    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.storage_client.tables = MagicMock()
    keboola_client.storage_client.tables.detail = MagicMock(
        return_value=mock_table_data["raw_table_data"]
    )

    workspace_manager = mcp_context_client.session.state["workspace_manager"]
    mock_fqn = MagicMock()
    mock_fqn.snowflake_fqn = mock_table_data["additional_data"]["snowflake_fqn"]
    workspace_manager.get_table_fqn = AsyncMock(return_value=mock_fqn)
    result = await get_table_metadata(mock_table_data["raw_table_data"]["id"], mcp_context_client)

    assert isinstance(result, TableDetail)
    assert result.id == mock_table_data["raw_table_data"]["id"]
    assert result.name == mock_table_data["raw_table_data"]["name"]
    assert result.primary_key == mock_table_data["raw_table_data"]["primary_key"]
    assert result.rows_count == mock_table_data["raw_table_data"]["rows_count"]
    assert result.data_size_bytes == mock_table_data["raw_table_data"]["data_size_bytes"]
    assert result.columns == mock_table_data["raw_table_data"]["columns"]
    assert result.db_identifier == mock_table_data["additional_data"]["snowflake_fqn"]
    for col_id, exp_col_id in zip(
        result.column_identifiers, mock_table_data["additional_data"]["column_identifiers"]
    ):
        assert col_id.name == exp_col_id["name"]
        assert col_id.db_identifier == exp_col_id["db_identifier"]
    keboola_client.storage_client.tables.detail.assert_called_once_with(
        mock_table_data["raw_table_data"]["id"]
    )
    workspace_manager.get_table_fqn.assert_awaited_once_with(mock_table_data["raw_table_data"])


@pytest.mark.asyncio
async def test_update_bucket_description_success(
    mcp_context_client, mock_update_bucket_description_response
) -> None:
    """Test successful update of bucket description."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.post = AsyncMock(return_value=mock_update_bucket_description_response)

    result = await update_bucket_description(
        bucket_id="in.c-test.bucket-id",
        description="Updated bucket description",
        ctx=mcp_context_client,
    )

    assert isinstance(result, UpdateBucketDescriptionResponse)
    assert result.success is True
    assert result.description == "Updated bucket description"
    assert result.timestamp == "2025-04-07T17:47:18+0200"
    keboola_client.post.assert_called_once_with(
        endpoint="buckets/in.c-test.bucket-id/metadata",
        data={
            "provider": "user",
            "metadata": [
                {"key": MetadataField.DESCRIPTION.value, "value": "Updated bucket description"}
            ],
        },
    )


@pytest.mark.asyncio
async def test_update_table_description_success(
    mcp_context_client, mock_update_table_description_response
) -> None:
    """Test successful update of table description."""

    # Mock the Keboola client post method
    keboola_client = mcp_context_client.session.state["sapi_client"]
    keboola_client.post = AsyncMock(return_value=mock_update_table_description_response)

    result = await update_table_description(
        table_id="in.c-test.test-table",
        description="Updated test description",
        ctx=mcp_context_client,
    )

    assert isinstance(result, UpdateTableDescriptionResponse)
    assert result.success is True
    assert result.description == "Updated test description"
    assert result.timestamp == "2025-04-07T16:47:18+0200"
    keboola_client.post.assert_called_once_with(
        endpoint="tables/in.c-test.test-table/metadata",
        data={
            "provider": "user",
            "metadata": [
                {"key": MetadataField.DESCRIPTION.value, "value": "Updated test description"}
            ],
        },
    )
