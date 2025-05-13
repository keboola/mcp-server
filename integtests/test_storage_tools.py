import csv
from pathlib import Path

import pytest
from mcp.server.fastmcp import Context

from integtests.conftest import BucketDef, TableDef
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    TableDetail,
    get_bucket_detail,
    get_table_detail,
    retrieve_buckets,
)


@pytest.mark.asyncio
async def test_retrieve_buckets(mcp_context_client: Context, test_buckets: list[BucketDef]):
    """Tests that retrieve_buckets returns a list of BucketDetail instances."""
    result = await retrieve_buckets(mcp_context_client)

    assert isinstance(result, list)
    for item in result:
        assert isinstance(item, BucketDetail)

    assert len(result) == len(test_buckets)


@pytest.mark.asyncio
async def test_get_bucket_detail(mcp_context_client: Context, test_buckets: list[BucketDef]):
    """Tests that get_bucket_detail returns a BucketDetail instance for each bucket."""
    for bucket in test_buckets:
        result = await get_bucket_detail(bucket.bucket_id, mcp_context_client)
        assert isinstance(result, BucketDetail)
        assert result.id == bucket.bucket_id


@pytest.mark.asyncio
async def test_get_table_detail(mcp_context_client: Context, test_tables: list[TableDef], shared_datadir: Path):
    """Tests that get_table_detail returns a TableDetail instance for each test table with correct fields."""

    # Test that the table detail is correct for the test_table_01 table
    for table in test_tables:
        table_path = shared_datadir / 'proj' / table.bucket_id / f'{table.table_name}.csv'
        with table_path.open('r', encoding='utf-8') as f:
            reader = csv.reader(f)
            columns = frozenset(next(reader))

        result = await get_table_detail(table.table_id, mcp_context_client)
        assert isinstance(result, TableDetail)
        assert result.id == table.table_id
        assert result.name == table.table_name
        assert result.columns is not None
        assert {col.name for col in result.columns} == columns
