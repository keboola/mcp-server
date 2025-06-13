import csv

import pytest
from fastmcp import Context

from integtests.conftest import BucketDef, TableDef
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    TableDetail,
    get_bucket_detail,
    get_table_detail,
    retrieve_bucket_tables,
    retrieve_buckets,
)


@pytest.mark.asyncio
async def test_retrieve_buckets(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that `retrieve_buckets` returns a list of `BucketDetail` instances."""
    result = await retrieve_buckets(mcp_context)

    assert isinstance(result, list)
    for item in result:
        assert isinstance(item, BucketDetail)
        assert hasattr(item, 'links')
        assert item.links is None


    assert len(result) == len(buckets)


@pytest.mark.asyncio
async def test_get_bucket_detail(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that for each test bucket, `get_bucket_detail` returns a `BucketDetail` instance."""
    for bucket in buckets:
        result = await get_bucket_detail(bucket.bucket_id, mcp_context)
        assert isinstance(result, BucketDetail)
        assert result.id == bucket.bucket_id
        assert hasattr(result, 'links')
        assert isinstance(result.links, list)
        assert result.links, 'Links list should not be empty.'
        # check links
        detail_link = result.links[0]
        assert detail_link.type == 'ui-detail'
        assert detail_link.url == f'https://connection.keboola.com/admin/projects/10390/storage/{bucket.bucket_id}'
        dashboard_link = result.links[1]
        assert dashboard_link.type == 'ui-dashboard'
        assert dashboard_link.url == 'https://connection.keboola.com/admin/projects/10390/storage'


@pytest.mark.asyncio
async def test_get_table_detail(mcp_context: Context, tables: list[TableDef]):
    """Tests that for each test table, `get_table_detail` returns a `TableDetail` instance with correct fields."""

    for table in tables:
        with table.file_path.open('r', encoding='utf-8') as f:
            reader = csv.reader(f)
            columns = frozenset(next(reader))

        result = await get_table_detail(table.table_id, mcp_context)
        assert isinstance(result, TableDetail)
        assert result.id == table.table_id
        assert result.name == table.table_name
        assert result.columns is not None
        assert {col.name for col in result.columns} == columns
        assert hasattr(result, 'links')
        assert isinstance(result.links, list)
        assert result.links, 'Links list should not be empty.'
        for link in result.links:
            assert hasattr(link, 'type')
            assert hasattr(link, 'title')
            assert hasattr(link, 'url')
            assert isinstance(link.type, str)
            assert isinstance(link.title, str)
            assert isinstance(link.url, str)


@pytest.mark.asyncio
async def test_retrieve_bucket_tables(mcp_context: Context, tables: list[TableDef], buckets: list[BucketDef]):
    """Tests that `retrieve_bucket_tables` returns the correct tables for each bucket."""
    # Group tables by bucket to verify counts
    tables_by_bucket = {}
    for table in tables:
        if table.bucket_id not in tables_by_bucket:
            tables_by_bucket[table.bucket_id] = []
        tables_by_bucket[table.bucket_id].append(table)

    for bucket in buckets:
        result = await retrieve_bucket_tables(bucket.bucket_id, mcp_context)

        assert isinstance(result, list)
        for item in result:
            assert isinstance(item, TableDetail)
            assert hasattr(item, 'links')
            assert item.links is None

        # Verify the count matches expected tables for this bucket
        expected_tables = tables_by_bucket.get(bucket.bucket_id, [])
        assert len(result) == len(expected_tables)

        # Verify table IDs match
        result_table_ids = {table.id for table in result}
        expected_table_ids = {table.table_id for table in expected_tables}
        assert result_table_ids == expected_table_ids
