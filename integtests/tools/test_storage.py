import csv
import logging

import pytest
from fastmcp import Context

from integtests.conftest import BucketDef, TableDef
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    ListBucketsOutput,
    ListTablesOutput,
    TableDetail,
    UpdateDescriptionOutput,
    get_bucket,
    get_table,
    list_buckets,
    list_tables,
    update_description,
)

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_list_buckets(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that `list_buckets` returns a list of `BucketDetail` instances."""
    result = await list_buckets(mcp_context)

    assert isinstance(result, ListBucketsOutput)
    for item in result.buckets:
        assert isinstance(item, BucketDetail)

    assert len(result.buckets) == len(buckets)


@pytest.mark.asyncio
async def test_get_bucket(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that for each test bucket, `get_bucket` returns a `BucketDetail` instance."""
    for bucket in buckets:
        result = await get_bucket(bucket.bucket_id, mcp_context)
        assert isinstance(result, BucketDetail)
        assert result.id == bucket.bucket_id


@pytest.mark.asyncio
async def test_get_table(mcp_context: Context, tables: list[TableDef]):
    """Tests that for each test table, `get_table` returns a `TableDetail` instance with correct fields."""

    for table in tables:
        with table.file_path.open('r', encoding='utf-8') as f:
            reader = csv.reader(f)
            columns = frozenset(next(reader))

        result = await get_table(table.table_id, mcp_context)
        assert isinstance(result, TableDetail)
        assert result.id == table.table_id
        assert result.name == table.table_name
        assert result.columns is not None
        assert {col.name for col in result.columns} == columns


@pytest.mark.asyncio
async def test_list_tables(mcp_context: Context, tables: list[TableDef], buckets: list[BucketDef]):
    """Tests that `list_tables` returns the correct tables for each bucket."""
    # Group tables by bucket to verify counts
    tables_by_bucket = {}
    for table in tables:
        if table.bucket_id not in tables_by_bucket:
            tables_by_bucket[table.bucket_id] = []
        tables_by_bucket[table.bucket_id].append(table)

    for bucket in buckets:
        result = await list_tables(bucket.bucket_id, mcp_context)

        assert isinstance(result, ListTablesOutput)
        for item in result.tables:
            assert isinstance(item, TableDetail)

        # Verify the count matches expected tables for this bucket
        expected_tables = tables_by_bucket.get(bucket.bucket_id, [])
        assert len(result.tables) == len(expected_tables)

        # Verify table IDs match
        result_table_ids = {table.id for table in result.tables}
        expected_table_ids = {table.table_id for table in expected_tables}
        assert result_table_ids == expected_table_ids


@pytest.mark.asyncio
async def test_update_bucket_description(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that `update_bucket_description` updates the description of a bucket."""
    bucket = buckets[0]
    md_id: str | None = None
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        result = await update_description(
            ctx=mcp_context,
            item_type='bucket',
            description='New Description',
            bucket_id=bucket.bucket_id,
        )
        assert isinstance(result, UpdateDescriptionOutput)
        assert result.description == 'New Description'

        metadata = await client.storage_client.bucket_metadata_get(bucket.bucket_id)
        metadata_entry = next((entry for entry in metadata if entry.get('key') == MetadataField.DESCRIPTION), None)
        assert metadata_entry is not None, f'Metadata entry for bucket {bucket.bucket_id} description not found'
        assert metadata_entry['value'] == 'New Description'
        md_id = str(metadata_entry['id'])
    finally:
        if md_id is not None:
            await client.storage_client.bucket_metadata_delete(bucket_id=bucket.bucket_id, metadata_id=md_id)


@pytest.mark.asyncio
async def test_update_table_description(mcp_context: Context, tables: list[TableDef]):
    """Tests that `update_table_description` updates the description of a table."""
    table = tables[0]
    md_id: str | None = None
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        result = await update_description(
            ctx=mcp_context,
            item_type='table',
            description='New Description',
            table_id=table.table_id,
        )
        assert isinstance(result, UpdateDescriptionOutput)
        assert result.description == 'New Description'

        metadata = await client.storage_client.table_metadata_get(table.table_id)
        metadata_entry = next((entry for entry in metadata if entry.get('key') == MetadataField.DESCRIPTION), None)
        assert metadata_entry is not None, f'Metadata entry for table {table.table_id} description not found'
        assert metadata_entry['value'] == 'New Description'
        md_id = str(metadata_entry['id'])
    finally:
        if md_id is not None:
            await client.storage_client.table_metadata_delete(table_id=table.table_id, metadata_id=md_id)


@pytest.mark.asyncio
async def test_update_column_description(mcp_context: Context, tables: list[TableDef]):
    """Tests that `update_column_description` updates the description of a column."""
    table = tables[0]

    # Get the first column name from the table CSV file
    with table.file_path.open('r', encoding='utf-8') as f:
        reader = csv.reader(f)
        columns = next(reader)

    column_name = columns[0]  # Use the first column
    test_description = 'New Column Description'

    # Test the update_column_description function
    result = await update_description(
        ctx=mcp_context,
        item_type='column',
        description=test_description,
        table_id=table.table_id,
        column_name=column_name,
    )
    LOG.error(result)

    # Verify the function returns expected result
    assert isinstance(result, UpdateDescriptionOutput)
    assert result.description == test_description
    assert result.success is True
    assert result.timestamp is not None
