import csv
import logging

import pytest
from fastmcp import Context

from integtests.conftest import BucketDef, TableDef
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    DescriptionUpdate,
    ListBucketsOutput,
    ListTablesOutput,
    TableDetail,
    UpdateDescriptionsOutput,
    get_bucket,
    get_table,
    list_buckets,
    list_tables,
    update_descriptions,
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
async def test_update_descriptions_bucket(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that `update_descriptions` updates bucket descriptions correctly."""
    bucket = buckets[0]
    md_id: str | None = None
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        result = await update_descriptions(
            ctx=mcp_context,
            updates=[DescriptionUpdate(item_id=bucket.bucket_id, description='New Description')],
        )

        assert isinstance(result, UpdateDescriptionsOutput)
        assert result.total_processed == 1
        assert result.successful == 1
        assert result.failed == 0
        assert len(result.results) == 1

        bucket_result = result.results[0]
        assert bucket_result.path == bucket.bucket_id
        assert bucket_result.success is True
        assert bucket_result.error is None
        assert bucket_result.timestamp is not None

        # Verify the description was actually updated
        metadata = await client.storage_client.bucket_metadata_get(bucket.bucket_id)
        metadata_entry = next((entry for entry in metadata if entry.get('key') == MetadataField.DESCRIPTION), None)
        assert metadata_entry is not None, f'Metadata entry for bucket {bucket.bucket_id} description not found'
        assert metadata_entry['value'] == 'New Description'
        md_id = str(metadata_entry['id'])
    finally:
        if md_id is not None:
            await client.storage_client.bucket_metadata_delete(bucket_id=bucket.bucket_id, metadata_id=md_id)


@pytest.mark.asyncio
async def test_update_descriptions_table(mcp_context: Context, tables: list[TableDef]):
    """Tests that `update_descriptions` updates table descriptions correctly."""
    table = tables[0]
    md_id: str | None = None
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        result = await update_descriptions(
            ctx=mcp_context,
            updates=[DescriptionUpdate(item_id=table.table_id, description='New Table Description')],
        )

        assert isinstance(result, UpdateDescriptionsOutput)
        assert result.total_processed == 1
        assert result.successful == 1
        assert result.failed == 0
        assert len(result.results) == 1

        table_result = result.results[0]
        assert table_result.path == table.table_id
        assert table_result.success is True
        assert table_result.error is None
        assert table_result.timestamp is not None

        # Verify the description was actually updated
        metadata = await client.storage_client.table_metadata_get(table.table_id)
        metadata_entry = next((entry for entry in metadata if entry.get('key') == MetadataField.DESCRIPTION), None)
        assert metadata_entry is not None, f'Metadata entry for table {table.table_id} description not found'
        assert metadata_entry['value'] == 'New Table Description'
        md_id = str(metadata_entry['id'])
    finally:
        if md_id is not None:
            await client.storage_client.table_metadata_delete(table_id=table.table_id, metadata_id=md_id)


@pytest.mark.asyncio
async def test_update_descriptions_mixed_types(mcp_context: Context, buckets: list[BucketDef], tables: list[TableDef]):
    """Tests that `update_descriptions` can handle mixed types in a single call."""
    bucket = buckets[0]
    table = tables[0]

    # Get the first column name from the table CSV file
    with table.file_path.open('r', encoding='utf-8') as f:
        reader = csv.reader(f)
        columns = next(reader)
    column_name = columns[0]

    md_ids: list[str] = []
    client = KeboolaClient.from_state(mcp_context.session.state)
    try:
        result = await update_descriptions(
            ctx=mcp_context,
            updates=[
                DescriptionUpdate(item_id=bucket.bucket_id, description='Mixed Bucket Description'),
                DescriptionUpdate(item_id=table.table_id, description='Mixed Table Description'),
                DescriptionUpdate(item_id=f'{table.table_id}.{column_name}', description='Mixed Column Description'),
            ],
        )

        assert isinstance(result, UpdateDescriptionsOutput)
        assert result.total_processed == 3
        assert result.successful == 3
        assert result.failed == 0
        assert len(result.results) == 3

        # Verify all results are successful
        for item_result in result.results:
            assert item_result.success is True
            assert item_result.error is None
            assert item_result.timestamp is not None

        # Verify bucket description was updated
        bucket_metadata = await client.storage_client.bucket_metadata_get(bucket.bucket_id)
        bucket_entry = next((entry for entry in bucket_metadata if entry.get('key') == MetadataField.DESCRIPTION), None)
        if bucket_entry:
            assert bucket_entry['value'] == 'Mixed Bucket Description'
            md_ids.append(('bucket', bucket.bucket_id, str(bucket_entry['id'])))

        # Verify table description was updated
        table_metadata = await client.storage_client.table_metadata_get(table.table_id)
        table_entry = next((entry for entry in table_metadata if entry.get('key') == MetadataField.DESCRIPTION), None)
        if table_entry:
            assert table_entry['value'] == 'Mixed Table Description'
            md_ids.append(('table', table.table_id, str(table_entry['id'])))

    finally:
        # Clean up metadata
        for md_type, item_id, md_id in md_ids:
            if md_type == 'bucket':
                await client.storage_client.bucket_metadata_delete(bucket_id=item_id, metadata_id=md_id)
            elif md_type == 'table':
                await client.storage_client.table_metadata_delete(table_id=item_id, metadata_id=md_id)


@pytest.mark.asyncio
async def test_update_descriptions_invalid_path(mcp_context: Context):
    """Tests that `update_descriptions` handles invalid paths gracefully."""
    result = await update_descriptions(
        ctx=mcp_context,
        updates=[DescriptionUpdate(item_id='invalid-path', description='This should fail')],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 0
    assert result.failed == 1
    assert len(result.results) == 1

    error_result = result.results[0]
    assert error_result.path == 'invalid-path'
    assert error_result.success is False
    assert error_result.error is not None
    assert 'Invalid path format' in error_result.error
    assert error_result.timestamp is None
