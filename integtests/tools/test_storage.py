import csv
import logging
from typing import Any, cast

import pytest
import toon_format
from fastmcp import Client, Context

from integtests.conftest import BucketDef, TableDef
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    DescriptionUpdate,
    GetBucketsOutput,
    GetTablesOutput,
    TableDetail,
    UpdateDescriptionsOutput,
    get_buckets,
    get_tables,
    update_descriptions,
)

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_get_buckets(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that `get_buckets` returns a list of `BucketDetail` instances."""
    result = await get_buckets(mcp_context)

    assert isinstance(result, GetBucketsOutput)
    for item in result.buckets:
        assert isinstance(item, BucketDetail)

    assert len(result.buckets) == len(buckets)
    assert result.bucket_counts.total_buckets == len(buckets)

    # Count buckets by stage from the actual result (since BucketDef doesn't have stage info)
    actual_input_count = sum(1 for bucket in result.buckets if bucket.stage == 'in')
    actual_output_count = sum(1 for bucket in result.buckets if bucket.stage == 'out')

    # Verify our counts match what we calculated
    assert result.bucket_counts.input_buckets == actual_input_count
    assert result.bucket_counts.output_buckets == actual_output_count

    # Verify the counts add up to the total
    assert (
        result.bucket_counts.input_buckets + result.bucket_counts.output_buckets == result.bucket_counts.total_buckets
    )


@pytest.mark.asyncio
async def test_get_buckets_output_format(mcp_client: Client, buckets: list[BucketDef]):
    """Tests that `get_buckets` returns the tool output in TOON format."""
    result = await mcp_client.call_tool('get_buckets')
    assert len(result.content) == 1
    assert result.content[0].type == 'text'
    result_text = result.content[0].text
    assert GetBucketsOutput.model_validate(toon_format.decode(result_text)) == GetBucketsOutput.model_validate(
        result.structured_content
    )
    # check that the tables are presented in tabular format
    assert result_text.startswith(
        f'buckets[{len(buckets)}]'
        '{id,name,display_name,description,stage,created,data_size_bytes,tables_count,links,source_project}:'
    )


@pytest.mark.asyncio
async def test_get_bucket(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that for each test bucket, `get_bucket` returns a `BucketDetail` instance."""
    for bucket in buckets:
        result = await get_buckets(mcp_context, [bucket.bucket_id])
        assert isinstance(result, GetBucketsOutput)
        assert len(result.buckets) == 1
        assert result.buckets[0].id == bucket.bucket_id


@pytest.mark.asyncio
async def test_get_table(mcp_context: Context, tables: list[TableDef]):
    """Tests that for each test table, `get_table` returns a `TableDetail` instance with correct fields."""

    for table_def in tables:
        with table_def.file_path.open('r', encoding='utf-8') as f:
            reader = csv.reader(f)
            col_names = frozenset(next(reader))

        result = await get_tables(mcp_context, table_ids=[table_def.table_id])
        assert isinstance(result, GetTablesOutput)
        assert len(result.tables) == 1
        assert result.tables[0].id == table_def.table_id
        assert result.tables[0].name == table_def.table_name
        assert result.tables[0].columns is not None
        assert {col.name for col in result.tables[0].columns} == col_names


@pytest.mark.asyncio
async def test_get_tables(mcp_context: Context, tables: list[TableDef], buckets: list[BucketDef]):
    """Tests that `get_tables` returns the correct tables for each bucket."""
    # Group tables by bucket to verify counts
    tables_by_bucket: dict[str, list[TableDef]] = {}
    for table_def in tables:
        if table_def.bucket_id not in tables_by_bucket:
            tables_by_bucket[table_def.bucket_id] = []
        tables_by_bucket[table_def.bucket_id].append(table_def)

    for bucket in buckets:
        result = await get_tables(mcp_context, [bucket.bucket_id])

        assert isinstance(result, GetTablesOutput)
        for table in result.tables:
            assert isinstance(table, TableDetail)

        # Verify the count matches expected tables for this bucket
        expected_tables = tables_by_bucket.get(bucket.bucket_id, [])
        assert len(result.tables) == len(expected_tables)

        # Verify table IDs match
        result_table_ids = {table.id for table in cast(list[TableDetail], result.tables)}
        expected_table_ids = {table_def.table_id for table_def in expected_tables}
        assert result_table_ids == expected_table_ids


@pytest.mark.asyncio
async def test_get_tables_output_format(mcp_client: Client, tables: list[TableDef], buckets: list[BucketDef]):
    """Tests that `get_tables` returns the tool output in TOON format."""
    result = await mcp_client.call_tool('get_tables', {'bucket_ids': [buckets[0].bucket_id]})
    assert len(result.content) == 1
    assert result.content[0].type == 'text'
    result_text = result.content[0].text
    assert GetTablesOutput.model_validate(toon_format.decode(result_text)) == GetTablesOutput.model_validate(
        result.structured_content
    )
    assert result_text.startswith(
        'tables[1]{id,name,display_name,description,primary_key,created,rows_count,'
        'data_size_bytes,columns,fully_qualified_name,links,source_project}:'
    )


@pytest.mark.asyncio
async def test_update_descriptions_bucket(mcp_context: Context, buckets: list[BucketDef]):
    """Tests that `update_descriptions` updates bucket descriptions correctly."""
    bucket = buckets[0]
    client = KeboolaClient.from_state(mcp_context.session.state)

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
    assert bucket_result.item_id == bucket.bucket_id
    assert bucket_result.success is True
    assert bucket_result.error is None
    assert bucket_result.timestamp is not None

    # Verify the description was actually updated
    metadata = await client.storage_client.bucket_metadata_get(bucket.bucket_id)
    assert get_metadata_property(metadata, MetadataField.DESCRIPTION) == 'New Description'


@pytest.mark.asyncio
async def test_update_descriptions_table(mcp_context: Context, mcp_client: Client, tables: list[TableDef]):
    """
    Tests that `update_descriptions` updates table descriptions correctly.
    Also tests that the tool output is in TOON format.
    """
    table = tables[0]
    storage_client = KeboolaClient.from_state(mcp_context.session.state).storage_client

    call_result = await mcp_client.call_tool(
        'update_descriptions',
        {
            'updates': [{'item_id': table.table_id, 'description': 'New Table Description'}],
        },
    )
    assert len(call_result.content) == 1
    assert call_result.content[0].type == 'text'

    result = UpdateDescriptionsOutput.model_validate(call_result.structured_content)

    toon_result = UpdateDescriptionsOutput.model_validate(toon_format.decode(call_result.content[0].text))
    assert toon_result == result

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 1
    assert result.failed == 0
    assert len(result.results) == 1

    table_result = result.results[0]
    assert table_result.item_id == table.table_id
    assert table_result.success is True
    assert table_result.error is None
    assert table_result.timestamp is not None

    # Verify the description was actually updated
    metadata = await storage_client.table_metadata_get(table.table_id)
    assert get_metadata_property(metadata, MetadataField.DESCRIPTION) == 'New Table Description'


@pytest.mark.asyncio
async def test_update_descriptions_table_column(mcp_context: Context, tables: list[TableDef]):
    """Tests that `update_descriptions` updates table descriptions correctly."""
    table = tables[0]

    with table.file_path.open('r', encoding='utf-8') as f:
        reader = csv.reader(f)
        col_names = next(reader)
    column_name = col_names[0]

    column_id = f'{table.table_id}.{column_name}'
    result = await update_descriptions(
        ctx=mcp_context,
        updates=[DescriptionUpdate(item_id=column_id, description='New Table Column Description')],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 1
    assert result.failed == 0
    assert len(result.results) == 1

    column_result = result.results[0]
    assert column_result.item_id == column_id
    assert column_result.success is True
    assert column_result.error is None
    assert column_result.timestamp is not None

    # Verify the description is available in the table detail
    tables_output = await get_tables(mcp_context, table_ids=[table.table_id])
    assert isinstance(tables_output, GetTablesOutput)
    assert len(tables_output.tables) == 1
    table_detail = tables_output.tables[0]
    assert table_detail.columns is not None
    column_detail = next((col for col in table_detail.columns if col.name == column_name), None)
    assert column_detail is not None
    assert column_detail.description == 'New Table Column Description'


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

    md_ids: list[tuple[str, str, str]] = []
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

        # Verify column description was updated
        table_detail = await client.storage_client.table_detail(table.table_id)
        assert 'columnMetadata' in table_detail
        column_metadata = cast(dict[str, list[dict[str, Any]]], table_detail['columnMetadata'])
        assert column_name in column_metadata
        column_entry = next(
            (entry for entry in column_metadata[column_name] if entry.get('key') == MetadataField.DESCRIPTION), None
        )
        if column_entry:
            assert column_entry['value'] == 'Mixed Column Description'
            md_ids.append(('column', f'{table.table_id}.{column_name}', str(column_entry['id'])))

    finally:
        # Clean up metadata
        for md_type, item_id, md_id in md_ids:
            if md_type == 'bucket':
                await client.storage_client.bucket_metadata_delete(bucket_id=item_id, metadata_id=md_id)
            elif md_type == 'table':
                await client.storage_client.table_metadata_delete(table_id=item_id, metadata_id=md_id)
            elif md_type == 'column':
                await client.storage_client.column_metadata_delete(column_id=item_id, metadata_id=md_id)


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
    assert error_result.item_id == 'invalid-path'
    assert error_result.success is False
    assert error_result.error is not None
    assert 'Invalid item_id format' in error_result.error
    assert error_result.timestamp is None
