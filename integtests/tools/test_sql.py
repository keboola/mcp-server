import csv
import logging
from io import StringIO

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.tools.sql import get_sql_dialect, query_data
from keboola_mcp_server.tools.storage import get_table, list_buckets, list_tables

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_query_data(mcp_context: Context):
    """Tests basic functionality of SQL tools: get_sql_dialect and query_data."""

    dialect = await get_sql_dialect(ctx=mcp_context)
    assert dialect in ['Snowflake', 'Bigquery']

    buckets_listing = await list_buckets(ctx=mcp_context)

    tables_listing = await list_tables(bucket_id=buckets_listing.buckets[0].id, ctx=mcp_context)
    table = await get_table(table_id=tables_listing.tables[0].id, ctx=mcp_context)
    LOG.error(table)
    assert table.fully_qualified_name is not None, 'Table should have fully qualified name'

    sql_query = f'SELECT COUNT(*) as row_count FROM {table.fully_qualified_name}'
    result = await query_data(sql_query=sql_query, ctx=mcp_context)

    # Verify result is CSV formatted string
    assert isinstance(result, str)
    assert len(result) > 0

    # Parse the CSV to verify structure
    csv_reader = csv.reader(StringIO(result))
    rows = list(csv_reader)

    # Should have header and one data row
    assert len(rows) == 2, 'COUNT query should return header + one data row'
    assert rows[0] == ['ROW_COUNT'], f'Header should be ["row_count"], got {rows[0]}'

    # Count should be a number
    count_value = rows[1][0]
    assert count_value.isdigit(), f'Count value should be numeric, got: {count_value}'


@pytest.mark.asyncio
async def test_query_data_invalid_query(mcp_context: Context):
    """Tests that `query_data` properly handles invalid SQL queries."""

    invalid_sql = 'INVALID SQL SYNTAX SELECT * FROM'

    with pytest.raises(ValueError, match='Failed to run SQL query'):
        await query_data(sql_query=invalid_sql, ctx=mcp_context)


@pytest.mark.asyncio
async def test_query_data_non_select_query(mcp_context: Context):
    """Tests that `query_data` handles non-SELECT queries and returns a message."""

    dialect = await get_sql_dialect(ctx=mcp_context)
    temp_table_name = 'temp_test_table_12345'

    try:
        if dialect == 'Snowflake':
            create_sql = f'CREATE OR REPLACE TABLE {temp_table_name} (id INT, name STRING)'
        elif dialect == 'Bigquery':
            create_sql = f'CREATE OR REPLACE TABLE {temp_table_name} (id INT64, name STRING)'
        else:
            create_sql = f'CREATE TABLE {temp_table_name} (id INT, name VARCHAR(100))'

        result = await query_data(sql_query=create_sql, ctx=mcp_context)

        # For non-SELECT queries, should return CSV with 'message' column
        assert isinstance(result, str)
        assert len(result) > 0

        # Parse the CSV
        csv_reader = csv.reader(StringIO(result))
        rows = list(csv_reader)

        # Should have header and one data row with message
        assert len(rows) == 2, 'Non-SELECT query should return header + one message row'
        assert rows[0] == ['message'], f'Header should be ["message"], got {rows[0]}'

        # Message should indicate success
        message = rows[1][0]
        assert isinstance(message, str), f'Message should be a string, got: {message}'
        assert len(message) > 0, 'Message should not be empty'

    finally:
        # Clean up: Drop the table
        try:
            drop_sql = f'DROP TABLE IF EXISTS {temp_table_name}'
            await query_data(sql_query=drop_sql, ctx=mcp_context)
        except Exception as e:
            LOG.warning(f'Failed to clean up temp table {temp_table_name}: {e}')
