import csv
import logging
from io import StringIO

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.tools.sql import QueryDataOutput, query_data
from keboola_mcp_server.tools.storage import get_table, list_buckets, list_tables

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_query_data(mcp_context: Context):
    """Tests basic functionality of SQL tools: get_sql_dialect and query_data."""

    buckets_listing = await list_buckets(ctx=mcp_context)

    tables_listing = await list_tables(bucket_id=buckets_listing.buckets[0].id, ctx=mcp_context)
    table = await get_table(table_id=tables_listing.tables[0].id, ctx=mcp_context)

    assert table.fully_qualified_name is not None, 'Table should have fully qualified name'

    sql_query = f'SELECT COUNT(*) as row_count FROM {table.fully_qualified_name}'
    result = await query_data(sql_query=sql_query, query_name='Row Count Query', ctx=mcp_context)

    # Verify result is structured output
    assert isinstance(result, QueryDataOutput)
    assert result.query_name == 'Row Count Query'
    assert isinstance(result.csv_data, str)
    assert len(result.csv_data) > 0

    # Parse the CSV to verify structure
    csv_reader = csv.reader(StringIO(result.csv_data))
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
        await query_data(sql_query=invalid_sql, query_name='Invalid Query Test', ctx=mcp_context)
