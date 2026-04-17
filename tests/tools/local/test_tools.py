"""Tests for local-mode tool implementation functions."""

from pathlib import Path

import pytest

from keboola_mcp_server.tools.local.backend import LocalBackend
from keboola_mcp_server.tools.local.tools import (
    LocalBucketsOutput,
    LocalProjectInfo,
    LocalSearchOutput,
    LocalTablesOutput,
    get_buckets_local,
    get_project_info_local,
    get_tables_local,
    query_data_local,
    search_local,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(data_dir=str(tmp_path))


@pytest.fixture
def tables_dir(backend: LocalBackend) -> Path:
    return backend.data_dir / 'tables'


def _csv(tables_dir: Path, name: str, content: str) -> Path:
    p = tables_dir / name
    p.write_text(content, encoding='utf-8')
    return p


# ---------------------------------------------------------------------------
# get_tables_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_tables_empty(backend: LocalBackend) -> None:
    result = await get_tables_local(backend)
    assert isinstance(result, LocalTablesOutput)
    assert result.tables == []
    assert result.total == 0


@pytest.mark.asyncio
async def test_get_tables_with_two_csvs(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'alpha.csv', 'id,name\n1,foo\n2,bar\n')
    _csv(tables_dir, 'beta.csv', 'x,y,z\n1,2,3\n')
    result = await get_tables_local(backend)
    assert result.total == 2
    names = {t.name for t in result.tables}
    assert names == {'alpha', 'beta'}


@pytest.mark.asyncio
async def test_get_tables_columns_populated(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'orders.csv', 'order_id,customer_id,amount\n1,10,99.5\n')
    result = await get_tables_local(backend)
    table = result.tables[0]
    assert table.columns == ['order_id', 'customer_id', 'amount']


@pytest.mark.asyncio
async def test_get_tables_rows_count(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'data.csv', 'a,b\n1,2\n3,4\n5,6\n')
    result = await get_tables_local(backend)
    assert result.tables[0].rows_count == 3


@pytest.mark.asyncio
async def test_get_tables_size_bytes(backend: LocalBackend, tables_dir: Path) -> None:
    content = 'id,val\n1,hello\n'
    _csv(tables_dir, 'sized.csv', content)
    result = await get_tables_local(backend)
    assert result.tables[0].size_bytes == len(content.encode('utf-8'))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('filter_names', 'expected_names'),
    [
        (['alpha'], {'alpha'}),
        (['alpha', 'gamma'], {'alpha'}),  # gamma doesn't exist
        ([], {'alpha', 'beta'}),  # empty list treated same as None — no filter
        (None, {'alpha', 'beta'}),  # no filter → all results
    ],
)
async def test_get_tables_filter(
    backend: LocalBackend,
    tables_dir: Path,
    filter_names: list[str] | None,
    expected_names: set[str],
) -> None:
    _csv(tables_dir, 'alpha.csv', 'id\n1\n')
    _csv(tables_dir, 'beta.csv', 'id\n2\n')
    result = await get_tables_local(backend, table_names=filter_names)
    assert {t.name for t in result.tables} == expected_names


# ---------------------------------------------------------------------------
# get_buckets_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_buckets_empty(backend: LocalBackend) -> None:
    result = await get_buckets_local(backend)
    assert isinstance(result, LocalBucketsOutput)
    assert len(result.buckets) == 1
    bucket = result.buckets[0]
    assert bucket.id == 'local'
    assert bucket.tables_count == 0
    assert bucket.table_names == []


@pytest.mark.asyncio
async def test_get_buckets_with_tables(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'foo.csv', 'id\n1\n')
    _csv(tables_dir, 'bar.csv', 'id\n2\n')
    result = await get_buckets_local(backend)
    bucket = result.buckets[0]
    assert bucket.tables_count == 2
    assert set(bucket.table_names) == {'foo', 'bar'}


@pytest.mark.asyncio
async def test_get_buckets_stage_is_in(backend: LocalBackend) -> None:
    result = await get_buckets_local(backend)
    assert result.buckets[0].stage == 'in'


# ---------------------------------------------------------------------------
# query_data_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_query_data_local_returns_markdown(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'items.csv', 'id,price\n1,9.99\n2,4.50\n')
    result = await query_data_local(backend, 'SELECT * FROM items ORDER BY id', 'All Items')
    assert '| id | price |' in result
    assert '9.99' in result


@pytest.mark.asyncio
async def test_query_data_local_uses_query_name_parameter(backend: LocalBackend, tables_dir: Path) -> None:
    # query_name is ignored in local mode (DuckDB returns results directly);
    # verify the function signature accepts it without error.
    _csv(tables_dir, 'x.csv', 'v\n1\n')
    result = await query_data_local(backend, 'SELECT v FROM x', 'My Query')
    assert '1' in result


# ---------------------------------------------------------------------------
# search_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_filename_match(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'customers.csv', 'id,name\n1,Alice\n')
    result = await search_local(backend, 'custom')
    assert isinstance(result, LocalSearchOutput)
    assert any(r.match_type == 'filename' and r.name == 'customers' for r in result.results)


@pytest.mark.asyncio
async def test_search_column_match(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'orders.csv', 'order_id,customer_email,amount\n1,a@b.com,10\n')
    result = await search_local(backend, 'email')
    assert any(r.match_type == 'column' and r.matched_value == 'customer_email' for r in result.results)


@pytest.mark.asyncio
async def test_search_no_match(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'products.csv', 'id,title\n1,Widget\n')
    result = await search_local(backend, 'zzznomatch')
    assert result.results == []


@pytest.mark.asyncio
async def test_search_case_insensitive(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'Customers.csv', 'CustomerID,Name\n1,Bob\n')
    result = await search_local(backend, 'CUSTOMERS')
    assert len(result.results) > 0


@pytest.mark.asyncio
async def test_search_no_duplicates(backend: LocalBackend, tables_dir: Path) -> None:
    # Both filename and column contain 'data'; should not duplicate the filename match.
    _csv(tables_dir, 'data_table.csv', 'data_col,other\n1,2\n')
    result = await search_local(backend, 'data')
    keys = [(r.name, r.match_type, r.matched_value) for r in result.results]
    assert len(keys) == len(set(keys))


@pytest.mark.asyncio
async def test_search_query_preserved(backend: LocalBackend, tables_dir: Path) -> None:
    result = await search_local(backend, 'myquery')
    assert result.query == 'myquery'


# ---------------------------------------------------------------------------
# get_project_info_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_project_info_mode(backend: LocalBackend) -> None:
    result = await get_project_info_local(backend)
    assert isinstance(result, LocalProjectInfo)
    assert result.mode == 'local'


@pytest.mark.asyncio
async def test_get_project_info_sql_engine(backend: LocalBackend) -> None:
    result = await get_project_info_local(backend)
    assert result.sql_engine == 'DuckDB'


@pytest.mark.asyncio
async def test_get_project_info_data_dir(backend: LocalBackend) -> None:
    result = await get_project_info_local(backend)
    assert result.data_dir == str(backend.data_dir.resolve())


@pytest.mark.asyncio
async def test_get_project_info_table_count(backend: LocalBackend, tables_dir: Path) -> None:
    _csv(tables_dir, 'a.csv', 'id\n1\n')
    _csv(tables_dir, 'b.csv', 'id\n2\n')
    result = await get_project_info_local(backend)
    assert result.table_count == 2


@pytest.mark.asyncio
async def test_get_project_info_llm_instruction_not_empty(backend: LocalBackend) -> None:
    result = await get_project_info_local(backend)
    assert result.llm_instruction
