"""Tests for LocalBackend: filesystem catalog and DuckDB SQL execution."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from keboola_mcp_server.tools.local.backend import LocalBackend

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(data_dir=str(tmp_path))


@pytest.fixture
def tables_dir(backend: LocalBackend) -> Path:
    return backend.data_dir / 'tables'


def _write_csv(tables_dir: Path, name: str, content: str) -> Path:
    p = tables_dir / name
    p.write_text(content, encoding='utf-8')
    return p


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def test_data_dir_created(tmp_path: Path) -> None:
    data_dir = tmp_path / 'new_dir'
    assert not data_dir.exists()
    LocalBackend(data_dir=str(data_dir))
    assert (data_dir / 'tables').is_dir()


def test_tables_subdir_created(tmp_path: Path) -> None:
    backend = LocalBackend(data_dir=str(tmp_path))
    assert (backend.data_dir / 'tables').is_dir()


# ---------------------------------------------------------------------------
# list_csv_tables
# ---------------------------------------------------------------------------


def test_list_csv_tables_empty(backend: LocalBackend, tables_dir: Path) -> None:
    assert backend.list_csv_tables() == []


def test_list_csv_tables_sorted(backend: LocalBackend, tables_dir: Path) -> None:
    for name in ('zebra.csv', 'alpha.csv', 'middle.csv'):
        _write_csv(tables_dir, name, 'id\n1')
    names = [p.name for p in backend.list_csv_tables()]
    assert names == ['alpha.csv', 'middle.csv', 'zebra.csv']


def test_list_csv_tables_ignores_non_csv(backend: LocalBackend, tables_dir: Path) -> None:
    _write_csv(tables_dir, 'data.csv', 'id\n1')
    (tables_dir / 'readme.txt').write_text('hi')
    result = backend.list_csv_tables()
    assert len(result) == 1
    assert result[0].name == 'data.csv'


# ---------------------------------------------------------------------------
# read_csv_headers
# ---------------------------------------------------------------------------


def test_read_csv_headers(backend: LocalBackend, tables_dir: Path) -> None:
    p = _write_csv(tables_dir, 'test.csv', 'id,name,value\n1,foo,10\n')
    assert backend.read_csv_headers(p) == ['id', 'name', 'value']


def test_read_csv_headers_empty_file(backend: LocalBackend, tables_dir: Path) -> None:
    p = tables_dir / 'empty.csv'
    p.write_text('', encoding='utf-8')
    assert backend.read_csv_headers(p) == []


def test_read_csv_headers_missing_file(backend: LocalBackend, tables_dir: Path) -> None:
    result = backend.read_csv_headers(tables_dir / 'nonexistent.csv')
    assert result == []


# ---------------------------------------------------------------------------
# query_local
# ---------------------------------------------------------------------------


def test_query_local_select(backend: LocalBackend, tables_dir: Path) -> None:
    _write_csv(tables_dir, 'customers.csv', 'id,name\n1,Alice\n2,Bob\n')
    result = backend.query_local('SELECT * FROM customers ORDER BY id')
    assert '| id | name |' in result
    assert 'Alice' in result
    assert 'Bob' in result


def test_query_local_no_rows(backend: LocalBackend, tables_dir: Path) -> None:
    _write_csv(tables_dir, 'empty_table.csv', 'id,name\n')
    result = backend.query_local('SELECT * FROM empty_table WHERE 1=0')
    assert '| id | name |' in result
    assert '| --- | --- |' in result


def test_query_local_aggregation(backend: LocalBackend, tables_dir: Path) -> None:
    _write_csv(tables_dir, 'sales.csv', 'region,amount\nEast,100\nWest,200\nEast,150\n')
    result = backend.query_local('SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region')
    assert 'East' in result
    assert '250' in result


def test_query_local_ddl_returns_string(backend: LocalBackend, tables_dir: Path) -> None:
    # DuckDB returns a Count result row for DDL statements rather than cursor.description=None.
    result = backend.query_local('CREATE TABLE test_tbl (id INTEGER)')
    assert isinstance(result, str)
    assert len(result) > 0


def test_query_local_null_values(backend: LocalBackend, tables_dir: Path) -> None:
    _write_csv(tables_dir, 'nulls.csv', 'id,val\n1,\n2,hello\n')
    result = backend.query_local('SELECT id, val FROM nulls ORDER BY id')
    lines = result.split('\n')
    # first data row should have empty val cell
    assert '|  |' in lines[2] or lines[2].endswith('|  |')


def test_query_local_table_name_sanitization(backend: LocalBackend, tables_dir: Path) -> None:
    # CSV file with a double-quote in stem must not crash.
    p = tables_dir / 'my"table.csv'
    p.write_text('x\n1\n', encoding='utf-8')
    # The table is registered as 'my_table'; querying it by that name should work.
    result = backend.query_local('SELECT * FROM "my_table"')
    assert '1' in result


@pytest.mark.parametrize(
    ('bad_query', 'exc_type'),
    [
        ('SELECT * FROM nonexistent_table_xyz', Exception),
        ('SELECT !!!', Exception),
    ],
)
def test_query_local_invalid_sql_raises(
    backend: LocalBackend, tables_dir: Path, bad_query: str, exc_type: type
) -> None:
    with pytest.raises(exc_type):
        backend.query_local(bad_query)


def test_query_local_missing_duckdb_raises_runtime_error(backend: LocalBackend, tables_dir: Path) -> None:
    with patch.dict(sys.modules, {'duckdb': None}):
        with pytest.raises(RuntimeError, match='duckdb is required'):
            backend.query_local('SELECT 1')
