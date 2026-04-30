"""Tests for LocalBackend: filesystem catalog and DuckDB SQL execution."""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from keboola_mcp_server.local_backend.backend import LocalBackend

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


# ---------------------------------------------------------------------------
# write_csv_table / delete_csv_table
# ---------------------------------------------------------------------------


def test_write_csv_table_creates_file(backend: LocalBackend) -> None:
    path = backend.write_csv_table('my_data', 'id,name\n1,Alice\n')
    assert path.exists()
    assert path.name == 'my_data.csv'
    assert path.read_text(encoding='utf-8') == 'id,name\n1,Alice\n'


def test_write_csv_table_strips_extension(backend: LocalBackend) -> None:
    path = backend.write_csv_table('my_data.csv', 'id\n1\n')
    assert path.name == 'my_data.csv'


def test_write_csv_table_overwrites_existing(backend: LocalBackend) -> None:
    backend.write_csv_table('t', 'a\n1\n')
    path = backend.write_csv_table('t', 'a\n99\n')
    assert path.read_text(encoding='utf-8') == 'a\n99\n'


@pytest.mark.parametrize('bad_name', ['', 'a/b', '../secret', 'a\\b'])
def test_write_csv_table_rejects_invalid_name(backend: LocalBackend, bad_name: str) -> None:
    with pytest.raises(ValueError, match='Invalid table name'):
        backend.write_csv_table(bad_name, 'id\n1\n')


def test_write_csv_table_is_queryable(backend: LocalBackend) -> None:
    backend.write_csv_table('nums', 'n\n5\n10\n')
    result = backend.query_local('SELECT SUM(n) AS total FROM nums')
    assert '15' in result


def test_delete_csv_table_existing(backend: LocalBackend) -> None:
    backend.write_csv_table('to_del', 'id\n1\n')
    assert backend.delete_csv_table('to_del') is True
    assert not (backend.data_dir / 'tables' / 'to_del.csv').exists()


def test_delete_csv_table_not_found(backend: LocalBackend) -> None:
    assert backend.delete_csv_table('nonexistent') is False


# ---------------------------------------------------------------------------
# Schema sidecar
# ---------------------------------------------------------------------------


def test_write_csv_table_creates_schema_sidecar(backend: LocalBackend) -> None:
    backend.write_csv_table('typed', 'flag,count\ntrue,1\nfalse,2\n')
    schema_path = backend.data_dir / 'tables' / 'typed.schema.json'
    assert schema_path.exists(), 'Schema sidecar should be created after write_csv_table'


def test_load_schema_returns_column_list(backend: LocalBackend) -> None:
    backend.write_csv_table('typed', 'flag,count\ntrue,1\nfalse,2\n')
    csv_path = backend.data_dir / 'tables' / 'typed.csv'
    cols = backend._load_schema(csv_path)
    assert cols is not None
    names = [c['name'] for c in cols]
    assert 'flag' in names
    assert 'count' in names
    for c in cols:
        assert 'duckdb_type' in c


def test_load_schema_returns_none_when_absent(backend: LocalBackend, tables_dir: Path) -> None:
    p = _write_csv(tables_dir, 'no_schema.csv', 'id\n1\n')
    assert backend._load_schema(p) is None


def test_duckdb_connection_uses_explicit_types(backend: LocalBackend) -> None:
    # Write a CSV with boolean column; after schema detection BOOLEAN type is enforced.
    backend.write_csv_table('flags', 'active\ntrue\nfalse\n')
    # Query using boolean literal — must not raise ConversionException.
    result = backend.query_local('SELECT COUNT(*) AS n FROM flags WHERE active = true')
    assert '1' in result


def test_duckdb_connection_generates_schema_on_first_load(backend: LocalBackend, tables_dir: Path) -> None:
    # Place a CSV without a sidecar (simulating externally-produced files).
    _write_csv(tables_dir, 'external.csv', 'x,y\n1,2\n3,4\n')
    schema_path = backend.data_dir / 'tables' / 'external.schema.json'
    assert not schema_path.exists()
    # Opening a connection should auto-generate the sidecar.
    con = backend._duckdb_connection()
    con.close()
    assert schema_path.exists(), 'Schema sidecar should be auto-generated on first connection'


def test_delete_csv_table_also_removes_schema(backend: LocalBackend) -> None:
    backend.write_csv_table('byebye', 'a\n1\n')
    schema_path = backend.data_dir / 'tables' / 'byebye.schema.json'
    assert schema_path.exists()
    backend.delete_csv_table('byebye')
    assert not schema_path.exists(), 'Schema sidecar should be removed together with the CSV'


def test_stale_schema_falls_back_to_auto_detect(backend: LocalBackend, tables_dir: Path) -> None:
    # Write a schema with a wrong/stale column definition.
    import json

    _write_csv(tables_dir, 'stale.csv', 'name,score\nalice,10\n')
    schema_path = backend.data_dir / 'tables' / 'stale.schema.json'
    schema_path.write_text(
        json.dumps({'version': 1, 'columns': [{'name': 'nonexistent_col', 'duckdb_type': 'BIGINT'}]}),
        encoding='utf-8',
    )
    # Query must succeed despite stale schema (fallback to read_csv_auto).
    result = backend.query_local('SELECT name FROM stale')
    assert 'alice' in result


def test_delete_csv_table_strips_extension(backend: LocalBackend) -> None:
    backend.write_csv_table('t', 'id\n1\n')
    assert backend.delete_csv_table('t.csv') is True


# ---------------------------------------------------------------------------
# configs dir created on init
# ---------------------------------------------------------------------------


def test_backend_creates_configs_dir(tmp_path: Path) -> None:
    b = LocalBackend(data_dir=str(tmp_path / 'newdir'))
    assert (b.data_dir / 'configs').is_dir()


# ---------------------------------------------------------------------------
# config delegation
# ---------------------------------------------------------------------------


def test_backend_save_and_load_config(backend: LocalBackend) -> None:
    from keboola_mcp_server.local_backend.config import ComponentConfig

    cfg = ComponentConfig(
        config_id='test-cfg',
        component_id='keboola.ex-http',
        name='Test',
        parameters={'k': 'v'},
    )
    saved = backend.save_config(cfg)
    loaded = backend.load_config('test-cfg')

    assert loaded.config_id == saved.config_id
    assert loaded.parameters == {'k': 'v'}


def test_backend_list_configs(backend: LocalBackend) -> None:
    from keboola_mcp_server.local_backend.config import ComponentConfig

    for i in range(3):
        backend.save_config(
            ComponentConfig(config_id=f'cfg-{i}', component_id='keboola.ex-http', name=f'C{i}', parameters={})
        )
    configs = backend.list_configs()
    assert len(configs) == 3


def test_backend_delete_config(backend: LocalBackend) -> None:
    from keboola_mcp_server.local_backend.config import ComponentConfig

    backend.save_config(ComponentConfig(config_id='to-del', component_id='keboola.ex-http', name='X', parameters={}))
    assert backend.delete_config('to-del') is True
    assert backend.delete_config('to-del') is False
