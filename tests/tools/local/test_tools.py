"""Tests for local-mode tool implementation functions."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server.tools.local.backend import LocalBackend
from keboola_mcp_server.tools.local.config import ComponentConfig
from keboola_mcp_server.tools.local.docker import ComponentRunResult, ComponentSetupResult
from keboola_mcp_server.tools.local.schema import ComponentSchemaResult
from keboola_mcp_server.tools.local.tools import (
    LocalBucketsOutput,
    LocalComponentSearchOutput,
    LocalProjectInfo,
    LocalSearchOutput,
    LocalTableInfo,
    LocalTablesOutput,
    delete_config_local,
    delete_table_local,
    find_component_id_local,
    get_buckets_local,
    get_component_schema_local,
    get_project_info_local,
    get_tables_local,
    list_configs_local,
    query_data_local,
    run_component_local,
    run_saved_config_local,
    save_config_local,
    search_local,
    setup_component_local,
    write_table_local,
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


# ---------------------------------------------------------------------------
# setup_component_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_setup_component_local_delegates_to_backend(backend: LocalBackend) -> None:
    expected = ComponentSetupResult(path='/tmp/repo', schema=None)
    with patch.object(backend, 'setup_component', return_value=expected) as mock_setup:
        result = await setup_component_local(backend, 'https://github.com/keboola/repo.git')

    mock_setup.assert_called_once_with('https://github.com/keboola/repo.git', False)
    assert result is expected


@pytest.mark.asyncio
async def test_setup_component_local_force_rebuild(backend: LocalBackend) -> None:
    expected = ComponentSetupResult(path='/tmp/repo')
    with patch.object(backend, 'setup_component', return_value=expected) as mock_setup:
        await setup_component_local(backend, 'https://github.com/keboola/repo.git', force_rebuild=True)

    mock_setup.assert_called_once_with('https://github.com/keboola/repo.git', True)


# ---------------------------------------------------------------------------
# run_component_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_component_local_image_mode(backend: LocalBackend) -> None:
    expected = ComponentRunResult(status='success', exit_code=0)
    with patch.object(backend, 'run_docker_component', return_value=expected) as mock_run:
        result = await run_component_local(backend, {'key': 'val'}, component_image='keboola/ex-http:1.0')

    mock_run.assert_called_once_with('keboola/ex-http:1.0', {'key': 'val'}, None, '4g')
    assert result.status == 'success'


@pytest.mark.asyncio
async def test_run_component_local_source_mode(backend: LocalBackend) -> None:
    expected = ComponentRunResult(status='success', exit_code=0)
    with patch.object(backend, 'run_source_component', return_value=expected) as mock_run:
        result = await run_component_local(
            backend, {}, git_url='https://github.com/keboola/repo.git', input_tables=['t1']
        )

    mock_run.assert_called_once_with('https://github.com/keboola/repo.git', {}, ['t1'], '4g')
    assert result.status == 'success'


@pytest.mark.asyncio
async def test_run_component_local_neither_raises(backend: LocalBackend) -> None:
    with pytest.raises(ValueError, match='Provide either'):
        await run_component_local(backend, {})


@pytest.mark.asyncio
async def test_run_component_local_both_raises(backend: LocalBackend) -> None:
    with pytest.raises(ValueError, match='not both'):
        await run_component_local(
            backend, {}, component_image='img:latest', git_url='https://github.com/keboola/repo.git'
        )


# ---------------------------------------------------------------------------
# get_component_schema_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_component_schema_local_calls_api() -> None:
    schema = ComponentSchemaResult(component_id='keboola.ex-http', name='HTTP', raw={})

    async def _fake_fetch(component_id):
        return schema

    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient') as mock_client_cls:
        instance = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        resp = MagicMock()
        resp.json.return_value = {'id': 'keboola.ex-http', 'name': 'HTTP'}
        resp.raise_for_status = MagicMock()
        instance.get = AsyncMock(return_value=resp)

        result = await get_component_schema_local('keboola.ex-http')

    assert result.component_id == 'keboola.ex-http'


# ---------------------------------------------------------------------------
# find_component_id_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_component_id_local_wraps_results() -> None:
    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient') as mock_client_cls:
        instance = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        resp = MagicMock()
        resp.json.return_value = [
            {'id': 'keboola.ex-http', 'name': 'HTTP Extractor'},
            {'id': 'keboola.ex-ftp', 'name': 'FTP Extractor'},
        ]
        resp.raise_for_status = MagicMock()
        instance.get = AsyncMock(return_value=resp)

        result = await find_component_id_local('http')

    assert isinstance(result, LocalComponentSearchOutput)
    assert result.query == 'http'
    assert len(result.results) == 2


@pytest.mark.asyncio
async def test_find_component_id_local_empty() -> None:
    with patch('keboola_mcp_server.tools.local.schema.httpx.AsyncClient') as mock_client_cls:
        instance = AsyncMock()
        mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        resp = MagicMock()
        resp.json.return_value = []
        resp.raise_for_status = MagicMock()
        instance.get = AsyncMock(return_value=resp)

        result = await find_component_id_local('zzznomatch')

    assert result.results == []
    assert result.query == 'zzznomatch'


# ---------------------------------------------------------------------------
# write_table_local / delete_table_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_table_local_creates_table(backend: LocalBackend) -> None:
    result = await write_table_local(backend, 'events', 'id,ts\n1,2024-01-01\n2,2024-01-02\n')

    assert isinstance(result, LocalTableInfo)
    assert result.name == 'events'
    assert result.columns == ['id', 'ts']
    assert result.rows_count == 2


@pytest.mark.asyncio
async def test_write_table_local_queryable(backend: LocalBackend) -> None:
    await write_table_local(backend, 'sales', 'amount\n10\n20\n30\n')
    total = backend.query_local('SELECT SUM(amount) FROM sales')
    assert '60' in total


@pytest.mark.asyncio
async def test_delete_table_local_existing(backend: LocalBackend) -> None:
    await write_table_local(backend, 'temp', 'id\n1\n')
    result = await delete_table_local(backend, 'temp')

    assert result == {'deleted': True, 'name': 'temp'}
    assert not (backend.data_dir / 'tables' / 'temp.csv').exists()


@pytest.mark.asyncio
async def test_delete_table_local_not_found(backend: LocalBackend) -> None:
    result = await delete_table_local(backend, 'ghost')
    assert result == {'deleted': False, 'name': 'ghost'}


# ---------------------------------------------------------------------------
# save_config_local / list_configs_local / delete_config_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_config_local_creates_config(backend: LocalBackend) -> None:
    result = await save_config_local(
        backend,
        config_id='http-001',
        component_id='keboola.ex-http',
        name='HTTP Extractor',
        parameters={'url': 'https://api.example.com'},
        component_image='keboola/ex-http:latest',
    )

    assert isinstance(result, ComponentConfig)
    assert result.config_id == 'http-001'
    assert result.created_at != ''


@pytest.mark.asyncio
async def test_list_configs_local_returns_saved(backend: LocalBackend) -> None:
    await save_config_local(backend, 'c1', 'keboola.ex-http', 'C1', {}, component_image='img:1')
    await save_config_local(backend, 'c2', 'keboola.ex-ftp', 'C2', {}, component_image='img:2')

    output = await list_configs_local(backend)
    assert output.total == 2
    ids = {c.config_id for c in output.configs}
    assert ids == {'c1', 'c2'}


@pytest.mark.asyncio
async def test_list_configs_local_empty(backend: LocalBackend) -> None:
    output = await list_configs_local(backend)
    assert output.total == 0
    assert output.configs == []


@pytest.mark.asyncio
async def test_delete_config_local_existing(backend: LocalBackend) -> None:
    await save_config_local(backend, 'to-del', 'keboola.ex-http', 'X', {}, component_image='img:1')
    result = await delete_config_local(backend, 'to-del')
    assert result == {'deleted': True, 'config_id': 'to-del'}


@pytest.mark.asyncio
async def test_delete_config_local_not_found(backend: LocalBackend) -> None:
    result = await delete_config_local(backend, 'ghost')
    assert result == {'deleted': False, 'config_id': 'ghost'}


# ---------------------------------------------------------------------------
# run_saved_config_local
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_saved_config_local_image_mode(backend: LocalBackend) -> None:
    await save_config_local(
        backend,
        'run-test',
        'keboola.ex-http',
        'Run Test',
        {'url': 'https://example.com'},
        component_image='keboola/ex-http:latest',
    )
    expected = ComponentRunResult(status='success', exit_code=0)
    with patch.object(backend, 'run_docker_component', return_value=expected):
        result = await run_saved_config_local(backend, 'run-test')

    assert result.status == 'success'


@pytest.mark.asyncio
async def test_run_saved_config_local_not_found_raises(backend: LocalBackend) -> None:
    with pytest.raises(FileNotFoundError):
        await run_saved_config_local(backend, 'no-such-config')


@pytest.mark.asyncio
async def test_run_saved_config_local_no_runner_raises(backend: LocalBackend) -> None:
    await save_config_local(backend, 'bad-cfg', 'keboola.ex-http', 'Bad', {}, component_image=None, git_url=None)
    with pytest.raises(ValueError, match='neither component_image nor git_url'):
        await run_saved_config_local(backend, 'bad-cfg')
