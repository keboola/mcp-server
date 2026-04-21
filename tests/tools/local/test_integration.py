"""End-to-end integration tests for the local-backend server.

These tests exercise complete user workflows without mocking the filesystem or
DuckDB — only Docker subprocess calls and HTTP calls are mocked.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server.tools.local.backend import LocalBackend
from keboola_mcp_server.tools.local.config import ComponentConfig
from keboola_mcp_server.tools.local.docker import ComponentRunResult
from keboola_mcp_server.tools.local.tools import (
    delete_config_local,
    delete_table_local,
    get_buckets_local,
    get_project_info_local,
    get_tables_local,
    list_configs_local,
    migrate_to_keboola_local,
    query_data_local,
    run_saved_config_local,
    save_config_local,
    search_local,
    write_table_local,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def backend(tmp_path: Path) -> LocalBackend:
    return LocalBackend(data_dir=str(tmp_path))


# ---------------------------------------------------------------------------
# Tool registration via create_local_server
# ---------------------------------------------------------------------------

EXPECTED_TOOLS = {
    'get_tables',
    'get_buckets',
    'query_data',
    'search',
    'get_project_info',
    'setup_component',
    'run_component',
    'get_component_schema',
    'find_component_id',
    'write_table',
    'delete_table',
    'save_config',
    'get_configs',
    'delete_config',
    'run_saved_config',
    'migrate_to_keboola',
    'create_data_app',
    'run_data_app',
    'list_data_apps',
    'stop_data_app',
    'delete_data_app',
}


@pytest.mark.asyncio
async def test_local_server_registers_all_tools(tmp_path: Path) -> None:
    from keboola_mcp_server.server import create_local_server

    server = create_local_server(str(tmp_path))
    registered = set((await server._tool_manager.get_tools()).keys())
    assert EXPECTED_TOOLS == registered


# ---------------------------------------------------------------------------
# Complete data workflow: write → query → project_info → search → delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_complete_data_workflow(backend: LocalBackend) -> None:
    # 1. Write two tables
    t1 = await write_table_local(backend, 'customers', 'id,name,city\n1,Alice,NY\n2,Bob,LA\n')
    t2 = await write_table_local(backend, 'orders', 'order_id,customer_id,amount\n1,1,100\n2,2,50\n3,1,75\n')
    assert t1.name == 'customers'
    assert t2.rows_count == 3

    # 2. Query with aggregation
    sql = (
        'SELECT c.name, SUM(o.amount) AS total '
        'FROM customers c JOIN orders o ON c.id = o.customer_id '
        'GROUP BY c.name ORDER BY total DESC'
    )
    result = await query_data_local(backend, sql, 'Customer Totals')
    assert 'Alice' in result
    assert '175' in result

    # 3. Project info reflects both tables
    info = await get_project_info_local(backend)
    assert info.table_count == 2
    assert info.mode == 'local'

    # 4. Buckets show both tables
    buckets = await get_buckets_local(backend)
    assert buckets.buckets[0].tables_count == 2
    assert set(buckets.buckets[0].table_names) == {'customers', 'orders'}

    # 5. Search finds by filename and column
    search_file = await search_local(backend, 'custom')
    assert any(r.match_type == 'filename' for r in search_file.results)

    search_col = await search_local(backend, 'amount')
    assert any(r.match_type == 'column' for r in search_col.results)

    # 6. Delete one table
    del_result = await delete_table_local(backend, 'orders')
    assert del_result['deleted'] is True

    tables_after = await get_tables_local(backend)
    assert tables_after.total == 1
    assert tables_after.tables[0].name == 'customers'


# ---------------------------------------------------------------------------
# Complete config workflow: save → list → run (mocked) → delete
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_complete_config_workflow(backend: LocalBackend) -> None:
    # 1. Save two configs
    cfg1 = await save_config_local(
        backend,
        config_id='ex-http-001',
        component_id='keboola.ex-http',
        name='HTTP Extractor',
        parameters={'url': 'https://api.example.com', 'method': 'GET'},
        component_image='keboola/ex-http:latest',
    )
    assert isinstance(cfg1, ComponentConfig)
    assert cfg1.created_at != ''

    await save_config_local(
        backend,
        config_id='ex-ftp-001',
        component_id='keboola.ex-ftp',
        name='FTP Extractor',
        parameters={'host': 'ftp.example.com'},
        component_image='keboola/ex-ftp:latest',
    )

    # 2. List — both visible
    listing = await list_configs_local(backend)
    assert listing.total == 2
    ids = {c.config_id for c in listing.configs}
    assert ids == {'ex-http-001', 'ex-ftp-001'}

    # 3. Project info counts configs
    info = await get_project_info_local(backend)
    assert info.config_count == 2

    # 4. Run saved config (Docker mocked)
    expected_run = ComponentRunResult(status='success', exit_code=0, output_tables=['result'])
    with patch.object(backend, 'run_docker_component', return_value=expected_run):
        run_result = await run_saved_config_local(backend, 'ex-http-001')
    assert run_result.status == 'success'
    assert 'result' in run_result.output_tables

    # 5. Delete one config
    del_result = await delete_config_local(backend, 'ex-ftp-001')
    assert del_result['deleted'] is True

    listing_after = await list_configs_local(backend)
    assert listing_after.total == 1
    assert listing_after.configs[0].config_id == 'ex-http-001'


# ---------------------------------------------------------------------------
# Complete migration workflow: write + save → migrate (HTTP mocked)
# ---------------------------------------------------------------------------


def _make_resp(status_code: int, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        from httpx import HTTPStatusError

        resp.raise_for_status.side_effect = HTTPStatusError(
            f'HTTP {status_code}', request=MagicMock(), response=MagicMock()
        )
    return resp


@pytest.mark.asyncio
async def test_complete_migration_workflow(backend: LocalBackend) -> None:
    # Prepare local data
    await write_table_local(backend, 'products', 'id,name,price\n1,Widget,9.99\n2,Gadget,24.99\n')
    await save_config_local(
        backend,
        config_id='extractor-001',
        component_id='keboola.ex-http',
        name='Product API',
        parameters={'url': 'https://api.example.com/products'},
    )

    responses = [
        _make_resp(201, {}),  # ensure_bucket
        _make_resp(201, {'id': 'in.c-local.products'}),  # upload table
        _make_resp(201, {'id': '555'}),  # create config
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        instance = AsyncMock()
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        instance.post = AsyncMock(side_effect=responses)

        result = await migrate_to_keboola_local(
            backend,
            storage_api_url='https://connection.keboola.com',
            storage_token='test-token',
        )

    assert result.tables_ok == 1
    assert result.configs_ok == 1
    assert result.tables_error == 0
    assert result.configs_error == 0
    assert result.tables[0].name == 'products'
    assert result.configs[0].config_id == 'extractor-001'
    assert result.configs[0].kbc_config_id == '555'
