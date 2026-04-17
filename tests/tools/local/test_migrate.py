"""Tests for migrate.py — migrate_to_keboola (httpx mocked)."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from keboola_mcp_server.tools.local.config import ComponentConfig
from keboola_mcp_server.tools.local.migrate import migrate_to_keboola

API_URL = 'https://connection.keboola.com'
TOKEN = 'test-token-123'


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


def _setup_client(mock_cls, post_responses: list) -> AsyncMock:
    """Configure AsyncClient mock with sequential post responses."""
    instance = AsyncMock()
    mock_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
    mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
    instance.post = AsyncMock(side_effect=post_responses)
    return instance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_csv(tables_dir: Path, name: str, content: str = 'id,val\n1,a\n') -> None:
    tables_dir.mkdir(parents=True, exist_ok=True)
    (tables_dir / f'{name}.csv').write_text(content, encoding='utf-8')


def _write_config(configs_dir: Path, cfg: ComponentConfig) -> None:
    from keboola_mcp_server.tools.local.config import save_config

    configs_dir.mkdir(parents=True, exist_ok=True)
    save_config(configs_dir, cfg)


# ---------------------------------------------------------------------------
# upload tables
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_uploads_tables(tmp_path: Path) -> None:
    _write_csv(tmp_path / 'tables', 'customers')
    _write_csv(tmp_path / 'tables', 'orders')

    responses = [
        _make_resp(201, {}),  # ensure_bucket
        _make_resp(201, {'id': 'in.c-local.customers'}),  # upload customers
        _make_resp(201, {'id': 'in.c-local.orders'}),  # upload orders
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        _setup_client(mock_cls, responses)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN)

    assert result.tables_ok == 2
    assert result.tables_error == 0
    assert {t.name for t in result.tables} == {'customers', 'orders'}
    assert all(t.status == 'uploaded' for t in result.tables)


@pytest.mark.asyncio
async def test_migrate_already_exists_is_ok(tmp_path: Path) -> None:
    _write_csv(tmp_path / 'tables', 'existing')

    responses = [
        _make_resp(201, {}),
        _make_resp(422, {'error': 'already exists'}),
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        _setup_client(mock_cls, responses)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN)

    assert result.tables_ok == 1  # already_exists counts as ok
    assert result.tables_error == 0
    assert result.tables[0].status == 'already_exists'


@pytest.mark.asyncio
async def test_migrate_table_error_captured(tmp_path: Path) -> None:
    _write_csv(tmp_path / 'tables', 'broken')

    responses = [
        _make_resp(201, {}),
        _make_resp(500),  # server error → raise_for_status fires
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        _setup_client(mock_cls, responses)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN)

    assert result.tables_error == 1
    assert result.tables[0].status == 'error'


@pytest.mark.asyncio
async def test_migrate_table_names_filter(tmp_path: Path) -> None:
    _write_csv(tmp_path / 'tables', 'alpha')
    _write_csv(tmp_path / 'tables', 'beta')
    _write_csv(tmp_path / 'tables', 'gamma')

    responses = [
        _make_resp(201, {}),
        _make_resp(201, {'id': 'in.c-local.alpha'}),
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        _setup_client(mock_cls, responses)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN, table_names=['alpha'])

    assert len(result.tables) == 1
    assert result.tables[0].name == 'alpha'


@pytest.mark.asyncio
async def test_migrate_no_tables(tmp_path: Path) -> None:
    (tmp_path / 'tables').mkdir()

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        instance = AsyncMock()
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        instance.post = AsyncMock()
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN)

    # no HTTP calls at all — nothing to migrate
    instance.post.assert_not_called()
    assert result.tables == []
    assert result.tables_ok == 0


# ---------------------------------------------------------------------------
# upload configs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_creates_configs(tmp_path: Path) -> None:
    cfg = ComponentConfig(
        config_id='ex-http-001',
        component_id='keboola.ex-http',
        name='My HTTP Extractor',
        parameters={'url': 'https://api.example.com'},
    )
    _write_config(tmp_path / 'configs', cfg)

    responses = [
        _make_resp(201, {}),  # bucket
        _make_resp(201, {'id': '12345'}),  # create config
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        _setup_client(mock_cls, responses)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN)

    assert result.configs_ok == 1
    assert result.configs[0].status == 'created'
    assert result.configs[0].kbc_config_id == '12345'


@pytest.mark.asyncio
async def test_migrate_config_ids_filter(tmp_path: Path) -> None:
    for cid in ('cfg-a', 'cfg-b', 'cfg-c'):
        _write_config(
            tmp_path / 'configs',
            ComponentConfig(config_id=cid, component_id='keboola.ex-http', name=cid, parameters={}),
        )

    responses = [
        _make_resp(201, {}),  # bucket
        _make_resp(201, {'id': '1'}),  # only cfg-a
    ]

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        _setup_client(mock_cls, responses)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN, config_ids=['cfg-a'])

    assert len(result.configs) == 1
    assert result.configs[0].config_id == 'cfg-a'


# ---------------------------------------------------------------------------
# bucket creation failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_bucket_error_marks_all_as_error(tmp_path: Path) -> None:
    _write_csv(tmp_path / 'tables', 'customers')

    from httpx import HTTPStatusError

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        instance = AsyncMock()
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        instance.post = AsyncMock(side_effect=HTTPStatusError('403', request=MagicMock(), response=MagicMock()))
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN)

    assert result.tables_error == 1
    assert result.tables[0].status == 'error'
    assert 'Bucket error' in (result.tables[0].message or '')


# ---------------------------------------------------------------------------
# custom bucket_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_migrate_custom_bucket_id(tmp_path: Path) -> None:
    _write_csv(tmp_path / 'tables', 'data')

    captured_calls: list = []

    async def record_post(url, **kwargs):
        captured_calls.append(url)
        if 'buckets' in url and 'tables' not in url:
            return _make_resp(201, {})
        return _make_resp(201, {'id': 'in.c-custom.data'})

    with patch('keboola_mcp_server.tools.local.migrate.httpx.AsyncClient') as mock_cls:
        instance = AsyncMock()
        mock_cls.return_value.__aenter__ = AsyncMock(return_value=instance)
        mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
        instance.post = AsyncMock(side_effect=record_post)
        result = await migrate_to_keboola(tmp_path, API_URL, TOKEN, bucket_id='in.c-custom')

    assert result.bucket_id == 'in.c-custom'
    assert any('in.c-custom/tables' in url for url in captured_calls)
