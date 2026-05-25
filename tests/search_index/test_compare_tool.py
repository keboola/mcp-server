"""compare_search_paths: A/B diagnostic between FTS5 index and live API."""

import sqlite3
from datetime import datetime, timezone

import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.search_index import storage
from keboola_mcp_server.search_index.types import VerifiedSession
from keboola_mcp_server.search_index.verify import VERIFIED_SESSION_STATE_KEY
from keboola_mcp_server.tools.search_index_admin import compare_search_paths

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def verified() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def ctx_disabled(mocker: MockerFixture):
    ctx = mocker.MagicMock()
    ctx.session.state = {}
    return ctx


@pytest.mark.asyncio
async def test_compare_reports_disabled_when_no_verified_session(ctx_disabled):
    result = await compare_search_paths(ctx_disabled, patterns=['customer'])
    assert result.enabled is False
    assert result.reason
    assert result.project_id is None
    assert result.patterns == ['customer']


@pytest.mark.asyncio
async def test_compare_runs_both_paths_and_reports_timing(verified, mocker, tmp_path, monkeypatch):
    # Build a small index file so the index path returns something deterministic.
    monkeypatch.setenv('KBC_SEARCH_INDEX_DIR', str(tmp_path))
    db_path = storage.path_for(verified, root=tmp_path)
    storage.ensure_parent_dirs(db_path, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        storage.init_schema(conn, verified)
        conn.executemany(
            'INSERT INTO search (project_id, kind, obj_id, name, description, content, metadata) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                (
                    verified.project_id,
                    'bucket',
                    'in.c-customers',
                    'customers',
                    '',
                    'in.c-customers customers Customer master data',
                    '{"updated": "2026-05-20"}',
                ),
                (
                    verified.project_id,
                    'table',
                    'in.c-customers.orders',
                    'orders',
                    '',
                    'in.c-customers.orders orders Order events customer revenue',
                    '{"updated": "2026-05-21"}',
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    # Stub the live fetchers with minimal results.
    from keboola_mcp_server.tools import search as search_mod

    async def fake_fetch_buckets(_client, _spec):
        return [
            search_mod.SearchHit(
                bucket_id='in.c-customers',
                item_type='bucket',
                updated='2026-05-20',
                name='customers',
            )
        ]

    async def fake_fetch_tables(_client, _spec):
        return [
            search_mod.SearchHit(
                table_id='in.c-customers.orders',
                item_type='table',
                updated='2026-05-21',
                name='orders',
            )
        ]

    monkeypatch.setattr(search_mod, '_fetch_buckets', fake_fetch_buckets)
    monkeypatch.setattr(search_mod, '_fetch_tables', fake_fetch_tables)
    monkeypatch.setattr(
        'keboola_mcp_server.tools.search_index_admin.KeboolaClient.from_state',
        lambda _state: mocker.MagicMock(),
    )

    ctx = mocker.MagicMock()
    ctx.session.state = {VERIFIED_SESSION_STATE_KEY: verified}

    result = await compare_search_paths(ctx, patterns=['customer'])

    assert result.enabled is True
    assert result.project_id == verified.project_id
    assert result.reason is None
    assert result.index_hit_count == 2
    assert result.live_hit_count == 2
    assert result.overlap_count == 2
    assert 'in.c-customers' in result.index_obj_ids_sample
    assert 'in.c-customers.orders' in result.index_obj_ids_sample
    assert result.index_duration_ms is not None
    assert result.live_duration_ms is not None
    assert result.speedup is not None


@pytest.mark.asyncio
async def test_compare_reports_index_unavailable(verified, mocker, tmp_path, monkeypatch):
    # No DB file — index path should fail gracefully.
    monkeypatch.setenv('KBC_SEARCH_INDEX_DIR', str(tmp_path))

    from keboola_mcp_server.tools import search as search_mod

    async def fake_fetch_buckets(_client, _spec):
        return []

    async def fake_fetch_tables(_client, _spec):
        return []

    monkeypatch.setattr(search_mod, '_fetch_buckets', fake_fetch_buckets)
    monkeypatch.setattr(search_mod, '_fetch_tables', fake_fetch_tables)
    monkeypatch.setattr(
        'keboola_mcp_server.tools.search_index_admin.KeboolaClient.from_state',
        lambda _state: mocker.MagicMock(),
    )

    ctx = mocker.MagicMock()
    ctx.session.state = {VERIFIED_SESSION_STATE_KEY: verified}

    result = await compare_search_paths(ctx, patterns=['anything'])
    assert result.enabled is True
    assert result.reason is not None
    assert result.index_hit_count == 0
    assert result.speedup is None
