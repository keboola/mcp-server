"""Admin MCP tools: get_search_index_status and rebuild_search_index."""

import sqlite3
from datetime import datetime, timezone

import pytest

from keboola_mcp_server.search_index import storage
from keboola_mcp_server.search_index.types import VerifiedSession
from keboola_mcp_server.search_index.verify import VERIFIED_SESSION_STATE_KEY
from keboola_mcp_server.tools.search_index_admin import (
    get_search_index_status,
    rebuild_search_index,
)

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def verified() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def ctx_with_session(mocker, verified, tmp_path, monkeypatch):
    monkeypatch.setenv('KBC_SEARCH_INDEX_DIR', str(tmp_path))
    ctx = mocker.MagicMock()
    ctx.session.state = {VERIFIED_SESSION_STATE_KEY: verified}
    return ctx


@pytest.fixture
def ctx_no_session(mocker):
    ctx = mocker.MagicMock()
    ctx.session.state = {}
    return ctx


@pytest.mark.asyncio
async def test_status_reports_disabled_when_no_verified_session(ctx_no_session):
    status = await get_search_index_status(ctx_no_session)
    assert status.enabled is False
    assert status.reason
    assert status.project_id is None


@pytest.mark.asyncio
async def test_status_reports_not_built_when_missing_db(ctx_with_session, verified):
    status = await get_search_index_status(ctx_with_session)
    assert status.enabled is True
    assert status.project_id == verified.project_id
    assert status.exists is False
    assert status.reason


@pytest.mark.asyncio
async def test_status_reports_built_index(ctx_with_session, verified, tmp_path):
    db_path = storage.path_for(verified, root=tmp_path)
    storage.ensure_parent_dirs(db_path, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        storage.init_schema(conn, verified)
        conn.execute(
            'INSERT INTO search (project_id, kind, obj_id, name, description, content, metadata) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (verified.project_id, 'bucket', 'in.c-x', 'x', '', 'x', '{}'),
        )
        conn.commit()
    finally:
        conn.close()

    status = await get_search_index_status(ctx_with_session)
    assert status.enabled is True
    assert status.exists is True
    assert status.row_counts == {'bucket': 1}
    assert status.schema_version == storage.SCHEMA_VERSION
    assert status.built_at_iso
    assert status.size_bytes is not None
    assert status.size_bytes > 0


@pytest.mark.asyncio
async def test_rebuild_reports_disabled_when_no_verified_session(ctx_no_session):
    status = await rebuild_search_index(ctx_no_session)
    assert status.enabled is False


@pytest.mark.asyncio
async def test_rebuild_invokes_build_index(ctx_with_session, verified, mocker, tmp_path, monkeypatch):
    # Place a sentinel DB so the admin tool has something to read after rebuild.
    db_path = storage.path_for(verified, root=tmp_path)
    storage.ensure_parent_dirs(db_path, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        storage.init_schema(conn, verified)
    finally:
        conn.close()

    # Stub out KeboolaClient.from_state and build_index so we don't touch any APIs.
    fake_client = mocker.MagicMock()
    monkeypatch.setattr(
        'keboola_mcp_server.tools.search_index_admin.KeboolaClient.from_state',
        lambda _state: fake_client,
    )
    fake_build = mocker.AsyncMock(return_value=db_path)
    monkeypatch.setattr('keboola_mcp_server.tools.search_index_admin.build_index', fake_build)

    status = await rebuild_search_index(ctx_with_session)
    fake_build.assert_awaited_once_with(verified, fake_client)
    assert status.enabled is True
    assert status.exists is True
    assert status.is_stale is False
