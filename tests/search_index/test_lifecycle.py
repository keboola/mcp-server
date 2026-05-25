"""Lifecycle: build scheduling, dedup, cold-start await semantics."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.search_index import lifecycle, query, storage
from keboola_mcp_server.search_index.lifecycle import IndexUnavailable, ensure_index_built, query_or_wait
from keboola_mcp_server.search_index.types import VerifiedSession

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _reset_state():
    lifecycle._reset_for_tests()
    yield
    lifecycle._reset_for_tests()


@pytest.fixture
def session() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def client(mocker) -> KeboolaClient:
    return mocker.AsyncMock(KeboolaClient)


def _fake_build_factory(tmp_path: Path, *, delay: float = 0.0, fail: bool = False):
    async def fake_build(session: VerifiedSession, _client, *, root=None):
        if delay:
            await asyncio.sleep(delay)
        if fail:
            raise RuntimeError('build failed for test')
        db_path = storage.path_for(session, root=tmp_path)
        storage.ensure_parent_dirs(db_path, root=tmp_path)
        # Empty FTS5 schema is enough — tests don't query rows here.
        import sqlite3

        conn = sqlite3.connect(db_path)
        try:
            storage.init_schema(conn, session)
        finally:
            conn.close()
        return db_path

    return fake_build


@pytest.mark.asyncio
async def test_ensure_index_built_schedules_one_task_when_concurrent(session, client, tmp_path, monkeypatch):
    fake = AsyncMock(side_effect=_fake_build_factory(tmp_path))
    monkeypatch.setattr(lifecycle.builder, 'build_index', fake)

    await asyncio.gather(
        ensure_index_built(session, client, root=tmp_path),
        ensure_index_built(session, client, root=tmp_path),
        ensure_index_built(session, client, root=tmp_path),
    )
    # Allow the scheduled task to complete.
    state = lifecycle._builds[(session.project_id, session.token_hash)]
    if state.task:
        await state.task

    fake.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_index_built_skips_when_fresh_db_exists(session, client, tmp_path, monkeypatch):
    db_path = storage.path_for(session, root=tmp_path)
    storage.ensure_parent_dirs(db_path, root=tmp_path)
    db_path.write_bytes(b'')  # fresh mtime

    fake = AsyncMock(side_effect=_fake_build_factory(tmp_path))
    monkeypatch.setattr(lifecycle.builder, 'build_index', fake)

    await ensure_index_built(session, client, root=tmp_path)
    fake.assert_not_called()


@pytest.mark.asyncio
async def test_query_or_wait_awaits_cold_start(session, client, tmp_path, monkeypatch):
    fake = AsyncMock(side_effect=_fake_build_factory(tmp_path, delay=0.05))
    monkeypatch.setattr(lifecycle.builder, 'build_index', fake)
    fake_query = AsyncMock(return_value=[query.IndexedHit(kind='bucket', obj_id='x', name='x', description='')])
    monkeypatch.setattr(lifecycle.query, 'run_query', lambda **kwargs: fake_query.return_value)

    await ensure_index_built(session, client, root=tmp_path)
    hits = await query_or_wait(session, patterns=['x'], root=tmp_path)
    assert hits
    assert hits[0].obj_id == 'x'


@pytest.mark.asyncio
async def test_query_or_wait_raises_when_no_index_and_no_build(session, tmp_path):
    with pytest.raises(IndexUnavailable):
        await query_or_wait(session, patterns=['x'], root=tmp_path)


@pytest.mark.asyncio
async def test_query_or_wait_raises_when_cold_start_build_fails(session, client, tmp_path, monkeypatch):
    fake = AsyncMock(side_effect=_fake_build_factory(tmp_path, fail=True))
    monkeypatch.setattr(lifecycle.builder, 'build_index', fake)

    await ensure_index_built(session, client, root=tmp_path)
    with pytest.raises(IndexUnavailable, match='Cold-start build failed'):
        await query_or_wait(session, patterns=['x'], root=tmp_path)
