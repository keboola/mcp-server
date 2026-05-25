import os
import sqlite3
import stat
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from keboola_mcp_server.search_index import storage
from keboola_mcp_server.search_index.types import VerifiedSession

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def session() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / 'cache'


def test_default_root_honours_env_var(monkeypatch, tmp_path):
    custom = tmp_path / 'custom-cache'
    monkeypatch.setenv('KBC_SEARCH_INDEX_DIR', str(custom))
    assert storage.default_root() == custom


def test_default_root_falls_back_to_home_cache(monkeypatch):
    monkeypatch.delenv('KBC_SEARCH_INDEX_DIR', raising=False)
    expected = Path.home() / '.cache' / 'keboola-mcp'
    assert storage.default_root() == expected


def test_path_for_builds_segregated_layout(session, root):
    path = storage.path_for(session, root=root)
    assert path == root.resolve() / '1234' / 'abcdef0123456789' / 'default.db'


def test_path_for_is_contained_within_root(session, root):
    path = storage.path_for(session, root=root)
    # The resolved DB path must remain underneath the resolved root, no traversal.
    assert str(path.resolve()).startswith(str(root.resolve()))


def test_tmp_path_for_appends_suffix(session, root):
    db = storage.path_for(session, root=root)
    tmp = storage.tmp_path_for(db)
    assert tmp.name == 'default.db.tmp'
    assert tmp.parent == db.parent


def test_ensure_parent_dirs_creates_with_0700(session, root):
    db = storage.path_for(session, root=root)
    storage.ensure_parent_dirs(db, root=root)
    assert db.parent.exists()
    assert db.parent.parent.exists()
    assert stat.S_IMODE(db.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(db.parent.parent.stat().st_mode) == 0o700


def test_is_stale_missing_file(session, root):
    db = storage.path_for(session, root=root)
    assert storage.is_stale(db, ttl_seconds=10) is True


def test_is_stale_fresh_file(tmp_path):
    db = tmp_path / 'fresh.db'
    db.write_bytes(b'')
    assert storage.is_stale(db, ttl_seconds=60) is False


def test_is_stale_aged_file(tmp_path):
    db = tmp_path / 'old.db'
    db.write_bytes(b'')
    past = time.time() - 3600
    os.utime(db, (past, past))
    assert storage.is_stale(db, ttl_seconds=60) is True


def test_atomic_publish_replaces_and_sets_perms(session, root):
    db = storage.path_for(session, root=root)
    tmp = storage.tmp_path_for(db)
    storage.ensure_parent_dirs(db, root=root)

    tmp.write_bytes(b'new-content')
    db.write_bytes(b'old-content')

    storage.atomic_publish(tmp, db)

    assert db.read_bytes() == b'new-content'
    assert not tmp.exists()
    assert stat.S_IMODE(db.stat().st_mode) == 0o600


def test_atomic_publish_leaves_old_db_intact_when_no_tmp(session, root):
    db = storage.path_for(session, root=root)
    storage.ensure_parent_dirs(db, root=root)
    db.write_bytes(b'old')

    tmp = storage.tmp_path_for(db)
    with pytest.raises(FileNotFoundError):
        storage.atomic_publish(tmp, db)

    assert db.read_bytes() == b'old'


def test_file_lock_round_trip(session, root):
    db = storage.path_for(session, root=root)
    storage.ensure_parent_dirs(db, root=root)
    lock_path = db.with_suffix('.db.lock')

    with storage.file_lock(lock_path):
        assert lock_path.exists()
        assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600

    # Lock released; re-entering must succeed.
    with storage.file_lock(lock_path):
        pass


def test_init_schema_creates_tables_and_meta(session, root):
    db = storage.path_for(session, root=root)
    storage.ensure_parent_dirs(db, root=root)

    conn = sqlite3.connect(db)
    try:
        storage.init_schema(conn, session)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")
        names = {row[0] for row in cursor.fetchall()}
        assert 'search' in names
        assert 'meta' in names

        meta = dict(conn.execute('SELECT key, value FROM meta').fetchall())
        assert meta['project_id'] == session.project_id
        assert meta['token_hash'] == session.token_hash
        assert meta['schema_version'] == storage.SCHEMA_VERSION
        assert meta['built_at_iso']
    finally:
        conn.close()


def test_init_schema_supports_fts5_match(session, root):
    db = storage.path_for(session, root=root)
    storage.ensure_parent_dirs(db, root=root)

    conn = sqlite3.connect(db)
    try:
        storage.init_schema(conn, session)
        conn.execute(
            'INSERT INTO search (project_id, kind, obj_id, name, description, content, metadata) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            (session.project_id, 'bucket', 'in.c-foo', 'foo', 'Customer bucket', 'foo customer revenue', '{}'),
        )
        conn.commit()

        rows = conn.execute(
            'SELECT obj_id FROM search WHERE search MATCH ? AND project_id = ?',
            ('customer', session.project_id),
        ).fetchall()
        assert rows == [('in.c-foo',)]
    finally:
        conn.close()


def test_defense_in_depth_project_id_filter(session, root):
    """Two rows with different project_ids in one DB; WHERE filter isolates them."""
    db = storage.path_for(session, root=root)
    storage.ensure_parent_dirs(db, root=root)

    other_project = '5678'

    conn = sqlite3.connect(db)
    try:
        storage.init_schema(conn, session)
        conn.executemany(
            'INSERT INTO search (project_id, kind, obj_id, name, description, content, metadata) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                (session.project_id, 'bucket', 'a', 'shared_name', '', 'data', '{}'),
                (other_project, 'bucket', 'b', 'shared_name', '', 'data', '{}'),
            ],
        )
        conn.commit()

        rows = conn.execute(
            'SELECT obj_id FROM search WHERE search MATCH ? AND project_id = ?',
            ('shared_name', session.project_id),
        ).fetchall()
        assert rows == [('a',)]
    finally:
        conn.close()
