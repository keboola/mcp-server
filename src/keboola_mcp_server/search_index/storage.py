"""Filesystem layout, locking, and schema bootstrap for the search index.

Path layout (per RFC ``feature_spec/search_index/RFC.md``)::

    <root>/<project_id>/<token_hash>/default.db

``root`` defaults to ``$KBC_SEARCH_INDEX_DIR`` or ``~/.cache/keboola-mcp``.
All directories are created with mode ``0o700`` and DB files with ``0o600``.
"""

import fcntl
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from keboola_mcp_server.search_index.types import VerifiedSession

LOG = logging.getLogger(__name__)

DEFAULT_TTL_SECONDS: int = 30 * 60
SCHEMA_VERSION: str = '1'

_ENV_ROOT_VAR: str = 'KBC_SEARCH_INDEX_DIR'
_DEFAULT_DB_NAME: str = 'default.db'
_DIR_MODE: int = 0o700
_FILE_MODE: int = 0o600

_CREATE_STATEMENTS: tuple[str, ...] = (
    """
    CREATE VIRTUAL TABLE search USING fts5(
        project_id UNINDEXED,
        kind UNINDEXED,
        obj_id UNINDEXED,
        name,
        description,
        content,
        metadata UNINDEXED,
        tokenize='porter unicode61'
    )
    """,
    """
    CREATE TABLE meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,
)


def default_root() -> Path:
    """Return the cache root, honouring ``$KBC_SEARCH_INDEX_DIR``."""
    env = os.environ.get(_ENV_ROOT_VAR)
    if env:
        return Path(env)
    return Path.home() / '.cache' / 'keboola-mcp'


def path_for(session: VerifiedSession, root: Path | None = None) -> Path:
    """Compute the DB file path for ``session``.

    ``session.project_id`` and ``session.token_hash`` were validated in
    ``VerifiedSession.__post_init__`` against ``^[A-Za-z0-9_-]+$`` and
    ``^[a-f0-9]{16}$`` respectively, so they are safe path components.
    """
    base = (root or default_root()).resolve()
    return base / session.project_id / session.token_hash / _DEFAULT_DB_NAME


def tmp_path_for(db_path: Path) -> Path:
    """Path used as the destination of an in-progress build."""
    return db_path.with_name(db_path.name + '.tmp')


def ensure_parent_dirs(db_path: Path, root: Path | None = None) -> None:
    """Create the per-project and per-token directories with ``0o700`` perms.

    ``mkdir`` respects the process umask, so each directory is re-``chmod``'d to
    guarantee the documented mode regardless of the calling environment.
    """
    base = (root or default_root()).resolve()
    project_dir = db_path.parent.parent
    token_dir = db_path.parent
    for d in (base, project_dir, token_dir):
        d.mkdir(parents=True, exist_ok=True)
    project_dir.chmod(_DIR_MODE)
    token_dir.chmod(_DIR_MODE)


def is_stale(db_path: Path, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> bool:
    """A missing or aged-out DB is stale; a fresh DB is not."""
    if not db_path.exists():
        return True
    age = time.time() - db_path.stat().st_mtime
    return age >= ttl_seconds


@contextmanager
def file_lock(lock_path: Path, *, exclusive: bool = True) -> Iterator[None]:
    """Acquire an OS-level advisory lock on ``lock_path``.

    Used to dedup concurrent rebuilds across processes. Within a single process
    the asyncio-level lock in ``lifecycle.py`` is the first line of dedup; this
    is the cross-process safety net.
    """
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.parent.chmod(_DIR_MODE)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, _FILE_MODE)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def atomic_publish(tmp_path: Path, final_path: Path) -> None:
    """Atomically replace ``final_path`` with ``tmp_path`` and tighten perms."""
    os.replace(tmp_path, final_path)
    final_path.chmod(_FILE_MODE)


def init_schema(conn: sqlite3.Connection, session: VerifiedSession) -> None:
    """Create FTS5 + meta tables and stamp the session identity into ``meta``."""
    for stmt in _CREATE_STATEMENTS:
        conn.execute(stmt)
    conn.executemany(
        'INSERT INTO meta (key, value) VALUES (?, ?)',
        [
            ('project_id', session.project_id),
            ('token_hash', session.token_hash),
            ('schema_version', SCHEMA_VERSION),
            ('built_at_iso', datetime.now(timezone.utc).isoformat()),
        ],
    )
    conn.commit()
