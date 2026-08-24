"""Tiny numbered-SQL-file migration runner.

One table doesn't earn a migration framework (alembic, etc.) -- this is the whole mechanism:
numbered ``.sql`` files applied in order, tracked in ``schema_migrations`` so re-running is a
no-op. Not general-purpose (no down-migrations, no branching) by design.
"""

import logging
from importlib import resources
from typing import cast

import asyncpg

LOG = logging.getLogger(__name__)

_CREATE_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _migration_files() -> list[tuple[str, str]]:
    """Returns (filename, sql) pairs for every ``*.sql`` file in this package's migrations/ dir,
    sorted by filename -- the numeric prefix (``0001_...``) is what defines application order."""
    migrations_dir = resources.files(__package__) / 'migrations'
    files = sorted(p for p in migrations_dir.iterdir() if p.name.endswith('.sql'))
    return [(p.name, p.read_text()) for p in files]


async def apply_migrations(pool: asyncpg.Pool) -> list[str]:
    """Applies every not-yet-applied migration file, in order, each in its own transaction.

    :return: filenames actually applied (empty if the schema was already up to date).
    """
    applied: list[str] = []
    async with pool.acquire() as conn:
        conn = cast(asyncpg.Connection, conn)
        await conn.execute(_CREATE_TRACKING_TABLE)
        already_applied = {r['filename'] for r in await conn.fetch('SELECT filename FROM schema_migrations')}
        for filename, sql in _migration_files():
            if filename in already_applied:
                continue
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute('INSERT INTO schema_migrations (filename) VALUES ($1)', filename)
            LOG.info(f'Applied migration: {filename}')
            applied.append(filename)
    return applied
