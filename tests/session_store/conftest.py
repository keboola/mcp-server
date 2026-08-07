import os

import asyncpg
import pytest
import pytest_asyncio

from keboola_mcp_server.session_store.crypto import KEY_SIZE
from keboola_mcp_server.session_store.kai_scope import PostgresKaiScopeStore
from keboola_mcp_server.session_store.migrator import apply_migrations
from keboola_mcp_server.session_store.repository import PostgresSessionStore

TEST_DSN = os.environ.get('KBC_TEST_POSTGRES_DSN', 'postgresql://keboola_mcp:keboola_mcp@localhost:5432/keboola_mcp')


def _postgres_available() -> bool:
    import socket
    from urllib.parse import urlparse

    parsed = urlparse(TEST_DSN)
    try:
        with socket.create_connection((parsed.hostname, parsed.port or 5432), timeout=0.5):
            return True
    except OSError:
        return False


requires_postgres = pytest.mark.skipif(
    not _postgres_available(), reason=f'No Postgres reachable at {TEST_DSN} (see docker-compose.yml)'
)


@pytest_asyncio.fixture
async def store():
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        # Clean slate per test: drop, then re-apply migrations -- standing in for the migration
        # Job that would normally run once, ahead of the app, in a real deployment.
        await pool.execute('DROP TABLE IF EXISTS oauth_sessions, kai_sessions, schema_migrations CASCADE')
        await apply_migrations(pool)
    finally:
        await pool.close()
    s = PostgresSessionStore(TEST_DSN, encryption_key=bytes([1] * KEY_SIZE))
    try:
        yield s
    finally:
        await s.close()


@pytest_asyncio.fixture
async def kai_store():
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await pool.execute('DROP TABLE IF EXISTS oauth_sessions, kai_sessions, schema_migrations CASCADE')
        await apply_migrations(pool)
    finally:
        await pool.close()
    s = PostgresKaiScopeStore(TEST_DSN)
    try:
        yield s
    finally:
        await s.close()
