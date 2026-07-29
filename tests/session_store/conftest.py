import os

import asyncpg
import pytest
import pytest_asyncio

from keboola_mcp_server.session_store.crypto import KEY_SIZE
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
        # Clean slate per test: drop and let apply_migrations (inside connect) recreate.
        await pool.execute('DROP TABLE IF EXISTS oauth_sessions, schema_migrations CASCADE')
    finally:
        await pool.close()
    s = await PostgresSessionStore.connect(TEST_DSN, encryption_key=bytes([1] * KEY_SIZE))
    try:
        yield s
    finally:
        await s.close()
