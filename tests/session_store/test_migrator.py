import asyncpg
import pytest
import pytest_asyncio

from keboola_mcp_server.session_store.migrator import apply_migrations
from tests.session_store.conftest import TEST_DSN, requires_postgres

pytestmark = [pytest.mark.asyncio, requires_postgres]


@pytest_asyncio.fixture(autouse=True)
async def _clean_slate():
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await pool.execute('DROP TABLE IF EXISTS oauth_sessions, schema_migrations CASCADE')
    finally:
        await pool.close()


async def test_applies_migrations_once() -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        applied = await apply_migrations(pool)
        assert applied == ['0001_oauth_sessions.sql']

        # Re-running is a no-op -- the table already exists, so re-applying the DDL would fail
        # if the tracking table didn't correctly skip it.
        applied_again = await apply_migrations(pool)
        assert applied_again == []
    finally:
        await pool.close()


async def test_creates_oauth_sessions_table() -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await apply_migrations(pool)
        columns = await pool.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'oauth_sessions'"
        )
        names = {r['column_name'] for r in columns}
        assert {'access_token_hash', 'kbc_access_token_enc', 'scope_project_ids', 'revoked_at'} <= names
    finally:
        await pool.close()
