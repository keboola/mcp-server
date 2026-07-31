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
        assert applied == ['0001_oauth_sessions.sql', '0002_partition_oauth_sessions.sql']

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


async def test_partitions_by_month_with_current_and_next_ready() -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await apply_migrations(pool)
        is_partitioned = await pool.fetchval("SELECT relkind = 'p' FROM pg_class WHERE relname = 'oauth_sessions'")
        assert is_partitioned is True

        tables = {
            r['tablename']
            for r in await pool.fetch("SELECT tablename FROM pg_tables WHERE tablename LIKE 'oauth_sessions%'")
        }
        # This month's + next month's partition exist immediately, plus the DEFAULT catch-all --
        # writes never fail for lack of a partition even before the monthly gc-sessions job runs.
        assert 'oauth_sessions_default' in tables
        assert sum(1 for t in tables if t not in ('oauth_sessions', 'oauth_sessions_default')) == 2
    finally:
        await pool.close()
