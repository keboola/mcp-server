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
        await pool.execute('DROP TABLE IF EXISTS oauth_sessions, kai_sessions, schema_migrations CASCADE')
    finally:
        await pool.close()


async def test_applies_migrations_once() -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        applied = await apply_migrations(pool)
        assert applied == [
            '0001_oauth_sessions.sql',
            '0002_partition_oauth_sessions.sql',
            '0003_default_partition_unique_indexes.sql',
            '0004_kai_sessions.sql',
        ]

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


async def test_partitions_table_with_default_catch_all() -> None:
    # Migration 0002 only creates the structure + a DEFAULT catch-all partition -- creating this
    # month's/next month's partition is the `migrate` CLI's job (it calls
    # session_store.retention.ensure_partitions() right after this), not the migration's. One
    # Python-side mechanism for partition creation instead of duplicating it here in SQL too.
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await apply_migrations(pool)
        is_partitioned = await pool.fetchval("SELECT relkind = 'p' FROM pg_class WHERE relname = 'oauth_sessions'")
        assert is_partitioned is True

        tables = {
            r['tablename']
            for r in await pool.fetch("SELECT tablename FROM pg_tables WHERE tablename LIKE 'oauth_sessions%'")
        }
        assert tables == {'oauth_sessions', 'oauth_sessions_default'}
    finally:
        await pool.close()


async def test_creates_kai_sessions_table() -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await apply_migrations(pool)
        columns = await pool.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'kai_sessions'"
        )
        names = {r['column_name'] for r in columns}
        assert {'session_key', 'project_ids', 'read_only', 'confirmed'} <= names
    finally:
        await pool.close()


async def test_default_partition_rejects_duplicate_access_token_hash() -> None:
    # Regression test: the parent's (access_token_hash, created_at) index alone doesn't reject a
    # duplicate hash (created_at differs per row) -- migration 0003's plain index on the
    # partition table itself must.
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await apply_migrations(pool)
        insert = (
            'INSERT INTO oauth_sessions_default '
            '(access_token_hash, client_id, kbc_access_token_enc, kbc_refresh_token_enc, kbc_access_expires_at) '
            "VALUES ($1, 'client', $2, $3, now())"
        )
        await pool.execute(insert, b'dup-hash', b'enc-access', b'enc-refresh')
        with pytest.raises(asyncpg.UniqueViolationError):
            await pool.execute(insert, b'dup-hash', b'enc-access', b'enc-refresh')
    finally:
        await pool.close()
