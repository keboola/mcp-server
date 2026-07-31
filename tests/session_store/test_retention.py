from datetime import date, datetime, timedelta, timezone

import asyncpg
import pytest
import pytest_asyncio

from keboola_mcp_server.session_store.migrator import apply_migrations
from keboola_mcp_server.session_store.retention import _add_months, _month_start, ensure_partitions
from tests.session_store.conftest import TEST_DSN, requires_postgres


@pytest.mark.parametrize(
    ('start', 'n', 'expected'),
    [
        (date(2026, 7, 15), 0, date(2026, 7, 1)),
        (date(2026, 7, 1), 1, date(2026, 8, 1)),
        (date(2026, 12, 1), 1, date(2027, 1, 1)),
        (date(2026, 7, 1), -2, date(2026, 5, 1)),
        (date(2026, 1, 1), -2, date(2025, 11, 1)),
    ],
)
def test_add_months(start: date, n: int, expected: date) -> None:
    assert _add_months(_month_start(start), n) == expected


@pytest.mark.asyncio
@requires_postgres
class TestEnsurePartitions:
    @pytest_asyncio.fixture(autouse=True)
    async def _clean_slate(self):
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            await pool.execute('DROP TABLE IF EXISTS oauth_sessions, schema_migrations CASCADE')
            await apply_migrations(pool)
        finally:
            await pool.close()

    @staticmethod
    async def _existing_partitions(pool: asyncpg.Pool) -> set[str]:
        rows = await pool.fetch(
            "SELECT tablename FROM pg_tables WHERE tablename ~ '^oauth_sessions_[0-9]{4}_[0-9]{2}$'"
        )
        return {r['tablename'] for r in rows}

    async def test_is_idempotent(self) -> None:
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            await ensure_partitions(pool)  # first run creates this month's + next month's partition
            before = await self._existing_partitions(pool)
            result = await ensure_partitions(pool)
            assert result == {'created': [], 'dropped': []}
            assert await self._existing_partitions(pool) == before
        finally:
            await pool.close()

    async def test_drops_only_partitions_older_than_retention(self) -> None:
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            this_month = _month_start(date.today())
            stale = _add_months(this_month, -3)
            kept = _add_months(this_month, -1)
            for month_start in (stale, kept):
                name = f'oauth_sessions_{month_start:%Y_%m}'
                end = _add_months(month_start, 1)
                await pool.execute(
                    f"CREATE TABLE {name} PARTITION OF oauth_sessions FOR VALUES FROM ('{month_start}') TO ('{end}')"
                )

            result = await ensure_partitions(pool, retention_months=2)

            assert result['dropped'] == [f'oauth_sessions_{stale:%Y_%m}']
            remaining = await self._existing_partitions(pool)
            assert f'oauth_sessions_{stale:%Y_%m}' not in remaining
            assert f'oauth_sessions_{kept:%Y_%m}' in remaining
        finally:
            await pool.close()

    async def test_drops_partition_exactly_retention_months_old(self) -> None:
        # Regression test: retention_months=2 must keep exactly 2 months (this + previous), so a
        # partition dated retention_months back (2 months old) is dropped, not kept.
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            this_month = _month_start(date.today())
            boundary = _add_months(this_month, -2)
            name = f'oauth_sessions_{boundary:%Y_%m}'
            end = _add_months(boundary, 1)
            await pool.execute(
                f"CREATE TABLE {name} PARTITION OF oauth_sessions FOR VALUES FROM ('{boundary}') TO ('{end}')"
            )

            result = await ensure_partitions(pool, retention_months=2)

            assert name in result['dropped']
            assert name not in await self._existing_partitions(pool)
        finally:
            await pool.close()

    async def test_creates_partition_when_default_has_overlapping_rows(self) -> None:
        # Regression test: a plain `CREATE TABLE ... PARTITION OF` fails with a CheckViolationError
        # if oauth_sessions_default already holds rows in the new partition's range -- e.g. the
        # one-time backlog copied over by migration 0002 on a stack with pre-existing sessions.
        # ensure_partitions() must move those rows into the new partition instead of erroring.
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            for name in await self._existing_partitions(pool):
                await pool.execute(f'DROP TABLE {name}')

            this_month = _month_start(date.today())
            mid_month = datetime(this_month.year, this_month.month, this_month.day, tzinfo=timezone.utc) + timedelta(
                days=1
            )
            await pool.execute(
                'INSERT INTO oauth_sessions_default '
                '(access_token_hash, client_id, kbc_access_token_enc, kbc_refresh_token_enc, '
                'kbc_access_expires_at, created_at) '
                "VALUES ($1, 'client', $2, $3, now() + interval '1 hour', $4)",
                b'token-hash',
                b'enc-access',
                b'enc-refresh',
                mid_month,
            )

            result = await ensure_partitions(pool)

            this_month_partition = f'oauth_sessions_{this_month:%Y_%m}'
            assert this_month_partition in result['created']
            row = await pool.fetchrow(f'SELECT client_id FROM {this_month_partition}')
            assert row['client_id'] == 'client'
            default_count = await pool.fetchval('SELECT count(*) FROM oauth_sessions_default')
            assert default_count == 0
        finally:
            await pool.close()

    async def test_created_partition_rejects_duplicate_access_token_hash(self) -> None:
        # Regression test: the parent's composite index doesn't reject a duplicate hash (created_at
        # differs per row) -- the plain index ensure_partitions() adds on the partition itself must.
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            for name in await self._existing_partitions(pool):
                await pool.execute(f'DROP TABLE {name}')

            result = await ensure_partitions(pool)
            this_month_partition = f'oauth_sessions_{_month_start(date.today()):%Y_%m}'
            assert this_month_partition in result['created']

            insert = (
                f'INSERT INTO {this_month_partition} '
                '(access_token_hash, client_id, kbc_access_token_enc, kbc_refresh_token_enc, kbc_access_expires_at) '
                "VALUES ($1, 'client', $2, $3, now())"
            )
            await pool.execute(insert, b'dup-hash', b'enc-access', b'enc-refresh')
            with pytest.raises(asyncpg.UniqueViolationError):
                await pool.execute(insert, b'dup-hash', b'enc-access', b'enc-refresh')
        finally:
            await pool.close()

    async def test_creates_missing_current_and_next_month(self) -> None:
        pool = await asyncpg.create_pool(TEST_DSN)
        try:
            # Simulate a fresh table with no partitions ensured yet.
            for name in await self._existing_partitions(pool):
                await pool.execute(f'DROP TABLE {name}')

            result = await ensure_partitions(pool)

            this_month = _month_start(date.today())
            next_month = _add_months(this_month, 1)
            expected = {f'oauth_sessions_{this_month:%Y_%m}', f'oauth_sessions_{next_month:%Y_%m}'}
            assert set(result['created']) == expected
            assert await self._existing_partitions(pool) == expected
        finally:
            await pool.close()
