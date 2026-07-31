from datetime import date

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
