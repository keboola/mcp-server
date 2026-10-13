import asyncio

import asyncpg
import pytest

from keboola_mcp_server.session_store.rate_limit import PostgresRateLimitStore, purge_expired

from .conftest import TEST_DSN, requires_postgres

pytestmark = [requires_postgres, pytest.mark.asyncio]


async def test_hit_counts_events_in_the_window_per_bucket(rate_store: PostgresRateLimitStore) -> None:
    assert [await rate_store.hit('validate', 3600) for _ in range(3)] == [1, 2, 3]
    assert await rate_store.hit('other', 3600) == 1  # another bucket has its own count


async def test_hit_counts_across_concurrent_callers(rate_store: PostgresRateLimitStore) -> None:
    """The point of the shared store: replicas hitting the same bucket at the same time must each get a distinct
    count, or the limit would let more through than it says."""
    counts = await asyncio.gather(*(rate_store.hit('validate', 3600) for _ in range(40)))

    assert sorted(counts) == list(range(1, 41))


async def test_hit_starts_a_fresh_count_in_the_next_window(rate_store: PostgresRateLimitStore) -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        assert await rate_store.hit('validate', 3600) == 1
        # move the row into an earlier window, as if the hour had passed
        await pool.execute("UPDATE oauth_rate_buckets SET window_start = window_start - interval '1 hour'")
        assert await rate_store.hit('validate', 3600) == 1
    finally:
        await pool.close()


async def test_claim_gives_out_at_most_the_cap_and_keeps_places_for_their_keys(
    rate_store: PostgresRateLimitStore,
) -> None:
    results = [await rate_store.claim('approval', key, 86400, 2) for key in ('a', 'b', 'c')]
    assert results == [True, True, False]
    assert await rate_store.claim('approval', 'a', 86400, 2) is True  # a key that holds a place keeps it
    assert await rate_store.claim('approval', 'c', 86400, 2) is False
    assert await rate_store.claim('other', 'x', 86400, 2) is True  # another prefix has its own places


async def test_claim_never_goes_over_the_cap_under_concurrency(rate_store: PostgresRateLimitStore) -> None:
    results = await asyncio.gather(*(rate_store.claim('approval', f'client-{i}', 86400, 2) for i in range(30)))

    assert sum(results) == 2


async def test_purge_expired_removes_only_old_windows(rate_store: PostgresRateLimitStore) -> None:
    pool = await asyncpg.create_pool(TEST_DSN)
    try:
        await rate_store.hit('fresh', 3600)
        await rate_store.hit('old', 3600)
        await pool.execute(
            "UPDATE oauth_rate_buckets SET window_start = now() - interval '3 days' WHERE bucket = 'old'"
        )

        assert await purge_expired(pool) == 1
        assert [r['bucket'] for r in await pool.fetch('SELECT bucket FROM oauth_rate_buckets')] == ['fresh']
    finally:
        await pool.close()
