"""Postgres-backed counters for the OAuth rate limits that must hold across every replica (AI-4005).

The process-local limiters in ``oauth.py`` bound one replica; Connection's ceiling on its client-validation endpoint is per
egress IP and shared by every replica, and a daily cap on new clients is meaningless if each replica keeps its own. Postgres
is already a hard dependency of the OAuth path (sessions live there), so the counters live in it too: one atomic upsert per
check, no extra service. The callers fall back to their process-local limiter when the database cannot be reached.
"""

import asyncio
from typing import Protocol

import asyncpg

from keboola_mcp_server.session_store import guard_db_errors

# Rows are useful for their own window only; keep a generous margin for clock skew between replicas and the database.
_PURGE_AFTER = '2 days'


class RateLimitStore(Protocol):
    async def hit(self, bucket: str, window_seconds: int) -> int:
        """Counts one event in `bucket` in the current fixed `window_seconds` window and returns how many events (this one
        included) the window holds across every replica."""
        ...

    async def claim(self, prefix: str, key: str, window_seconds: int, max_keys: int) -> bool:
        """Claims a place for `key` among at most `max_keys` distinct keys per fixed window under `prefix`, across every
        replica. A key that already holds a place in the window gets True without using another one."""
        ...

    async def close(self) -> None: ...


class PostgresRateLimitStore:
    """Schema migrations are NOT applied here -- see `PostgresSessionStore`'s docstring for why (same reasoning, same
    `migrate` CLI/Job applies them). The connection pool is created lazily, on first use, for the same sync-construction
    reason `PostgresSessionStore` does."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:  # re-check: another task may have won the lock race first
                    self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=4)
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    @guard_db_errors
    async def hit(self, bucket: str, window_seconds: int) -> int:
        pool = await self._get_pool()
        return await pool.fetchval(
            """
            INSERT INTO oauth_rate_buckets (bucket, window_start, hits)
            VALUES ($1, to_timestamp(floor(extract(epoch FROM now()) / $2::float8) * $2::float8), 1)
            ON CONFLICT (bucket, window_start) DO UPDATE SET hits = oauth_rate_buckets.hits + 1
            RETURNING hits
            """,
            bucket,
            window_seconds,
        )

    @guard_db_errors
    async def claim(self, prefix: str, key: str, window_seconds: int, max_keys: int) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            # One claim at a time per prefix, so "count the places, then take one" cannot be raced into going over the cap.
            await conn.execute('SELECT pg_advisory_xact_lock(hashtext($1))', prefix)
            window = await conn.fetchval(
                'SELECT to_timestamp(floor(extract(epoch FROM now()) / $1::float8) * $1::float8)', window_seconds
            )
            bucket = f'{prefix}:{key}'
            if await conn.fetchval(
                'SELECT 1 FROM oauth_rate_buckets WHERE bucket = $1 AND window_start = $2', bucket, window
            ):
                return True
            taken = await conn.fetchval(
                'SELECT count(*) FROM oauth_rate_buckets WHERE window_start = $1 AND starts_with(bucket, $2)',
                window,
                f'{prefix}:',
            )
            if taken >= max_keys:
                return False
            await conn.execute(
                'INSERT INTO oauth_rate_buckets (bucket, window_start, hits) VALUES ($1, $2, 1)', bucket, window
            )
            return True


async def purge_expired(pool: asyncpg.Pool) -> int:
    """Deletes counters of windows that are long over. Idempotent; run by `gc-sessions`.

    :return: how many rows were removed."""
    result = await pool.execute(
        f"DELETE FROM oauth_rate_buckets WHERE window_start < now() - interval '{_PURGE_AFTER}'"
    )
    return int(result.rsplit(' ', 1)[-1])
