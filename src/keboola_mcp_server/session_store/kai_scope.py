"""Postgres-backed scope persistence for deployed header-token (Kai) sessions.

See ``feature_spec/pat_token_support/RFC.md`` ("Kai (header-token) session-scope persistence",
increment 6) for the design. Unlike OAuth sessions (`session_store/repository.py`), Kai's raw
Keboola token is refreshed by Kai's own regime and is not stable across that refresh, so rows are
keyed by ``sha256(conversation_id:user_id)`` rather than a hash of the token itself. No credential
material is stored here, so unlike `OAuthSession` nothing needs encryption at rest.
"""

import asyncio
import dataclasses
import hashlib
from typing import Protocol

import asyncpg

from keboola_mcp_server.session_store import guard_db_errors


def _hash_key(conversation_id: str, user_id: int) -> bytes:
    return hashlib.sha256(f'{conversation_id}:{user_id}'.encode()).digest()


@dataclasses.dataclass(frozen=True)
class KaiScope:
    project_ids: list[int]
    read_only: bool
    confirmed: bool


class KaiScopeStore(Protocol):
    async def get(self, conversation_id: str, user_id: int) -> KaiScope | None: ...

    async def upsert(
        self, conversation_id: str, user_id: int, *, project_ids: list[int], read_only: bool, confirmed: bool
    ) -> None: ...

    async def drop(self, conversation_id: str, user_id: int) -> None: ...


class PostgresKaiScopeStore:
    """Schema migrations are NOT applied here -- see `PostgresSessionStore`'s docstring for why
    (same reasoning, same `migrate` CLI/Job applies both). The connection pool is created lazily,
    on first use, for the same sync-construction reason `PostgresSessionStore` does.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None
        self._pool_lock = asyncio.Lock()

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:  # re-check: another task may have won the lock race first
                    self._pool = await asyncpg.create_pool(self._dsn)
        return self._pool

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

    @guard_db_errors
    async def get(self, conversation_id: str, user_id: int) -> KaiScope | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            'UPDATE kai_sessions SET last_used_at = now() WHERE session_key = $1 RETURNING *',
            _hash_key(conversation_id, user_id),
        )
        if row is None:
            return None
        return KaiScope(project_ids=list(row['project_ids']), read_only=row['read_only'], confirmed=row['confirmed'])

    @guard_db_errors
    async def upsert(
        self, conversation_id: str, user_id: int, *, project_ids: list[int], read_only: bool, confirmed: bool
    ) -> None:
        pool = await self._get_pool()
        await pool.execute(
            """
            INSERT INTO kai_sessions (session_key, project_ids, read_only, confirmed)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (session_key) DO UPDATE
            SET project_ids = EXCLUDED.project_ids, read_only = EXCLUDED.read_only,
                confirmed = EXCLUDED.confirmed, updated_at = now(), last_used_at = now()
            """,
            _hash_key(conversation_id, user_id),
            project_ids,
            read_only,
            confirmed,
        )

    @guard_db_errors
    async def drop(self, conversation_id: str, user_id: int) -> None:
        pool = await self._get_pool()
        await pool.execute('DELETE FROM kai_sessions WHERE session_key = $1', _hash_key(conversation_id, user_id))
