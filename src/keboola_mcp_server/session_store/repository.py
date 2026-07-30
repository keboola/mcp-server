"""``SessionStore`` protocol + Postgres implementation (oauth_session_persistence RFC).

The protocol exists so `oauth.py`/`mcp.py` logic can be unit-tested against an in-memory fake
without a real database; `PostgresSessionStore` is the only production implementation.
"""

import asyncio
import dataclasses
import hashlib
import secrets
from datetime import datetime
from typing import Protocol

import asyncpg

from keboola_mcp_server.session_store import crypto

# Length of the opaque, randomly-generated access/refresh tokens handed to the MCP client. 256
# bits: not guessable, and this is the *entire* security check for these tokens (no signature to
# verify) -- see repository/RFC for why that's sufficient once the real credential lives server-side.
_TOKEN_BYTES = 32


def generate_opaque_token() -> str:
    return secrets.token_urlsafe(_TOKEN_BYTES)


def _hash_token(token: str) -> bytes:
    return hashlib.sha256(token.encode('utf-8')).digest()


@dataclasses.dataclass(frozen=True)
class OAuthSession:
    id: str
    client_id: str
    user_email: str | None
    kbc_access_token: str
    kbc_refresh_token: str
    kbc_access_expires_at: datetime
    scope_project_ids: list[int] | None
    scope_read_only: bool
    scope_confirmed: bool
    scope_scoped_token: str | None
    scope_scoped_expires_at: datetime | None


class SessionStore(Protocol):
    async def create(
        self,
        *,
        client_id: str,
        user_email: str | None,
        kbc_access_token: str,
        kbc_refresh_token: str,
        kbc_access_expires_at: datetime,
    ) -> tuple[str, str, OAuthSession]:
        """Creates a session row. Returns (opaque_access_token, opaque_refresh_token, session)."""
        ...

    async def get_by_access_token(self, access_token: str) -> OAuthSession | None:
        """None if the token doesn't exist, is revoked, or its underlying row is gone."""
        ...

    async def get_by_refresh_token(self, refresh_token: str) -> OAuthSession | None: ...

    async def rotate_kbc_tokens(
        self, session_id: str, *, kbc_access_token: str, kbc_refresh_token: str, kbc_access_expires_at: datetime
    ) -> None:
        """Replaces the encrypted Keboola credentials in place (server-managed refresh)."""
        ...

    async def rotate_opaque_tokens(self, session_id: str) -> tuple[str, str]:
        """Issues a fresh opaque access/refresh token pair for an existing session (OAuth refresh
        grant, per OAuth 2.1's refresh-token-rotation recommendation), invalidating the old pair.

        :return: (new_opaque_access_token, new_opaque_refresh_token)
        """
        ...

    async def update_scope(
        self,
        session_id: str,
        *,
        project_ids: list[int],
        read_only: bool,
        confirmed: bool,
        scoped_token: str | None,
        scoped_expires_at: datetime | None,
    ) -> None: ...

    async def revoke(self, session_id: str) -> None: ...


class PostgresSessionStore:
    """Schema migrations are NOT applied here -- that's the `keboola-mcp-server migrate` CLI/Job's
    job, run once per deployment before this app starts (oauth_session_persistence RFC). This class
    only ever reads/writes rows, assuming the schema is already in place.

    The connection pool is created lazily, on first use, so construction stays synchronous (no
    event loop required) -- `server.py`'s `create_server()` is a plain sync function, and forcing
    every one of its many call sites (including a dozen-plus sync tests) to become async just to
    accommodate this would be a much bigger, unrelated change.
    """

    def __init__(self, dsn: str, encryption_key: bytes) -> None:
        self._dsn = dsn
        self._key = encryption_key
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

    def _to_session(self, row: asyncpg.Record) -> OAuthSession:
        return OAuthSession(
            id=str(row['id']),
            client_id=row['client_id'],
            user_email=row['user_email'],
            kbc_access_token=crypto.decrypt(row['kbc_access_token_enc'], self._key).decode('utf-8'),
            kbc_refresh_token=crypto.decrypt(row['kbc_refresh_token_enc'], self._key).decode('utf-8'),
            kbc_access_expires_at=row['kbc_access_expires_at'],
            scope_project_ids=list(row['scope_project_ids']) if row['scope_project_ids'] is not None else None,
            scope_read_only=row['scope_read_only'],
            scope_confirmed=row['scope_confirmed'],
            scope_scoped_token=(
                crypto.decrypt(row['scope_scoped_token_enc'], self._key).decode('utf-8')
                if row['scope_scoped_token_enc'] is not None
                else None
            ),
            scope_scoped_expires_at=row['scope_scoped_expires_at'],
        )

    async def create(
        self,
        *,
        client_id: str,
        user_email: str | None,
        kbc_access_token: str,
        kbc_refresh_token: str,
        kbc_access_expires_at: datetime,
    ) -> tuple[str, str, OAuthSession]:
        access_token = generate_opaque_token()
        refresh_token = generate_opaque_token()
        pool = await self._get_pool()
        row = await pool.fetchrow(
            """
            INSERT INTO oauth_sessions (
                access_token_hash, refresh_token_hash, client_id, user_email,
                kbc_access_token_enc, kbc_refresh_token_enc, kbc_access_expires_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
            """,
            _hash_token(access_token),
            _hash_token(refresh_token),
            client_id,
            user_email,
            crypto.encrypt(kbc_access_token.encode('utf-8'), self._key),
            crypto.encrypt(kbc_refresh_token.encode('utf-8'), self._key),
            kbc_access_expires_at,
        )
        assert row is not None
        return access_token, refresh_token, self._to_session(row)

    async def get_by_access_token(self, access_token: str) -> OAuthSession | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            'UPDATE oauth_sessions SET last_used_at = now() '
            'WHERE access_token_hash = $1 AND revoked_at IS NULL RETURNING *',
            _hash_token(access_token),
        )
        return self._to_session(row) if row is not None else None

    async def get_by_refresh_token(self, refresh_token: str) -> OAuthSession | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            'SELECT * FROM oauth_sessions WHERE refresh_token_hash = $1 AND revoked_at IS NULL',
            _hash_token(refresh_token),
        )
        return self._to_session(row) if row is not None else None

    async def rotate_kbc_tokens(
        self, session_id: str, *, kbc_access_token: str, kbc_refresh_token: str, kbc_access_expires_at: datetime
    ) -> None:
        pool = await self._get_pool()
        await pool.execute(
            """
            UPDATE oauth_sessions
            SET kbc_access_token_enc = $2, kbc_refresh_token_enc = $3, kbc_access_expires_at = $4,
                updated_at = now()
            WHERE id = $1
            """,
            session_id,
            crypto.encrypt(kbc_access_token.encode('utf-8'), self._key),
            crypto.encrypt(kbc_refresh_token.encode('utf-8'), self._key),
            kbc_access_expires_at,
        )

    async def rotate_opaque_tokens(self, session_id: str) -> tuple[str, str]:
        access_token = generate_opaque_token()
        refresh_token = generate_opaque_token()
        pool = await self._get_pool()
        await pool.execute(
            """
            UPDATE oauth_sessions
            SET access_token_hash = $2, refresh_token_hash = $3, updated_at = now()
            WHERE id = $1
            """,
            session_id,
            _hash_token(access_token),
            _hash_token(refresh_token),
        )
        return access_token, refresh_token

    async def update_scope(
        self,
        session_id: str,
        *,
        project_ids: list[int],
        read_only: bool,
        confirmed: bool,
        scoped_token: str | None,
        scoped_expires_at: datetime | None,
    ) -> None:
        pool = await self._get_pool()
        await pool.execute(
            """
            UPDATE oauth_sessions
            SET scope_project_ids = $2, scope_read_only = $3, scope_confirmed = $4,
                scope_scoped_token_enc = $5, scope_scoped_expires_at = $6, updated_at = now()
            WHERE id = $1
            """,
            session_id,
            project_ids,
            read_only,
            confirmed,
            crypto.encrypt(scoped_token.encode('utf-8'), self._key) if scoped_token is not None else None,
            scoped_expires_at,
        )

    async def revoke(self, session_id: str) -> None:
        pool = await self._get_pool()
        await pool.execute('UPDATE oauth_sessions SET revoked_at = now() WHERE id = $1', session_id)
