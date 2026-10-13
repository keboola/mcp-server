"""``SessionStore`` protocol + Postgres implementation (oauth_session_persistence RFC).

The protocol exists so `oauth.py`/`mcp.py` logic can be unit-tested against an in-memory fake
without a real database; `PostgresSessionStore` is the only production implementation.
"""

import asyncio
import dataclasses
import hashlib
import secrets
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime
from typing import Protocol

import asyncpg

from keboola_mcp_server.session_store import crypto, guard_db_errors

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
    # Whether the Connection OAuth scope requested for this session included 'projectless'.
    # Always True for a session created now (RFC Decision §10: every registered client gets
    # 'claudai projectless'); False only for a session persisted by an earlier build of AI-2883,
    # where a dynamically-approved client got 'claudai' alone. Kept so load_access_token /
    # load_refresh_token advertise the scope such a session actually has.
    oauth_projectless: bool


class LockedSession:
    """A session held under `SessionStore.lock_session`, with the writes that belong inside that
    lock. Every statement runs on the locked connection -- see `lock_session` on why a second one
    must not be acquired here."""

    def __init__(self, conn: asyncpg.Connection, session: OAuthSession | None, key: bytes) -> None:
        self.session = session
        self._conn = conn
        self._key = key

    async def rotate_kbc_tokens(
        self, *, kbc_access_token: str, kbc_refresh_token: str, kbc_access_expires_at: datetime
    ) -> None:
        assert self.session is not None, 'Cannot rotate the credentials of a session that is gone.'
        await self._conn.execute(
            """
            UPDATE oauth_sessions
            SET kbc_access_token_enc = $2, kbc_refresh_token_enc = $3, kbc_access_expires_at = $4,
                updated_at = now()
            WHERE id = $1
            """,
            self.session.id,
            crypto.encrypt(kbc_access_token.encode('utf-8'), self._key),
            crypto.encrypt(kbc_refresh_token.encode('utf-8'), self._key),
            kbc_access_expires_at,
        )

    async def revoke(self) -> None:
        """Ends the session. No compare-and-swap needed: the lock already makes this the only
        worker that could have rotated the credentials since they were read."""
        assert self.session is not None, 'Cannot revoke a session that is gone.'
        await self._conn.execute('UPDATE oauth_sessions SET revoked_at = now() WHERE id = $1', self.session.id)


class SessionStore(Protocol):
    async def create(
        self,
        *,
        client_id: str,
        user_email: str | None,
        kbc_access_token: str,
        kbc_refresh_token: str,
        kbc_access_expires_at: datetime,
        oauth_projectless: bool = True,
    ) -> tuple[str, str, OAuthSession]:
        """Creates a session row. Returns (opaque_access_token, opaque_refresh_token, session)."""
        ...

    async def get_by_access_token(self, access_token: str) -> OAuthSession | None:
        """None if the token doesn't exist, is revoked, or its underlying row is gone."""
        ...

    async def get_by_refresh_token(self, refresh_token: str) -> OAuthSession | None: ...

    async def get_by_id(self, session_id: str) -> OAuthSession | None:
        """Looks a session up by its row id rather than by a token the caller presented.

        Used by a provisioned (agent) session, whose caller holds only the row's handle inside its
        `scope_token` and never a token of its own -- see the remote_agent_provisioning RFC.
        """
        ...

    def lock_session(self, session_id: str) -> AbstractAsyncContextManager['LockedSession']:
        """Serializes work on one session across every worker and replica, yielding a handle whose
        `session` is the row as it is *after* the lock was taken (None if it is gone).

        For credential refresh: Connection rotates refresh tokens, so two concurrent refreshes of
        the same session would have one spend a token the other already invalidated, and a late
        write-back could persist that dead pair. Re-reading under the lock lets the loser of the
        race use the winner's fresh credentials instead of refreshing again.

        Writes go through the handle, on the connection the lock is already held on. Taking a
        second pooled connection while holding this one would deadlock the pool: every holder and
        waiter occupies a connection, so once as many refreshes overlap as the pool is wide, nobody
        can acquire the second one and every session lookup on the replica stalls behind them.
        """
        ...

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

    async def revoke_if_kbc_refresh_token(self, session_id: str, kbc_refresh_token: str) -> bool:
        """Revokes the session only if it still holds this Keboola refresh token, so a concurrent
        rotation that already replaced it is never killed by a stale caller.

        :return: True if the session was revoked, False if it was rotated, already revoked or gone.
        """
        ...


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
            oauth_projectless=row['oauth_projectless'],
        )

    @guard_db_errors
    async def create(
        self,
        *,
        client_id: str,
        user_email: str | None,
        kbc_access_token: str,
        kbc_refresh_token: str,
        kbc_access_expires_at: datetime,
        oauth_projectless: bool = True,
    ) -> tuple[str, str, OAuthSession]:
        access_token = generate_opaque_token()
        refresh_token = generate_opaque_token()
        pool = await self._get_pool()
        row = await pool.fetchrow(
            """
            INSERT INTO oauth_sessions (
                access_token_hash, refresh_token_hash, client_id, user_email,
                kbc_access_token_enc, kbc_refresh_token_enc, kbc_access_expires_at, oauth_projectless
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *
            """,
            _hash_token(access_token),
            _hash_token(refresh_token),
            client_id,
            user_email,
            crypto.encrypt(kbc_access_token.encode('utf-8'), self._key),
            crypto.encrypt(kbc_refresh_token.encode('utf-8'), self._key),
            kbc_access_expires_at,
            oauth_projectless,
        )
        assert row is not None
        return access_token, refresh_token, self._to_session(row)

    @guard_db_errors
    async def get_by_access_token(self, access_token: str) -> OAuthSession | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            'UPDATE oauth_sessions SET last_used_at = now() '
            'WHERE access_token_hash = $1 AND revoked_at IS NULL RETURNING *',
            _hash_token(access_token),
        )
        return self._to_session(row) if row is not None else None

    @guard_db_errors
    async def get_by_refresh_token(self, refresh_token: str) -> OAuthSession | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            'SELECT * FROM oauth_sessions WHERE refresh_token_hash = $1 AND revoked_at IS NULL',
            _hash_token(refresh_token),
        )
        return self._to_session(row) if row is not None else None

    @guard_db_errors
    async def get_by_id(self, session_id: str) -> OAuthSession | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            'UPDATE oauth_sessions SET last_used_at = now() WHERE id = $1 AND revoked_at IS NULL RETURNING *',
            session_id,
        )
        return self._to_session(row) if row is not None else None

    @asynccontextmanager
    async def _lock_session(self, session_id: str) -> AsyncIterator[LockedSession]:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            # A transaction-scoped advisory lock: released on commit, so a crashed worker cannot
            # wedge the session. It does not block reads or writes of the row itself -- it only
            # serializes the holders of this same lock, which is exactly the refresh path.
            await conn.execute('SELECT pg_advisory_xact_lock(hashtext($1)::bigint)', session_id)
            row = await conn.fetchrow('SELECT * FROM oauth_sessions WHERE id = $1 AND revoked_at IS NULL', session_id)
            yield LockedSession(conn, self._to_session(row) if row is not None else None, self._key)

    def lock_session(self, session_id: str) -> AbstractAsyncContextManager[LockedSession]:
        return self._lock_session(session_id)

    @guard_db_errors
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

    @guard_db_errors
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

    @guard_db_errors
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

    @guard_db_errors
    async def revoke(self, session_id: str) -> None:
        pool = await self._get_pool()
        await pool.execute('UPDATE oauth_sessions SET revoked_at = now() WHERE id = $1', session_id)

    @guard_db_errors
    async def revoke_if_kbc_refresh_token(self, session_id: str, kbc_refresh_token: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            # The credential is encrypted with a random nonce, so it can't be compared in SQL:
            # lock the row, decrypt, compare, then revoke.
            row = await conn.fetchrow(
                'SELECT kbc_refresh_token_enc FROM oauth_sessions WHERE id = $1 AND revoked_at IS NULL FOR UPDATE',
                session_id,
            )
            if row is None or crypto.decrypt(row['kbc_refresh_token_enc'], self._key).decode('utf-8') != (
                kbc_refresh_token
            ):
                return False
            await conn.execute('UPDATE oauth_sessions SET revoked_at = now() WHERE id = $1', session_id)
            return True
