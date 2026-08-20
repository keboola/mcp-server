"""Unit tests for the Postgres-connectivity error translation (no real database needed -- see
``tests/session_store/test_repository.py``/``test_kai_scope.py`` for the ``requires_postgres``
integration-style tests)."""

import asyncio
from unittest.mock import AsyncMock

import asyncpg
import pytest

from keboola_mcp_server.session_store import DatabaseUnavailableError, guard_db_errors
from keboola_mcp_server.session_store.kai_scope import PostgresKaiScopeStore
from keboola_mcp_server.session_store.repository import PostgresSessionStore


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'raised',
    [
        ConnectionRefusedError('refused'),
        TimeoutError('timed out'),
        asyncio.TimeoutError('timed out'),
        asyncpg.exceptions.PostgresConnectionError('connection lost'),
        asyncpg.exceptions.InterfaceError('pool closed'),
        asyncpg.exceptions.TooManyConnectionsError('too many connections'),
    ],
    ids=[
        'connection_refused',
        'timeout_error',
        'asyncio_timeout',
        'pg_connection_error',
        'interface_error',
        'too_many_connections',
    ],
)
async def test_guard_db_errors_translates_connectivity_failures(raised: Exception) -> None:
    @guard_db_errors
    async def flaky():
        raise raised

    with pytest.raises(DatabaseUnavailableError) as exc_info:
        await flaky()
    assert exc_info.value.__cause__ is raised


@pytest.mark.asyncio
async def test_guard_db_errors_does_not_swallow_unrelated_errors() -> None:
    @guard_db_errors
    async def flaky():
        raise ValueError('a genuine bug, not a connectivity problem')

    with pytest.raises(ValueError, match='genuine bug'):
        await flaky()


@pytest.mark.asyncio
async def test_guard_db_errors_passes_through_on_success() -> None:
    @guard_db_errors
    async def ok():
        return 42

    assert await ok() == 42


@pytest.mark.asyncio
async def test_postgres_session_store_methods_raise_database_unavailable(mocker) -> None:
    # Every public PostgresSessionStore method must be decorated -- simulate the pool itself
    # failing to materialize (the most common real-world case: Postgres is simply down).
    store = PostgresSessionStore('postgresql://irrelevant', encryption_key=bytes([1] * 32))
    mocker.patch.object(store, '_get_pool', AsyncMock(side_effect=OSError('connection refused')))

    with pytest.raises(DatabaseUnavailableError):
        await store.get_by_access_token('token')
    with pytest.raises(DatabaseUnavailableError):
        await store.get_by_refresh_token('token')
    with pytest.raises(DatabaseUnavailableError):
        await store.revoke('session-1')


@pytest.mark.asyncio
async def test_postgres_kai_scope_store_methods_raise_database_unavailable(mocker) -> None:
    store = PostgresKaiScopeStore('postgresql://irrelevant')
    mocker.patch.object(store, '_get_pool', AsyncMock(side_effect=OSError('connection refused')))

    with pytest.raises(DatabaseUnavailableError):
        await store.get('convo-1', 42)
    with pytest.raises(DatabaseUnavailableError):
        await store.drop('convo-1', 42)
