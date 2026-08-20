"""Postgres-backed OAuth session storage (PSGO-261, oauth_session_persistence RFC).

Replaces the self-contained OAuth access/refresh JWTs with an opaque, server-side session
reference: the MCP client holds only a random lookup key, never the real Keboola credentials.
See ``feature_spec/oauth_session_persistence/RFC.md`` for the design.
"""

import asyncio
from collections.abc import Callable, Coroutine
from functools import wraps
from typing import Any, TypeVar

import asyncpg

F = TypeVar('F', bound=Callable[..., Coroutine[Any, Any, Any]])


class DatabaseUnavailableError(RuntimeError):
    """Raised when Postgres itself is unreachable -- connection refused, timeout, DNS failure, or
    the pool/server is out of resources -- as opposed to a query or logic error against a
    reachable database. Distinguished so the HTTP layer (``cli.py``'s ``_exception_handlers``) can
    map it to a retryable ``503`` instead of an opaque, generic ``500``.
    """


# Connection-level failures only. A pool that connects fine but returns a genuine query error
# (bad SQL, constraint violation, etc.) is a bug, not a "Postgres is down" condition, and must not
# be swallowed into a misleadingly retryable 503.
_CONNECTIVITY_EXCEPTIONS = (
    OSError,  # socket-level failure (connection refused, DNS failure, ...) during pool creation
    TimeoutError,  # connection/pool-acquire timeout; same class as asyncio.TimeoutError on 3.11+
    asyncio.TimeoutError,  # a distinct class from TimeoutError on 3.10, unified on 3.11+
    asyncpg.exceptions.PostgresConnectionError,  # lost/refused connection on an already-open pool
    asyncpg.exceptions.InterfaceError,  # asyncpg-level interface/pool issues (e.g. pool closed)
    asyncpg.exceptions.InsufficientResourcesError,  # DB refusing new connections (too many, etc.)
)


def guard_db_errors(func: F) -> F:
    """Decorator for ``PostgresSessionStore``/``PostgresKaiScopeStore`` methods: translates a
    Postgres-connectivity failure into ``DatabaseUnavailableError`` so a down/unreachable database
    surfaces as a clean, retryable signal instead of an opaque crash. Apply to every public method
    that touches the pool (including the first call that creates it) -- not to ``close()``.
    """

    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await func(*args, **kwargs)
        except _CONNECTIVITY_EXCEPTIONS as e:
            raise DatabaseUnavailableError(f'Postgres is unavailable ({type(e).__name__}).') from e

    return wrapper  # type: ignore[return-value]
