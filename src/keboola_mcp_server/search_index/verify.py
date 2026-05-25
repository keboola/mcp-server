"""Token verification with a process-wide TTL cache.

The streamable HTTP middleware rebuilds session state on every request
(``mcp.py:189``), so caching the verified session on ``ctx.session.state`` would
hit the API on every call. The cache here is keyed by ``token_hash`` and lives
for the lifetime of the process.
"""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone

from keboola_mcp_server.clients.storage import AsyncStorageClient
from keboola_mcp_server.search_index.types import VerifiedSession

LOG = logging.getLogger(__name__)

VERIFIED_SESSION_STATE_KEY: str = 'search_index.verified_session'
DEFAULT_VERIFY_TTL_SECONDS: int = 15 * 60

_TOKEN_HASH_LEN: int = 16

_cache: dict[str, VerifiedSession] = {}
_cache_lock = asyncio.Lock()


async def verify_and_cache(
    storage_client: AsyncStorageClient,
    storage_token: str,
    *,
    ttl_seconds: int = DEFAULT_VERIFY_TTL_SECONDS,
) -> VerifiedSession:
    """Return a current ``VerifiedSession`` for ``storage_token``.

    Calls ``tokens/verify`` at most once per ``ttl_seconds`` per distinct token.
    A failing verify is never silently substituted by a stale cache.
    """
    th = token_hash(storage_token)

    async with _cache_lock:
        cached = _cache.get(th)
        if cached is not None and _within_ttl(cached, ttl_seconds):
            return cached

    info = await storage_client.verify_token()
    owner = info.get('owner') if isinstance(info, dict) else None
    if not isinstance(owner, dict) or not owner.get('id'):
        raise ValueError('verify_token response is missing owner.id')

    session = VerifiedSession(
        project_id=str(owner['id']),
        token_hash=th,
        verified_at=datetime.now(timezone.utc),
    )

    async with _cache_lock:
        _cache[th] = session

    LOG.info('Verified storage token for project_id=%s', session.project_id)
    return session


def token_hash(token: str) -> str:
    """Stable, short, non-reversible identifier derived from a storage token."""
    return hashlib.sha256(token.encode('utf-8')).hexdigest()[:_TOKEN_HASH_LEN]


def _within_ttl(session: VerifiedSession, ttl_seconds: int) -> bool:
    age = (datetime.now(timezone.utc) - session.verified_at).total_seconds()
    return age < ttl_seconds


def _clear_cache() -> None:
    """Test helper. Not for production use."""
    _cache.clear()
