"""Build orchestration: dedup, scheduling, cold-start await.

State is process-global so multiple streamable-HTTP requests for the same
project share a single in-flight build.
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.search_index import builder, query, storage
from keboola_mcp_server.search_index.types import VerifiedSession

LOG = logging.getLogger(__name__)


class IndexUnavailable(Exception):
    """Raised when the index cannot serve a query (e.g. never built, cold-start build failed)."""


@dataclass
class _BuildState:
    db_path: Path
    task: asyncio.Task[Path] | None


_builds: dict[tuple[str, str], _BuildState] = {}
_builds_lock = asyncio.Lock()


async def ensure_index_built(
    session: VerifiedSession,
    client: KeboolaClient,
    *,
    ttl_seconds: int = storage.DEFAULT_TTL_SECONDS,
    root: Path | None = None,
) -> None:
    """Schedule a background build for ``session`` if missing or stale.

    Returns immediately. The build runs as an ``asyncio.Task`` and is deduped
    against other concurrent calls for the same ``(project_id, token_hash)``.
    """
    db_path = storage.path_for(session, root=root)
    key = (session.project_id, session.token_hash)

    async with _builds_lock:
        existing = _builds.get(key)
        if existing and existing.task is not None and not existing.task.done():
            return  # already building
        if not storage.is_stale(db_path, ttl_seconds=ttl_seconds):
            _builds[key] = _BuildState(db_path=db_path, task=None)
            return

        task = asyncio.create_task(builder.build_index(session, client, root=root))
        _builds[key] = _BuildState(db_path=db_path, task=task)

    def _on_done(t: asyncio.Task[Path]) -> None:
        if t.cancelled():
            LOG.warning('Search index build for project_id=%s was cancelled', session.project_id)
            return
        exc = t.exception()
        if exc is not None:
            LOG.warning('Search index build for project_id=%s failed: %s', session.project_id, exc)

    task.add_done_callback(_on_done)


async def list_index_rows(
    session: VerifiedSession,
    kinds: Iterable[str],
    *,
    root: Path | None = None,
    cold_start_timeout: float = 60.0,
) -> list[query.IndexedHit]:
    """Return all indexed rows for ``kinds`` without an FTS5 match.

    Used by config-based search; awaits a cold-start build the same way
    ``query_or_wait`` does, and raises ``IndexUnavailable`` for the same reasons.
    """
    db_path = storage.path_for(session, root=root)
    key = (session.project_id, session.token_hash)

    async with _builds_lock:
        state = _builds.get(key)

    if state is not None and state.task is not None and not state.task.done():
        if not db_path.exists():
            try:
                await asyncio.wait_for(asyncio.shield(state.task), timeout=cold_start_timeout)
            except asyncio.TimeoutError as e:
                raise IndexUnavailable('Cold-start build timed out') from e
            except Exception as e:
                raise IndexUnavailable(f'Cold-start build failed: {e}') from e

    if not db_path.exists():
        raise IndexUnavailable(f'No index file at {db_path}')

    return query.list_by_kinds(db_path=db_path, project_id=session.project_id, kinds=kinds)


async def query_or_wait(
    session: VerifiedSession,
    patterns: Sequence[str],
    kinds: Iterable[str] | None = None,
    limit: int = 100,
    *,
    root: Path | None = None,
    cold_start_timeout: float = 60.0,
) -> list[query.IndexedHit]:
    """Run a query against the session's index, awaiting a cold-start build if needed.

    Raises ``IndexUnavailable`` if no DB exists and no build is in flight, or
    if the cold-start build fails.
    """
    db_path = storage.path_for(session, root=root)
    key = (session.project_id, session.token_hash)

    async with _builds_lock:
        state = _builds.get(key)

    if state is not None and state.task is not None and not state.task.done():
        if not db_path.exists():
            try:
                await asyncio.wait_for(asyncio.shield(state.task), timeout=cold_start_timeout)
            except asyncio.TimeoutError as e:
                raise IndexUnavailable('Cold-start build timed out') from e
            except Exception as e:
                raise IndexUnavailable(f'Cold-start build failed: {e}') from e

    if not db_path.exists():
        raise IndexUnavailable(f'No index file at {db_path}')

    return query.run_query(
        db_path=db_path,
        project_id=session.project_id,
        patterns=patterns,
        kinds=kinds,
        limit=limit,
    )


def _reset_for_tests() -> None:
    """Test helper. Not for production use."""
    _builds.clear()
