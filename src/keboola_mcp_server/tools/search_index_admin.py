"""Debug tools for the per-project search index.

Exposes ``get_search_index_status``, ``rebuild_search_index`` and
``compare_search_paths`` so the index is observable, operable, and verifiable
from the MCP client without server-side shell access.
"""

import asyncio
import logging
import sqlite3
import time
from datetime import datetime, timezone
from typing import Annotated, Any, Literal

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.search_index import (
    VERIFIED_SESSION_STATE_KEY,
    IndexUnavailable,
    VerifiedSession,
    build_index,
    is_stale,
    path_for,
)

LOG = logging.getLogger(__name__)

SEARCH_INDEX_ADMIN_TAG = 'search-index-admin'


class SearchIndexStatus(BaseModel):
    enabled: bool = Field(description='Whether the search index is active for this session.')
    project_id: str | None = Field(default=None, description='Verified project ID for the current token.')
    db_path: str | None = Field(default=None, description='Filesystem path of the index database.')
    exists: bool = Field(default=False, description='Whether the index file is present on disk.')
    is_stale: bool = Field(default=False, description='True when the DB is older than the freshness TTL.')
    size_bytes: int | None = Field(default=None, description='Size of the index file in bytes.')
    mtime_iso: str | None = Field(default=None, description='Last-modified timestamp of the index file (ISO 8601).')
    row_counts: dict[str, int] = Field(default_factory=dict, description='Indexed row count per kind.')
    schema_version: str | None = Field(default=None, description='Schema version stamped into the index.')
    built_at_iso: str | None = Field(default=None, description='When the index was last built (ISO 8601).')
    reason: str | None = Field(default=None, description='Reason why the index is unavailable, if applicable.')


class SearchPathComparison(BaseModel):
    enabled: bool = Field(description='Whether the search index is active for this session.')
    project_id: str | None = Field(default=None, description='Verified project ID for the current token.')
    patterns: list[str] = Field(description='Patterns that were compared.')
    item_types: list[str] = Field(description='Kinds that were compared (bucket and/or table).')
    index_duration_ms: float | None = Field(default=None, description='Wall-clock time the index path took.')
    index_hit_count: int | None = Field(default=None, description='Hits returned by the FTS5 index.')
    index_obj_ids_sample: list[str] = Field(
        default_factory=list, description='Up to 10 object IDs from the indexed result set.'
    )
    live_duration_ms: float | None = Field(default=None, description='Wall-clock time the live API path took.')
    live_hit_count: int | None = Field(default=None, description='Hits returned by the live Storage API path.')
    live_obj_ids_sample: list[str] = Field(
        default_factory=list, description='Up to 10 object IDs from the live result set.'
    )
    speedup: float | None = Field(
        default=None, description='live_duration_ms / index_duration_ms (higher = index is faster).'
    )
    overlap_count: int | None = Field(default=None, description='Number of object IDs returned by both paths.')
    reason: str | None = Field(default=None, description='If index path failed, the IndexUnavailable reason.')


def add_search_index_admin_tools(mcp: FastMCP) -> None:
    """Register the index admin tools on the MCP server."""
    LOG.info(f'Adding tool {get_search_index_status.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            get_search_index_status,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SEARCH_INDEX_ADMIN_TAG},
        )
    )

    LOG.info(f'Adding tool {rebuild_search_index.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            rebuild_search_index,
            annotations=ToolAnnotations(destructiveHint=False),
            tags={SEARCH_INDEX_ADMIN_TAG},
        )
    )

    LOG.info(f'Adding tool {compare_search_paths.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            compare_search_paths,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={SEARCH_INDEX_ADMIN_TAG},
        )
    )

    LOG.info('Search index admin tools initialized.')


@tool_errors()
async def get_search_index_status(ctx: Context) -> SearchIndexStatus:
    """Report the current session's search index status (path, freshness, row counts).

    Use this to confirm the index is being built and queried on your token. It is a
    diagnostic tool — it does not mutate any state.
    """
    verified: VerifiedSession | None = ctx.session.state.get(VERIFIED_SESSION_STATE_KEY)
    if verified is None:
        return SearchIndexStatus(
            enabled=False,
            reason='No verified session attached. Feature flag disabled, branch is non-default, or verify failed.',
        )

    db_path = path_for(verified)
    if not db_path.exists():
        return SearchIndexStatus(
            enabled=True,
            project_id=verified.project_id,
            db_path=str(db_path),
            exists=False,
            reason='Index file not built yet. Make a few requests or call rebuild_search_index.',
        )

    stat = db_path.stat()
    counts, meta = _read_index_metadata(db_path)
    return SearchIndexStatus(
        enabled=True,
        project_id=verified.project_id,
        db_path=str(db_path),
        exists=True,
        is_stale=is_stale(db_path),
        size_bytes=stat.st_size,
        mtime_iso=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        row_counts={k: int(v) for k, v in counts.items()},
        schema_version=meta.get('schema_version'),
        built_at_iso=meta.get('built_at_iso'),
    )


@tool_errors()
async def rebuild_search_index(ctx: Context) -> SearchIndexStatus:
    """Force a synchronous rebuild of the current session's search index.

    Use this when you want a fresh index immediately rather than waiting for the
    background refresh. The call returns the new index status after the rebuild.
    """
    verified: VerifiedSession | None = ctx.session.state.get(VERIFIED_SESSION_STATE_KEY)
    if verified is None:
        return SearchIndexStatus(
            enabled=False,
            reason='No verified session attached; cannot rebuild.',
        )

    client = KeboolaClient.from_state(ctx.session.state)
    db_path = await build_index(verified, client)
    stat = db_path.stat()
    counts, meta = _read_index_metadata(db_path)
    return SearchIndexStatus(
        enabled=True,
        project_id=verified.project_id,
        db_path=str(db_path),
        exists=True,
        is_stale=False,
        size_bytes=stat.st_size,
        mtime_iso=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        row_counts={k: int(v) for k, v in counts.items()},
        schema_version=meta.get('schema_version'),
        built_at_iso=meta.get('built_at_iso'),
    )


def _read_index_metadata(db_path) -> tuple[dict[str, Any], dict[str, Any]]:
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    try:
        counts = dict(conn.execute('SELECT kind, COUNT(*) FROM search GROUP BY kind').fetchall())
        meta = dict(conn.execute('SELECT key, value FROM meta').fetchall())
    finally:
        conn.close()
    return counts, meta


@tool_errors()
async def compare_search_paths(
    ctx: Context,
    patterns: Annotated[
        list[str],
        Field(
            description='One or more literal patterns (OR-combined). Examples: ["customer"] or ["sales", "revenue"].'
        ),
    ],
    item_types: Annotated[
        tuple[Literal['bucket', 'table'], ...],
        Field(
            description='Subset of bucket/table to compare. Defaults to both. These are the only kinds Phase 2 indexes.'
        ),
    ] = ('bucket', 'table'),
) -> SearchPathComparison:
    """Diagnostic A/B test: run the FTS5 index path and the live API path for the same query in parallel.

    Use this to prove the index is serving queries and to measure the speedup on your project. Both paths
    should return overlapping object IDs; the index path should be substantially faster on non-trivial projects.
    Only ``bucket`` and ``table`` are supported (the kinds the Phase 2 index covers).
    """
    # Local imports to avoid circulars at module load.
    from keboola_mcp_server.tools.search import (
        SearchSpec,
        _fetch_buckets,
        _fetch_tables,
        _search_indexed_buckets_and_tables,
    )

    verified: VerifiedSession | None = ctx.session.state.get(VERIFIED_SESSION_STATE_KEY)
    if verified is None:
        return SearchPathComparison(
            enabled=False,
            patterns=patterns,
            item_types=list(item_types),
            reason='No verified session attached. Feature flag disabled, branch is non-default, or verify failed.',
        )

    spec = SearchSpec(
        patterns=patterns,
        item_types=item_types,
        pattern_mode='literal',
        search_type='textual',
    )
    kinds = set(item_types)
    client = KeboolaClient.from_state(ctx.session.state)

    async def _timed_index() -> tuple[list, float, str | None]:
        t0 = time.monotonic()
        try:
            hits = await _search_indexed_buckets_and_tables(verified, spec, kinds)
            return hits, (time.monotonic() - t0) * 1000, None
        except IndexUnavailable as e:
            return [], (time.monotonic() - t0) * 1000, str(e)

    async def _timed_live() -> tuple[list, float]:
        t0 = time.monotonic()
        hits: list = []
        if 'bucket' in kinds:
            hits.extend(await _fetch_buckets(client, spec))
        if 'table' in kinds:
            hits.extend(await _fetch_tables(client, spec))
        return hits, (time.monotonic() - t0) * 1000

    (index_hits, index_ms, index_err), (live_hits, live_ms) = await asyncio.gather(_timed_index(), _timed_live())

    def _ids(hits: list) -> set[str]:
        return {h.bucket_id or h.table_id for h in hits if (h.bucket_id or h.table_id)}

    index_ids = _ids(index_hits)
    live_ids = _ids(live_hits)
    overlap = index_ids & live_ids
    speedup = round(live_ms / index_ms, 2) if index_ms > 0 and index_err is None else None

    return SearchPathComparison(
        enabled=True,
        project_id=verified.project_id,
        patterns=list(patterns),
        item_types=list(item_types),
        index_duration_ms=round(index_ms, 1),
        index_hit_count=len(index_hits),
        index_obj_ids_sample=sorted(index_ids)[:10],
        live_duration_ms=round(live_ms, 1),
        live_hit_count=len(live_hits),
        live_obj_ids_sample=sorted(live_ids)[:10],
        speedup=speedup,
        overlap_count=len(overlap),
        reason=index_err,
    )
