"""Debug tools for the per-project search index.

Exposes ``get_search_index_status`` and ``rebuild_search_index`` so the index
state is observable from the MCP client without server-side shell access.
"""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.search_index import (
    VERIFIED_SESSION_STATE_KEY,
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
