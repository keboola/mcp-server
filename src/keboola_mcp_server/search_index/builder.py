"""Fetches project data and writes it into the FTS5 index.

Phase 2 scope: buckets and tables (with column names + column descriptions).
Other ``kind`` values (configurations, flows, semantic objects) are added in
later phases by extending ``build_index``.
"""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.search_index import storage
from keboola_mcp_server.search_index.types import VerifiedSession
from keboola_mcp_server.tools.storage_helpers import merged_bucket_list, merged_bucket_table_list

LOG = logging.getLogger(__name__)

_INSERT_SQL = (
    'INSERT INTO search (project_id, kind, obj_id, name, description, content, metadata) '
    'VALUES (?, ?, ?, ?, ?, ?, ?)'
)


async def build_index(
    session: VerifiedSession,
    client: KeboolaClient,
    *,
    root: Path | None = None,
) -> Path:
    """Build a fresh index for ``session`` and atomically publish it.

    Single ``merged_bucket_list`` round-trip; per-bucket ``tables`` calls fan out
    concurrently. Output is written to ``<db>.tmp`` and atomically renamed.
    """
    db_path = storage.path_for(session, root=root)
    tmp_path = storage.tmp_path_for(db_path)
    storage.ensure_parent_dirs(db_path, root=root)
    lock_path = db_path.with_suffix(db_path.suffix + '.lock')

    LOG.info('Building search index for project_id=%s at %s', session.project_id, db_path)

    buckets = await merged_bucket_list(client)
    table_lists = await _fetch_tables_for_buckets(client, buckets)

    with storage.file_lock(lock_path):
        if tmp_path.exists():
            tmp_path.unlink()

        conn = sqlite3.connect(tmp_path)
        try:
            storage.init_schema(conn, session)
            bucket_count = _insert_bucket_rows(conn, buckets, session)
            table_count = _insert_table_rows(conn, buckets, table_lists, session)
            conn.commit()
        finally:
            conn.close()

        storage.atomic_publish(tmp_path, db_path)

    LOG.info(
        'Search index built for project_id=%s: %s',
        session.project_id,
        {'bucket': bucket_count, 'table': table_count},
    )
    return db_path


async def _fetch_tables_for_buckets(client: KeboolaClient, buckets: list[JsonDict]) -> list[list[JsonDict]]:
    """Issue one ``tables`` request per bucket, in parallel."""
    bucket_ids = [bucket.get('id') for bucket in buckets]
    return await asyncio.gather(
        *(
            merged_bucket_table_list(client, bid, include=['columns', 'columnMetadata']) if bid else _empty_table_list()
            for bid in bucket_ids
        )
    )


async def _empty_table_list() -> list[JsonDict]:
    return []


def _insert_bucket_rows(conn: sqlite3.Connection, buckets: list[JsonDict], session: VerifiedSession) -> int:
    rows = []
    for bucket in buckets:
        bucket_id = bucket.get('id')
        if not bucket_id:
            continue
        name = bucket.get('name') or ''
        display_name = bucket.get('displayName') or ''
        description = get_metadata_property(bucket.get('metadata', []), MetadataField.DESCRIPTION) or ''
        updated = bucket.get('lastChangeDate') or bucket.get('updated') or bucket.get('created') or ''

        content = ' '.join(filter(None, [bucket_id, name, display_name, description]))
        metadata_json = json.dumps(
            {
                'name': name,
                'display_name': display_name,
                'description': description,
                'updated': updated,
            }
        )
        rows.append((session.project_id, 'bucket', bucket_id, name, description, content, metadata_json))

    if rows:
        conn.executemany(_INSERT_SQL, rows)
    return len(rows)


def _insert_table_rows(
    conn: sqlite3.Connection,
    buckets: list[JsonDict],
    table_lists: list[list[JsonDict]],
    session: VerifiedSession,
) -> int:
    rows = []
    for bucket, tables in zip(buckets, table_lists):
        bucket_id = bucket.get('id')
        if not bucket_id:
            continue
        for table in tables:
            table_id = table.get('id')
            if not table_id:
                continue

            name = table.get('name') or ''
            display_name = table.get('displayName') or ''
            description = get_metadata_property(table.get('metadata', []), MetadataField.DESCRIPTION) or ''
            updated = table.get('lastChangeDate') or table.get('created') or ''

            col_names = list(table.get('columns') or [])
            col_descs = [
                get_metadata_property(col_meta, MetadataField.DESCRIPTION) or ''
                for col_meta in (table.get('columnMetadata') or {}).values()
            ]
            col_descs = [d for d in col_descs if d]

            content_parts = [table_id, name, display_name, description, *col_names, *col_descs]
            content = ' '.join(filter(None, content_parts))

            metadata_json = json.dumps(
                {
                    'name': name,
                    'display_name': display_name,
                    'description': description,
                    'updated': updated,
                    'bucket_id': bucket_id,
                    'columns': col_names,
                    'column_descriptions': col_descs,
                }
            )
            rows.append((session.project_id, 'table', table_id, name, description, content, metadata_json))

    if rows:
        conn.executemany(_INSERT_SQL, rows)
    return len(rows)
