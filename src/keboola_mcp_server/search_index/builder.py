"""Fetches project data and writes it into the FTS5 index.

Indexed kinds:

- ``bucket`` and ``table`` (Phase 2) — from ``merged_bucket_list`` and
  ``merged_bucket_table_list``.
- ``flow``, ``transformation``, ``configuration``, ``configuration-row``,
  ``data-app``, ``workspace`` (Phase 3) — from ``component_list(include=
  configuration,rows)``.

The two source families are fetched concurrently. Each populator returns the
list of FTS5 rows to insert plus a per-kind row count for the build log.
"""

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import (
    CONDITIONAL_FLOW_COMPONENT_ID,
    DATA_APP_COMPONENT_ID,
    ORCHESTRATOR_COMPONENT_ID,
    KeboolaClient,
    get_metadata_property,
)
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.search_index import storage
from keboola_mcp_server.search_index.types import VerifiedSession
from keboola_mcp_server.tools.storage_helpers import merged_bucket_list, merged_bucket_table_list

LOG = logging.getLogger(__name__)

_WORKSPACE_COMPONENT_ID = 'keboola.sandboxes'

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

    Storage data (buckets, tables) and component data (configs, flows,
    transformations, data-apps, workspaces, configuration-rows) are fetched
    concurrently. Output is written to ``<db>.tmp`` and atomically renamed.
    """
    db_path = storage.path_for(session, root=root)
    tmp_path = storage.tmp_path_for(db_path)
    storage.ensure_parent_dirs(db_path, root=root)
    lock_path = db_path.with_suffix(db_path.suffix + '.lock')

    LOG.info('Building search index for project_id=%s at %s', session.project_id, db_path)

    (buckets, table_lists), components = await asyncio.gather(
        _fetch_storage_data(client),
        client.storage_client.component_list(include=['configuration', 'rows']),
    )

    with storage.file_lock(lock_path):
        if tmp_path.exists():
            tmp_path.unlink()

        conn = sqlite3.connect(tmp_path)
        try:
            storage.init_schema(conn, session)
            counts: dict[str, int] = {}
            counts['bucket'] = _insert_bucket_rows(conn, buckets, session)
            counts['table'] = _insert_table_rows(conn, buckets, table_lists, session)
            component_counts = _insert_component_rows(conn, components, session)
            counts.update(component_counts)
            conn.commit()
        finally:
            conn.close()

        storage.atomic_publish(tmp_path, db_path)

    LOG.info('Search index built for project_id=%s: %s', session.project_id, counts)
    return db_path


async def _fetch_storage_data(
    client: KeboolaClient,
) -> tuple[list[JsonDict], list[list[JsonDict]]]:
    """Return ``(buckets, table_lists)`` with one ``tables`` call per bucket in parallel."""
    buckets = await merged_bucket_list(client)
    table_lists = await _fetch_tables_for_buckets(client, buckets)
    return buckets, table_lists


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


def _insert_component_rows(
    conn: sqlite3.Connection, components: list[JsonDict], session: VerifiedSession
) -> dict[str, int]:
    """Walk components and emit one FTS5 row per configuration + per row.

    The ``obj_id`` for a configuration is ``<component_id>:<config_id>``; for a
    configuration row it's ``<component_id>:<config_id>:<row_id>``. Original
    IDs are stored individually in ``metadata`` for hydration into ``SearchHit``.
    """
    rows: list[tuple] = []
    counts: dict[str, int] = {
        'flow': 0,
        'transformation': 0,
        'configuration': 0,
        'configuration-row': 0,
        'data-app': 0,
        'workspace': 0,
    }

    for component in components:
        component_id = component.get('id')
        if not component_id:
            continue
        kind = _derive_component_kind(component_id, component.get('type'))

        for config in component.get('configurations') or []:
            config_id = config.get('id')
            if not config_id:
                continue

            cfg_name = config.get('name') or ''
            cfg_description = config.get('description') or ''
            cfg_updated = _config_updated(config)

            obj_id = f'{component_id}:{config_id}'
            content = ' '.join(filter(None, [component_id, config_id, cfg_name, cfg_description]))
            metadata_json = json.dumps(
                {
                    'name': cfg_name,
                    'description': cfg_description,
                    'updated': cfg_updated,
                    'component_id': component_id,
                    'configuration_id': config_id,
                    # Full configuration body kept so config-based search can run against
                    # the index instead of re-fetching /components live. The data is already
                    # part of the component_list response we just used — no extra API cost.
                    'configuration': config.get('configuration'),
                }
            )
            rows.append((session.project_id, kind, obj_id, cfg_name, cfg_description, content, metadata_json))
            counts[kind] += 1

            for row in config.get('rows') or []:
                row_id = row.get('id')
                if not row_id:
                    continue

                row_name = row.get('name') or ''
                row_description = row.get('description') or ''
                row_updated = cfg_updated or row.get('created') or ''

                row_obj_id = f'{component_id}:{config_id}:{row_id}'
                row_content = ' '.join(filter(None, [component_id, config_id, row_id, row_name, row_description]))
                row_metadata_json = json.dumps(
                    {
                        'name': row_name,
                        'description': row_description,
                        'updated': row_updated,
                        'component_id': component_id,
                        'configuration_id': config_id,
                        'configuration_row_id': row_id,
                        # See parent configuration: row's own JSON body is stored so
                        # config-based search can walk it locally.
                        'configuration': row.get('configuration'),
                    }
                )
                rows.append(
                    (
                        session.project_id,
                        'configuration-row',
                        row_obj_id,
                        row_name,
                        row_description,
                        row_content,
                        row_metadata_json,
                    )
                )
                counts['configuration-row'] += 1

    if rows:
        conn.executemany(_INSERT_SQL, rows)
    return counts


def _derive_component_kind(component_id: str, component_type: str | None) -> str:
    """Map a Keboola component to one of the indexed kinds.

    Matches the classification used in ``tools/search.py::_fetch_configs`` so
    indexed results are interchangeable with live results.
    """
    if component_id in (ORCHESTRATOR_COMPONENT_ID, CONDITIONAL_FLOW_COMPONENT_ID):
        return 'flow'
    if component_type == 'transformation':
        return 'transformation'
    if component_id == _WORKSPACE_COMPONENT_ID:
        return 'workspace'
    if component_id == DATA_APP_COMPONENT_ID:
        return 'data-app'
    if component_type in ('extractor', 'writer', 'application'):
        return 'configuration'
    return 'configuration'


def _config_updated(config: dict[str, Any]) -> str:
    current_version = config.get('currentVersion')
    if isinstance(current_version, dict):
        if created := current_version.get('created'):
            return str(created)
    return str(config.get('created') or '')


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
