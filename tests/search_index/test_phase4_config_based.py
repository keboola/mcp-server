"""Phase 4: config-based search routed through the index.

Covers the storage helper (``list_by_kinds``), the lifecycle wrapper
(``list_index_rows``), and the tool-level adapter
(``_config_based_search_via_index``).
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from keboola_mcp_server.search_index import lifecycle, query, storage
from keboola_mcp_server.search_index.types import VerifiedSession

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _reset_state():
    lifecycle._reset_for_tests()
    yield
    lifecycle._reset_for_tests()


@pytest.fixture
def session() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def db_path(session: VerifiedSession, tmp_path: Path) -> Path:
    path = storage.path_for(session, root=tmp_path)
    storage.ensure_parent_dirs(path, root=tmp_path)
    conn = sqlite3.connect(path)
    try:
        storage.init_schema(conn, session)
        conn.executemany(
            'INSERT INTO search (project_id, kind, obj_id, name, description, content, metadata) '
            'VALUES (?, ?, ?, ?, ?, ?, ?)',
            [
                (
                    session.project_id,
                    'configuration',
                    'cmp:cfg-1',
                    'cfg-1',
                    '',
                    'content',
                    json.dumps(
                        {
                            'component_id': 'cmp',
                            'configuration_id': 'cfg-1',
                            'configuration': {'parameters': {'host': 'example.com'}},
                        }
                    ),
                ),
                (
                    session.project_id,
                    'configuration',
                    'cmp:cfg-2',
                    'cfg-2',
                    '',
                    'content',
                    json.dumps(
                        {
                            'component_id': 'cmp',
                            'configuration_id': 'cfg-2',
                            'configuration': {'parameters': {'host': 'other.example.org'}},
                        }
                    ),
                ),
                (
                    session.project_id,
                    'configuration-row',
                    'cmp:cfg-1:r-1',
                    'r-1',
                    '',
                    'content',
                    json.dumps(
                        {
                            'component_id': 'cmp',
                            'configuration_id': 'cfg-1',
                            'configuration_row_id': 'r-1',
                            'configuration': {'parameters': {'tag': 'production'}},
                        }
                    ),
                ),
                (
                    '5678',
                    'configuration',
                    'cmp:other',
                    'other',
                    '',
                    'content',
                    json.dumps(
                        {
                            'component_id': 'cmp',
                            'configuration_id': 'other',
                            'configuration': {'parameters': {'host': 'should-not-leak.example'}},
                        }
                    ),
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


def test_list_by_kinds_returns_all_rows_of_kinds(db_path, session):
    rows = query.list_by_kinds(db_path, session.project_id, ['configuration'])
    obj_ids = {r.obj_id for r in rows}
    assert obj_ids == {'cmp:cfg-1', 'cmp:cfg-2'}


def test_list_by_kinds_isolates_by_project(db_path):
    rows = query.list_by_kinds(db_path, '1234', ['configuration'])
    assert all(r.metadata.get('component_id') == 'cmp' for r in rows)
    assert 'should-not-leak.example' not in {
        r.metadata.get('configuration', {}).get('parameters', {}).get('host') for r in rows
    }


def test_list_by_kinds_multiple_kinds(db_path, session):
    rows = query.list_by_kinds(db_path, session.project_id, ['configuration', 'configuration-row'])
    kinds = {r.kind for r in rows}
    assert kinds == {'configuration', 'configuration-row'}


def test_list_by_kinds_empty_kind_list_returns_empty(db_path, session):
    assert query.list_by_kinds(db_path, session.project_id, []) == []


@pytest.mark.asyncio
async def test_list_index_rows_returns_indexed_hits(db_path, session, tmp_path):
    rows = await lifecycle.list_index_rows(session, ['configuration'], root=tmp_path)
    assert sorted(r.obj_id for r in rows) == ['cmp:cfg-1', 'cmp:cfg-2']


@pytest.mark.asyncio
async def test_list_index_rows_raises_when_no_db(session, tmp_path):
    with pytest.raises(lifecycle.IndexUnavailable):
        await lifecycle.list_index_rows(session, ['configuration'], root=tmp_path)


@pytest.mark.asyncio
async def test_config_based_search_via_index_walks_configuration_body(db_path, session, tmp_path, monkeypatch):
    """The tool-level adapter walks the cached configuration JSON and produces SearchHit objects."""
    monkeypatch.setenv('KBC_SEARCH_INDEX_DIR', str(tmp_path))

    from keboola_mcp_server.tools import search as search_mod

    spec = search_mod.SearchSpec(
        patterns=['example.com'],
        item_types=['configuration'],
        pattern_mode='literal',
        search_type='config-based',
        search_scopes=['parameters'],
        return_all_matched_patterns=True,
    )

    ctx = MagicMock()
    ctx.session.state = {}

    hits = await search_mod._config_based_search_via_index(session, spec, {'configuration'})

    obj_ids = {(h.component_id, h.configuration_id) for h in hits}
    # Only cfg-1 ("example.com") matches; cfg-2 has "other.example.org" — must NOT match the
    # literal "example.com" pattern.
    assert obj_ids == {('cmp', 'cfg-1')}


@pytest.mark.asyncio
async def test_config_based_search_via_index_rejects_other_project(db_path, session, tmp_path, monkeypatch):
    """Defense in depth: rows stamped with another project_id must be invisible."""
    monkeypatch.setenv('KBC_SEARCH_INDEX_DIR', str(tmp_path))
    from keboola_mcp_server.tools import search as search_mod

    spec = search_mod.SearchSpec(
        patterns=['should-not-leak'],
        item_types=['configuration'],
        pattern_mode='literal',
        search_type='config-based',
        return_all_matched_patterns=True,
    )

    hits = await search_mod._config_based_search_via_index(session, spec, {'configuration'})
    assert hits == []
