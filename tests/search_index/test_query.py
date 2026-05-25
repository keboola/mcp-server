"""Query layer: FTS5 lookups against a populated index.

Uses the real ``init_schema`` + direct SQL inserts so we test the production
schema, not a mock.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from keboola_mcp_server.search_index import storage
from keboola_mcp_server.search_index.query import run_query
from keboola_mcp_server.search_index.types import VerifiedSession

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def session() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def db_path(tmp_path: Path, session: VerifiedSession) -> Path:
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
                    'bucket',
                    'in.c-customers',
                    'customers',
                    'Customer master data',
                    'in.c-customers customers Customer master data',
                    json.dumps({'updated': '2026-05-20'}),
                ),
                (
                    session.project_id,
                    'table',
                    'in.c-customers.orders',
                    'orders',
                    'Order events',
                    'in.c-customers.orders orders Order events revenue total',
                    json.dumps({'updated': '2026-05-21', 'columns': ['id', 'revenue', 'total']}),
                ),
                (
                    session.project_id,
                    'table',
                    'in.c-products.skus',
                    'skus',
                    '',
                    'in.c-products.skus skus inventory products',
                    json.dumps({'updated': '2026-05-22'}),
                ),
                (
                    # Row belonging to a different project, must be invisible.
                    '5678',
                    'table',
                    'in.c-other.private',
                    'private',
                    'leak',
                    'in.c-other.private customers private',
                    json.dumps({'updated': '2026-05-23'}),
                ),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return path


def test_query_returns_hits_for_simple_pattern(db_path, session):
    hits = run_query(db_path, session.project_id, ['customers'])
    obj_ids = {h.obj_id for h in hits}
    assert 'in.c-customers' in obj_ids
    assert 'in.c-customers.orders' in obj_ids


def test_query_respects_kind_filter(db_path, session):
    hits = run_query(db_path, session.project_id, ['customers'], kinds=['bucket'])
    assert [h.obj_id for h in hits] == ['in.c-customers']


def test_query_returns_no_hits_for_other_project(db_path):
    """Defense-in-depth: row stamped with project_id 5678 is invisible to project 1234."""
    hits = run_query(db_path, '1234', ['private'])
    assert hits == []


def test_query_rehydrates_metadata(db_path, session):
    hits = run_query(db_path, session.project_id, ['orders'], kinds=['table'])
    assert len(hits) == 1
    assert hits[0].metadata['columns'] == ['id', 'revenue', 'total']


def test_query_multiple_patterns_or_combine(db_path, session):
    hits = run_query(db_path, session.project_id, ['customers', 'inventory'])
    obj_ids = {h.obj_id for h in hits}
    assert 'in.c-customers' in obj_ids
    assert 'in.c-products.skus' in obj_ids


def test_query_respects_limit(db_path, session):
    hits = run_query(db_path, session.project_id, ['customers'], limit=1)
    assert len(hits) == 1


def test_query_empty_patterns_returns_no_hits(db_path, session):
    assert run_query(db_path, session.project_id, []) == []
    assert run_query(db_path, session.project_id, ['', '  ']) == []


def test_query_escapes_quotes_in_patterns(db_path, session):
    # Must not raise even with embedded quotes.
    hits = run_query(db_path, session.project_id, ['nothing"weird'])
    assert hits == []
