"""Builder: turns Keboola data into an FTS5 index.

The Keboola client is mocked. We verify the on-disk DB contents directly so
we test the actual SQL inserts, not the mock interactions.
"""

import sqlite3
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.search_index import builder, storage
from keboola_mcp_server.search_index.types import VerifiedSession

_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)

_BUCKETS = [
    {
        'id': 'in.c-customers',
        'name': 'customers',
        'displayName': 'Customers',
        'metadata': [{'key': 'KBC.description', 'value': 'Customer master data'}],
        'lastChangeDate': '2026-05-20T10:00:00+0000',
    },
    {
        'id': 'in.c-products',
        'name': 'products',
        'displayName': 'Products',
        'metadata': [],
        'lastChangeDate': '2026-05-21T10:00:00+0000',
    },
]

_TABLES_BY_BUCKET = {
    'in.c-customers': [
        {
            'id': 'in.c-customers.orders',
            'name': 'orders',
            'displayName': 'Orders',
            'columns': ['id', 'customer_id', 'revenue'],
            'columnMetadata': {
                'revenue': [{'key': 'KBC.description', 'value': 'Order revenue in USD'}],
            },
            'lastChangeDate': '2026-05-22T10:00:00+0000',
            'metadata': [],
        },
    ],
    'in.c-products': [],
}


@pytest.fixture
def session() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def client(mocker) -> KeboolaClient:
    return mocker.AsyncMock(KeboolaClient)


@pytest.fixture
def _patched_fetchers():
    async def fake_bucket_list(_client, **_kw):
        return _BUCKETS

    async def fake_table_list(_client, bucket_id, **_kw):
        return _TABLES_BY_BUCKET[bucket_id]

    with (
        patch('keboola_mcp_server.search_index.builder.merged_bucket_list', AsyncMock(side_effect=fake_bucket_list)),
        patch(
            'keboola_mcp_server.search_index.builder.merged_bucket_table_list', AsyncMock(side_effect=fake_table_list)
        ),
    ):
        yield


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_creates_db_with_buckets_and_tables(client, session, tmp_path):
    db_path = await builder.build_index(session, client, root=tmp_path)
    assert db_path.exists()

    conn = sqlite3.connect(db_path)
    try:
        kinds = dict(
            conn.execute(
                'SELECT kind, COUNT(*) FROM search WHERE project_id = ? GROUP BY kind',
                (session.project_id,),
            ).fetchall()
        )
    finally:
        conn.close()

    assert kinds == {'bucket': 2, 'table': 1}


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_includes_column_names_in_content(client, session, tmp_path):
    db_path = await builder.build_index(session, client, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            'SELECT obj_id FROM search WHERE search MATCH ? AND project_id = ?',
            ('revenue', session.project_id),
        ).fetchall()
    finally:
        conn.close()

    assert ('in.c-customers.orders',) in rows


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_atomically_replaces_existing(client, session, tmp_path):
    db_path = await builder.build_index(session, client, root=tmp_path)
    first_mtime = db_path.stat().st_mtime

    # Rebuild — should replace the existing file in place.
    db_path2 = await builder.build_index(session, client, root=tmp_path)
    assert db_path2 == db_path
    assert db_path.stat().st_mtime >= first_mtime
    # Tmp file is cleaned up after publish.
    assert not storage.tmp_path_for(db_path).exists()


@pytest.mark.asyncio
async def test_build_index_fetches_bucket_list_once(client, session, tmp_path):
    """Regression: previous builder called merged_bucket_list separately from
    _populate_buckets and _populate_tables, doubling /buckets traffic."""
    bucket_call_count = 0

    async def counting_bucket_list(_client, **_kw):
        nonlocal bucket_call_count
        bucket_call_count += 1
        return _BUCKETS

    async def fake_table_list(_client, bucket_id, **_kw):
        return _TABLES_BY_BUCKET[bucket_id]

    with (
        patch(
            'keboola_mcp_server.search_index.builder.merged_bucket_list',
            AsyncMock(side_effect=counting_bucket_list),
        ),
        patch(
            'keboola_mcp_server.search_index.builder.merged_bucket_table_list',
            AsyncMock(side_effect=fake_table_list),
        ),
    ):
        await builder.build_index(session, client, root=tmp_path)

    assert bucket_call_count == 1


@pytest.mark.asyncio
async def test_build_index_fetches_tables_in_parallel(client, session, tmp_path):
    """Per-bucket table fetches must run concurrently, not sequentially."""
    import asyncio
    import time

    async def fake_bucket_list(_client, **_kw):
        return _BUCKETS

    table_call_times: list[float] = []

    async def slow_table_list(_client, bucket_id, **_kw):
        table_call_times.append(time.monotonic())
        await asyncio.sleep(0.05)
        return _TABLES_BY_BUCKET[bucket_id]

    with (
        patch(
            'keboola_mcp_server.search_index.builder.merged_bucket_list',
            AsyncMock(side_effect=fake_bucket_list),
        ),
        patch(
            'keboola_mcp_server.search_index.builder.merged_bucket_table_list',
            AsyncMock(side_effect=slow_table_list),
        ),
    ):
        t0 = time.monotonic()
        await builder.build_index(session, client, root=tmp_path)
        elapsed = time.monotonic() - t0

    # Two buckets × 0.05s sequential would be ≥ 0.10s. Parallel should be ~0.05s.
    assert elapsed < 0.09, f'tables were not fetched in parallel: elapsed={elapsed:.3f}s'
    # Start times should be within a few ms of each other.
    assert max(table_call_times) - min(table_call_times) < 0.01


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_records_description_from_metadata(client, session, tmp_path):
    db_path = await builder.build_index(session, client, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        descriptions = dict(
            conn.execute(
                'SELECT obj_id, description FROM search WHERE project_id = ? AND kind = ?',
                (session.project_id, 'bucket'),
            ).fetchall()
        )
    finally:
        conn.close()
    assert descriptions['in.c-customers'] == 'Customer master data'
    assert descriptions['in.c-products'] == ''
