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

_COMPONENTS: list[dict] = [
    {
        'id': 'keboola.ex-google-analytics',
        'type': 'extractor',
        'configurations': [
            {
                'id': '111',
                'name': 'GA pipeline',
                'description': 'Daily GA pull',
                'currentVersion': {'created': '2026-05-10T10:00:00+0000'},
                'configuration': {
                    'parameters': {'profile_id': 'UA-12345', 'metrics': ['sessions', 'pageviews']},
                    'storage': {'output': {'tables': [{'destination': 'in.c-ga.events'}]}},
                },
                'rows': [
                    {
                        'id': 'r1',
                        'name': 'profile',
                        'description': 'sessions row',
                        'created': '2026-05-10',
                        'configuration': {'parameters': {'metric': 'sessions'}},
                    },
                ],
            }
        ],
    },
    {
        'id': 'keboola.snowflake-transformation',
        'type': 'transformation',
        'configurations': [
            {'id': '222', 'name': 'denormalize', 'description': '', 'created': '2026-05-11'},
        ],
    },
    {
        'id': 'keboola.orchestrator',
        'type': 'other',
        'configurations': [
            {'id': '333', 'name': 'nightly_flow', 'description': '', 'created': '2026-05-12'},
        ],
    },
    {
        'id': 'keboola.data-apps',
        'type': 'application',
        'configurations': [
            {'id': '444', 'name': 'sales_dashboard', 'description': '', 'created': '2026-05-13'},
        ],
    },
    {
        'id': 'keboola.sandboxes',
        'type': 'other',
        'configurations': [
            {'id': '555', 'name': 'snowflake-ws', 'description': '', 'created': '2026-05-14'},
        ],
    },
]


@pytest.fixture
def session() -> VerifiedSession:
    return VerifiedSession(project_id='1234', token_hash='abcdef0123456789', verified_at=_NOW)


@pytest.fixture
def client(mocker) -> KeboolaClient:
    client = mocker.AsyncMock(KeboolaClient)
    client.storage_client.component_list = AsyncMock(return_value=_COMPONENTS)
    return client


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
async def test_build_index_creates_db_with_all_kinds(client, session, tmp_path):
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

    assert kinds == {
        'bucket': 2,
        'table': 1,
        'configuration': 1,
        'configuration-row': 1,
        'transformation': 1,
        'flow': 1,
        'data-app': 1,
        'workspace': 1,
    }


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_stores_configuration_body_for_phase4(client, session, tmp_path):
    """Phase 4: full ``configuration`` JSON body must be present in metadata so
    config-based search can walk it locally without a live ``component_list`` call."""
    import json

    db_path = await builder.build_index(session, client, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        cfg_row = conn.execute(
            'SELECT metadata FROM search WHERE kind = ? AND obj_id = ?',
            ('configuration', 'keboola.ex-google-analytics:111'),
        ).fetchone()
        row_row = conn.execute(
            'SELECT metadata FROM search WHERE kind = ? AND obj_id = ?',
            ('configuration-row', 'keboola.ex-google-analytics:111:r1'),
        ).fetchone()
    finally:
        conn.close()

    cfg_meta = json.loads(cfg_row[0])
    assert cfg_meta['configuration'] == {
        'parameters': {'profile_id': 'UA-12345', 'metrics': ['sessions', 'pageviews']},
        'storage': {'output': {'tables': [{'destination': 'in.c-ga.events'}]}},
    }

    row_meta = json.loads(row_row[0])
    assert row_meta['configuration'] == {'parameters': {'metric': 'sessions'}}


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_obj_id_layout_for_components(client, session, tmp_path):
    db_path = await builder.build_index(session, client, root=tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        configuration = conn.execute(
            'SELECT obj_id, metadata FROM search WHERE kind = ?', ('configuration',)
        ).fetchone()
        row = conn.execute('SELECT obj_id, metadata FROM search WHERE kind = ?', ('configuration-row',)).fetchone()
        flow = conn.execute('SELECT obj_id FROM search WHERE kind = ?', ('flow',)).fetchone()
    finally:
        conn.close()

    import json

    assert configuration[0] == 'keboola.ex-google-analytics:111'
    cfg_meta = json.loads(configuration[1])
    assert cfg_meta['component_id'] == 'keboola.ex-google-analytics'
    assert cfg_meta['configuration_id'] == '111'

    assert row[0] == 'keboola.ex-google-analytics:111:r1'
    row_meta = json.loads(row[1])
    assert row_meta['configuration_row_id'] == 'r1'

    assert flow[0] == 'keboola.orchestrator:333'


@pytest.mark.asyncio
@pytest.mark.usefixtures('_patched_fetchers')
async def test_build_index_storage_and_components_fetched_concurrently(client, session, tmp_path, monkeypatch):
    """Storage data and component_list should fan out in parallel, not sequentially."""
    import asyncio
    import time

    storage_started: list[float] = []
    storage_finished: list[float] = []
    component_started: list[float] = []
    component_finished: list[float] = []

    async def slow_bucket_list(_client, **_kw):
        storage_started.append(time.monotonic())
        await asyncio.sleep(0.05)
        storage_finished.append(time.monotonic())
        return _BUCKETS

    async def fast_table_list(_client, bucket_id, **_kw):
        return _TABLES_BY_BUCKET[bucket_id]

    async def slow_component_list(**_kw):
        component_started.append(time.monotonic())
        await asyncio.sleep(0.05)
        component_finished.append(time.monotonic())
        return _COMPONENTS

    client.storage_client.component_list = slow_component_list
    monkeypatch.setattr(
        'keboola_mcp_server.search_index.builder.merged_bucket_list',
        AsyncMock(side_effect=slow_bucket_list),
    )
    monkeypatch.setattr(
        'keboola_mcp_server.search_index.builder.merged_bucket_table_list',
        AsyncMock(side_effect=fast_table_list),
    )

    await builder.build_index(session, client, root=tmp_path)

    # Both branches should overlap, not stack.
    assert min(storage_started[0], component_started[0]) <= max(storage_started[0], component_started[0]) + 0.01


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
