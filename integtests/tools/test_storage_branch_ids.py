"""Integration tests: bucket/table IDs on dev branches must be real physical IDs.

Verifies the fix from PR #464: MCP tools must return real Storage API IDs
on dev branches, not stripped/normalized prod_id values. Tests both branch
storage models (prefix-based and branched-storage) by detecting the
`storage-branches` feature flag from token verification.

These tests use the standard INTEGTEST_STORAGE_TOKENS pool. For full coverage,
the pool should contain at least one project WITH and one WITHOUT the
`storage-branches` feature. If only one type is available, only that type is
tested (no skipping — the available type always runs).
"""

import asyncio
import logging
import uuid
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import pytest_asyncio
from fastmcp import Context
from mcp.server.session import ServerSession
from mcp.shared.context import RequestContext

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.mcp import CONVERSATION_ID, ServerState
from keboola_mcp_server.tools.storage.tools import get_buckets, get_tables
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


async def _wait_for_job(client: KeboolaClient, job: dict, timeout: float = 120.0) -> dict:
    """Poll a Storage API async job until terminal state."""
    job_id = job['id']
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = await client.storage_client.get(endpoint=f'jobs/{job_id}')
        if result.get('status') in ('success', 'error'):
            if result['status'] == 'error':
                raise RuntimeError(f'Storage job {job_id} failed: {result}')
            return result
        await asyncio.sleep(2)
    raise TimeoutError(f'Storage job {job_id} did not complete within {timeout}s')


async def _detect_branch_model(client: KeboolaClient) -> str:
    """Detect whether the project uses branched-storage or prefix-based model."""
    token_info = await client.storage_client.get(endpoint='tokens/verify')
    features = token_info.get('owner', {}).get('features', [])
    return 'branched-storage' if 'storage-branches' in features else 'prefix-based'


def _make_mcp_context(client: KeboolaClient) -> Context:
    """Build a minimal MCP context backed by a real KeboolaClient."""
    ctx = MagicMock(Context)
    ctx.session = MagicMock(ServerSession)
    ctx.session.state = {
        KeboolaClient.STATE_KEY: client,
        WorkspaceManager.STATE_KEY: MagicMock(WorkspaceManager),
        CONVERSATION_ID: 'integration-test-branch-ids',
    }
    ctx.session.client_params = None
    ctx.session_id = None
    ctx.client_id = None
    ctx.request_context = MagicMock(RequestContext)
    ctx.request_context.lifespan_context = ServerState(Config(), ServerRuntimeInfo(transport='stdio'))
    return ctx


@pytest.fixture(scope='module')
def branch_test_clients(storage_api_url: str, storage_api_token: str) -> dict[str, Any]:
    """Classify the test project by its branch model.

    Returns dict with keys: client, model ('prefix-based' or 'branched-storage').
    Uses the standard integtest pool token acquired by the project_lock fixture.
    """
    import asyncio

    client = KeboolaClient(storage_api_url=storage_api_url, storage_api_token=storage_api_token)
    model = asyncio.get_event_loop().run_until_complete(_detect_branch_model(client))
    LOG.info(f'Branch model for test project: {model}')
    return {'client': client, 'model': model}


@pytest_asyncio.fixture
async def dev_branch(branch_test_clients: dict[str, Any]):
    """Create a temporary dev branch, yield its ID, then delete it."""
    client = branch_test_clients['client']
    branch_name = f'integ-branchid-{uuid.uuid4().hex[:8]}'

    job = await client.storage_client.post(
        endpoint='dev-branches',
        data={'name': branch_name, 'description': 'PR #464 branch ID integration test'},
    )
    completed = await _wait_for_job(client, job)
    branch_id = str(completed['results']['id'])
    LOG.info(f'Created dev branch {branch_id} ({branch_name})')

    yield branch_id

    try:
        await client.storage_client.delete(endpoint=f'dev-branches/{branch_id}')
        LOG.info(f'Deleted dev branch {branch_id}')
    except Exception as exc:
        LOG.warning(f'Failed to delete dev branch {branch_id}: {exc}')


@pytest_asyncio.fixture
async def branch_client(branch_test_clients: dict[str, Any], dev_branch: str) -> KeboolaClient:
    """Create a KeboolaClient scoped to the dev branch."""
    return await branch_test_clients['client'].with_branch_id(dev_branch)


@pytest_asyncio.fixture
async def test_bucket(branch_client: KeboolaClient) -> str:
    """Create a bucket on the dev branch, yield its ID."""
    bucket_name = f'test-{uuid.uuid4().hex[:8]}'
    result = await branch_client.storage_client.post(
        endpoint='buckets',
        data={'name': bucket_name, 'stage': 'in', 'description': 'PR #464 integration test'},
    )
    bucket_id = result['id']
    LOG.info(f'Created test bucket {bucket_id}')

    yield bucket_id

    try:
        await branch_client.storage_client.raw_client.delete(
            endpoint=f'buckets/{bucket_id}?force=true&async=1',
        )
    except Exception:
        pass


@pytest_asyncio.fixture
async def test_table(branch_client: KeboolaClient, test_bucket: str) -> str:
    """Create a table in the test bucket (async — poll job)."""
    table_name = f'test_{uuid.uuid4().hex[:8]}'
    job = await branch_client.storage_client.post(
        endpoint=f'buckets/{test_bucket}/tables-definition',
        data={
            'name': table_name,
            'columns': [
                {'name': 'id', 'definition': {'type': 'INTEGER'}},
                {'name': 'name', 'definition': {'type': 'VARCHAR', 'length': '255'}},
            ],
        },
    )
    completed = await _wait_for_job(branch_client, job)
    table_id = completed['results']['id']
    LOG.info(f'Created test table {table_id}')
    return table_id


@pytest.fixture
def branch_mcp_context(branch_client: KeboolaClient) -> Context:
    """MCP context backed by the dev-branch client."""
    return _make_mcp_context(branch_client)


# ── Bucket ID tests ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_bucket_by_id_returns_real_id(
    branch_mcp_context: Context, test_bucket: str, branch_test_clients: dict
):
    """Fetching a dev-branch bucket by its real ID must return that same ID."""
    result = await get_buckets(branch_mcp_context, bucket_ids=[test_bucket])

    assert result.buckets, f'No buckets returned for ID {test_bucket}'
    returned_id = result.buckets[0].id
    model = branch_test_clients['model']

    assert returned_id == test_bucket, (
        f'[{model}] get_buckets returned id={returned_id!r} but the real bucket id is '
        f'{test_bucket!r}. The branch prefix was likely stripped (prod_id bug).'
    )


@pytest.mark.asyncio
async def test_list_all_buckets_includes_dev_bucket(
    branch_mcp_context: Context, test_bucket: str, branch_test_clients: dict
):
    """Listing all buckets must include our dev-branch bucket with its real ID."""
    result = await get_buckets(branch_mcp_context, bucket_ids=[])
    all_ids = [b.id for b in result.buckets]
    model = branch_test_clients['model']

    assert test_bucket in all_ids, (
        f'[{model}] Test bucket {test_bucket!r} not found in bucket list. Found: {all_ids}'
    )


@pytest.mark.asyncio
async def test_returned_bucket_id_is_api_callable(
    branch_mcp_context: Context, branch_client: KeboolaClient, test_bucket: str, branch_test_clients: dict
):
    """The ID returned by get_buckets must work in subsequent Storage API calls."""
    result = await get_buckets(branch_mcp_context, bucket_ids=[test_bucket])
    assert result.buckets
    returned_id = result.buckets[0].id
    model = branch_test_clients['model']

    try:
        detail = await branch_client.storage_client.bucket_detail(returned_id)
        assert detail['id'] == returned_id
    except httpx.HTTPStatusError as exc:
        pytest.fail(
            f'[{model}] Storage API rejected bucket ID from get_buckets: '
            f'{returned_id!r} -> HTTP {exc.response.status_code}.'
        )


# ── Table ID tests ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_tables_returns_real_id(
    branch_mcp_context: Context, test_table: str, test_bucket: str, branch_test_clients: dict
):
    """get_tables must return the real table ID, not a stripped version."""
    result = await get_tables(branch_mcp_context, bucket_ids=[test_bucket])
    all_table_ids = [t.id for t in result.tables]
    model = branch_test_clients['model']

    assert test_table in all_table_ids, (
        f'[{model}] Table {test_table!r} not found in get_tables output. '
        f'Found: {all_table_ids}. IDs may have been stripped.'
    )


@pytest.mark.asyncio
async def test_returned_table_id_is_api_callable(
    branch_mcp_context: Context, branch_client: KeboolaClient, test_table: str, test_bucket: str,
    branch_test_clients: dict
):
    """The table ID returned by get_tables must work in subsequent API calls."""
    result = await get_tables(branch_mcp_context, bucket_ids=[test_bucket])
    returned_table = next((t for t in result.tables if t.id == test_table), None)
    model = branch_test_clients['model']

    assert returned_table, (
        f'[{model}] Table {test_table!r} not in get_tables output: '
        f'{[t.id for t in result.tables]}'
    )

    try:
        detail = await branch_client.storage_client.table_detail(returned_table.id)
        assert detail['id'] == returned_table.id
    except httpx.HTTPStatusError as exc:
        pytest.fail(
            f'[{model}] Storage API rejected table ID from get_tables: '
            f'{returned_table.id!r} -> HTTP {exc.response.status_code}.'
        )
