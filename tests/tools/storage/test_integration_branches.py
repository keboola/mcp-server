"""Integration tests for storage tools on real projects with dev branches.

Verifies that bucket/table IDs returned by MCP tools on dev branches
are real, physical Storage API IDs that work in subsequent API calls.

Tests BOTH branch models:
  - Prefix-based (branched storage OFF): bucket IDs have c-{branch_id}- prefix
  - Branched storage (ON): bucket IDs are the same as production

Requires environment variables:
    TEST_KBC_TOKEN_PREFIX: Token for a project with branched storage OFF
    TEST_KBC_TOKEN_BRANCHED: Token for a project with branched storage ON
    TEST_KBC_URL: Storage API URL (default: https://connection.keboola.com)

Run with:
    TEST_KBC_TOKEN_PREFIX=xxx TEST_KBC_TOKEN_BRANCHED=yyy \
        pytest tests/tools/storage/test_integration_branches.py -v

Skip if env vars not set (safe for CI without credentials).
"""

import asyncio
import os
import uuid
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

TOKEN_PREFIX = os.environ.get('TEST_KBC_TOKEN_PREFIX')
TOKEN_BRANCHED = os.environ.get('TEST_KBC_TOKEN_BRANCHED')
STORAGE_URL = os.environ.get('TEST_KBC_URL', 'https://connection.keboola.com')

# Parametrize: each test runs against both project types
_PARAMS = []
if TOKEN_PREFIX:
    _PARAMS.append(pytest.param((TOKEN_PREFIX, False), id='prefix-based'))
if TOKEN_BRANCHED:
    _PARAMS.append(pytest.param((TOKEN_BRANCHED, True), id='branched-storage'))

pytestmark = [
    pytest.mark.skipif(not _PARAMS, reason='TEST_KBC_TOKEN_PREFIX or TEST_KBC_TOKEN_BRANCHED required'),
    pytest.mark.asyncio,
]


async def _wait_for_job(client: KeboolaClient, job: dict, timeout: float = 120.0) -> dict:
    """Poll a Storage API async job until it reaches a terminal state."""
    job_id = job['id']
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        result = await client.storage_client.get(endpoint=f'jobs/{job_id}')
        status = result.get('status')
        if status in ('success', 'error'):
            if status == 'error':
                raise RuntimeError(f'Storage job {job_id} failed: {result}')
            return result
        await asyncio.sleep(2)
    raise TimeoutError(f'Storage job {job_id} did not complete within {timeout}s')


async def _verify_feature(client: KeboolaClient, expect_branched: bool) -> None:
    """Assert the project has (or lacks) the storage-branches feature."""
    token_info = await client.storage_client.get(endpoint='tokens/verify')
    features = token_info.get('owner', {}).get('features', [])
    has_branched = 'storage-branches' in features
    if expect_branched and not has_branched:
        pytest.skip('Project does not have storage-branches feature (expected ON)')
    if not expect_branched and has_branched:
        pytest.skip('Project has storage-branches feature (expected OFF)')


@pytest_asyncio.fixture(params=_PARAMS)
async def prod_client(request):
    """Create a real KeboolaClient and verify the project features match."""
    token, is_branched = request.param
    client = KeboolaClient(storage_api_url=STORAGE_URL, storage_api_token=token)
    await _verify_feature(client, is_branched)
    return client


@pytest_asyncio.fixture
async def dev_branch(prod_client):
    """Create a temporary dev branch, yield its ID, then delete it."""
    branch_name = f'integ-{uuid.uuid4().hex[:8]}'

    job = await prod_client.storage_client.post(
        endpoint='dev-branches',
        data={'name': branch_name, 'description': 'Integration test PR #464'},
    )
    completed = await _wait_for_job(prod_client, job)
    branch_id = str(completed['results']['id'])

    yield branch_id

    try:
        await prod_client.storage_client.delete(endpoint=f'dev-branches/{branch_id}')
    except Exception:
        pass


@pytest_asyncio.fixture
async def branch_client(prod_client, dev_branch):
    """Create a KeboolaClient scoped to the dev branch."""
    return await prod_client.with_branch_id(dev_branch)


@pytest_asyncio.fixture
async def test_bucket(branch_client):
    """Create a bucket on the dev branch, yield its ID."""
    bucket_name = f'test-{uuid.uuid4().hex[:8]}'

    result = await branch_client.storage_client.post(
        endpoint='buckets',
        data={'name': bucket_name, 'stage': 'in', 'description': 'PR #464 integration test'},
    )
    bucket_id = result['id']

    yield bucket_id

    # Cleanup is best-effort; branch deletion cleans up its buckets
    try:
        await branch_client.storage_client.raw_client.delete(
            endpoint=f'buckets/{bucket_id}?force=true&async=1',
        )
    except Exception:
        pass


@pytest_asyncio.fixture
async def test_table(branch_client, test_bucket):
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

    yield table_id


@pytest.fixture
def mcp_context(branch_client):
    """Build a minimal MCP context backed by the real branch client."""
    ctx = MagicMock(Context)
    ctx.session = MagicMock(ServerSession)
    ctx.session.state = {
        KeboolaClient.STATE_KEY: branch_client,
        WorkspaceManager.STATE_KEY: MagicMock(WorkspaceManager),
        CONVERSATION_ID: 'integration-test',
    }
    ctx.session.client_params = None
    ctx.session_id = None
    ctx.client_id = None
    ctx.request_context = MagicMock(RequestContext)
    ctx.request_context.lifespan_context = ServerState(Config(), ServerRuntimeInfo(transport='stdio'))
    return ctx


class TestDevBranchBucketIds:
    """get_buckets must return real (physical) bucket IDs on dev branches."""

    async def test_get_bucket_by_id_returns_same_id(self, mcp_context, test_bucket):
        """Fetching a dev-branch bucket by its real ID must return that same ID."""
        result = await get_buckets(mcp_context, bucket_ids=[test_bucket])

        assert result.buckets, f'No buckets returned for ID {test_bucket}'
        returned_id = result.buckets[0].id

        assert returned_id == test_bucket, (
            f'get_buckets returned id={returned_id!r} but the real bucket id is '
            f'{test_bucket!r}. The branch prefix was likely stripped (prod_id bug).'
        )

    async def test_list_all_buckets_includes_dev_bucket(self, mcp_context, test_bucket):
        """Listing all buckets must include our dev-branch bucket with its real ID."""
        result = await get_buckets(mcp_context, bucket_ids=[])

        all_ids = [b.id for b in result.buckets]
        assert test_bucket in all_ids, (
            f'Test bucket {test_bucket!r} not found in bucket list. Found: {all_ids}'
        )

    async def test_returned_id_is_api_callable(self, mcp_context, branch_client, test_bucket):
        """The ID returned by get_buckets must work in subsequent Storage API calls."""
        result = await get_buckets(mcp_context, bucket_ids=[test_bucket])
        assert result.buckets
        returned_id = result.buckets[0].id

        try:
            detail = await branch_client.storage_client.bucket_detail(returned_id)
            assert detail['id'] == returned_id
        except httpx.HTTPStatusError as exc:
            pytest.fail(
                f'Storage API rejected bucket ID from get_buckets: '
                f'{returned_id!r} -> HTTP {exc.response.status_code}. '
                f'This means the returned ID is not a real physical ID.'
            )


class TestDevBranchTableIds:
    """get_tables must return real (physical) table IDs on dev branches."""

    async def test_get_tables_returns_real_id(self, mcp_context, test_table, test_bucket):
        """get_tables must return the real table ID, not a stripped version."""
        result = await get_tables(mcp_context, bucket_ids=[test_bucket])

        all_table_ids = [t.id for t in result.tables]
        assert test_table in all_table_ids, (
            f'Table {test_table!r} not found in get_tables output. '
            f'Found: {all_table_ids}. IDs may have been stripped.'
        )

    async def test_returned_table_id_is_api_callable(self, mcp_context, branch_client, test_table, test_bucket):
        """The table ID returned by get_tables must work in subsequent API calls."""
        result = await get_tables(mcp_context, bucket_ids=[test_bucket])

        returned_table = next((t for t in result.tables if t.id == test_table), None)
        assert returned_table, (
            f'Table {test_table!r} not in get_tables output: '
            f'{[t.id for t in result.tables]}'
        )

        try:
            detail = await branch_client.storage_client.table_detail(returned_table.id)
            assert detail['id'] == returned_table.id
        except httpx.HTTPStatusError as exc:
            pytest.fail(
                f'Storage API rejected table ID from get_tables: '
                f'{returned_table.id!r} -> HTTP {exc.response.status_code}.'
            )
