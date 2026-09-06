"""Integration tests for the merge-request tools (Branches 2.0, non-SOX flow).

They need a dedicated project WITH the `branches-merge-requests` feature, addressed by the
`INTEGTEST_STORAGE_TOKEN_MERGE_REQUESTS` token (outside the pool). The whole module is skipped when the
variable is not set, so the suite stays green until such a project exists.

A merge deletes the source branch, so every test creates its own branch; production configurations
created for the conflict scenario are deleted afterwards.
"""

import json
import logging
import os
import time
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
import pytest_asyncio
from fastmcp import Context
from mcp.server.session import ServerSession
from mcp.types import ClientCapabilities, InitializeRequestParams

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import ServerRuntimeInfo, ServerState
from keboola_mcp_server.tools.merge_requests.models import MergeRequestsDetailOutput
from keboola_mcp_server.tools.merge_requests.tools import (
    create_merge_request,
    get_merge_request_conflicts,
    get_merge_requests,
    merge_merge_request,
    resolve_merge_request_conflict,
)
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

MERGE_REQUESTS_TOKEN_ENV_VAR = 'INTEGTEST_STORAGE_TOKEN_MERGE_REQUESTS'
COMPONENT_ID = 'keboola.python-transformation-v2'
BRANCH_GONE_TIMEOUT_SEC = 120


def _api_request(method: str, url: str, token: str, **kwargs: Any) -> Any:
    headers = {'X-StorageApi-Token': token, 'Content-Type': 'application/json'}
    resp = httpx.request(method, url, headers=headers, timeout=60, **kwargs)
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _wait_for_storage_job(base_url: str, token: str, job_id: str, timeout: int = 120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = _api_request('GET', f'{base_url}/v2/storage/jobs/{job_id}', token)
        if job.get('status') == 'success':
            return job
        if job.get('status') in ('error', 'cancelled'):
            raise RuntimeError(f'Storage job {job_id} failed: {job}')
        time.sleep(2)
    raise TimeoutError(f'Storage job {job_id} did not complete within {timeout}s')


def _create_branch(base_url: str, token: str, name: str) -> str:
    job = _api_request('POST', f'{base_url}/v2/storage/dev-branches', token, json={'name': name})
    job = _wait_for_storage_job(base_url, token, str(job['id']))
    return str(job['results']['id'])


def _branch_exists(base_url: str, token: str, branch_id: str) -> bool:
    try:
        _api_request('GET', f'{base_url}/v2/storage/dev-branches/{branch_id}', token)
        return True
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return False
        raise


def _delete_branch(base_url: str, token: str, branch_id: str) -> None:
    try:
        if _branch_exists(base_url, token, branch_id):
            job = _api_request('DELETE', f'{base_url}/v2/storage/dev-branches/{branch_id}', token)
            _wait_for_storage_job(base_url, token, str(job['id']))
    except Exception:
        LOG.exception(f'Failed to delete branch {branch_id}')


def _config_path(base_url: str, branch_id: str | None, config_id: str | None = None) -> str:
    prefix = f'{base_url}/v2/storage' + (f'/branch/{branch_id}' if branch_id else '')
    return f'{prefix}/components/{COMPONENT_ID}/configs' + (f'/{config_id}' if config_id else '')


def _create_config(base_url: str, token: str, branch_id: str | None, name: str, parameters: dict) -> str:
    result = _api_request(
        'POST',
        _config_path(base_url, branch_id),
        token,
        json={
            'name': name,
            'description': 'Integration test config',
            'configuration': json.dumps({'parameters': parameters}),
        },
    )
    return str(result['id'])


def _update_config(
    base_url: str, token: str, branch_id: str | None, config_id: str, parameters: dict, change: str
) -> None:
    _api_request(
        'PUT',
        _config_path(base_url, branch_id, config_id),
        token,
        json={'configuration': json.dumps({'parameters': parameters}), 'changeDescription': change},
    )


def _delete_config(base_url: str, token: str, config_id: str) -> None:
    try:
        _api_request('DELETE', _config_path(base_url, None, config_id), token)
    except Exception:
        LOG.exception(f'Failed to delete production config {config_id}')


@dataclass
class MergeRequestProject:
    storage_api_url: str
    storage_api_token: str


@pytest.fixture(scope='session')
def mr_project(storage_api_url: str, env_file_loaded: bool) -> MergeRequestProject:
    """A dedicated project with the `branches-merge-requests` feature; skips the module when not configured."""
    token = os.getenv(MERGE_REQUESTS_TOKEN_ENV_VAR, '').strip()
    if not token:
        pytest.skip(f'{MERGE_REQUESTS_TOKEN_ENV_VAR} not set; skipping merge-request integration tests.')
    token_info = _api_request('GET', f'{storage_api_url}/v2/storage/tokens/verify', token)
    features = token_info.get('owner', {}).get('features', [])
    if 'branches-merge-requests' not in features:
        pytest.fail(f'project {token_info["owner"]["name"]!r} must have the branches-merge-requests feature enabled')
    return MergeRequestProject(storage_api_url=storage_api_url, storage_api_token=token)


@pytest.fixture
def dev_branch(mr_project: MergeRequestProject) -> Generator[str, Any, None]:
    """A fresh development branch per test (a merge deletes it)."""
    branch_id = _create_branch(
        mr_project.storage_api_url, mr_project.storage_api_token, f'integtest-mr-{uuid.uuid4().hex[:8]}'
    )
    try:
        yield branch_id
    finally:
        _delete_branch(mr_project.storage_api_url, mr_project.storage_api_token, branch_id)


async def _build_context(mocker, project: MergeRequestProject, *, branch_id: str | None) -> Context:
    keboola_client = KeboolaClient(
        storage_api_url=project.storage_api_url,
        legacy_storage_token=project.storage_api_token,
        headers={'User-Agent': 'KeboolaMCPServer/integtest'},
    )
    if branch_id is not None:
        keboola_client = await keboola_client.with_branch_id(branch_id)
    workspace_manager = await WorkspaceManager.create(keboola_client)
    ctx = mocker.MagicMock(Context)
    ctx.session = mocker.MagicMock(ServerSession)
    ctx.session.state = {KeboolaClient.STATE_KEY: keboola_client, WorkspaceManager.STATE_KEY: workspace_manager}
    ctx.session.client_params = InitializeRequestParams(
        protocolVersion='1', capabilities=ClientCapabilities(), clientInfo={'name': 'integtest-mr', 'version': '0.0.1'}
    )
    ctx.client_id = 'KeboolaMCPServer/integtest'
    ctx.session_id = None
    ctx.request_context = mocker.MagicMock()
    ctx.request_context.lifespan_context = ServerState(
        Config(storage_api_url=project.storage_api_url, storage_token=project.storage_api_token),
        ServerRuntimeInfo(transport='stdio'),
    )
    return ctx


@pytest_asyncio.fixture
async def branch_context(mocker, mr_project: MergeRequestProject, dev_branch: str) -> Context:
    return await _build_context(mocker, mr_project, branch_id=dev_branch)


@pytest_asyncio.fixture
async def production_context(mocker, mr_project: MergeRequestProject) -> Context:
    return await _build_context(mocker, mr_project, branch_id=None)


def _wait_until_branch_gone(project: MergeRequestProject, branch_id: str) -> None:
    deadline = time.time() + BRANCH_GONE_TIMEOUT_SEC
    while time.time() < deadline:
        if not _branch_exists(project.storage_api_url, project.storage_api_token, branch_id):
            return
        time.sleep(3)
    raise TimeoutError(f'Source branch {branch_id} still exists {BRANCH_GONE_TIMEOUT_SEC}s after the merge')


@pytest.mark.asyncio
async def test_happy_path_create_then_merge(
    branch_context: Context, production_context: Context, mr_project: MergeRequestProject, dev_branch: str
) -> None:
    """Non-SOX default (0 approvals): create → merge, no review step; the MR is published and the branch is deleted."""
    config_id = _create_config(
        mr_project.storage_api_url,
        mr_project.storage_api_token,
        dev_branch,
        f'integtest-new-{uuid.uuid4().hex[:6]}',
        {'x': 1},
    )
    try:
        created = await create_merge_request(branch_context, title='integtest happy path')
        assert created.state == 'development'
        assert created.status.mergeable is True
        assert 'merge_merge_request' in created.status.next_step

        merged = await merge_merge_request(branch_context)
        assert merged.merged is True, merged
        assert merged.state == 'published'
        assert merged.source_branch_deleting is True
        assert 'production' in merged.next_step

        detail = await get_merge_requests(production_context, merge_request_ids=[created.id])
        assert isinstance(detail, MergeRequestsDetailOutput)
        published = detail.merge_requests[0]
        assert published.derived_state == 'merged'
        assert {c.configuration_id for c in published.changed_configurations} == {config_id}

        _wait_until_branch_gone(mr_project, dev_branch)
    finally:
        _delete_config(mr_project.storage_api_url, mr_project.storage_api_token, config_id)


@pytest.mark.asyncio
async def test_conflict_is_reported_and_resolved(mocker, mr_project: MergeRequestProject) -> None:
    """Change the same configuration on the branch and in production → conflict → take ours → merge succeeds."""
    url, token = mr_project.storage_api_url, mr_project.storage_api_token
    # A conflict needs the configuration on both sides, so it must exist in production BEFORE the branch is
    # created (a development branch is a copy of production at creation time).
    config_id = _create_config(url, token, None, f'integtest-conflict-{uuid.uuid4().hex[:6]}', {'query': 'SELECT 1'})
    branch_id = _create_branch(url, token, f'integtest-mr-conflict-{uuid.uuid4().hex[:8]}')
    try:
        ctx = await _build_context(mocker, mr_project, branch_id=branch_id)
        _update_config(url, token, branch_id, config_id, {'query': 'SELECT 2'}, 'branch change')
        _update_config(url, token, None, config_id, {'query': 'SELECT 3'}, 'production change')

        created = await create_merge_request(ctx, title='integtest conflict')
        assert created.status.merge_blockers == ['conflicts']
        assert created.status.mergeable is False

        refused = await merge_merge_request(ctx)
        assert refused.merged is False
        assert refused.refusal == 'conflicts'
        assert refused.conflicts is not None and {c.configuration_id for c in refused.conflicts} == {config_id}

        conflicts = await get_merge_request_conflicts(ctx)
        assert [c.configuration_id for c in conflicts.conflicts] == [config_id]
        conflict = conflicts.conflicts[0]
        assert conflict.conflicting_paths == ['/configuration/parameters/query']
        assert {c.changed_by for c in conflict.changes} == {'both'}
        assert conflict.suggested_take is None

        resolved = await resolve_merge_request_conflict(
            ctx, component_id=COMPONENT_ID, configuration_id=config_id, take='ours'
        )
        assert resolved.resolved is True and resolved.mode == 'ours'
        assert resolved.remaining_conflicts == []
        assert 'merge_merge_request' in resolved.next_step

        merged = await merge_merge_request(ctx)
        assert merged.merged is True, merged
        production = _api_request('GET', _config_path(url, None, config_id), token)
        assert production['configuration'] == {'parameters': {'query': 'SELECT 2'}}
        _wait_until_branch_gone(mr_project, branch_id)
    finally:
        _delete_branch(url, token, branch_id)
        _delete_config(url, token, config_id)
