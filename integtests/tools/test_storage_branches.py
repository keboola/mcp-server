"""Integration tests for branched storage — validates deference mechanism for both old-style and storage-branches."""

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Generator

import httpx
import pytest
import pytest_asyncio
from fastmcp import Context
from mcp.server.session import ServerSession
from mcp.types import ClientCapabilities, InitializeRequestParams

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import ServerRuntimeInfo, ServerState
from keboola_mcp_server.tools.project import get_project_info
from keboola_mcp_server.tools.storage.tools import get_buckets, get_tables
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

PYTHON_TRANSFORMATION_COMPONENT = 'keboola.python-transformation-v2'
STORAGE_BRANCHES_TOKEN_ENV_VAR = 'INTEGTEST_STORAGE_TOKEN_STORAGE_BRANCHES'
OLD_BRANCHES_TOKEN_ENV_VAR = 'INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES'


# --- Helper functions ---


def _python_transform_config(destination: str, csv_filename: str, fieldnames: list[str], row: dict[str, str]) -> dict:
    """Build a Python transformation config that generates a single-row CSV and writes to destination."""
    fields_str = str(fieldnames)
    row_str = str(row)
    script = (
        'import csv\n'
        'import os\n'
        "os.makedirs('out/tables', exist_ok=True)\n"
        f"with open('out/tables/{csv_filename}', mode='wt', encoding='utf-8') as f:\n"
        f"    writer = csv.DictWriter(f, fieldnames={fields_str}, dialect='kbc')\n"
        '    writer.writeheader()\n'
        f'    writer.writerow({row_str})'
    )
    return {
        'storage': {
            'output': {
                'tables': [
                    {
                        'source': csv_filename,
                        'destination': destination,
                        'primary_key': ['id'],
                    }
                ]
            }
        },
        'parameters': {
            'blocks': [
                {
                    'name': 'Generate data',
                    'codes': [
                        {
                            'name': 'script',
                            'script': [script],
                        }
                    ],
                }
            ],
            'packages': [],
        },
    }


def _api_request(method: str, url: str, token: str, **kwargs: Any) -> dict:
    """Make a synchronous HTTP request to the Keboola API."""
    headers = {'X-StorageApi-Token': token, 'Content-Type': 'application/json'}
    resp = httpx.request(method, url, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json() if resp.content else {}


def _wait_for_storage_job(base_url: str, token: str, job_id: str, timeout: int = 120) -> None:
    """Wait for a Storage API job to complete."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = _api_request('GET', f'{base_url}/v2/storage/jobs/{job_id}', token)
        status = job.get('status')
        if status == 'success':
            return
        if status in ('error', 'cancelled'):
            raise RuntimeError(f'Storage job {job_id} failed: {job}')
        time.sleep(2)
    raise TimeoutError(f'Storage job {job_id} did not complete within {timeout}s')


def _wait_for_queue_job(base_url: str, token: str, job_id: str, timeout: int = 300) -> None:
    """Wait for a Job Queue job to complete."""
    queue_url = base_url.replace('connection.', 'queue.')
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = httpx.get(f'{queue_url}/jobs/{job_id}', headers={'X-StorageApi-Token': token})
        resp.raise_for_status()
        job = resp.json()
        status = job.get('status')
        if status == 'success':
            return
        if status in ('error', 'cancelled', 'terminated'):
            raise RuntimeError(
                f'Queue job {job_id} failed with status={status}: {json.dumps(job.get("result", {}))[:500]}'
            )
        time.sleep(5)
    raise TimeoutError(f'Queue job {job_id} did not complete within {timeout}s')


def _create_branch(base_url: str, token: str, name: str) -> str:
    """Create a dev branch and return its ID."""
    job = _api_request('POST', f'{base_url}/v2/storage/dev-branches', token, json={'name': name})
    job_id = str(job['id'])
    _wait_for_storage_job(base_url, token, job_id)
    job = _api_request('GET', f'{base_url}/v2/storage/jobs/{job_id}', token)
    branch_id = str(job['results']['id'])
    LOG.info(f'Created branch {name!r} with id={branch_id}')
    return branch_id


def _delete_branch(base_url: str, token: str, branch_id: str) -> None:
    """Delete a dev branch."""
    try:
        job = _api_request('DELETE', f'{base_url}/v2/storage/dev-branches/{branch_id}', token)
        job_id = str(job['id'])
        _wait_for_storage_job(base_url, token, job_id)
        LOG.info(f'Deleted branch {branch_id}')
    except Exception:
        LOG.exception(f'Failed to delete branch {branch_id}')


def _ensure_bucket(base_url: str, token: str, name: str, stage: str = 'in') -> str:
    """Create a bucket if it doesn't already exist. Returns the bucket ID."""
    bucket_id = f'{stage}.c-{name}'
    try:
        _api_request('GET', f'{base_url}/v2/storage/buckets/{bucket_id}', token)
        LOG.info(f'Bucket {bucket_id} already exists')
    except httpx.HTTPStatusError as e:
        if e.response.status_code != 404:
            raise
        result = _api_request(
            'POST',
            f'{base_url}/v2/storage/buckets',
            token,
            json={'name': name, 'stage': stage, 'description': 'Integration test bucket'},
        )
        bucket_id = result['id']
        LOG.info(f'Created bucket {bucket_id}')
    return bucket_id


def _ensure_table(base_url: str, token: str, bucket_id: str, table_name: str, csv_data: str) -> str:
    """Create a table if it doesn't already exist. Returns the table ID."""
    table_id = f'{bucket_id}.{table_name}'
    try:
        _api_request('GET', f'{base_url}/v2/storage/tables/{table_id}', token)
        LOG.info(f'Table {table_id} already exists')
    except httpx.HTTPStatusError as e:
        if e.response.status_code != 404:
            raise
        resp = httpx.post(
            f'{base_url}/v2/storage/buckets/{bucket_id}/tables',
            headers={'X-StorageApi-Token': token},
            data={'name': table_name, 'delimiter': ',', 'dataString': csv_data},
        )
        resp.raise_for_status()
        result = resp.json()
        table_id = result['id']
        LOG.info(f'Created table {table_id}')
    return table_id


def _create_config_in_branch(
    base_url: str, token: str, branch_id: str, component_id: str, name: str, config: dict
) -> str:
    """Create a component configuration in a specific branch. Returns the config ID."""
    result = _api_request(
        'POST',
        f'{base_url}/v2/storage/branch/{branch_id}/components/{component_id}/configs',
        token,
        json={'name': name, 'description': f'Integration test config: {name}', 'configuration': json.dumps(config)},
    )
    config_id = str(result['id'])
    LOG.info(f'Created config {name!r} (id={config_id}) in branch {branch_id}')
    return config_id


def _run_job_in_branch(base_url: str, token: str, branch_id: str, component_id: str, config_id: str) -> None:
    """Run a job in a specific branch and wait for completion."""
    queue_url = base_url.replace('connection.', 'queue.')
    resp = httpx.post(
        f'{queue_url}/jobs',
        headers={'X-StorageApi-Token': token, 'Content-Type': 'application/json'},
        json={'component': component_id, 'config': config_id, 'mode': 'run', 'branchId': branch_id},
    )
    resp.raise_for_status()
    job = resp.json()
    job_id = str(job['id'])
    LOG.info(f'Started job {job_id} for {component_id}/{config_id} in branch {branch_id}')
    _wait_for_queue_job(base_url, token, job_id)
    LOG.info(f'Job {job_id} completed successfully')


# --- Test data setup/teardown ---


@dataclass
class BranchTestProject:
    """A project with branches set up for testing."""

    storage_api_url: str
    storage_api_token: str
    has_storage_branches: bool
    branch_a_id: str
    branch_b_id: str
    label: str


def _setup_branch_test_project(
    storage_api_url: str,
    token: str,
    label: str,
) -> BranchTestProject:
    """
    Set up production data, branches, and branched data in a project.

    Production data (bucket + table) is created idempotently so multiple
    concurrent sessions can share the same project.

    Branch_A:
      - Updates in.c-test_bucket_01.test_table_01 (creates branched version)
      - Creates new bucket in.c-test_branch with test_table_branch

    Branch_B:
      - Creates new bucket in.c-test_branch_2 with test_table_branch
    """
    token_info = _api_request('GET', f'{storage_api_url}/v2/storage/tokens/verify', token)
    project_name = token_info['owner']['name']
    features = token_info.get('owner', {}).get('features', [])
    has_sb = 'storage-branches' in features
    LOG.info(f'[{label}] Setting up project {project_name!r} (storage-branches={has_sb})')

    _ensure_bucket(storage_api_url, token, 'test_bucket_01')
    _ensure_table(
        storage_api_url,
        token,
        'in.c-test_bucket_01',
        'test_table_01',
        '"id","name","item_count"\n1,"item1",10\n2,"item2",20',
    )

    # Create branches
    uid = str(uuid.uuid4())[:8]
    branch_a_id = _create_branch(storage_api_url, token, f'integtest-branch-A-{uid}')
    branch_b_id = _create_branch(storage_api_url, token, f'integtest-branch-B-{uid}')

    # Branch A: update existing table (creates branched version)
    config = _python_transform_config(
        destination='in.c-test_bucket_01.test_table_01',
        csv_filename='test_table_01.csv',
        fieldnames=['id', 'name', 'item_count'],
        row={'id': '99', 'name': 'branched_item', 'item_count': '999'},
    )
    cid = _create_config_in_branch(
        storage_api_url, token, branch_a_id, PYTHON_TRANSFORMATION_COMPONENT, 'update-tbl', config
    )
    _run_job_in_branch(storage_api_url, token, branch_a_id, PYTHON_TRANSFORMATION_COMPONENT, cid)

    # Branch A: create new bucket + table
    config = _python_transform_config(
        destination='in.c-test_branch.test_table_branch',
        csv_filename='test_table_branch.csv',
        fieldnames=['id', 'name', 'value'],
        row={'id': '1', 'name': 'branch_a_data', 'value': '100'},
    )
    cid = _create_config_in_branch(
        storage_api_url, token, branch_a_id, PYTHON_TRANSFORMATION_COMPONENT, 'create-tbl', config
    )
    _run_job_in_branch(storage_api_url, token, branch_a_id, PYTHON_TRANSFORMATION_COMPONENT, cid)

    # Branch B: create new bucket + table
    config = _python_transform_config(
        destination='in.c-test_branch_2.test_table_branch',
        csv_filename='test_table_branch.csv',
        fieldnames=['id', 'name', 'value'],
        row={'id': '1', 'name': 'branch_b_data', 'value': '200'},
    )
    cid = _create_config_in_branch(
        storage_api_url, token, branch_b_id, PYTHON_TRANSFORMATION_COMPONENT, 'create-b-tbl', config
    )
    _run_job_in_branch(storage_api_url, token, branch_b_id, PYTHON_TRANSFORMATION_COMPONENT, cid)

    LOG.info(f'[{label}] Setup complete: branch_a={branch_a_id}, branch_b={branch_b_id}')
    return BranchTestProject(
        storage_api_url=storage_api_url,
        storage_api_token=token,
        has_storage_branches=has_sb,
        branch_a_id=branch_a_id,
        branch_b_id=branch_b_id,
        label=label,
    )


def _teardown_branch_test_project(project: BranchTestProject) -> None:
    """Clean up branches only. Production data is kept for reuse across sessions."""
    LOG.info(f'[{project.label}] Tearing down')
    _delete_branch(project.storage_api_url, project.storage_api_token, project.branch_a_id)
    _delete_branch(project.storage_api_url, project.storage_api_token, project.branch_b_id)


# --- Fixtures ---


@pytest.fixture(scope='session')
def branch_test_projects(
    storage_api_url: str,
    env_file_loaded: bool,
) -> Generator[list[BranchTestProject], Any, None]:
    """
    Sets up branch tests on two dedicated projects (outside the pool):
    - One with the storage-branches feature (INTEGTEST_STORAGE_TOKEN_STORAGE_BRANCHES)
    - One without it (INTEGTEST_STORAGE_TOKEN_OLD_BRANCHES)

    Both use idempotent production data setup and unique branch names,
    so multiple concurrent sessions can safely share the same projects.
    """
    sb_token = os.getenv(STORAGE_BRANCHES_TOKEN_ENV_VAR, '').strip()
    if not sb_token:
        pytest.fail(
            f'{STORAGE_BRANCHES_TOKEN_ENV_VAR} must be set to a storage token '
            f'for a project WITH the storage-branches feature'
        )

    old_token = os.getenv(OLD_BRANCHES_TOKEN_ENV_VAR, '').strip()
    if not old_token:
        pytest.fail(
            f'{OLD_BRANCHES_TOKEN_ENV_VAR} must be set to a storage token '
            f'for a project WITHOUT the storage-branches feature'
        )

    projects: list[BranchTestProject] = []
    try:
        sb_project = _setup_branch_test_project(storage_api_url, sb_token, 'storage-branches')
        projects.append(sb_project)

        old_project = _setup_branch_test_project(storage_api_url, old_token, 'old-branches')
        projects.append(old_project)

        # Verify features match expectations
        if not sb_project.has_storage_branches:
            pytest.fail(
                f'{STORAGE_BRANCHES_TOKEN_ENV_VAR} must point to a project ' f'WITH the storage-branches feature'
            )
        if old_project.has_storage_branches:
            pytest.fail(
                f'{OLD_BRANCHES_TOKEN_ENV_VAR} must point to a project ' f'WITHOUT the storage-branches feature'
            )

        yield projects

    finally:
        for p in projects:
            _teardown_branch_test_project(p)


@pytest.fixture(scope='session', params=['storage-branches', 'old-branches'])
def branch_project(request, branch_test_projects: list[BranchTestProject]) -> BranchTestProject:
    """Parametrized fixture: yields each project type in turn."""
    want_sb = request.param == 'storage-branches'
    for p in branch_test_projects:
        if p.has_storage_branches == want_sb:
            return p
    pytest.skip(f'No project matching {request.param}')


async def _build_context(
    mocker,
    branch_project: BranchTestProject,
    *,
    branch_id: str | None,
) -> Context:
    """Build an MCP context bound to a specific branch (or the default branch when None)."""
    keboola_client = KeboolaClient(
        storage_api_url=branch_project.storage_api_url,
        storage_api_token=branch_project.storage_api_token,
        headers={'User-Agent': 'KeboolaMCPServer/integtest'},
    )
    if branch_id is not None:
        keboola_client = await keboola_client.with_branch_id(branch_id)
    workspace_manager = await WorkspaceManager.create(keboola_client)

    mcp_config = Config(
        storage_api_url=branch_project.storage_api_url,
        storage_token=branch_project.storage_api_token,
    )
    ctx = mocker.MagicMock(Context)
    ctx.session = mocker.MagicMock(ServerSession)
    ctx.session.state = {
        KeboolaClient.STATE_KEY: keboola_client,
        WorkspaceManager.STATE_KEY: workspace_manager,
    }
    ctx.session.client_params = InitializeRequestParams(
        protocolVersion='1',
        capabilities=ClientCapabilities(),
        clientInfo={'name': 'integtest-branches', 'version': '0.0.1'},
    )
    ctx.client_id = 'KeboolaMCPServer/integtest'
    ctx.session_id = None
    ctx.request_context = mocker.MagicMock()
    ctx.request_context.lifespan_context = ServerState(mcp_config, ServerRuntimeInfo(transport='stdio'))
    return ctx


@pytest_asyncio.fixture
async def branch_context(
    mocker,
    branch_project: BranchTestProject,
) -> Context:
    """MCP context bound to Branch A of the current parametrized project."""
    return await _build_context(mocker, branch_project, branch_id=branch_project.branch_a_id)


@pytest_asyncio.fixture
async def default_branch_context(
    mocker,
    branch_project: BranchTestProject,
) -> Context:
    """MCP context bound to the default/production branch of the current parametrized project."""
    return await _build_context(mocker, branch_project, branch_id=None)


# --- Tests ---


@pytest.mark.asyncio
async def test_list_buckets_includes_branch_a_bucket(
    branch_context: Context,
    branch_project: BranchTestProject,
) -> None:
    """get_buckets from Branch A should include production buckets + Branch A's new bucket, but not Branch B's."""
    result = await get_buckets(branch_context)
    bucket_ids = {b.id for b in result.buckets}

    expected_ids = {'in.c-test_bucket_01', 'in.c-test_branch'}
    assert bucket_ids == expected_ids, f'Expected exactly {expected_ids}, got {bucket_ids}'


@pytest.mark.asyncio
async def test_list_tables_in_branched_bucket(
    branch_context: Context,
    branch_project: BranchTestProject,
) -> None:
    """get_tables for Branch A's new bucket should return the table with a production-like ID."""
    result = await get_tables(branch_context, bucket_ids=['in.c-test_branch'])

    assert len(result.tables) == 1
    table = result.tables[0]
    assert table.name == 'test_table_branch'
    assert table.id == 'in.c-test_branch.test_table_branch'
    assert table.branch_id is None


@pytest.mark.asyncio
async def test_deference_branched_table(
    branch_context: Context,
    branch_project: BranchTestProject,
) -> None:
    """get_tables for the production bucket should include the branched version of test_table_01."""
    result = await get_tables(branch_context, bucket_ids=['in.c-test_bucket_01'])
    table_ids = {t.id for t in result.tables}

    assert 'in.c-test_bucket_01.test_table_01' in table_ids, f'Branched table missing. Got: {table_ids}'

    table = next(t for t in result.tables if t.id == 'in.c-test_bucket_01.test_table_01')
    assert table.branch_id is None


@pytest.mark.asyncio
async def test_get_project_info_reports_dev_branch(
    branch_context: Context,
    branch_project: BranchTestProject,
) -> None:
    """get_project_info from a dev-branch context should populate branch fields and is_development_branch=True."""
    result = await get_project_info(branch_context)

    assert str(result.branch_id) == str(branch_project.branch_a_id)
    assert isinstance(result.branch_name, str)
    assert result.branch_name
    assert result.is_development_branch is True


@pytest.mark.asyncio
async def test_get_project_info_reports_default_branch(
    default_branch_context: Context,
    branch_project: BranchTestProject,
) -> None:
    """get_project_info from a default-branch context should populate branch fields and is_development_branch=False."""
    result = await get_project_info(default_branch_context)

    assert isinstance(result.branch_id, (str, int))
    assert isinstance(result.branch_name, str)
    assert result.branch_name
    assert result.is_development_branch is False
    # Sanity: the default branch must not be the dev branch we created for this session.
    assert str(result.branch_id) != str(branch_project.branch_a_id)
    assert str(result.branch_id) != str(branch_project.branch_b_id)
