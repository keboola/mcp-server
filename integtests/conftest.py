import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator

import pytest
from dotenv import load_dotenv
from fastmcp import Context
from mcp.server.session import ServerSession

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.server import SessionState
from keboola_mcp_server.tools.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

STORAGE_API_TOKEN_ENV_VAR = 'INTEGTEST_STORAGE_TOKEN'
STORAGE_API_URL_ENV_VAR = 'INTEGTEST_STORAGE_API_URL'
WORKSPACE_SCHEMA_ENV_VAR = 'INTEGTEST_WORKSPACE_SCHEMA'


@dataclass(frozen=True)
class BucketDef:
    # Cannot be called TestBucket because Pytest would consider it a test class
    bucket_id: str
    display_name: str


@dataclass(frozen=True)
class TableDef:
    bucket_id: str
    table_name: str
    table_id: str


class StatefulServerSession(ServerSession):
    state: SessionState


@pytest.fixture(scope='session')
def buckets() -> list[BucketDef]:
    return [
        BucketDef('in.c-test_bucket_01', 'test_bucket_01'),
        BucketDef('in.c-test_bucket_02', 'test_bucket_02'),
    ]


@pytest.fixture(scope='session')
def tables() -> list[TableDef]:
    return [
        TableDef(
            bucket_id='in.c-test_bucket_01',
            table_name='test_table_01',
            table_id='in.c-test_bucket_01.test_table_01',
        ),
    ]


@pytest.fixture(scope='session')
def env_file_loaded() -> bool:
    return load_dotenv()


@pytest.fixture(scope='session')
def storage_api_url(env_file_loaded: bool) -> str:
    storage_api_url = os.getenv(STORAGE_API_URL_ENV_VAR)
    assert storage_api_url, f'{STORAGE_API_URL_ENV_VAR} must be set'
    return storage_api_url


@pytest.fixture(scope='session')
def storage_api_token(env_file_loaded: bool) -> str:
    storage_api_token = os.getenv(STORAGE_API_TOKEN_ENV_VAR)
    assert storage_api_token, f'{STORAGE_API_TOKEN_ENV_VAR} must be set'
    return storage_api_token


@pytest.fixture(scope='session')
def workspace_schema(env_file_loaded: bool) -> str:
    workspace_schema = os.getenv(WORKSPACE_SCHEMA_ENV_VAR)
    assert workspace_schema, f'{WORKSPACE_SCHEMA_ENV_VAR} must be set'
    return workspace_schema


def _keboola_client(storage_api_token: str, storage_api_url: str) -> KeboolaClient:
    return KeboolaClient(storage_api_token=storage_api_token, storage_api_url=storage_api_url)


@pytest.fixture(scope='session')
def shared_datadir_ro() -> Path:
    """
    Session-scoped access to shared data directory for integration tests.
    Do not modify the data in this directory.
    For function-scoped access to the data, use `shared_datadir` fixture provided by `pytest-datadir`,
    which creates a temporary copy of the data which can therefore be modified.
    """
    return Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def keboola_project(
    storage_api_token: str, storage_api_url: str,
    env_file_loaded: bool, shared_datadir_ro: Path, buckets: list[BucketDef], tables: list[TableDef]
) -> Generator[str, Any, None]:
    """
    Sets up a Keboola project with items needed for integration tests.
    After the tests, the project is cleaned up.
    """
    # Cannot use keboola_client fixture because it is function-scoped
    storage_client = _keboola_client(storage_api_token, storage_api_url).storage_client_sync
    token_info = storage_client.tokens.verify()
    project_id: str = token_info['owner']['id']
    LOG.info(f'Setting up Keboola project with ID={project_id}')

    current_buckets = storage_client.buckets.list()
    if current_buckets:
        pytest.fail(f'Expecting empty Keboola project, but found {len(current_buckets)} buckets')

    for bucket in buckets:
        LOG.info(f'Creating bucket with display name={bucket.display_name}')
        storage_client.buckets.create(bucket.display_name)

    current_tables = storage_client.tables.list()
    if current_tables:
        pytest.fail(f'Expecting empty Keboola project, but found {len(current_tables)} tables')

    for table in tables:
        LOG.info(f'Creating table with name={table.table_name}')
        storage_client.tables.create(
            bucket_id=table.bucket_id,
            name=table.table_name,
            file_path=shared_datadir_ro / 'proj' / table.bucket_id / f'{table.table_name}.csv',
        )

    LOG.info(f'Test setup for project {project_id} complete')
    yield project_id

    LOG.info(f'Cleaning up Keboola project with ID={project_id}')
    current_buckets = storage_client.buckets.list()
    for bucket in current_buckets:
        bucket_id = bucket['id']
        LOG.info(f'Deleting bucket with ID={bucket_id}')
        storage_client.buckets.delete(bucket_id, force=True)


@pytest.fixture
def keboola_client(
    env_file_loaded: bool, storage_api_token: str, storage_api_url: str
) -> KeboolaClient:
    return _keboola_client(storage_api_token, storage_api_url)


@pytest.fixture
def workspace_manager(keboola_client: KeboolaClient, workspace_schema: str) -> WorkspaceManager:
    return WorkspaceManager(keboola_client, workspace_schema)


@pytest.fixture
def mcp_context_client(
    mocker, keboola_client: KeboolaClient, workspace_manager: WorkspaceManager, keboola_project: str
) -> Context:
    client_context = mocker.MagicMock(Context)
    client_context.session = mocker.MagicMock(StatefulServerSession)
    client_context.session.state = {
        KeboolaClient.STATE_KEY: keboola_client,
        WorkspaceManager.STATE_KEY: workspace_manager,
    }
    return client_context
