import os

import pytest
from dotenv import load_dotenv
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.mcp import StatefulServerSession
from keboola_mcp_server.tools.sql import WorkspaceManager

STORAGE_API_TOKEN_ENV_VAR = 'INTEGTEST_STORAGE_TOKEN'
STORAGE_API_URL_ENV_VAR = 'INTEGTEST_STORAGE_API_URL'
WORKSPACE_SCHEMA_ENV_VAR = 'INTEGTEST_WORKSPACE_SCHEMA'


@pytest.fixture(scope='module')
def env_file_loaded() -> bool:
    return load_dotenv()


@pytest.fixture
def keboola_client(env_file_loaded: bool, mocker) -> KeboolaClient:
    storage_api_url = os.getenv(STORAGE_API_URL_ENV_VAR)
    storage_api_token = os.getenv(STORAGE_API_TOKEN_ENV_VAR)
    assert (
        storage_api_url and storage_api_token
    ), f'{STORAGE_API_URL_ENV_VAR} and {STORAGE_API_TOKEN_ENV_VAR} must be set'
    return KeboolaClient(storage_api_token=storage_api_token, storage_api_url=storage_api_url)


@pytest.fixture
def workspace_manager(keboola_client: KeboolaClient) -> WorkspaceManager:
    workspace_schema = os.getenv(WORKSPACE_SCHEMA_ENV_VAR)
    assert workspace_schema, f'{WORKSPACE_SCHEMA_ENV_VAR} must be set'
    return WorkspaceManager(keboola_client, workspace_schema)


@pytest.fixture
def mcp_context_client(mocker, keboola_client: KeboolaClient, workspace_manager: WorkspaceManager) -> Context:
    client_context = mocker.MagicMock(Context)
    client_context.session = mocker.MagicMock(StatefulServerSession)
    client_context.session.state = {}
    client_context.session.state[KeboolaClient.STATE_KEY] = keboola_client
    client_context.session.state[WorkspaceManager.STATE_KEY] = workspace_manager
    return client_context
