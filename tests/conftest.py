import pytest
from kbcstorage.client import Client as SyncStorageClient
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import (
    AIServiceClient,
    AsyncStorageClient,
    JobsQueueClient,
    KeboolaClient,
    RawKeboolaClient,
)
from keboola_mcp_server.mcp import StatefulServerSession
from keboola_mcp_server.tools.sql import WorkspaceManager


@pytest.fixture
def keboola_client(mocker) -> KeboolaClient:
    """Creates mocked `KeboolaClient` instance with mocked sub-clients."""
    client = mocker.MagicMock(KeboolaClient)
    # Mock synchronous client
    client.storage_client_sync = mocker.MagicMock(SyncStorageClient)
    # Mock asynchronous clients
    client.storage_client = mocker.MagicMock(AsyncStorageClient)
    client.storage_client.branch_id = 'default'
    client.jobs_queue_client = mocker.MagicMock(JobsQueueClient)
    client.ai_service_client = mocker.MagicMock(AIServiceClient)
    # Mock the underlying api_client for async clients if needed for deeper testing
    client.storage_client.api_client = mocker.MagicMock(RawKeboolaClient)
    client.jobs_queue_client.api_client = mocker.MagicMock(RawKeboolaClient)
    client.ai_service_client.api_client = mocker.MagicMock(RawKeboolaClient)

    return client


@pytest.fixture
def workspace_manager(mocker) -> WorkspaceManager:
    """Creates mocked `WorkspaceManager` instance."""
    return mocker.MagicMock(WorkspaceManager)


@pytest.fixture
def empty_context(mocker) -> Context:
    """Creates the mocked `mcp.server.fastmcp.Context` instance with the `StatefulServerSession` and empty state."""
    ctx = mocker.MagicMock(Context)
    ctx.session = (session := mocker.MagicMock(StatefulServerSession))
    type(session).state = (state := mocker.PropertyMock())
    state.return_value = {}
    return ctx


@pytest.fixture
def mcp_context_client(
    keboola_client: KeboolaClient, workspace_manager: WorkspaceManager, empty_context: Context
) -> Context:
    """Fills the empty_context's state with the `KeboolaClient` and `WorkspaceManager` mocks."""
    client_context = empty_context
    client_context.session.state[WorkspaceManager.STATE_KEY] = workspace_manager
    client_context.session.state[KeboolaClient.STATE_KEY] = keboola_client
    return client_context
