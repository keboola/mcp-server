import pytest
from fastmcp import Context
from mcp.server.session import ServerSession
from mcp.shared.context import RequestContext

from keboola_mcp_server.client import (
    AIServiceClient,
    AsyncStorageClient,
    JobsQueueClient,
    KeboolaClient,
    RawKeboolaClient,
)
from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import ServerState
from keboola_mcp_server.workspace import WorkspaceManager


@pytest.fixture
def keboola_client(mocker) -> KeboolaClient:
    """Creates mocked `KeboolaClient` instance with mocked sub-clients."""
    client = mocker.MagicMock(KeboolaClient)

    # Mock API clients
    client.storage_client = mocker.MagicMock(AsyncStorageClient)
    client.storage_client.base_api_url = 'test://api.keboola.com'
    client.storage_client.branch_id = 'default'
    client.storage_client.project_id.return_value = '69420'
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
    """Creates the mocked `mcp.server.fastmcp.Context` instance with the `ServerSession` and empty state."""
    ctx = mocker.MagicMock(Context)
    ctx.session = mocker.MagicMock(ServerSession)
    ctx.session.state = {}
    ctx.session.client_params = None
    ctx.session_id = None
    ctx.client_id = None
    ctx.request_context = mocker.MagicMock(RequestContext)
    ctx.request_context.lifespan_context = ServerState(Config())
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
