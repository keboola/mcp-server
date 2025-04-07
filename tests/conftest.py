from unittest.mock import MagicMock

import pytest
from kbcstorage.client import Client
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.mcp import StatefullServerSession
from keboola_mcp_server.sql_tools import WorkspaceManager


@pytest.fixture
def keboola_client(mocker) -> KeboolaClient:
    """Creates mocked `KeboolaClient` instance."""
    client = mocker.AsyncMock(KeboolaClient)
    client.storage_client = mocker.AsyncMock(Client)
    return client


@pytest.fixture()
def empty_context(mocker) -> Context:
    """Creates the mocked `mcp.server.fastmcp.Context` instance with the `StatefullServerSession` and empty state."""
    ctx = mocker.MagicMock(Context)
    ctx.session = (session := mocker.MagicMock(StatefullServerSession))
    type(session).state = (state := mocker.PropertyMock())
    state.return_value = {}
    return ctx


@pytest.fixture
def mcp_context_client(keboola_client: KeboolaClient, empty_context: Context) -> Context:
    """Fills the empty_context's state with the `KeboolaClient` and `WorkspaceManager` mocks."""
    empty_context.session.state[WorkspaceManager.STATE_KEY] = MagicMock(spec=WorkspaceManager)
    empty_context.session.state[KeboolaClient.STATE_KEY] = keboola_client
    return empty_context
