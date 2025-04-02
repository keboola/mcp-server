from unittest.mock import MagicMock

import pytest
from kbcstorage.client import Client
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.sql_tools import WorkspaceManager


@pytest.fixture
def keboola_client() -> KeboolaClient:
    """Create a mock jobs client."""
    mock_client = MagicMock(spec=KeboolaClient)
    mock_client.storage_client = MagicMock(spec=Client)
    return mock_client


@pytest.fixture
def mcp_context() -> Context:
    """Create a mock context."""
    context = MagicMock(spec=Context)
    return context


@pytest.fixture
def mcp_context_client(keboola_client: KeboolaClient, mcp_context: Context) -> Context:
    """Create a mock context with mocked SAPI client."""
    context = mcp_context
    context.session.state = {}
    # Mock KeboolaClient
    workspace_manager = MagicMock(spec=WorkspaceManager)
    context.session.state["workspace_manager"] = workspace_manager

    context.session.state["sapi_client"] = keboola_client
    return context
