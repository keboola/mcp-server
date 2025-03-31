from unittest.mock import MagicMock

import pytest
from kbcstorage.client import Client
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import JobsQueue, KeboolaClient


@pytest.fixture
def keboola_client() -> KeboolaClient:
    """Create a mock jobs client."""
    mock_client = MagicMock(spec=KeboolaClient)
    mock_client.storage_client = MagicMock(spec=Client)
    mock_client.jobs_queue = MagicMock(spec=JobsQueue)
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
    context.session.state["sapi_client"] = keboola_client
    return context
