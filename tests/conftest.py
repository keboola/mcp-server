from typing import Any, Dict, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from kbcstorage.client import Client
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.database import ConnectionManager, DatabasePathManager


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
    """Create a mock context with mocked SAPI client.

    Returns:
        Tuple containing:
        - Mocked Context with mocked SAPI client
    """
    context = mcp_context
    context.session.state = {}
    # Mock KeboolaClient
    context.session.state["sapi_client"] = keboola_client
    return context



