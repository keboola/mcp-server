from typing import Any, Dict, Tuple
from unittest.mock import AsyncMock, MagicMock

import pytest
from kbcstorage.client import Client
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.database import ConnectionManager, DatabasePathManager


@pytest.fixture
def mock_context() -> Tuple[Context, KeboolaClient]:
    """Create a mock context with mocked SAPI client and component client.

    Returns:
        Tuple containing:
        - Mocked Context
        - Mocked KeboolaClient
    """
    context = MagicMock(spec=Context)
    context.session.state = {}

    # Mock KeboolaClient
    mock_client = MagicMock(spec=KeboolaClient)
    mock_client.storage_client = MagicMock(spec=Client)
    mock_client.storage_client.components = MagicMock()

    context.session.state["sapi_client"] = mock_client
    return context, mock_client


@pytest.fixture
def mock_context_with_db() -> Tuple[Context, KeboolaClient, ConnectionManager, DatabasePathManager]:
    """Create a mock context with mocked SAPI client, component client, and database managers.

    Returns:
        Tuple containing:
        - Mocked Context
        - Mocked KeboolaClient
        - Mocked ComponentClient
        - Mocked ConnectionManager
        - Mocked DatabasePathManager
    """
    context = MagicMock(spec=Context)
    context.session.state = {}

    # Mock KeboolaClient
    mock_client = MagicMock(spec=KeboolaClient)
    mock_client.storage_client = MagicMock(spec=Client)
    mock_client.storage_client.components = MagicMock()

    # Mock database managers
    mock_connection_manager = MagicMock(spec=ConnectionManager)
    mock_db_path_manager = MagicMock(spec=DatabasePathManager)

    context.session.state["sapi_client"] = mock_client
    context.session.state["connection_manager"] = mock_connection_manager
    context.session.state["db_path_manager"] = mock_db_path_manager

    return context, mock_client, mock_connection_manager, mock_db_path_manager


@pytest.fixture
def mock_snowflake_connection():
    """Create a mock Snowflake connection and cursor.

    Returns:
        Tuple containing:
        - Mocked connection
        - Mocked cursor
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor
