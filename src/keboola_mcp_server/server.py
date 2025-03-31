"""MCP server implementation for Keboola Connection."""

import csv
import logging
from io import StringIO
from typing import Annotated, Any, Dict, List, Optional, TypedDict, cast

import snowflake.connector
from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.database import ConnectionManager, DatabasePathManager
from keboola_mcp_server.mcp import (
    KeboolaMcpServer,
    SessionParams,
    SessionState,
    SessionStateFactory,
)
from keboola_mcp_server.storage_tools import add_storage_tools

logger = logging.getLogger(__name__)


def _create_session_state_factory(config: Optional[Config] = None) -> SessionStateFactory:
    def _(params: SessionParams) -> SessionState:
        logger.info(f"Creating SessionState for params: {params.keys()}.")

        if not config:
            cfg = Config.from_dict(params)
        else:
            cfg = config.replace_by(params)

        logger.info(f"Creating SessionState from config: {cfg}.")

        state: SessionState = {}
        # Create Keboola client instance
        try:
            client = KeboolaClient(cfg.storage_token, cfg.storage_api_url)
            state["sapi_client"] = client
            logger.info("Successfully initialized Storage API client.")
        except Exception as e:
            logger.error(f"Failed to initialize Keboola client: {e}")
            raise

        connection_manager = ConnectionManager(cfg)
        db_path_manager = DatabasePathManager(cfg, connection_manager)
        state["connection_manager"] = connection_manager
        state["db_path_manager"] = db_path_manager
        logger.info("Successfully initialized DB connection and path managers.")

        return state

    return _


def create_server(config: Optional[Config] = None) -> FastMCP:
    """Create and configure the MCP server.

    Args:
        config: Server configuration. If None, loads from environment.

    Returns:
        Configured FastMCP server instance
    """
    # Configure logging
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(config.log_level)

    # Initialize FastMCP server with system instructions
    mcp = KeboolaMcpServer(
        "Keboola Explorer",
        session_state_factory=_create_session_state_factory(config),
        dependencies=[
            "keboola.storage-api-client",
            "httpx",
            "pandas",
            "snowflake-connector-python",
        ],
    )

    add_storage_tools(mcp)

    @mcp.tool()
    async def query_table(sql_query: str, ctx: Context) -> str:
        """
        Execute a SQL query through the proxy service to get data from Storage.
        Before forming the query always check the get_table_metadata tool to get
        the correct database name and table name.
        - The {{db_identifier}} is available in the tool response.

        Note: SQL queries must include the full path including database name, e.g.:
        'SELECT * FROM {{db_identifier}}."test_identify"'. Snowflake is case sensitive so always
        wrap the column names in double quotes.
        """
        connection_manager = ctx.session.state["connection_manager"]
        assert isinstance(connection_manager, ConnectionManager)

        conn = None
        cursor = None

        try:
            conn = connection_manager.create_snowflake_connection()
            cursor = conn.cursor()
            cursor.execute(sql_query)
            result = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            # Convert to CSV
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows(result)

            return output.getvalue()

        except snowflake.connector.errors.ProgrammingError as e:
            raise ValueError(f"Snowflake query error: {str(e)}")

        except Exception as e:
            raise ValueError(f"Unexpected error during query execution: {str(e)}")

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    @mcp.tool()
    async def list_components(ctx: Context) -> str:
        """List all available components and their configurations."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        components = cast(List[Dict[str, Any]], await client.get("components"))
        return "\n".join(f"- {comp['id']}: {comp['name']}" for comp in components)

    @mcp.tool()
    async def list_component_configs(component_id: str, ctx: Context) -> str:
        """List all configurations for a specific component."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        configs = cast(List[Dict[str, Any]], await client.get(f"components/{component_id}/configs"))
        return "\n".join(
            f"Configuration: {config['id']}\n"
            f"Name: {config['name']}\n"
            f"Description: {config.get('description', 'No description')}\n"
            f"Created: {config['created']}\n"
            f"---"
            for config in configs
        )

    return mcp
