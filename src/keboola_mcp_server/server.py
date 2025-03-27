"""MCP server implementation for Keboola Connection."""

import logging
from enum import Enum
from typing import Annotated, Any, Dict, List, Optional, TypedDict, cast

import httpx
from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from keboola_mcp_server import sql_tools
from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config
from keboola_mcp_server.database import ConnectionManager, DatabasePathManager
from keboola_mcp_server.mcp import (
    KeboolaMcpServer,
    SessionParams,
    SessionState,
    SessionStateFactory,
)

logger = logging.getLogger(__name__)


class BucketInfo(TypedDict):
    id: str
    name: str
    description: str
    stage: str
    created: str
    tablesCount: int
    dataSizeBytes: int


class TableColumnInfo(TypedDict):
    name: str
    db_identifier: str


class TableDetail(TypedDict):
    id: str
    name: str
    primary_key: List[str]
    created: str
    row_count: int
    data_size_bytes: int
    columns: List[str]
    column_identifiers: List[TableColumnInfo]
    db_identifier: str


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

    mcp.add_tool(sql_tools.query_table)

    # Tools
    @mcp.tool()
    async def list_all_buckets(ctx: Context) -> str:
        """List all buckets in the project with their basic information."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        buckets = cast(List[BucketInfo], client.storage_client.buckets.list())

        header = "# Bucket List\n\n"
        header += f"Total Buckets: {len(buckets)}\n\n"
        header += "## Details"

        bucket_details = []
        for bucket in buckets:
            detail = f"### {bucket['name']} ({bucket['id']})"
            detail += f"\n    - Stage: {bucket.get('stage', 'N/A')}"
            detail += f"\n    - Description: {bucket.get('description', 'N/A')}"
            detail += f"\n    - Created: {bucket.get('created', 'N/A')}"
            detail += f"\n    - Tables: {bucket.get('tablesCount', 0)}"
            detail += f"\n    - Size: {bucket.get('dataSizeBytes', 0)} bytes"
            bucket_details.append(detail)

        return header + "\n" + "\n".join(bucket_details)

    @mcp.tool()
    async def get_bucket_metadata(bucket_id: str, ctx: Context) -> str:
        """Get detailed information about a specific bucket."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        bucket = cast(Dict[str, Any], client.storage_client.buckets.detail(bucket_id))

        return (
            f"Bucket Information:\n"
            f"ID: {bucket['id']}\n"
            f"Name: {bucket['name']}\n"
            f"Description: {bucket.get('description', 'No description')}\n"
            f"Created: {bucket['created']}\n"
            f"Tables Count: {bucket.get('tablesCount', 0)}\n"
            f"Data Size Bytes: {bucket.get('dataSizeBytes', 0)}"
        )

    @mcp.tool()
    async def get_table_metadata(table_id: str, ctx: Context) -> str:
        """Get detailed information about a specific table including its DB identifier and column information."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        table = cast(Dict[str, Any], client.storage_client.tables.detail(table_id))

        # Get column info
        columns = table.get("columns", [])
        column_info = [TableColumnInfo(name=col, db_identifier=f'"{col}"') for col in columns]

        db_path_manager = ctx.session.state["db_path_manager"]
        assert isinstance(db_path_manager, DatabasePathManager)
        table_fqn = db_path_manager.get_table_fqn(table) or "N/A"

        return (
            f"Table Information:\n"
            f"ID: {table['id']}\n"
            f"Name: {table['name']}\n"
            f"Primary Key: {', '.join(table['primaryKey']) if table['primaryKey'] else 'None'}\n"
            f"Created: {table['created']}\n"
            f"Row Count: {table['rowsCount']}\n"
            f"Data Size: {table['dataSizeBytes']} bytes\n"
            f"Columns: {', '.join(str(ci) for ci in column_info)}\n"
            f"Fully qualified table name: {table_fqn.snowflake_fqn}"
        )

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

    @mcp.tool()
    async def list_bucket_tables(bucket_id: str, ctx: Context) -> str:
        """List all tables in a specific bucket with their basic information."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        tables = cast(List[Dict[str, Any]], client.storage_client.buckets.list_tables(bucket_id))
        return "\n".join(
            f"Table ID: {table['id']}\n"
            f"Table Name: {table.get('name', 'N/A')}\n"
            f"Rows: {table.get('rowsCount', 'N/A')}\n"
            f"Size: {table.get('dataSizeBytes', 'N/A')} bytes\n"
            f"Columns: {', '.join(table.get('columns', []))}\n"
            f"---"
            for table in tables
        )

    @mcp.tool()
    async def update_bucket_description(
        bucket_id: Annotated[str, Field(description="The ID of the bucket to update.")],
        description: Annotated[str, Field(description="The new description for the bucket.")],
        ctx: Context,
    ):
        """
        Update the description for a given Keboola bucket. This tool is used to update the description of a bucket.

        Args:
            bucket_id: The ID of the bucket to update.
            description: The new description for the bucket.

        Returns:
            The response from the API.
        """
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)

        metadata_endpoint = f"buckets/{bucket_id}/metadata"

        data = {"provider": "user", "metadata": [{"key": "KBC.description", "value": description}]}

        response = await client.post(metadata_endpoint, data)
        return response

    @mcp.tool()
    async def update_table_description(
        table_id: Annotated[str, Field(description="The ID of the table to update.")],
        description: Annotated[str, Field(description="The new description for the table.")],
        ctx: Context,
    ):
        """Update the description for a given Keboola table. This tool is used to update the description of a table.

        Args:
            table_id: The ID of the table to update.
            description: The new description for the table.

        Returns:
            The response from the API.
        """
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)

        metadata_endpoint = f"tables/{table_id}/metadata"

        data = {"provider": "user", "metadata": [{"key": "KBC.description", "value": description}]}

        response = await client.post(metadata_endpoint, data)
        return response

    @mcp.tool()
    async def update_component_description(
        component_id: Annotated[str, Field(description="The ID of the component to update.")],
        description: Annotated[str, Field(description="The new description for the component.")],
        ctx: Context,
    ):
        """
        Update the description for a given Keboola component. This tool is used to update the description of a component.

        Args:
            component_id: The ID of the component to update.
            description: The new description for the component.

        Returns:
            The response from the API.
        """
        client = ctx.session.state["sapi_client"]
        metadata_endpoint = f"components/{component_id}/metadata"

        data = {"provider": "user", "metadata": [{"key": "KBC.description", "value": description}]}

        response = await client.post(metadata_endpoint, data)
        return response

    return mcp
