"""MCP server implementation for Keboola Connection."""

import csv
import logging
from io import StringIO
from typing import Any, cast, Dict, List, Optional, TypedDict
from pydantic import BaseModel, Field

import snowflake.connector
from mcp.server.fastmcp import Context, FastMCP

from keboola_mcp_server.mcp import (
    KeboolaMcpServer,
    SessionParams,
    SessionState,
    SessionStateFactory,
)
from .client import KeboolaClient
from .config import Config
from .database import ConnectionManager, DatabasePathManager

logger = logging.getLogger(__name__)


class BucketInfo(BaseModel):
    id: str = Field(..., description="Unique identifier for the bucket")
    name: str = Field(..., description="Name of the bucket")
    description: Optional[str] = Field(None, description="Description of the bucket")
    stage: Optional[str] = Field(None, description="Stage of the bucket (e.g., production, development)")
    created: Optional[str] = Field(None, description="Creation timestamp of the bucket")
    tables_count: int = Field(..., description="Number of tables in the bucket")
    data_size_bytes: int = Field(..., description="Total data size of the bucket in bytes")



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

    # Tools
    @mcp.tool()
    async def list_all_buckets(ctx: Context) -> List[BucketInfo]:
        """List all buckets in the project with their basic information."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        buckets = client.storage_client.buckets.list()
        bucket_info_list = [BucketInfo(**bucket) for bucket in buckets]

        return bucket_info_list

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

        return (
            f"Table Information:\n"
            f"ID: {table['id']}\n"
            f"Name: {table['name']}\n"
            f"Primary Key: {', '.join(table['primaryKey']) if table['primaryKey'] else 'None'}\n"
            f"Created: {table['created']}\n"
            f"Row Count: {table['rowsCount']}\n"
            f"Data Size: {table['dataSizeBytes']} bytes\n"
            f"Columns: {', '.join(str(ci) for ci in column_info)}\n"
            f"Database Identifier: {db_path_manager.get_table_db_path(table)}\n"
            f"Schema: {table['id'].split('.')[0]}\n"
            f"Table: {table['id'].split('.')[1]}"
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
            f"Table: {table['id']}\n"
            f"Name: {table.get('name', 'N/A')}\n"
            f"Rows: {table.get('rowsCount', 'N/A')}\n"
            f"Size: {table.get('dataSizeBytes', 'N/A')} bytes\n"
            f"Columns: {', '.join(table.get('columns', []))}\n"
            f"---"
            for table in tables
        )

    return mcp