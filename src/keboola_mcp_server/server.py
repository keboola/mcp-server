"""MCP server implementation for Keboola Connection."""

import csv
import logging
from io import StringIO
from typing import Annotated, Any, Dict, List, Optional, TypedDict, cast

import snowflake.connector
from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

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
    id: str = Field(description="Unique identifier for the bucket")
    name: str = Field(description="Name of the bucket")
    description: Optional[str] = Field(None, description="Description of the bucket")
    stage: Optional[str] = Field(
        None, description="Stage of the bucket ('in' for input stage, 'out' for output stage)"
    )
    created: str = Field(description="Creation timestamp of the bucket")
    table_count: Optional[int] = Field(
        None,
        description="Number of tables in the bucket",
        validation_alias=AliasChoices("tableCount", "table_count", "table-count"),
        serialization_alias="tableCount",
    )
    data_size_bytes: Optional[int] = Field(
        None,
        description="Total data size of the bucket in bytes",
        validation_alias=AliasChoices("dataSizeBytes", "data_size_bytes", "data-size-bytes"),
        serialization_alias="dataSizeBytes",
    )


class TableColumnInfo(BaseModel):
    name: str = Field(description="Name of the column")
    db_identifier: str = Field(
        description="Fully qualified database identifier for the column",
        validation_alias=AliasChoices("dbIdentifier", "db_identifier", "db-identifier"),
        serialization_alias="dbIdentifier",
    )


class TableDetail(BaseModel):
    id: str = Field(description="Unique identifier for the table")
    name: str = Field(description="Name of the table")
    primary_key: Optional[List[str]] = Field(
        None,
        description="List of primary key columns",
        validation_alias=AliasChoices("primaryKey", "primary_key", "primary-key"),
        serialization_alias="primaryKey",
    )
    created: Optional[str] = Field(None, description="Creation timestamp of the table")
    row_count: Optional[int] = Field(
        None,
        description="Number of rows in the table",
        validation_alias=AliasChoices("rowCount", "row_count", "row-count"),
        serialization_alias="rowCount",
    )
    data_size_bytes: Optional[int] = Field(
        None,
        description="Total data size of the table in bytes",
        validation_alias=AliasChoices("dataSizeBytes", "data_size_bytes", "data-size-bytes"),
        serialization_alias="dataSizeBytes",
    )
    columns: Optional[List[str]] = Field(None, description="List of column names")
    column_identifiers: Optional[List[TableColumnInfo]] = Field(
        None,
        description="List of column information including database identifiers",
        validation_alias=AliasChoices(
            "columnIdentifiers", "column_identifiers", "column-identifiers"
        ),
        serialization_alias="columnIdentifiers",
    )
    db_identifier: Optional[str] = Field(
        None,
        description="Fully qualified database identifier for the table",
        validation_alias=AliasChoices("dbIdentifier", "db_identifier", "db-identifier"),
        serialization_alias="dbIdentifier",
    )


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
    async def list_bucket_info(ctx: Context) -> List[BucketInfo]:
        """List information about all buckets in the project."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        raw_bucket_data = client.storage_client.buckets.list()

        return [BucketInfo(**raw_bucket) for raw_bucket in raw_bucket_data]

    @mcp.tool()
    async def get_bucket_metadata(
        bucket_id: Annotated[str, Field(description="Unique ID of the bucket.")], ctx: Context
    ) -> BucketInfo:
        """Get detailed information about a specific bucket."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        raw_bucket = cast(Dict[str, Any], client.storage_client.buckets.detail(bucket_id))

        return BucketInfo(**raw_bucket)

    @mcp.tool()
    async def get_table_metadata(
        table_id: Annotated[str, Field(description="Unique ID of the table.")], ctx: Context
    ) -> TableDetail:
        """Get detailed information about a specific table including its DB identifier and column information."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        raw_table = cast(Dict[str, Any], client.storage_client.tables.detail(table_id))

        # Get column info
        columns = raw_table.get("columns", [])
        column_info = [TableColumnInfo(name=col, db_identifier=f'"{col}"') for col in columns]

        db_path_manager = ctx.session.state["db_path_manager"]
        assert isinstance(db_path_manager, DatabasePathManager)

        return TableDetail(
            **raw_table,
            column_identifiers=column_info,
            db_identifier=db_path_manager.get_table_db_path(raw_table),
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
    async def list_bucket_tables(
        bucket_id: Annotated[str, Field(description="Unique ID of the bucket.")], ctx: Context
    ) -> list[TableDetail]:
        """List all tables in a specific bucket with their basic information."""
        client = ctx.session.state["sapi_client"]
        assert isinstance(client, KeboolaClient)
        raw_tables = cast(
            List[Dict[str, Any]], client.storage_client.buckets.list_tables(bucket_id)
        )
        return [TableDetail(**raw_table) for raw_table in raw_tables]

        # QUESTION: why does get_table_metadata add this additional info - column_identifiers and db_identifier?
        # db_path_manager = ctx.session.state["db_path_manager"]
        # assert isinstance(db_path_manager, DatabasePathManager)

        # return TableDetail(
        #     **raw_table,
        #     column_identifiers=column_info,
        #     db_identifier=db_path_manager.get_table_db_path(raw_table),
        # )

    return mcp
