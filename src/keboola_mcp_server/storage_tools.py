"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from typing import Annotated, Any, Dict, List, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.database import DatabasePathManager

logger = logging.getLogger(__name__)


def add_storage_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_bucket_info)
    mcp.add_tool(get_bucket_metadata)
    mcp.add_tool(list_bucket_tables)
    mcp.add_tool(get_table_metadata)

    logger.info("Component tools added to the MCP server.")


class BucketInfo(BaseModel):
    id: str = Field(description="Unique identifier for the bucket")
    name: str = Field(description="Name of the bucket")
    description: Optional[str] = Field(None, description="Description of the bucket")
    stage: Optional[str] = Field(
        None, description="Stage of the bucket ('in' for input stage, 'out' for output stage)"
    )
    created: str = Field(description="Creation timestamp of the bucket")
    data_size_bytes: Optional[int] = Field(
        None,
        description="Total data size of the bucket in bytes",
        validation_alias=AliasChoices("dataSizeBytes", "data_size_bytes", "data-size-bytes"),
        serialization_alias="dataSizeBytes",
    )

    tables_count: Optional[int] = Field(
        default=None,
        description="Number of tables in the bucket",
        validation_alias=AliasChoices("tablesCount", "tables_count", "tables-count"),
        serialization_alias="tablesCount",
    )

    @model_validator(mode="before")
    @classmethod
    def set_table_count(cls, values):
        if isinstance(values.get("tables"), list):
            values["tables_count"] = len(values["tables"])
        else:
            values["tables_count"] = None
        return values


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


async def get_bucket_metadata(
    bucket_id: Annotated[str, Field(description="Unique ID of the bucket.")], ctx: Context
) -> BucketInfo:
    """Get detailed information about a specific bucket."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)
    raw_bucket = cast(Dict[str, Any], client.storage_client.buckets.detail(bucket_id))

    return BucketInfo(**raw_bucket)


async def list_bucket_info(ctx: Context) -> List[BucketInfo]:
    """List information about all buckets in the project."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)
    raw_bucket_data = client.storage_client.buckets.list()

    return [BucketInfo(**raw_bucket) for raw_bucket in raw_bucket_data]


async def get_table_metadata(
    table_id: Annotated[str, Field(description="Unique ID of the table.")], ctx: Context
) -> TableDetail:
    """Get detailed information about a specific table including its DB identifier and column information."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)
    raw_table = cast(Dict[str, Any], client.storage_client.tables.detail(table_id))

    columns = raw_table.get("columns", [])
    column_info = [TableColumnInfo(name=col, db_identifier=f'"{col}"') for col in columns]

    db_path_manager = ctx.session.state["db_path_manager"]
    assert isinstance(db_path_manager, DatabasePathManager)

    return TableDetail(
        **raw_table,
        column_identifiers=column_info,
        db_identifier=db_path_manager.get_table_fqn(raw_table).snowflake_fqn,
    )


async def list_bucket_tables(
    bucket_id: Annotated[str, Field(description="Unique ID of the bucket.")], ctx: Context
) -> list[TableDetail]:
    """List all tables in a specific bucket with their basic information."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)
    raw_tables = cast(List[Dict[str, Any]], client.storage_client.buckets.list_tables(bucket_id))

    return [TableDetail(**raw_table) for raw_table in raw_tables]
