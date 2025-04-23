"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from typing import Annotated, Any, Dict, List, Mapping, Optional, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.sql_tools import WorkspaceManager

LOG = logging.getLogger(__name__)


def add_storage_tools(mcp: FastMCP) -> None:
    """Adds tools to the MCP server."""
    mcp.add_tool(get_bucket_detail)
    mcp.add_tool(retrieve_buckets)
    mcp.add_tool(get_table_detail)
    mcp.add_tool(retrieve_bucket_tables)
    mcp.add_tool(update_bucket_description)
    mcp.add_tool(update_table_description)

    LOG.info("Storage tools added to the MCP server.")


def extract_description(values: Dict[str, Any]) -> Optional[str]:
    """Extracts the description from values or metadata."""
    if description := values.get("description"):
        return description
    else:
        metadata = values.get("metadata", [])
        return next(
            (
                value
                for item in metadata
                if (
                    item.get("key") == MetadataField.DESCRIPTION.value
                    and (value := item.get("value"))
                )
            ),
            None,
        )


class BucketDetail(BaseModel):
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

    @model_validator(mode="before")
    @classmethod
    def set_description(cls, values):
        values["description"] = extract_description(values)
        return values


class TableColumnInfo(BaseModel):
    name: str = Field(description="Plain name of the column.")
    quoted_name: str = Field(
        description="The properly quoted name of the column.",
        validation_alias=AliasChoices("quotedName", "quoted_name", "quoted-name"),
        serialization_alias="quotedName",
    )


class TableDetail(BaseModel):
    id: str = Field(description="Unique identifier for the table")
    name: str = Field(description="Name of the table")
    description: Optional[str] = Field(None, description="Description of the table")
    primary_key: Optional[List[str]] = Field(
        None,
        description="List of primary key columns",
        validation_alias=AliasChoices("primaryKey", "primary_key", "primary-key"),
        serialization_alias="primaryKey",
    )
    created: Optional[str] = Field(None, description="Creation timestamp of the table")
    rows_count: Optional[int] = Field(
        None,
        description="Number of rows in the table",
        validation_alias=AliasChoices("rowsCount", "rows_count", "rows-count"),
        serialization_alias="rowsCount",
    )
    data_size_bytes: Optional[int] = Field(
        None,
        description="Total data size of the table in bytes",
        validation_alias=AliasChoices("dataSizeBytes", "data_size_bytes", "data-size-bytes"),
        serialization_alias="dataSizeBytes",
    )
    columns: Optional[List[TableColumnInfo]] = Field(
        None,
        description="List of column information including database identifiers",
    )
    fully_qualified_name: Optional[str] = Field(
        None,
        description="Fully qualified name of the table.",
        validation_alias=AliasChoices(
            "fullyQualifiedName", "fully_qualified_name", "fully-qualified-name"
        ),
        serialization_alias="fullyQualifiedName",
    )

    @model_validator(mode="before")
    @classmethod
    def set_description(cls, values):
        values["description"] = extract_description(values)
        return values


class UpdateBucketDescriptionResponse(BaseModel):
    success: bool = True
    description: str = Field(description="The updated description value")
    timestamp: str = Field(description="When the description was updated")

    @model_validator(mode="before")
    def extract_from_response(cls, values):
        if isinstance(values, list) and values:
            data = values[0]  # the response returns a list - elements for each update
            return {
                "success": True,
                "description": data.get("value"),
                "timestamp": data.get("timestamp"),
            }
        else:
            raise ValueError(
                "Expected input data in UpdateBucketDescriptionResponse to be non-empty list."
            )


class UpdateTableDescriptionResponse(BaseModel):
    success: bool = True
    description: str = Field(description="The updated table description value")
    timestamp: str = Field(description="When the description was updated")

    @model_validator(mode="before")
    def extract_metadata(cls, values):
        metadata = values.get("metadata", [])
        if isinstance(metadata, list) and metadata:
            entry = metadata[0]
            return {
                "success": True,
                "description": entry.get("value"),
                "timestamp": entry.get("timestamp"),
            }
        else:
            raise ValueError(
                "Expected input data in UpdateTableDescriptionResponse to have non-empty list in 'metadata' field."
            )


async def get_bucket_detail(
    bucket_id: Annotated[str, Field(description="Unique ID of the bucket.")], ctx: Context
) -> BucketDetail:
    """Gets detailed information about a specific bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    assert isinstance(client, KeboolaClient)
    raw_bucket = cast(Dict[str, Any], client.storage_client.buckets.detail(bucket_id))

    return BucketDetail(**raw_bucket)


async def retrieve_buckets(ctx: Context) -> List[BucketDetail]:
    """Retrieves information about all buckets in the project."""
    client = KeboolaClient.from_state(ctx.session.state)
    assert isinstance(client, KeboolaClient)
    raw_bucket_data = client.storage_client.buckets.list()

    return [BucketDetail(**raw_bucket) for raw_bucket in raw_bucket_data]


async def get_table_detail(
    table_id: Annotated[str, Field(description="Unique ID of the table.")], ctx: Context
) -> TableDetail:
    """Gets detailed information about a specific table including its DB identifier and column information."""
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)

    raw_table = cast(Mapping[str, Any], client.storage_client.tables.detail(table_id))
    column_info = [
        TableColumnInfo(name=col, quoted_name=await workspace_manager.get_quoted_name(col))
        for col in raw_table.get("columns", [])
    ]

    table_fqn = await workspace_manager.get_table_fqn(raw_table)

    return TableDetail(
        **{**raw_table, "columns": column_info, "fully_qualified_name": table_fqn.identifier}
    )


async def retrieve_bucket_tables(
    bucket_id: Annotated[str, Field(description="Unique ID of the bucket.")], ctx: Context
) -> list[TableDetail]:
    """Retrieves all tables in a specific bucket with their basic information."""
    client = KeboolaClient.from_state(ctx.session.state)
    # TODO: requesting "metadata" to get the table description;
    #  We could also request "columns" and use WorkspaceManager to prepare the table's FQN and columns' quoted names.
    #  This could take time for larger buckets, but could save calls to get_table_metadata() later.
    raw_tables = cast(
        List[Mapping[str, Any]],
        client.storage_client.buckets.list_tables(bucket_id, include=["metadata"]),
    )
    return [TableDetail(**raw_table) for raw_table in raw_tables]


async def update_bucket_description(
    bucket_id: Annotated[str, Field(description="The ID of the bucket to update.")],
    description: Annotated[str, Field(description="The new description for the bucket.")],
    ctx: Context,
) -> Annotated[
    UpdateBucketDescriptionResponse,
    Field(description="The response object of the Bucket description update."),
]:
    """Update the description for a given Keboola bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    metadata_endpoint = f"buckets/{bucket_id}/metadata"

    data = {
        "provider": "user",
        "metadata": [{"key": MetadataField.DESCRIPTION.value, "value": description}],
    }
    response = await client.post(endpoint=metadata_endpoint, data=data)

    return UpdateBucketDescriptionResponse.model_validate(response)


async def update_table_description(
    table_id: Annotated[str, Field(description="The ID of the table to update.")],
    description: Annotated[str, Field(description="The new description for the table.")],
    ctx: Context,
) -> Annotated[
    UpdateTableDescriptionResponse,
    Field(description="The response object of the Table description update."),
]:
    """Update the description for a given Keboola table."""
    client = KeboolaClient.from_state(ctx.session.state)
    metadata_endpoint = f"tables/{table_id}/metadata"

    data = {
        "provider": "user",
        "metadata": [{"key": MetadataField.DESCRIPTION.value, "value": description}],
    }
    response = await client.post(endpoint=metadata_endpoint, data=data)

    return UpdateTableDescriptionResponse.model_validate(response)
