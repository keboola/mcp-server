"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from datetime import datetime
from typing import Annotated, Any, Optional, cast

from fastmcp import Context
from fastmcp.tools import FunctionTool
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import JsonDict, KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, listing_output_serializer, with_session_state
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

TOOL_GROUP_NAME = 'STORAGE'


def add_storage_tools(mcp: KeboolaMcpServer) -> None:
    """Adds tools to the MCP server."""
    mcp.add_tool(FunctionTool.from_function(get_bucket))
    mcp.add_tool(FunctionTool.from_function(list_buckets, serializer=listing_output_serializer))
    mcp.add_tool(FunctionTool.from_function(get_table))
    mcp.add_tool(FunctionTool.from_function(list_tables, serializer=listing_output_serializer))
    mcp.add_tool(FunctionTool.from_function(update_bucket_description))
    mcp.add_tool(FunctionTool.from_function(update_table_description, serializer=listing_output_serializer))
    mcp.add_tool(FunctionTool.from_function(update_column_description, serializer=listing_output_serializer))

    LOG.info('Storage tools added to the MCP server.')


def extract_description(values: dict[str, Any]) -> Optional[str]:
    """Extracts the description from values or metadata."""
    if description := values.get('description'):
        return description
    else:
        metadata = values.get('metadata', [])
        return next(
            (
                value
                for item in metadata
                if item.get('key') == MetadataField.DESCRIPTION and (value := item.get('value'))
            ),
            None,
        )


class BucketDetail(BaseModel):
    id: str = Field(description='Unique identifier for the bucket.')
    name: str = Field(description='Name of the bucket.')
    display_name: str = Field(
        description='The display name of the bucket.',
        validation_alias=AliasChoices('displayName', 'display_name', 'display-name'),
        serialization_alias='displayName',
    )
    description: Optional[str] = Field(None, description='Description of the bucket.')
    stage: Optional[str] = Field(None, description='Stage of the bucket (in for input stage, out for output stage).')
    created: str = Field(description='Creation timestamp of the bucket.')
    data_size_bytes: Optional[int] = Field(
        None,
        description='Total data size of the bucket in bytes.',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes', 'data-size-bytes'),
        serialization_alias='dataSizeBytes',
    )

    tables_count: Optional[int] = Field(
        default=None,
        description='Number of tables in the bucket.',
        validation_alias=AliasChoices('tablesCount', 'tables_count', 'tables-count'),
        serialization_alias='tablesCount',
    )
    links: Optional[list[Link]] = Field(default=None, description='The links relevant to the bucket.')

    @model_validator(mode='before')
    @classmethod
    def set_table_count(cls, values: dict[str, Any]) -> dict[str, Any]:
        if isinstance(values.get('tables'), list):
            values['tables_count'] = len(values['tables'])
        else:
            values['tables_count'] = None
        return values

    @model_validator(mode='before')
    @classmethod
    def set_description(cls, values: dict[str, Any]) -> dict[str, Any]:
        values['description'] = extract_description(values)
        return values


class ListBucketsOutput(BaseModel):
    buckets: list[BucketDetail] = Field(..., description='List of buckets.')
    links: list[Link] = Field(..., description='Links relevant to the bucket listing.')


class TableColumnInfo(BaseModel):
    name: str = Field(description='Plain name of the column.')
    quoted_name: str = Field(
        description='The properly quoted name of the column.',
        validation_alias=AliasChoices('quotedName', 'quoted_name', 'quoted-name'),
        serialization_alias='quotedName',
    )


class TableDetail(BaseModel):
    id: str = Field(description='Unique identifier for the table.')
    name: str = Field(description='Name of the table.')
    display_name: str = Field(
        description='The display name of the table.',
        validation_alias=AliasChoices('displayName', 'display_name', 'display-name'),
        serialization_alias='displayName',
    )
    description: Optional[str] = Field(None, description='Description of the table.')
    primary_key: Optional[list[str]] = Field(
        None,
        description='List of primary key columns.',
        validation_alias=AliasChoices('primaryKey', 'primary_key', 'primary-key'),
        serialization_alias='primaryKey',
    )
    created: Optional[str] = Field(None, description='Creation timestamp of the table.')
    rows_count: Optional[int] = Field(
        None,
        description='Number of rows in the table.',
        validation_alias=AliasChoices('rowsCount', 'rows_count', 'rows-count'),
        serialization_alias='rowsCount',
    )
    data_size_bytes: Optional[int] = Field(
        None,
        description='Total data size of the table in bytes.',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes', 'data-size-bytes'),
        serialization_alias='dataSizeBytes',
    )
    columns: Optional[list[TableColumnInfo]] = Field(
        None,
        description='List of column information including database identifiers.',
    )
    fully_qualified_name: Optional[str] = Field(
        None,
        description='Fully qualified name of the table.',
        validation_alias=AliasChoices('fullyQualifiedName', 'fully_qualified_name', 'fully-qualified-name'),
        serialization_alias='fullyQualifiedName',
    )
    links: Optional[list[Link]] = Field(default=None, description='The links relevant to the table.')

    @model_validator(mode='before')
    @classmethod
    def set_description(cls, values: dict[str, Any]) -> dict[str, Any]:
        values['description'] = extract_description(values)
        return values


class ListTablesOutput(BaseModel):
    tables: list[TableDetail] = Field(description='List of tables.')
    links: list[Link] = Field(description='Links relevant to the table listing.')


class UpdateDescriptionOutput(BaseModel):
    description: str = Field(description='The updated description value.', alias='value')
    timestamp: datetime = Field(description='The timestamp of the description update.')
    success: bool = Field(default=True, description='Indicates if the update succeeded.')
    links: Optional[list[Link]] = Field(None, description='Links relevant to the description update.')


@tool_errors()
@with_session_state()
async def get_bucket(
    bucket_id: Annotated[str, Field(description='Unique ID of the bucket.')], ctx: Context
) -> BucketDetail:
    """Gets detailed information about a specific bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    assert isinstance(client, KeboolaClient)
    raw_bucket = await client.storage_client.bucket_detail(bucket_id)
    links = links_manager.get_bucket_links(bucket_id, raw_bucket.get('name') or bucket_id)
    bucket = BucketDetail.model_validate(raw_bucket | {'links': links})

    return bucket


@tool_errors()
@with_session_state()
async def list_buckets(ctx: Context) -> ListBucketsOutput:
    """Retrieves information about all buckets in the project."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_bucket_data = await client.storage_client.bucket_list(include=['metadata'])
    production_branch_raw_buckets = [
        bucket
        for bucket in raw_bucket_data
        if not (any(meta.get('key') == MetadataField.FAKE_DEVELOPMENT_BRANCH for meta in bucket.get('metadata', [])))
    ]  # filter out buckets from "Fake development branches"

    return ListBucketsOutput(
        buckets=[BucketDetail.model_validate(bucket) for bucket in production_branch_raw_buckets],
        links=[links_manager.get_bucket_dashboard_link()],
    )


@tool_errors()
@with_session_state()
async def get_table(
    table_id: Annotated[str, Field(description='Unique ID of the table.')], ctx: Context
) -> TableDetail:
    """Gets detailed information about a specific table including its DB identifier and column information."""
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_table = await client.storage_client.table_detail(table_id)
    raw_columns = cast(list[str], raw_table.get('columns', []))
    column_info = [
        TableColumnInfo(name=col, quoted_name=await workspace_manager.get_quoted_name(col)) for col in raw_columns
    ]

    table_fqn = await workspace_manager.get_table_fqn(raw_table)
    links = links_manager.get_table_links(raw_table['bucket']['id'], raw_table['name'])

    return TableDetail.model_validate(
        raw_table
        | {'columns': column_info, 'fully_qualified_name': table_fqn.identifier if table_fqn else None, 'links': links}
    )


@tool_errors()
@with_session_state()
async def list_tables(
    bucket_id: Annotated[str, Field(description='Unique ID of the bucket.')], ctx: Context
) -> ListTablesOutput:
    """Retrieves all tables in a specific bucket with their basic information."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    # TODO: requesting "metadata" to get the table description;
    #  We could also request "columns" and use WorkspaceManager to prepare the table's FQN and columns' quoted names.
    #  This could take time for larger buckets, but could save calls to get_table_metadata() later.
    raw_tables = await client.storage_client.bucket_table_list(bucket_id, include=['metadata'])

    return ListTablesOutput(
        tables=[TableDetail.model_validate(raw_table) for raw_table in raw_tables],
        links=[links_manager.get_bucket_detail_link(bucket_id=bucket_id, bucket_name=bucket_id)],
    )


@tool_errors()
@with_session_state()
async def update_bucket_description(
    bucket_id: Annotated[str, Field(description='The ID of the bucket to update.')],
    description: Annotated[str, Field(description='The new description for the bucket.')],
    ctx: Context,
) -> Annotated[
    UpdateDescriptionOutput,
    Field(description='The response object of the Bucket description update.'),
]:
    """Updates the description for a given Keboola bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manger = await ProjectLinksManager.from_client(client)

    response = await client.storage_client.bucket_metadata_update(
        bucket_id=bucket_id,
        metadata={MetadataField.DESCRIPTION: description},
    )

    description_entry = next(entry for entry in response if entry.get('key') == MetadataField.DESCRIPTION)
    links = [links_manger.get_bucket_detail_link(bucket_id=bucket_id, bucket_name=bucket_id)]

    return UpdateDescriptionOutput.model_validate(description_entry | {'links': links})


@tool_errors()
@with_session_state()
async def update_table_description(
    table_id: Annotated[str, Field(description='The ID of the table to update.')],
    description: Annotated[str, Field(description='The new description for the table.')],
    ctx: Context,
) -> Annotated[
    UpdateDescriptionOutput,
    Field(description='The response object of the Table description update.'),
]:
    """Updates the description for a given Keboola table."""
    client = KeboolaClient.from_state(ctx.session.state)
    response = await client.storage_client.table_metadata_update(
        table_id=table_id,
        metadata={MetadataField.DESCRIPTION: description},
        columns_metadata={},
    )
    raw_metadata = cast(list[JsonDict], response.get('metadata', []))
    description_entry = next(entry for entry in raw_metadata if entry.get('key') == MetadataField.DESCRIPTION)

    return UpdateDescriptionOutput.model_validate(description_entry)


@tool_errors()
@with_session_state()
async def update_column_description(
    table_id: Annotated[str, Field(description='The ID of the table that contains the column.')],
    column_name: Annotated[str, Field(description='The name of the column to update.')],
    description: Annotated[str, Field(description='The new description for the column.')],
    ctx: Context,
) -> Annotated[
    UpdateDescriptionOutput,
    Field(description='The response object of the column description update.'),
]:
    """Updates the description for a given column in a Keboola table."""
    client = KeboolaClient.from_state(ctx.session.state)

    response = await client.storage_client.table_metadata_update(
        table_id=table_id,
        columns_metadata={
            column_name: [{'key': MetadataField.DESCRIPTION, 'value': description, 'columnName': column_name}]
        },
    )
    column_metadata = cast(dict[str, list[JsonDict]], response.get('columnsMetadata', {}))
    description_entry = next(
        entry for entry in column_metadata.get(column_name, []) if entry.get('key') == MetadataField.DESCRIPTION
    )

    return UpdateDescriptionOutput.model_validate(description_entry)
