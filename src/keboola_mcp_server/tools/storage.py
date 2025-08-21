"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from datetime import datetime
from typing import Annotated, Any, Literal, Optional, cast

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, exclude_none_serializer
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

STORAGE_TOOLS_TAG = 'storage'


def add_storage_tools(mcp: KeboolaMcpServer) -> None:
    """Adds tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            get_bucket,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            list_buckets,
            serializer=exclude_none_serializer,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_table,
            serializer=exclude_none_serializer,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            list_tables,
            serializer=exclude_none_serializer,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            update_description,
            serializer=exclude_none_serializer,
            annotations=ToolAnnotations(destructiveHint=True),
            tags={STORAGE_TOOLS_TAG},
        )
    )

    LOG.info('Storage tools added to the MCP server.')


def _extract_description(values: dict[str, Any]) -> Optional[str]:
    """Extracts the description from values or metadata."""
    if description := values.get('description'):
        return description
    else:
        return get_metadata_property(values.get('metadata', []), MetadataField.DESCRIPTION)


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
        values['description'] = _extract_description(values)
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
    native_type: str = Field(description='The database type of data in the column.')
    nullable: bool = Field(description='Whether the column can contain null values.')


class TableDetail(BaseModel):
    id: str = Field(description='Unique identifier for the table.')
    name: str = Field(description='Name of the table.')
    display_name: str = Field(
        description='The display name of the table.',
        validation_alias=AliasChoices('displayName', 'display_name', 'display-name'),
        serialization_alias='displayName',
    )
    description: str | None = Field(default=None, description='Description of the table.')
    primary_key: list[str] | None = Field(
        default=None,
        description='List of primary key columns.',
        validation_alias=AliasChoices('primaryKey', 'primary_key', 'primary-key'),
        serialization_alias='primaryKey',
    )
    created: str | None = Field(default=None, description='Creation timestamp of the table.')
    rows_count: int | None = Field(
        default=None,
        description='Number of rows in the table.',
        validation_alias=AliasChoices('rowsCount', 'rows_count', 'rows-count'),
        serialization_alias='rowsCount',
    )
    data_size_bytes: int | None = Field(
        default=None,
        description='Total data size of the table in bytes.',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes', 'data-size-bytes'),
        serialization_alias='dataSizeBytes',
    )
    columns: list[TableColumnInfo] | None = Field(
        default=None,
        description='List of column information including database identifiers.',
    )
    fully_qualified_name: str | None = Field(
        default=None,
        description='Fully qualified name of the table.',
        validation_alias=AliasChoices('fullyQualifiedName', 'fully_qualified_name', 'fully-qualified-name'),
        serialization_alias='fullyQualifiedName',
    )
    links: list[Link] | None = Field(default=None, description='The links relevant to the table.')

    @model_validator(mode='before')
    @classmethod
    def set_description(cls, values: dict[str, Any]) -> dict[str, Any]:
        values['description'] = _extract_description(values)
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
async def get_table(
    table_id: Annotated[str, Field(description='Unique ID of the table.')], ctx: Context
) -> TableDetail:
    """Gets detailed information about a specific table including its DB identifier and column information."""
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_table = await client.storage_client.table_detail(table_id)
    raw_columns = cast(list[str], raw_table.get('columns', []))
    raw_column_metadata = cast(dict[str, list[dict[str, Any]]], raw_table.get('columnMetadata', {}))
    raw_primary_key = cast(list[str], raw_table.get('primaryKey', []))

    column_info = []
    for col_name in raw_columns:
        col_meta = raw_column_metadata.get(col_name, [])
        native_type: str | None = get_metadata_property(col_meta, MetadataField.DATATYPE_TYPE)
        if native_type:
            raw_nullable = get_metadata_property(col_meta, MetadataField.DATATYPE_NULLABLE) or ''
            nullable = raw_nullable.lower() in ['1', 'yes', 'true']
        else:
            # default values for untyped columns
            sql_dialect = await workspace_manager.get_sql_dialect()
            native_type = 'STRING' if sql_dialect == 'BigQuery' else 'VARCHAR'
            nullable = col_name not in raw_primary_key

        column_info.append(
            TableColumnInfo(
                name=col_name,
                quoted_name=await workspace_manager.get_quoted_name(col_name),
                native_type=native_type,
                nullable=nullable,
            )
        )

    table_fqn = await workspace_manager.get_table_fqn(raw_table)
    bucket_info = cast(dict[str, Any], raw_table.get('bucket', {}))
    bucket_id = cast(str, bucket_info.get('id', ''))
    table_name = cast(str, raw_table.get('name', ''))
    links = links_manager.get_table_links(bucket_id, table_name)

    return TableDetail.model_validate(
        raw_table
        | {'columns': column_info, 'fully_qualified_name': table_fqn.identifier if table_fqn else None, 'links': links}
    )


@tool_errors()
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
async def update_description(
    ctx: Context,
    item_type: Annotated[
        Literal['bucket', 'table', 'column'],
        Field(description='Type of the item to update. One of: bucket, table, column.'),
    ],
    description: Annotated[str, Field(description='The new description to set for the specified item.')],
    bucket_id: Annotated[str, Field(default='', description='Bucket ID. Required when item_type is "bucket".')] = '',
    table_id: Annotated[
        str, Field(default='', description='Table ID. Required when item_type is "table" or "column".')
    ] = '',
    column_name: Annotated[
        str, Field(default='', description='Column name. Required when item_type is "column".')
    ] = '',
) -> Annotated[
    UpdateDescriptionOutput,
    Field(description='The response object for the description update.'),
]:
    """
    Updates the description for a Keboola storage item.

    The tool supports three item types and validates the required identifiers based on the selected type:

    - item_type = "bucket": requires bucket_id
    - item_type = "table": requires table_id
    - item_type = "column": requires table_id and column_name

    Usage examples:
    - Update a bucket: item_type="bucket", bucket_id="in.c-my-bucket",
      description="New bucket description"
    - Update a table: item_type="table", table_id="in.c-my-bucket.my-table",
      description="New table description"
    - Update a column: item_type="column", table_id="in.c-my-bucket.my-table",
      column_name="my_column", description="New column description"

    :return: The update result containing the stored description, timestamp, success flag, and optional links.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    if item_type == 'bucket':
        if not bucket_id:
            raise ValueError('bucket_id is required when item_type is "bucket"')
        links_manger = await ProjectLinksManager.from_client(client)
        response = await client.storage_client.bucket_metadata_update(
            bucket_id=bucket_id,
            metadata={MetadataField.DESCRIPTION: description},
        )

        description_entry = next(entry for entry in response if entry.get('key') == MetadataField.DESCRIPTION)
        links = [links_manger.get_bucket_detail_link(bucket_id=bucket_id, bucket_name=bucket_id)]
        return UpdateDescriptionOutput.model_validate(description_entry | {'links': links})

    if item_type == 'table':
        if not table_id:
            raise ValueError('table_id is required when item_type is "table"')
        response = await client.storage_client.table_metadata_update(
            table_id=table_id,
            metadata={MetadataField.DESCRIPTION: description},
            columns_metadata={},
        )
        raw_metadata = cast(list[JsonDict], response.get('metadata', []))
        description_entry = next(entry for entry in raw_metadata if entry.get('key') == MetadataField.DESCRIPTION)
        return UpdateDescriptionOutput.model_validate(description_entry)

    # item_type == 'column'
    if not table_id:
        raise ValueError('table_id is required when item_type is "column"')
    if not column_name:
        raise ValueError('column_name is required when item_type is "column"')

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
