"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from datetime import datetime
from typing import Annotated, Any, Mapping, Optional, cast

from mcp.server.fastmcp import Context
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.sql import WorkspaceManager

LOG = logging.getLogger(__name__)

TOOL_GROUP_NAME = 'STORAGE'


def add_storage_tools(mcp: KeboolaMcpServer) -> None:
    """Adds tools to the MCP server."""
    mcp.add_tool(get_bucket_detail)
    mcp.add_tool(retrieve_buckets)
    mcp.add_tool(get_table_detail)
    mcp.add_tool(retrieve_bucket_tables)
    mcp.add_tool(update_bucket_description)
    mcp.add_tool(update_table_description)
    mcp.add_tool(update_column_description)

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
                if (item.get('key') == MetadataField.DESCRIPTION.value and (value := item.get('value')))
            ),
            None,
        )


class BucketDetail(BaseModel):
    id: str = Field(description='Unique identifier for the bucket')
    name: str = Field(description='Name of the bucket')
    description: Optional[str] = Field(None, description='Description of the bucket')
    stage: Optional[str] = Field(None, description='Stage of the bucket (in for input stage, out for output stage)')
    created: str = Field(description='Creation timestamp of the bucket')
    data_size_bytes: Optional[int] = Field(
        None,
        description='Total data size of the bucket in bytes',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes', 'data-size-bytes'),
        serialization_alias='dataSizeBytes',
    )

    tables_count: Optional[int] = Field(
        default=None,
        description='Number of tables in the bucket',
        validation_alias=AliasChoices('tablesCount', 'tables_count', 'tables-count'),
        serialization_alias='tablesCount',
    )

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


class TableColumnInfo(BaseModel):
    name: str = Field(description='Plain name of the column.')
    quoted_name: str = Field(
        description='The properly quoted name of the column.',
        validation_alias=AliasChoices('quotedName', 'quoted_name', 'quoted-name'),
        serialization_alias='quotedName',
    )


class TableDetail(BaseModel):
    id: str = Field(description='Unique identifier for the table')
    name: str = Field(description='Name of the table')
    description: Optional[str] = Field(None, description='Description of the table')
    primary_key: Optional[list[str]] = Field(
        None,
        description='List of primary key columns',
        validation_alias=AliasChoices('primaryKey', 'primary_key', 'primary-key'),
        serialization_alias='primaryKey',
    )
    created: Optional[str] = Field(None, description='Creation timestamp of the table')
    rows_count: Optional[int] = Field(
        None,
        description='Number of rows in the table',
        validation_alias=AliasChoices('rowsCount', 'rows_count', 'rows-count'),
        serialization_alias='rowsCount',
    )
    data_size_bytes: Optional[int] = Field(
        None,
        description='Total data size of the table in bytes',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes', 'data-size-bytes'),
        serialization_alias='dataSizeBytes',
    )
    columns: Optional[list[TableColumnInfo]] = Field(
        None,
        description='List of column information including database identifiers',
    )
    fully_qualified_name: Optional[str] = Field(
        None,
        description='Fully qualified name of the table.',
        validation_alias=AliasChoices('fullyQualifiedName', 'fully_qualified_name', 'fully-qualified-name'),
        serialization_alias='fullyQualifiedName',
    )

    @model_validator(mode='before')
    @classmethod
    def set_description(cls, values: dict[str, Any]) -> dict[str, Any]:
        values['description'] = extract_description(values)
        return values


class UpdateDescriptionResponse(BaseModel):
    description: str = Field(..., description='The updated description value.', alias='value')
    timestamp: datetime = Field(..., description='The timestamp of the description update.')
    success: bool = Field(default=True, description='Indicates if the update succeeded.')


@tool_errors()
async def get_bucket_detail(
    bucket_id: Annotated[str, Field(description='Unique ID of the bucket.')], ctx: Context
) -> BucketDetail:
    """Gets detailed information about a specific bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    assert isinstance(client, KeboolaClient)
    raw_bucket = cast(dict[str, Any], client.storage_client_sync.buckets.detail(bucket_id))

    return BucketDetail(**raw_bucket)


@tool_errors()
async def retrieve_buckets(ctx: Context) -> list[BucketDetail]:
    """Retrieves information about all buckets in the project."""
    client = KeboolaClient.from_state(ctx.session.state)
    assert isinstance(client, KeboolaClient)
    raw_bucket_data = client.storage_client_sync.buckets.list()

    return [BucketDetail(**raw_bucket) for raw_bucket in raw_bucket_data]


@tool_errors()
async def get_table_detail(
    table_id: Annotated[str, Field(description='Unique ID of the table.')], ctx: Context
) -> TableDetail:
    """Gets detailed information about a specific table including its DB identifier and column information."""
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)

    raw_table = cast(Mapping[str, Any], client.storage_client_sync.tables.detail(table_id))
    column_info = [
        TableColumnInfo(name=col, quoted_name=await workspace_manager.get_quoted_name(col))
        for col in raw_table.get('columns', [])
    ]

    table_fqn = await workspace_manager.get_table_fqn(raw_table)

    return TableDetail(
        **{
            **raw_table,
            'columns': column_info,
            'fully_qualified_name': table_fqn.identifier,
        }
    )


@tool_errors()
async def retrieve_bucket_tables(
    bucket_id: Annotated[str, Field(description='Unique ID of the bucket.')], ctx: Context
) -> list[TableDetail]:
    """Retrieves all tables in a specific bucket with their basic information."""
    client = KeboolaClient.from_state(ctx.session.state)
    # TODO: requesting "metadata" to get the table description;
    #  We could also request "columns" and use WorkspaceManager to prepare the table's FQN and columns' quoted names.
    #  This could take time for larger buckets, but could save calls to get_table_metadata() later.
    raw_tables = cast(
        list[Mapping[str, Any]],
        client.storage_client_sync.buckets.list_tables(bucket_id, include=['metadata']),
    )
    return [TableDetail(**raw_table) for raw_table in raw_tables]


@tool_errors()
async def update_bucket_description(
    bucket_id: Annotated[str, Field(description='The ID of the bucket to update.')],
    description: Annotated[str, Field(description='The new description for the bucket.')],
    ctx: Context,
) -> Annotated[
    UpdateDescriptionResponse,
    Field(description='The response object of the Bucket description update.'),
]:
    """Update the description for a given Keboola bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    metadata_endpoint = f'buckets/{bucket_id}/metadata'

    data = {
        'provider': 'user',
        'metadata': [{'key': MetadataField.DESCRIPTION.value, 'value': description}],
    }
    response = await client.storage_client.post(endpoint=metadata_endpoint, data=data)
    description_entry = next(entry for entry in response if entry.get('key') == MetadataField.DESCRIPTION.value)

    return UpdateDescriptionResponse.model_validate(description_entry)


@tool_errors()
async def update_table_description(
    table_id: Annotated[str, Field(description='The ID of the table to update.')],
    description: Annotated[str, Field(description='The new description for the table.')],
    ctx: Context,
) -> Annotated[
    UpdateDescriptionResponse,
    Field(description='The response object of the Table description update.'),
]:
    """Update the description for a given Keboola table."""
    client = KeboolaClient.from_state(ctx.session.state)
    metadata_endpoint = f'tables/{table_id}/metadata'

    data = {
        'provider': 'user',
        'metadata': [{'key': MetadataField.DESCRIPTION.value, 'value': description}],
    }
    response = await client.storage_client.post(endpoint=metadata_endpoint, data=data)
    description_entry = next(
        entry for entry in response.get('metadata') if entry.get('key') == MetadataField.DESCRIPTION.value
    )

    return UpdateDescriptionResponse.model_validate(description_entry)


@tool_errors()
async def update_column_description(
    table_id: Annotated[str, Field(description='The ID of the table that contains the column.')],
    column_name: Annotated[str, Field(description='The name of the column to update.')],
    description: Annotated[str, Field(description='The new description for the column.')],
    ctx: Context,
) -> Annotated[
    UpdateDescriptionResponse,
    Field(description='The response object of the column description update.'),
]:
    """Update the description for a given column in a Keboola table."""
    client = KeboolaClient.from_state(ctx.session.state)
    metadata_endpoint = f'tables/{table_id}/metadata'

    data = {
        'provider': 'user',
        'columnsMetadata': {
            f'{column_name}': [
                {
                    'key': MetadataField.DESCRIPTION.value,
                    'value': description,
                    'columnName': column_name,
                }
            ]
        },
    }

    response = await client.storage_client.post(endpoint=metadata_endpoint, data=data)
    description_entry = next(
        entry
        for entry in response.get('columnsMetadata').get(column_name)
        if entry.get('key') == MetadataField.DESCRIPTION.value
    )

    return UpdateDescriptionResponse.model_validate(description_entry)
