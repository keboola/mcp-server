"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Annotated, Any, Literal, Optional, cast

import httpx
from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.clients import AsyncStorageClient
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


def _sum(a: int | None, b: int | None) -> int | None:
    if a is None and b is None:
        return None
    else:
        return (a or 0) + (b or 0)


def _extract_description(values: dict[str, Any]) -> Optional[str]:
    """Extracts the description from values or metadata."""
    if not (description := values.get('description')):
        description = get_metadata_property(values.get('metadata', []), MetadataField.DESCRIPTION)
    return description or None


class BucketDetail(BaseModel):
    id: str = Field(description='Unique identifier for the bucket.')
    name: str = Field(description='Name of the bucket.')
    display_name: str = Field(
        description='The display name of the bucket.',
        validation_alias=AliasChoices('displayName', 'display_name', 'display-name'),
        serialization_alias='displayName',
    )
    description: Optional[str] = Field(None, description='Description of the bucket.')
    stage: str = Field(description='Stage of the bucket (in for input stage, out for output stage).')
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

    # these are internal fields not meant to be exposed to LLMs
    branch_id: Optional[str] = Field(
        default=None, exclude=True, description='The ID of the branch the bucket belongs to.'
    )
    prod_id: str = Field(default='', exclude=True, description='The ID of the production branch bucket.')
    # TODO: add prod_name too to strip the '{branch_id}-' prefix from the name'

    def shade_by(self, other: 'BucketDetail', branch_id: str | None, links: list[Link] | None = None) -> 'BucketDetail':
        if self.branch_id:
            raise ValueError(
                f'Dev branch buckets cannot be shaded: ' f'bucket.id={self.id}, bucket.branch_id={self.branch_id}'
            )
        if not other.branch_id:
            raise ValueError(
                f'Prod branch buckets cannot shade others: ' f'bucket.id={other.id}, bucket.branch_id={other.branch_id}'
            )
        if other.branch_id != branch_id:
            raise ValueError(
                f'Dev branch mismatch: '
                f'bucket.id={other.id}, bucket.branch_id={other.branch_id}, branch_id={branch_id}'
            )
        if other.prod_id != self.id:
            raise ValueError(f'Prod and dev buckets mismatch: prod_bucket.id={self.id}, dev_bucket.id={other.id}')
        changes: dict[str, int | None | list[Link] | str] = {
            # TODO: The name and display_name of a branch bucket typically contains the branch ID
            #  and we may not wont to show that.
            # 'name': other.name,
            # 'display_name': other.display_name,
            # 'description': other.description,
            # TODO: These bytes and counts are approximated by summing the values of the two buckets.
            'data_size_bytes': _sum(self.data_size_bytes, other.data_size_bytes),
            'tables_count': _sum(self.tables_count, other.tables_count),
        }
        if links:
            changes['links'] = links
        return self.model_copy(update=changes)

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

    @model_validator(mode='before')
    @classmethod
    def set_branch_id(cls, values: dict[str, Any]) -> dict[str, Any]:
        branch_id = get_metadata_property(values.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
        if branch_id:
            values['branch_id'] = branch_id
            values['prod_id'] = values['id'].replace(f'c-{branch_id}-', 'c-')
        else:
            values['branch_id'] = None
            values['prod_id'] = values['id']
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

    # these are internal fields not meant to be exposed to LLMs
    branch_id: Optional[str] = Field(
        default=None, exclude=True, description='The ID of the branch the bucket belongs to.'
    )
    prod_id: str = Field(default='', exclude=True, description='The ID of the production branch bucket.')

    @model_validator(mode='before')
    @classmethod
    def set_description(cls, values: dict[str, Any]) -> dict[str, Any]:
        values['description'] = _extract_description(values)
        return values

    @model_validator(mode='before')
    @classmethod
    def set_branch_id(cls, values: dict[str, Any]) -> dict[str, Any]:
        branch_id = get_metadata_property(values.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
        if branch_id:
            values['branch_id'] = branch_id
            values['prod_id'] = values['id'].replace(f'c-{branch_id}-', 'c-')
        else:
            values['branch_id'] = None
            values['prod_id'] = values['id']
        return values


class ListTablesOutput(BaseModel):
    tables: list[TableDetail] = Field(description='List of tables.')
    links: list[Link] = Field(description='Links relevant to the table listing.')


class UpdateDescriptionOutput(BaseModel):
    description: str = Field(description='The updated description value.', alias='value')
    timestamp: datetime = Field(description='The timestamp of the description update.')
    success: bool = Field(default=True, description='Indicates if the update succeeded.')
    links: Optional[list[Link]] = Field(None, description='Links relevant to the description update.')


async def _get_bucket_detail(client: AsyncStorageClient, bucket_id: str) -> JsonDict | None:
    try:
        return await client.bucket_detail(bucket_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        raise


async def _get_table_detail(client: AsyncStorageClient, table_id: str) -> JsonDict | None:
    try:
        return await client.table_detail(table_id)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None
        raise


async def _find_buckets(client: KeboolaClient, bucket_id: str) -> tuple[BucketDetail | None, BucketDetail | None]:
    prod_bucket: BucketDetail | None = None
    dev_bucket: BucketDetail | None = None

    if raw := await _get_bucket_detail(client.storage_client, bucket_id):
        bucket = BucketDetail.model_validate(raw)
        if not bucket.branch_id:
            prod_bucket = bucket
        elif bucket.branch_id == client.branch_id:
            dev_bucket = bucket

    if client.branch_id:
        if not dev_bucket:
            dev_id = bucket_id.replace('c-', f'c-{client.branch_id}-')
            if raw := await _get_bucket_detail(client.storage_client, dev_id):
                bucket = BucketDetail.model_validate(raw)
                if bucket.branch_id == client.branch_id:
                    dev_bucket = bucket

        if not prod_bucket and f'.c-{client.branch_id}-' in bucket_id:
            prod_id = bucket_id.replace(f'c-{client.branch_id}-', 'c-')
            if raw := await _get_bucket_detail(client.storage_client, prod_id):
                bucket = BucketDetail.model_validate(raw)
                if not bucket.branch_id:
                    prod_bucket = bucket

    return prod_bucket, dev_bucket


async def _combine_buckets(
    client: KeboolaClient,
    links_manager: ProjectLinksManager,
    prod_bucket: BucketDetail | None,
    dev_bucket: BucketDetail | None,
) -> BucketDetail:

    if prod_bucket and dev_bucket:
        # generate a URL link to the dev bucket but with the prod bucket's name
        links = links_manager.get_bucket_links(dev_bucket.id, prod_bucket.name or prod_bucket.id)
        bucket = prod_bucket.shade_by(dev_bucket, client.branch_id, links)
    elif prod_bucket:
        links = links_manager.get_bucket_links(prod_bucket.id, prod_bucket.name or prod_bucket.id)
        bucket = prod_bucket.model_copy(update={'links': links})
    elif dev_bucket:
        links = links_manager.get_bucket_links(dev_bucket.id, dev_bucket.name or dev_bucket.id)
        bucket = dev_bucket.model_copy(update={'id': dev_bucket.prod_id, 'branch_id': None, 'links': links})
    else:
        raise ValueError('No buckets specified.')

    return bucket


@tool_errors()
async def get_bucket(
    bucket_id: Annotated[str, Field(description='Unique ID of the bucket.')], ctx: Context
) -> BucketDetail:
    """Gets detailed information about a specific bucket."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    prod_bucket, dev_bucket = await _find_buckets(client, bucket_id)
    if not prod_bucket and not dev_bucket:
        raise ValueError(f'Bucket not found: {bucket_id}')
    else:
        return await _combine_buckets(client, links_manager, prod_bucket, dev_bucket)


@tool_errors()
async def list_buckets(ctx: Context) -> ListBucketsOutput:
    """Retrieves information about all buckets in the project."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_bucket_data = await client.storage_client.bucket_list(include=['metadata'])

    # group buckets by their ID as it would appear on the production branch
    buckets_by_prod_id: dict[str, list[BucketDetail]] = defaultdict(list)
    for raw in raw_bucket_data:
        bucket = BucketDetail.model_validate(raw)
        if bucket.branch_id and bucket.branch_id != client.branch_id:
            # a dev branch bucket from a different branch
            continue
        buckets_by_prod_id[bucket.prod_id].append(bucket)

    buckets: list[BucketDetail] = []
    for prod_id, group in buckets_by_prod_id.items():
        prod_bucket: BucketDetail | None = None
        dev_buckets: list[BucketDetail] = []
        for b in group:
            if b.branch_id:
                dev_buckets.append(b)
            else:
                prod_bucket = b

        if not prod_bucket and not dev_buckets:
            # should not happen
            raise Exception(f'No buckets in the group: prod_id={prod_id}')

        else:
            bucket = await _combine_buckets(client, links_manager, prod_bucket, next(iter(dev_buckets), None))
            buckets.append(bucket.model_copy(update={'links': None}))  # no links when listing buckets

    return ListBucketsOutput(buckets=buckets, links=[links_manager.get_bucket_dashboard_link()])


@tool_errors()
async def get_table(
    table_id: Annotated[str, Field(description='Unique ID of the table.')], ctx: Context
) -> TableDetail:
    """Gets detailed information about a specific table including its DB identifier and column information."""
    client = KeboolaClient.from_state(ctx.session.state)

    prod_table: JsonDict | None = await _get_table_detail(client.storage_client, table_id)
    if prod_table:
        branch_id = get_metadata_property(prod_table.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
        if branch_id:
            # The table should be from the prod branch; pretend that the table does not exist.
            prod_table = None

    dev_table: JsonDict | None = None
    if client.branch_id:
        dev_id = table_id.replace('c-', f'c-{client.branch_id}-')
        dev_table = await _get_table_detail(client.storage_client, dev_id)
        if dev_table:
            branch_id = get_metadata_property(dev_table.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
            if branch_id != client.branch_id:
                # The table's branch ID does not match; pretend that the table does not exist.
                dev_table = None

    raw_table = dev_table or prod_table
    if not raw_table:
        raise ValueError(f'Table not found: {table_id}')

    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    raw_columns = cast(list[str], raw_table.get('columns', []))
    raw_column_metadata = cast(dict[str, list[dict[str, Any]]], raw_table.get('columnMetadata', {}))
    raw_primary_key = cast(list[str], raw_table.get('primaryKey', []))
    sql_dialect = await workspace_manager.get_sql_dialect()

    column_info = []
    for col_name in raw_columns:
        col_meta = raw_column_metadata.get(col_name, [])
        native_type: str | None = get_metadata_property(col_meta, MetadataField.DATATYPE_TYPE)
        if native_type:
            raw_nullable = get_metadata_property(col_meta, MetadataField.DATATYPE_NULLABLE) or ''
            nullable = raw_nullable.lower() in ['1', 'yes', 'true']
        else:
            # default values for untyped columns
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

    bucket_info = cast(dict[str, Any], raw_table.get('bucket', {}))
    bucket_id = cast(str, bucket_info.get('id', ''))
    prod_bucket, dev_bucket = await _find_buckets(client, bucket_id)
    bucket = await _combine_buckets(client, links_manager, prod_bucket, dev_bucket)

    table_fqn = await workspace_manager.get_table_fqn(raw_table)
    table_name = cast(str, raw_table.get('name', ''))
    links = links_manager.get_table_links(bucket_id, bucket.name, table_name)

    table = TableDetail.model_validate(
        raw_table
        | {
            'columns': column_info,
            'fully_qualified_name': table_fqn.identifier if table_fqn else None,
            'links': links,
        }
    )
    return table.model_copy(update={'id': table.prod_id, 'branch_id': None})


@tool_errors()
async def list_tables(
    bucket_id: Annotated[str, Field(description='Unique ID of the bucket.')], ctx: Context
) -> ListTablesOutput:
    """Retrieves all tables in a specific bucket with their basic information."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    prod_bucket, dev_bucket = await _find_buckets(client, bucket_id)

    # TODO: requesting "metadata" to get the table description;
    #  We could also request "columns" and use WorkspaceManager to prepare the table's FQN and columns' quoted names.
    #  This could take time for larger buckets, but could save calls to get_table_metadata() later.

    tables_by_prod_id: dict[str, TableDetail] = {}
    if prod_bucket:
        raw_table_data = await client.storage_client.bucket_table_list(prod_bucket.id, include=['metadata'])
        for raw in raw_table_data:
            table = TableDetail.model_validate(raw)
            tables_by_prod_id[table.prod_id] = table

    if dev_bucket:
        raw_table_data = await client.storage_client.bucket_table_list(dev_bucket.id, include=['metadata'])
        for raw in raw_table_data:
            table = TableDetail.model_validate(raw)
            tables_by_prod_id[table.prod_id] = table.model_copy(update={'id': table.prod_id, 'branch_id': None})

    bucket = await _combine_buckets(client, links_manager, prod_bucket, dev_bucket)
    return ListTablesOutput(tables=list(tables_by_prod_id.values()), links=bucket.links or [])


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
