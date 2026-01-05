"""Storage-related tools for the MCP server (buckets, tables, etc.)."""

import logging
from collections import defaultdict
from datetime import datetime
from typing import Annotated, Any, Iterable, Literal, Sequence, cast

import httpx
from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import AliasChoices, BaseModel, Field, field_serializer, model_validator

from keboola_mcp_server.clients import AsyncStorageClient
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, process_concurrently, toon_serializer, unwrap_results
from keboola_mcp_server.tools.components.utils import get_nested
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

STORAGE_TOOLS_TAG = 'storage'

BUCKET_ID_PARTS = 2
TABLE_ID_PARTS = 3
COLUMN_ID_PARTS = 4


def add_storage_tools(mcp: KeboolaMcpServer) -> None:
    """Adds tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            get_buckets,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer,
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            get_tables,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer,
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            update_descriptions,
            annotations=ToolAnnotations(destructiveHint=True),
            serializer=toon_serializer,
            tags={STORAGE_TOOLS_TAG},
        )
    )

    LOG.info('Storage tools added to the MCP server.')


def _sum(a: int | None, b: int | None) -> int | None:
    if a is None and b is None:
        return None
    else:
        return (a or 0) + (b or 0)


def _extract_description(values: dict[str, Any]) -> str | None:
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
    description: str | None = Field(None, description='Description of the bucket.')
    stage: str = Field(description='Stage of the bucket (in for input stage, out for output stage).')
    created: str = Field(description='Creation timestamp of the bucket.')
    data_size_bytes: int | None = Field(
        None,
        description='Total data size of the bucket in bytes.',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes', 'data-size-bytes'),
        serialization_alias='dataSizeBytes',
    )
    tables_count: int | None = Field(
        default=None,
        description='Number of tables in the bucket.',
        validation_alias=AliasChoices('tablesCount', 'tables_count', 'tables-count'),
        serialization_alias='tablesCount',
    )
    links: list[Link] | None = Field(default=None, description='The links relevant to the bucket.')
    source_project: str | None = Field(
        default=None, description='The source Keboola project of the linked bucket, None otherwise.'
    )

    # these are internal fields not meant to be exposed to LLMs
    branch_id: str | None = Field(default=None, exclude=True, description='The ID of the branch the bucket belongs to.')
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
        if links is not None:
            changes['links'] = links if links else None
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

    @model_validator(mode='before')
    @classmethod
    def set_source_project(cls, values: dict[str, Any]) -> dict[str, Any]:
        if source_project_raw := cast(dict[str, Any], get_nested(values, 'sourceBucket.project')):
            values['source_project'] = f'{source_project_raw["name"]} (ID: {source_project_raw["id"]})'
        return values


class BucketCounts(BaseModel):
    total_buckets: int = Field(description='Total number of buckets.')
    input_buckets: int = Field(description='Number of input stage buckets.')
    output_buckets: int = Field(description='Number of output stage buckets.')


class GetBucketsOutput(BaseModel):
    buckets: list[BucketDetail] = Field(description='List of buckets.')
    links: list[Link] = Field(description='Links relevant to the bucket listing.')
    buckets_not_found: list[str] | None = Field(default=None, description='List of bucket IDs that were not found.')
    bucket_counts: BucketCounts | None = Field(default=None, description='Bucket counts by stage.')

    def pack_links(self) -> 'GetBucketsOutput':
        """Move links from particular BucketDetail objects to GetBucketsOutput object to optimize TOON serialization."""
        buckets: list[BucketDetail] = []
        links: set[Link] = set()
        for bucket in self.buckets:
            links.update(bucket.links or [])
            buckets.append(bucket.model_copy(update={'links': None}))
        links.update(self.links)

        return self.model_copy(update={'buckets': buckets, 'links': sorted(links, key=lambda x: (x.type, x.title))})


class TableColumnInfo(BaseModel):
    name: str = Field(description='Plain name of the column.')
    quoted_name: str = Field(
        description='The properly quoted name of the column.',
        validation_alias=AliasChoices('quotedName', 'quoted_name', 'quoted-name'),
        serialization_alias='quotedName',
    )
    database_native_type: str = Field(description='The native, backend-specific data type.')
    nullable: bool = Field(description='Whether the column can contain null values.')
    keboola_base_type: str | None = Field(default=None, description='The storage backend agnostic data type.')
    description: str | None = Field(default=None, description='Description of the column.')


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
    source_project: str | None = Field(
        default=None, description='The source Keboola project of the linked table, None otherwise.'
    )

    # these are internal fields not meant to be exposed to LLMs
    branch_id: str | None = Field(default=None, exclude=True, description='The ID of the branch the bucket belongs to.')
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

    @model_validator(mode='before')
    @classmethod
    def set_source_project(cls, values: dict[str, Any]) -> dict[str, Any]:
        if source_project_raw := cast(dict[str, Any], get_nested(values, 'sourceTable.project')):
            values['source_project'] = f'{source_project_raw["name"]} (ID: {source_project_raw["id"]})'
        return values

    @field_serializer('primary_key')
    # Serialize the primary key as a string so the whole TableDetail is serialized
    # as tabular data in Toon format.
    def serialize_primary_key(self, primary_key: list[str] | None) -> str | None:
        return '|'.join(primary_key) if primary_key else None


class GetTablesOutput(BaseModel):
    tables: list[TableDetail] = Field(description='List of tables.')
    links: list[Link] = Field(description='Links relevant to the table listing.')
    tables_not_found: list[str] | None = Field(default=None, description='List of table IDs that were not found.')

    def pack_links(self) -> 'GetTablesOutput':
        """Move links from particular TableDetail objects to GetTablesOutput object to optimize TOON serialization."""
        tables: list[TableDetail] = []
        links: set[Link] = set()
        for table in self.tables:
            links.update(table.links or [])
            tables.append(table.model_copy(update={'links': None}))
        links.update(self.links)

        return self.model_copy(update={'tables': tables, 'links': sorted(links, key=lambda x: (x.type, x.title))})


class UpdateItemResult(BaseModel):
    item_id: str = Field(description='The storage item identifier that was updated.')
    success: bool = Field(description='Whether the update succeeded.')
    error: str | None = Field(default=None, description='Error message if the update failed.')
    timestamp: datetime | None = Field(default=None, description='Timestamp of the update if successful.')


class UpdateDescriptionsOutput(BaseModel):
    results: list[UpdateItemResult] = Field(description='Results for each update attempt.')
    total_processed: int = Field(description='Total number of items processed.')
    successful: int = Field(description='Number of successful updates.')
    failed: int = Field(description='Number of failed updates.')


class DescriptionUpdate(BaseModel):
    """Structured update describing a storage item and its new description."""

    item_id: str = Field(
        description='Storage item name: "bucket_id", "bucket_id.table_id", "bucket_id.table_id.column_name"'
    )
    description: str = Field(description='New description to set for the storage item.')


class StorageItemId(BaseModel):
    """Represents a parsed storage item ID."""

    item_type: Literal['bucket', 'table', 'column'] = Field(description='Type of storage item.')
    bucket_id: str | None = Field(default=None, description='Bucket identifier.')
    table_id: str | None = Field(default=None, description='Table identifier.')
    column_name: str | None = Field(default=None, description='Column name.')


class DescriptionUpdateGroups(BaseModel):
    """Groups description updates by type."""

    bucket_updates: dict[str, str] = Field(description='Bucket description updates by bucket ID.')
    table_updates: dict[str, str] = Field(description='Table description updates by table ID.')
    column_updates_by_table: dict[str, dict[str, str]] = Field(description='Column updates by table ID.')


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
    links_manager: ProjectLinksManager | None,
    prod_bucket: BucketDetail | None,
    dev_bucket: BucketDetail | None,
) -> BucketDetail:
    def _links(_id: str, name: str) -> list[Link] | None:
        if links_manager:
            return [links_manager.get_bucket_detail_link(_id, name)]
        else:
            return None

    if prod_bucket and dev_bucket:
        # generate a URL link to the dev bucket but with the prod bucket's name
        links = _links(dev_bucket.id, prod_bucket.name or prod_bucket.id)
        bucket = prod_bucket.shade_by(dev_bucket, client.branch_id, links or [])
    elif prod_bucket:
        links = _links(prod_bucket.id, prod_bucket.name or prod_bucket.id)
        bucket = prod_bucket.model_copy(update={'links': links})
    elif dev_bucket:
        links = _links(dev_bucket.id, dev_bucket.name or dev_bucket.id)
        bucket = dev_bucket.model_copy(update={'id': dev_bucket.prod_id, 'branch_id': None, 'links': links})
    else:
        raise ValueError('No buckets specified.')

    return bucket


@tool_errors()
async def get_buckets(
    ctx: Context, bucket_ids: Annotated[Sequence[str], Field(description='Filter by specific bucket IDs.')] = tuple()
) -> GetBucketsOutput:
    """
    Lists buckets or retrieves full details of specific buckets.

    EXAMPLES:
    - `bucket_ids=[]` → summaries of all buckets in the project
    - `bucket_ids=["id1", ...]` → full details of the buckets with the specified IDs
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    if bucket_ids:

        async def _fetch_bucket_detail(bucket_id: str) -> BucketDetail | str:
            prod_bucket, dev_bucket = await _find_buckets(client, bucket_id)
            if prod_bucket or dev_bucket:
                return await _combine_buckets(client, links_manager, prod_bucket, dev_bucket)
            else:
                return bucket_id

        results = await process_concurrently(bucket_ids, _fetch_bucket_detail)
        buckets: list[BucketDetail] = []
        missing_ids: list[str] = []

        for bucket_detail_or_id in unwrap_results(results, 'Failed to fetch one or more buckets'):
            if isinstance(bucket_detail_or_id, BucketDetail):
                buckets.append(bucket_detail_or_id)
            elif isinstance(bucket_detail_or_id, str):
                missing_ids.append(bucket_detail_or_id)

        output = GetBucketsOutput(
            buckets=buckets,
            buckets_not_found=missing_ids if missing_ids else None,
            links=[links_manager.get_bucket_dashboard_link()],
        )

    else:
        output = await _list_buckets(client, links_manager)

    return output.pack_links()


async def _list_buckets(client: KeboolaClient, links_manager: ProjectLinksManager) -> GetBucketsOutput:
    """Retrieves information about all buckets in the project."""
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
            buckets.append(bucket)

    # Count buckets by stage (only count input, derive output)
    total_count = len(buckets)
    input_count = sum(1 for bucket in buckets if bucket.stage == 'in')
    output_count = total_count - input_count

    bucket_counts = BucketCounts(total_buckets=total_count, input_buckets=input_count, output_buckets=output_count)

    return GetBucketsOutput(
        buckets=buckets, bucket_counts=bucket_counts, links=[links_manager.get_bucket_dashboard_link()]
    )


@tool_errors()
async def get_tables(
    ctx: Context,
    bucket_ids: Annotated[Sequence[str], Field(description='Filter by specific bucket IDs.')] = tuple(),
    table_ids: Annotated[Sequence[str], Field(description='Filter by specific table IDs.')] = tuple(),
) -> GetTablesOutput:
    """
    Lists tables in buckets or retrieves full details of specific tables, including fully qualified database name,
    column definitions, and metadata.

    RETURNS:
    - With `bucket_ids`: Summaries of tables (ID, name, description, primary key).
    - With `table_ids`: Full details including columns, data types, and fully qualified database names.

    COLUMN DATA TYPES:
    - database_native_type: The actual type in the storage backend (Snowflake, BigQuery, etc.)
      with precision, scale, and other implementation details
    - keboola_base_type: Standardized type indicating the semantic data type. May not always be
      available. When present, it reveals the actual type of data stored in the column - for example,
      a column with database_native_type VARCHAR might have keboola_base_type INTEGER, indicating
      it stores integer values despite being stored as text in the backend.

    EXAMPLES:
    - `bucket_ids=["id1", ...]` → summary info of the tables in the buckets with the specified IDs
    - `table_ids=["id1", ...]` → detailed info of the tables specified by their IDs
    - `bucket_ids=[]` and `table_ids=[]` → empty list; you have to specify at least one filter

    """
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    tables_by_id: dict[str, TableDetail] = {}
    missing_ids: list[str] = []

    if bucket_ids:
        for table in await _list_tables(bucket_ids, client, links_manager):
            tables_by_id[table.id] = table

    if table_ids:

        async def _fetch_table_detail(_table_id: str) -> TableDetail | str:
            if _table := await _get_table(_table_id, client, workspace_manager, links_manager):
                return _table
            else:
                return _table_id

        # Touch the WorkspaceManager to initialize the workspace before launching the concurrent tasks
        # to prevent race condition and initializing multiple database backend workspaces.
        _ = workspace_manager.get_workspace_id()
        results = await process_concurrently(table_ids, _fetch_table_detail)

        for table_detail_or_id in unwrap_results(results, 'Failed to fetch one or more tables'):
            if isinstance(table_detail_or_id, TableDetail):
                tables_by_id[table_detail_or_id.id] = table_detail_or_id
            elif isinstance(table_detail_or_id, str):
                missing_ids.append(table_detail_or_id)

    return GetTablesOutput(
        tables=list(tables_by_id.values()),
        tables_not_found=missing_ids if missing_ids else None,
        links=[links_manager.get_bucket_dashboard_link()],
    ).pack_links()


async def _get_table(
    table_id: str, client: KeboolaClient, workspace_manager: WorkspaceManager, links_manager: ProjectLinksManager
) -> TableDetail | None:
    prod_table: JsonDict | None = await _get_table_detail(client.storage_client, table_id)
    if prod_table:
        branch_id = get_metadata_property(prod_table.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
        if branch_id:
            # The table should be from the prod branch; pretend that the table does not exist.
            prod_table = None

    dev_table: JsonDict | None = None
    if client.branch_id:
        if f'c-{client.branch_id}-' in table_id:
            # we already deal with the dev table ID
            dev_id = table_id
        else:
            # convert the prod table ID to the dev table ID
            dev_id = table_id.replace('c-', f'c-{client.branch_id}-')

        dev_table = await _get_table_detail(client.storage_client, dev_id)
        if dev_table:
            branch_id = get_metadata_property(dev_table.get('metadata', []), MetadataField.FAKE_DEVELOPMENT_BRANCH)
            if branch_id != client.branch_id:
                # The table's branch ID does not match; pretend that the table does not exist.
                dev_table = None

    raw_table = dev_table or prod_table
    if not raw_table:
        return None

    raw_columns = cast(list[str], raw_table.get('columns', []))
    raw_column_metadata = cast(dict[str, list[dict[str, Any]]], raw_table.get('columnMetadata', {}))
    raw_primary_key = cast(list[str], raw_table.get('primaryKey', []))

    sql_dialect = await workspace_manager.get_sql_dialect()
    db_table_info = await workspace_manager.get_table_info(raw_table)

    column_info = []
    for col_name in raw_columns:
        col_meta = raw_column_metadata.get(col_name, [])
        description: str | None = get_metadata_property(col_meta, MetadataField.DESCRIPTION)
        base_type: str | None = get_metadata_property(
            col_meta, MetadataField.DATATYPE_BASETYPE, preferred_providers=['user']
        )
        if db_table_info and (db_column_info := db_table_info.columns.get(col_name)):
            native_type = db_column_info.native_type
            nullable = db_column_info.nullable
        else:
            # should not happen
            native_type = 'STRING' if sql_dialect == 'BigQuery' else 'VARCHAR'
            nullable = col_name not in raw_primary_key
            LOG.warning(
                f'No column info from the database: '
                f'col_name={col_name}, sql_dialect={sql_dialect}, db_table_info={db_table_info}'
            )

        column_info.append(
            TableColumnInfo(
                name=col_name,
                quoted_name=await workspace_manager.get_quoted_name(col_name),
                database_native_type=native_type,
                nullable=nullable,
                keboola_base_type=base_type,
                description=description,
            )
        )

    bucket_info = cast(dict[str, Any], raw_table.get('bucket', {}))
    bucket_id = cast(str, bucket_info.get('id', ''))

    table_name = cast(str, raw_table.get('name', ''))
    links = [links_manager.get_table_detail_link(bucket_id, table_name)]

    table = TableDetail.model_validate(
        raw_table
        | {
            'columns': column_info,
            'fully_qualified_name': db_table_info.fqn.identifier if db_table_info else None,
            'links': links,
        }
    )
    return table.model_copy(update={'id': table.prod_id, 'branch_id': None})


async def _list_tables(
    bucket_ids: Sequence[str],
    client: KeboolaClient,
    links_manager: ProjectLinksManager,
) -> Iterable[TableDetail]:
    """Retrieves all tables in a specific bucket with their basic information."""
    tables_by_prod_id: dict[str, TableDetail] = {}

    for bucket_id in bucket_ids:
        prod_bucket, dev_bucket = await _find_buckets(client, bucket_id)

        # TODO: requesting "metadata" to get the table description;
        #  We could also request "columns" and use WorkspaceManager to prepare the table's FQN
        #  and columns' quoted names.
        #  This could take time for larger buckets, but could save calls to get_table_metadata() later.

        if prod_bucket:
            raw_table_data = await client.storage_client.bucket_table_list(prod_bucket.id, include=['metadata'])
            for raw in raw_table_data:
                table_name = cast(str, raw.get('name', ''))
                table = TableDetail.model_validate(
                    raw | {'links': [links_manager.get_table_detail_link(prod_bucket.id, table_name)]}
                )
                assert table.id == table.prod_id, f'Table ID mismatch: {table.id} != {table.prod_id}'
                tables_by_prod_id[table.id] = table

        if dev_bucket:
            raw_table_data = await client.storage_client.bucket_table_list(dev_bucket.id, include=['metadata'])
            for raw in raw_table_data:
                table = TableDetail.model_validate(raw)
                tables_by_prod_id[table.prod_id] = table.model_copy(
                    update={
                        'id': table.prod_id,
                        'branch_id': None,
                        'links': [links_manager.get_table_detail_link(dev_bucket.id, table.name)],
                    }
                )

    return tables_by_prod_id.values()


def _parse_item_id(item_id: str) -> StorageItemId:
    """
    Parse an item_id string to extract item type and identifiers.

    :param item_id: Item ID (e.g., "in.c-bucket", "in.c-bucket.table", "in.c-bucket.table.column")
    :return: StorageItemId object with structured data
    """
    if not item_id.startswith(('in.', 'out.')):
        raise ValueError(f'Invalid item_id format: {item_id} - must start with in. or out.')

    parts = item_id.split('.')

    if len(parts) == BUCKET_ID_PARTS:
        return StorageItemId(item_type='bucket', bucket_id=item_id)
    elif len(parts) == TABLE_ID_PARTS:
        bucket_id = f'{parts[0]}.{parts[1]}'
        return StorageItemId(item_type='table', bucket_id=bucket_id, table_id=item_id)
    elif len(parts) == COLUMN_ID_PARTS:
        bucket_id = f'{parts[0]}.{parts[1]}'
        table_id = f'{parts[0]}.{parts[1]}.{parts[2]}'
        return StorageItemId(item_type='column', bucket_id=bucket_id, table_id=table_id, column_name=parts[3])
    else:
        raise ValueError(f'Invalid item_id format: {item_id}')


def _group_updates_by_type(updates: list[DescriptionUpdate]) -> DescriptionUpdateGroups:
    """Group updates by type for efficient processing."""
    bucket_updates: dict[str, str] = {}
    table_updates: dict[str, str] = {}
    column_updates_by_table: dict[str, dict[str, str]] = defaultdict(dict)

    for update in updates:
        parsed = _parse_item_id(update.item_id)

        if parsed.item_type == 'bucket':
            bucket_updates[parsed.bucket_id] = update.description
        elif parsed.item_type == 'table':
            table_updates[parsed.table_id] = update.description
        elif parsed.item_type == 'column':
            column_updates_by_table[parsed.table_id][parsed.column_name] = update.description

    return DescriptionUpdateGroups(
        bucket_updates=bucket_updates,
        table_updates=table_updates,
        column_updates_by_table=dict(column_updates_by_table),
    )


async def _update_bucket_description(client: KeboolaClient, bucket_id: str, description: str) -> UpdateItemResult:
    """Update a bucket description."""
    try:
        response = await client.storage_client.bucket_metadata_update(
            bucket_id=bucket_id,
            metadata={MetadataField.DESCRIPTION: description},
        )
        description_entry = next(entry for entry in response if entry.get('key') == MetadataField.DESCRIPTION)
        return UpdateItemResult(item_id=bucket_id, success=True, timestamp=description_entry['timestamp'])
    except Exception as e:
        return UpdateItemResult(item_id=bucket_id, success=False, error=str(e))


async def _update_table_description(client: KeboolaClient, table_id: str, description: str) -> UpdateItemResult:
    """Update a table description."""
    try:
        response = await client.storage_client.table_metadata_update(
            table_id=table_id,
            metadata={MetadataField.DESCRIPTION: description},
            columns_metadata={},
        )
        raw_metadata = cast(list[JsonDict], response.get('metadata', []))
        description_entry = next(entry for entry in raw_metadata if entry.get('key') == MetadataField.DESCRIPTION)
        return UpdateItemResult(item_id=table_id, success=True, timestamp=description_entry['timestamp'])
    except Exception as e:
        return UpdateItemResult(item_id=table_id, success=False, error=str(e))


async def _update_column_descriptions(
    client: KeboolaClient, table_id: str, column_updates: dict[str, str]
) -> list[UpdateItemResult]:
    """Update multiple column descriptions for a single table."""
    try:
        columns_metadata = {
            column_name: [{'key': MetadataField.DESCRIPTION, 'value': description, 'columnName': column_name}]
            for column_name, description in column_updates.items()
        }

        response = await client.storage_client.table_metadata_update(
            table_id=table_id,
            columns_metadata=columns_metadata,
        )

        column_metadata = cast(dict[str, list[JsonDict]], response.get('columnsMetadata', {}))
        results = []

        for column_name in column_updates.keys():
            try:
                description_entry = next(
                    entry
                    for entry in column_metadata.get(column_name, [])
                    if entry.get('key') == MetadataField.DESCRIPTION
                )
                results.append(
                    UpdateItemResult(
                        item_id=f'{table_id}.{column_name}', success=True, timestamp=description_entry['timestamp']
                    )
                )
            except Exception as e:
                results.append(UpdateItemResult(item_id=f'{table_id}.{column_name}', success=False, error=str(e)))

        return results
    except Exception as e:
        # If the entire table update fails, mark all columns as failed
        return [
            UpdateItemResult(item_id=f'{table_id}.{column_name}', success=False, error=str(e))
            for column_name in column_updates.keys()
        ]


@tool_errors()
async def update_descriptions(
    ctx: Context,
    updates: Annotated[
        list[DescriptionUpdate],
        Field(
            description='List of DescriptionUpdate objects with storage item_id and new description. '
            'Examples: "bucket_id", "bucket_id.table_id", "bucket_id.table_id.column_name"'
        ),
    ],
) -> UpdateDescriptionsOutput:
    """Updates the description for a Keboola storage item.

    This tool supports three item types, inferred from the provided item_id:

    - bucket: item_id = "in.c-bucket"
    - table: item_id = "in.c-bucket.table"
    - column: item_id = "in.c-bucket.table.column"

    Usage examples (payload uses a list of DescriptionUpdate objects):
    - Update a bucket:
      updates=[DescriptionUpdate(item_id="in.c-my-bucket", description="New bucket description")]
    - Update a table:
      updates=[DescriptionUpdate(item_id="in.c-my-bucket.my-table", description="New table description")]
    - Update a column:
      updates=[DescriptionUpdate(item_id="in.c-my-bucket.my-table.my_column", description="New column description")]
    """
    client = KeboolaClient.from_state(ctx.session.state)
    results: list[UpdateItemResult] = []
    valid_updates: list[DescriptionUpdate] = []

    # Handle invalid item_ids first and filter valid ones
    for update in updates:
        try:
            _parse_item_id(update.item_id)
            valid_updates.append(update)
        except ValueError as e:
            results.append(
                UpdateItemResult(item_id=update.item_id, success=False, error=f'Invalid item_id format: {e}')
            )

    # Process valid updates
    grouped_updates = _group_updates_by_type(valid_updates)
    for bucket_id, description in grouped_updates.bucket_updates.items():
        result = await _update_bucket_description(client, bucket_id, description)
        results.append(result)

    for table_id, description in grouped_updates.table_updates.items():
        result = await _update_table_description(client, table_id, description)
        results.append(result)

    for table_id, column_updates in grouped_updates.column_updates_by_table.items():
        table_results = await _update_column_descriptions(client, table_id, column_updates)
        results.extend(table_results)

    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful

    return UpdateDescriptionsOutput(results=results, total_processed=len(results), successful=successful, failed=failed)
