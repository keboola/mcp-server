import json
from datetime import datetime
from typing import Any, Mapping, Sequence
from unittest.mock import AsyncMock, call

import httpx
import pytest
from fastmcp import Client, FastMCP
from mcp.server.fastmcp import Context
from mcp.types import TextContent
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.config import Config, MetadataField, ServerRuntimeInfo
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.server import create_server
from keboola_mcp_server.tools.storage import (
    BucketCounts,
    BucketDetail,
    DescriptionUpdate,
    GetBucketsOutput,
    GetTablesOutput,
    TableColumnInfo,
    TableDetail,
    UpdateDescriptionsOutput,
    get_buckets,
    get_tables,
    update_descriptions,
)
from keboola_mcp_server.workspace import DbColumnInfo, DbTableInfo, TableFqn, WorkspaceManager


def parse_iso_timestamp(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


def _get_sapi_tables(details: bool | None = None) -> list[dict[str, Any]]:
    tables = [
        # users table in c-foo bucket in the production branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/tables/in.c-foo.users',
            'id': 'in.c-foo.users',
            'name': 'users',
            'displayName': 'All system users.',
            'transactional': False,
            'primaryKey': ['user_id'],
            'indexType': None,
            'indexKey': [],
            'distributionType': None,
            'distributionKey': [],
            'syntheticPrimaryKeyEnabled': False,
            'created': '2025-08-17T07:39:18+0200',
            'lastImportDate': '2025-08-20T19:11:52+0200',
            'lastChangeDate': '2025-08-20T19:11:52+0200',
            'rowsCount': 10,
            'dataSizeBytes': 10240,
            'isAlias': False,
            'isAliasable': True,
            'isTyped': False,
            'tableType': 'table',
            'path': '/users',
            'attributes': [],
            'metadata': [],
            'columns': ['user_id', 'name', 'surname'],
            'columnMetadata': {
                'user_id': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                ],
                'name': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'VARCHAR'},
                    {'id': '1234', 'key': 'KBC.description', 'value': 'Name of the user.'},
                ],
                'surname': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'VARCHAR'},
                ],
            },
            'bucket': {'id': 'in.c-foo', 'name': 'c-foo'},
        },
        # emails table in c-foo bucket in the production branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/tables/in.c-foo.emails',
            'id': 'in.c-foo.emails',
            'name': 'emails',
            'displayName': 'All user emails.',
            'transactional': False,
            'primaryKey': ['email_id'],
            'indexType': None,
            'indexKey': [],
            'distributionType': None,
            'distributionKey': [],
            'syntheticPrimaryKeyEnabled': False,
            'created': '2025-08-17T07:39:18+0200',
            'lastImportDate': '2025-08-20T19:11:52+0200',
            'lastChangeDate': '2025-08-20T19:11:52+0200',
            'rowsCount': 33,
            'dataSizeBytes': 332211,
            'isAlias': False,
            'isAliasable': True,
            'isTyped': False,
            'tableType': 'table',
            'path': '/emails',
            'attributes': [],
            'metadata': [],
            'columns': ['email_id', 'address', 'user_id'],
            'columnMetadata': {
                'email_id': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                ],
                'address': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'VARCHAR'},
                    {'id': '1234', 'key': 'KBC.description', 'value': 'Email address. 1'},
                ],
                'user_id': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                ],
            },
            'bucket': {'id': 'in.c-foo', 'name': 'c-foo'},
        },
        # emails table in c-foo bucket in the dev branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/tables/in.c-1246948-foo.emails',
            'id': 'in.c-1246948-foo.emails',
            'name': 'emails',
            'displayName': 'All user emails.',
            'transactional': False,
            'primaryKey': ['email_id'],
            'indexType': None,
            'indexKey': [],
            'distributionType': None,
            'distributionKey': [],
            'syntheticPrimaryKeyEnabled': False,
            'created': '2025-08-21T01:02:03+0400',
            'lastImportDate': '2025-08-21T01:02:03+0400',
            'lastChangeDate': '2025-08-21T01:02:03+0400',
            'rowsCount': 22,
            'dataSizeBytes': 2211,
            'isAlias': False,
            'isAliasable': True,
            'isTyped': False,
            'tableType': 'table',
            'path': '/emails',
            'attributes': [],
            'metadata': [{'id': '1726664231', 'key': 'KBC.createdBy.branch.id', 'value': '1246948'}],
            'columns': ['email_id', 'address', 'user_id'],
            'columnMetadata': {
                'email_id': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                ],
                'address': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'VARCHAR'},
                    {'id': '1234', 'key': 'KBC.description', 'value': 'Email address. 2'},
                ],
                'user_id': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                ],
            },
            'bucket': {'id': 'in.c-1246948-foo', 'name': 'c-1246948-foo'},
        },
        # assets table in c-foo bucket in the dev branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/tables/in.c-1246948-foo.assets',
            'id': 'in.c-1246948-foo.assets',
            'name': 'assets',
            'displayName': 'Company assets.',
            'transactional': False,
            'primaryKey': ['asset_id'],
            'indexType': None,
            'indexKey': [],
            'distributionType': None,
            'distributionKey': [],
            'syntheticPrimaryKeyEnabled': False,
            'created': '2025-08-22T11:22:33+0200',
            'lastImportDate': '2025-08-22T11:22:33+0200',
            'lastChangeDate': '2025-08-22T11:22:33+0200',
            'rowsCount': 123,
            'dataSizeBytes': 123456,
            'isAlias': False,
            'isAliasable': True,
            'isTyped': False,
            'tableType': 'table',
            'path': '/assets',
            'attributes': [],
            'metadata': [{'id': '1726664231', 'key': 'KBC.createdBy.branch.id', 'value': '1246948'}],
            'columns': ['asset_id', 'name', 'value'],
            'columnMetadata': {
                'asset_id': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                ],
                'name': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'VARCHAR'},
                ],
                'value': [
                    {'id': '1234', 'key': 'KBC.datatype.type', 'value': 'INT'},
                    {'id': '1234', 'key': 'KBC.datatype.nullable', 'value': '1'},
                ],
            },
            'bucket': {'id': 'in.c-1246948-foo', 'name': 'c-1246948-foo'},
            'sourceTable': {
                'project': {
                    'name': 'Source Project',
                    'id': '1234',
                }
            },
        },
    ]
    if not details:
        for t in tables:
            t.pop('columns')
            t.pop('columnMetadata')
            t.pop('bucket')
    return tables


def _bucket_table_list_side_effect(bid: str, *, include: list[str]) -> list[dict[str, Any]]:
    prefix = f'{bid}.'
    return [table for table in _get_sapi_tables() if table['id'].startswith(prefix)]


def _table_detail_side_effect(tid: str) -> JsonDict:
    for table in _get_sapi_tables(details=True):
        if table['id'] == tid:
            return table

    raise httpx.HTTPStatusError(
        message=f'Table not found: {tid}', request=AsyncMock(), response=httpx.Response(status_code=404)
    )


def _get_sapi_buckets() -> list[dict[str, Any]]:
    return [
        # foo bucket in the production branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/buckets/in.c-foo',
            'id': 'in.c-foo',
            'name': 'c-foo',
            'displayName': 'foo',
            'idBranch': 792027,
            'stage': 'in',
            'description': 'The foo bucket.',
            'tables': 'https://connection.keboola.com/v2/storage/buckets/in.c-foo',
            'created': '2025-07-03T11:02:54+0200',
            'lastChangeDate': '2025-08-17T07:37:42+0200',
            'updated': None,
            'isReadOnly': False,
            'dataSizeBytes': 1024,
            'rowsCount': 5,
            'isMaintenance': False,
            'backend': 'snowflake',
            'sharing': None,
            'hasExternalSchema': False,
            'databaseName': '',
            'path': 'in.c-foo',
            'isSnowflakeSharedDatabase': False,
            'color': None,
            'owner': None,
            'metadata': [],
        },
        # foo bucket in the dev branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/buckets/in.c-1246948-foo',
            'id': 'in.c-1246948-foo',
            'name': 'c-1246948-foo',
            'displayName': '1246948-foo',
            'idBranch': 792027,
            'stage': 'in',
            'description': 'The dev branch foo bucket.',
            'tables': 'https://connection.keboola.com/v2/storage/buckets/in.c-1246948-foo',
            'created': '2025-08-17T07:39:14+0200',
            'lastChangeDate': '2025-08-17T07:39:26+0200',
            'updated': None,
            'isReadOnly': False,
            'dataSizeBytes': 4608,
            'rowsCount': 14,
            'isMaintenance': False,
            'backend': 'snowflake',
            'sharing': None,
            'hasExternalSchema': False,
            'databaseName': '',
            'path': 'in.c-1246948-foo',
            'isSnowflakeSharedDatabase': False,
            'color': None,
            'owner': None,
            'metadata': [
                {'id': '1726664228', 'key': 'KBC.createdBy.branch.id', 'value': '1246948'},
            ],
        },
        # bar bucket in the production branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/buckets/in.c-bar',
            'id': 'out.c-bar',
            'name': 'c-bar',
            'displayName': 'bar',
            'idBranch': 792027,
            'stage': 'out',
            'description': 'Sample of Restaurant Reviews',
            'tables': 'https://connection.keboola.com/v2/storage/buckets/in.c-bar',
            'created': '2024-04-03T14:11:53+0200',
            'lastChangeDate': None,
            'updated': None,
            'isReadOnly': True,
            'dataSizeBytes': 2048,
            'rowsCount': 3,
            'isMaintenance': False,
            'backend': 'snowflake',
            'sharing': None,
            'hasExternalSchema': False,
            'databaseName': '',
            'path': 'out.c-bar',
            'isSnowflakeSharedDatabase': False,
            'color': None,
            'owner': None,
            'sourceBucket': {
                'id': 'out.c-bar',
                'name': 'c-bar',
                'displayName': 'bar',
                'stage': 'out',
                'description': 'Sample of Restaurant Reviews',
                'sharing': 'organization',
                'created': '2017-04-07T14:15:24+0200',
                'lastChangeDate': '2017-04-07T14:20:36+0200',
                'dataSizeBytes': 900096,
                'rowsCount': 2239,
                'backend': 'snowflake',
                'hasExternalSchema': False,
                'databaseName': '',
                'path': 'out.c-bar',
                'project': {'id': 1234, 'name': 'A demo project'},
                'tables': [
                    {
                        'id': 'in.c-bar.restaurants',
                        'name': 'restaurants',
                        'displayName': 'restaurants',
                        'path': '/406653-restaurants',
                    },
                    {'id': 'in.c-bar.reviews', 'name': 'reviews', 'displayName': 'reviews', 'path': '/406653-reviews'},
                ],
                'color': None,
                'sharingParameters': [],
                'sharedBy': {'id': None, 'name': None, 'date': ''},
                'owner': None,
            },
            'metadata': [],
        },
        # baz bucket in the dev branch
        {
            'uri': 'https://connection.keboola.com/v2/storage/buckets/in.c-1246948-baz',
            'id': 'in.c-1246948-baz',
            'name': 'c-1246948-baz',
            'displayName': '1246948-baz',
            'idBranch': 792027,
            'stage': 'in',
            'description': 'The dev branch baz bucket.',
            'tables': 'https://connection.keboola.com/v2/storage/buckets/in.c-1246948-baz',
            'created': '2025-01-02T03:04:05+0600',
            'lastChangeDate': '2025-01-02T03:04:55+0600',
            'updated': None,
            'isReadOnly': False,
            'dataSizeBytes': 987654321,
            'rowsCount': 123,
            'isMaintenance': False,
            'backend': 'snowflake',
            'sharing': None,
            'hasExternalSchema': False,
            'databaseName': '',
            'path': 'in.c-1246948-baz',
            'isSnowflakeSharedDatabase': False,
            'color': None,
            'owner': None,
            'metadata': [
                {'id': '1726664228', 'key': 'KBC.createdBy.branch.id', 'value': '1246948'},
            ],
        },
    ]


def _bucket_detail_side_effect(bid: str) -> JsonDict:
    for bucket in _get_sapi_buckets():
        if bucket['id'] == bid:
            return bucket

    raise httpx.HTTPStatusError(
        message=f'Bucket not found: {bid}', request=AsyncMock(), response=httpx.Response(status_code=404)
    )


@pytest.fixture
def mock_update_bucket_description_response() -> Sequence[Mapping[str, Any]]:
    """Mock valid response list for updating a bucket description."""
    return [
        {
            'id': '999',
            'key': MetadataField.DESCRIPTION,
            'value': 'Updated bucket description',
            'provider': 'user',
            'timestamp': '2024-01-01T00:00:00Z',
        }
    ]


@pytest.fixture
def mock_update_table_description_response() -> Mapping[str, Any]:
    """Mock valid response from the Keboola API for table description update."""
    return {
        'metadata': [
            {
                'id': '1724427984',
                'key': 'KBC.description',
                'value': 'Updated table description',
                'provider': 'user',
                'timestamp': '2024-01-01T00:00:00Z',
            }
        ],
        'columnsMetadata': {
            'text': [
                {
                    'id': '1725066342',
                    'key': 'KBC.description',
                    'value': 'Updated column description',
                    'provider': 'user',
                    'timestamp': '2024-01-01T00:00:00Z',
                }
            ]
        },
    }


@pytest.fixture
def mock_update_column_description_response() -> Mapping[str, Any]:
    """Mock valid response from the Keboola API for column description update."""
    return {
        'metadata': [
            {
                'id': '1724427984',
                'key': 'KBC.description',
                'value': 'Updated table description',
                'provider': 'user',
                'timestamp': '2024-01-01T00:00:00Z',
            }
        ],
        'columnsMetadata': {
            'column_name': [
                {
                    'id': '1725066342',
                    'key': 'KBC.description',
                    'value': 'Updated column description',
                    'provider': 'user',
                    'timestamp': '2024-01-01T00:00:00Z',
                }
            ]
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'bucket_id', 'expected_bucket'),
    [
        (
            None,
            'in.c-foo',
            BucketDetail(
                id='in.c-foo',
                name='c-foo',
                display_name='foo',
                description='The foo bucket.',
                stage='in',
                created='2025-07-03T11:02:54+0200',
                data_size_bytes=1024,
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: c-foo',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage/in.c-foo',
                    ),
                ],
            ),
        ),
        (
            '1246948',
            'in.c-foo',
            BucketDetail(
                # all fields come from the prod bucket except for data_size_bytes
                id='in.c-foo',
                name='c-foo',
                display_name='foo',
                description='The foo bucket.',
                stage='in',
                created='2025-07-03T11:02:54+0200',
                data_size_bytes=4608 + 1024,
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: c-foo',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo',
                    ),
                ],
            ),
        ),
        (
            None,
            'out.c-bar',
            BucketDetail(
                id='out.c-bar',
                name='c-bar',
                display_name='bar',
                description='Sample of Restaurant Reviews',
                stage='out',
                created='2024-04-03T14:11:53+0200',
                data_size_bytes=2048,
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: c-bar',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage/out.c-bar',
                    ),
                ],
                source_project='A demo project (ID: 1234)',
            ),
        ),
        (
            '1246948',  # no in.c-bar on this branch
            'out.c-bar',
            BucketDetail(
                id='out.c-bar',
                name='c-bar',
                display_name='bar',
                description='Sample of Restaurant Reviews',
                stage='out',
                created='2024-04-03T14:11:53+0200',
                data_size_bytes=2048,
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: c-bar',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage/out.c-bar',
                    ),
                ],
                source_project='A demo project (ID: 1234)',
            ),
        ),
        (
            '1246948',
            'in.c-baz',
            BucketDetail(
                id='in.c-baz',
                name='c-1246948-baz',
                display_name='1246948-baz',
                description='The dev branch baz bucket.',
                stage='in',
                created='2025-01-02T03:04:05+0600',
                data_size_bytes=987654321,
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: c-1246948-baz',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-baz',
                    ),
                ],
            ),
        ),
        (None, 'in.c-not-existing', None),
    ],
)
async def test_get_bucket(
    branch_id: str | None,
    bucket_id: str,
    expected_bucket: BucketDetail | None,
    mocker: MockerFixture,
    mcp_context_client: Context,
):
    """Test get_bucket tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)

    result = await get_buckets(mcp_context_client, [bucket_id])

    if branch_id:
        keboola_client.storage_client.bucket_detail.assert_has_calls(
            [call(bucket_id), call(bucket_id.replace('c-', f'c-{branch_id}-'))]
        )
        dashboard_url = f'https://connection.test.keboola.com/admin/projects/69420/branch/{branch_id}/storage'
    else:
        keboola_client.storage_client.bucket_detail.assert_called_once_with(bucket_id)
        dashboard_url = 'https://connection.test.keboola.com/admin/projects/69420/storage'

    assert isinstance(result, GetBucketsOutput)
    if expected_bucket is not None:
        expected_result = GetBucketsOutput(
            buckets=[expected_bucket],
            links=[Link(type='ui-dashboard', title='Buckets in the project', url=dashboard_url)],
        ).pack_links()
        assert result == expected_result
    else:
        expectd_result = GetBucketsOutput(
            buckets=[],
            buckets_not_found=[bucket_id],
            links=[Link(type='ui-dashboard', title='Buckets in the project', url=dashboard_url)],
        )
        assert result == expectd_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'expected_buckets'),
    [
        (
            None,  # production branch
            [
                BucketDetail(
                    id='in.c-foo',
                    name='c-foo',
                    display_name='foo',
                    description='The foo bucket.',
                    stage='in',
                    created='2025-07-03T11:02:54+0200',
                    data_size_bytes=1024,
                ),
                BucketDetail(
                    id='out.c-bar',
                    name='c-bar',
                    display_name='bar',
                    description='Sample of Restaurant Reviews',
                    stage='out',
                    created='2024-04-03T14:11:53+0200',
                    data_size_bytes=2048,
                    source_project='A demo project (ID: 1234)',
                ),
            ],
        ),
        (
            '1246948',  # development branch
            [
                BucketDetail(
                    id='in.c-foo',
                    name='c-foo',
                    display_name='foo',
                    description='The foo bucket.',
                    stage='in',
                    created='2025-07-03T11:02:54+0200',
                    data_size_bytes=4608 + 1024,
                ),
                BucketDetail(
                    id='out.c-bar',
                    name='c-bar',
                    display_name='bar',
                    description='Sample of Restaurant Reviews',
                    stage='out',
                    created='2024-04-03T14:11:53+0200',
                    data_size_bytes=2048,
                    source_project='A demo project (ID: 1234)',
                ),
                BucketDetail(
                    id='in.c-baz',
                    name='c-1246948-baz',
                    display_name='1246948-baz',
                    description='The dev branch baz bucket.',
                    stage='in',
                    created='2025-01-02T03:04:05+0600',
                    data_size_bytes=987654321,
                ),
            ],
        ),
    ],
)
async def test_get_buckets(
    branch_id: str | None, expected_buckets: list[BucketDetail], mocker: MockerFixture, mcp_context_client: Context
) -> None:
    """Test the get_buckets tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=_get_sapi_buckets())

    result = await get_buckets(mcp_context_client)

    assert isinstance(result, GetBucketsOutput)
    assert result.buckets == expected_buckets
    assert result.bucket_counts.total_buckets == len(expected_buckets)

    # Count expected buckets by stage
    expected_input_count = sum(1 for bucket in expected_buckets if bucket.stage == 'in')
    expected_output_count = sum(1 for bucket in expected_buckets if bucket.stage == 'out')

    assert result.bucket_counts.input_buckets == expected_input_count
    assert result.bucket_counts.output_buckets == expected_output_count
    keboola_client.storage_client.bucket_list.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'table_id', 'expected_table'),
    [
        (
            None,
            'in.c-foo.users',
            TableDetail(
                id='in.c-foo.users',
                name='users',
                display_name='All system users.',
                primary_key=['user_id'],
                created='2025-08-17T07:39:18+0200',
                rows_count=10,
                data_size_bytes=10240,
                columns=[
                    TableColumnInfo(
                        name='user_id',
                        quoted_name='#user_id#',
                        database_native_type='INT',
                        nullable=False,
                        description=None,
                    ),
                    TableColumnInfo(
                        name='name',
                        quoted_name='#name#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description='Name of the user.',
                    ),
                    TableColumnInfo(
                        name='surname',
                        quoted_name='#surname#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description=None,
                    ),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-foo#.#users#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: users',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage/in.c-foo/table/users',
                    ),
                ],
            ),
        ),
        (
            '1246948',
            'in.c-foo.users',
            TableDetail(
                id='in.c-foo.users',
                name='users',
                display_name='All system users.',
                primary_key=['user_id'],
                created='2025-08-17T07:39:18+0200',
                rows_count=10,
                data_size_bytes=10240,
                columns=[
                    TableColumnInfo(
                        name='user_id',
                        quoted_name='#user_id#',
                        database_native_type='INT',
                        nullable=False,
                        description=None,
                    ),
                    TableColumnInfo(
                        name='name',
                        quoted_name='#name#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description='Name of the user.',
                    ),
                    TableColumnInfo(
                        name='surname',
                        quoted_name='#surname#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description=None,
                    ),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-foo#.#users#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: users',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage/in.c-foo'
                        '/table/users',
                    ),
                ],
            ),
        ),
        (
            None,
            'in.c-foo.emails',
            TableDetail(
                id='in.c-foo.emails',
                name='emails',
                display_name='All user emails.',
                primary_key=['email_id'],
                created='2025-08-17T07:39:18+0200',
                rows_count=33,
                data_size_bytes=332211,
                columns=[
                    TableColumnInfo(
                        name='email_id', quoted_name='#email_id#', database_native_type='INT', nullable=False
                    ),
                    TableColumnInfo(
                        name='address',
                        quoted_name='#address#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description='Email address. 1',
                    ),
                    TableColumnInfo(
                        name='user_id', quoted_name='#user_id#', database_native_type='INT', nullable=False
                    ),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-foo#.#emails#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: emails',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage/in.c-foo/table/emails',
                    ),
                ],
            ),
        ),
        (
            '1246948',
            'in.c-foo.emails',
            TableDetail(
                id='in.c-foo.emails',
                name='emails',
                display_name='All user emails.',
                primary_key=['email_id'],
                created='2025-08-21T01:02:03+0400',
                rows_count=22,
                data_size_bytes=2211,
                columns=[
                    TableColumnInfo(
                        name='email_id', quoted_name='#email_id#', database_native_type='INT', nullable=False
                    ),
                    TableColumnInfo(
                        name='address',
                        quoted_name='#address#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description='Email address. 2',
                    ),
                    TableColumnInfo(
                        name='user_id', quoted_name='#user_id#', database_native_type='INT', nullable=False
                    ),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-1246948-foo#.#emails#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: emails',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo/table/emails',
                    ),
                ],
            ),
        ),
        (None, 'in.c-1246948-foo.assets', None),
        (
            '1246948',
            'in.c-1246948-foo.emails',
            TableDetail(
                id='in.c-foo.emails',
                name='emails',
                display_name='All user emails.',
                primary_key=['email_id'],
                created='2025-08-21T01:02:03+0400',
                rows_count=22,
                data_size_bytes=2211,
                columns=[
                    TableColumnInfo(
                        name='email_id', quoted_name='#email_id#', database_native_type='INT', nullable=False
                    ),
                    TableColumnInfo(
                        name='address',
                        quoted_name='#address#',
                        database_native_type='VARCHAR',
                        nullable=False,
                        description='Email address. 2',
                    ),
                    TableColumnInfo(
                        name='user_id', quoted_name='#user_id#', database_native_type='INT', nullable=False
                    ),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-1246948-foo#.#emails#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: emails',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo/table/emails',
                    ),
                ],
            ),
        ),
        (
            '1246948',
            'in.c-foo.assets',
            TableDetail(
                id='in.c-foo.assets',
                name='assets',
                display_name='Company assets.',
                primary_key=['asset_id'],
                created='2025-08-22T11:22:33+0200',
                rows_count=123,
                data_size_bytes=123456,
                columns=[
                    TableColumnInfo(
                        name='asset_id', quoted_name='#asset_id#', database_native_type='INT', nullable=False
                    ),
                    TableColumnInfo(name='name', quoted_name='#name#', database_native_type='VARCHAR', nullable=False),
                    TableColumnInfo(name='value', quoted_name='#value#', database_native_type='INT', nullable=True),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-1246948-foo#.#assets#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: assets',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo/table/assets',
                    ),
                ],
                source_project='Source Project (ID: 1234)',
            ),
        ),
    ],
)
async def test_get_table(
    branch_id: str | None,
    table_id: str,
    expected_table: TableDetail | None,
    mocker: MockerFixture,
    mcp_context_client: Context,
) -> None:
    """Test get_table tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.table_detail = mocker.AsyncMock(side_effect=_table_detail_side_effect)

    workspace_manager = WorkspaceManager.from_state(mcp_context_client.session.state)
    workspace_manager.get_table_info = mocker.AsyncMock(
        side_effect=lambda sapi_table: DbTableInfo(
            id=sapi_table['id'],
            fqn=TableFqn(
                db_name='SAPI_TEST',
                schema_name=sapi_table['bucket']['id'],
                table_name=sapi_table['id'].rsplit('.')[-1],
                quote_char='#',
            ),
            columns={
                col_name: DbColumnInfo(
                    name=col_name,
                    quoted_name=f'#{col_name}#',
                    native_type=get_metadata_property(col_meta, MetadataField.DATATYPE_TYPE),
                    nullable=get_metadata_property(col_meta, MetadataField.DATATYPE_NULLABLE) == '1',
                )
                for col_name, col_meta in sapi_table['columnMetadata'].items()
            },
        )
    )
    workspace_manager.get_quoted_name = mocker.AsyncMock(side_effect=lambda name: f'#{name}#')
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value='test-sql-dialect')

    result = await get_tables(mcp_context_client, table_ids=[table_id])
    assert isinstance(result, GetTablesOutput)

    if branch_id:
        keboola_client.storage_client.table_detail.assert_has_calls(
            [
                call(table_id),
                call(table_id.replace('c-', f'c-{branch_id}-') if f'c-{branch_id}-' not in table_id else table_id),
            ]
        )
        dashboard_url = f'https://connection.test.keboola.com/admin/projects/69420/branch/{branch_id}/storage'
    else:
        keboola_client.storage_client.table_detail.assert_called_once_with(table_id)
        dashboard_url = 'https://connection.test.keboola.com/admin/projects/69420/storage'

    if expected_table:
        expected_result = GetTablesOutput(
            tables=[expected_table],
            links=[Link(type='ui-dashboard', title='Buckets in the project', url=dashboard_url)],
        ).pack_links()
        assert result == expected_result
        workspace_manager.get_sql_dialect.assert_called_once()
        workspace_manager.get_table_info.assert_called_once()
        workspace_manager.get_quoted_name.assert_has_calls([call(col_info.name) for col_info in expected_table.columns])

    else:
        expected_result = GetTablesOutput(
            tables=[],
            tables_not_found=[table_id],
            links=[Link(type='ui-dashboard', title='Buckets in the project', url=dashboard_url)],
        ).pack_links()
        assert result == expected_result
        workspace_manager.get_sql_dialect.assert_not_called()
        workspace_manager.get_table_info.assert_not_called()
        workspace_manager.get_quoted_name.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'bucket_id', 'expected_tables'),
    [
        (
            None,
            'in.c-foo',
            [
                TableDetail(
                    id='in.c-foo.users',
                    name='users',
                    display_name='All system users.',
                    primary_key=['user_id'],
                    created='2025-08-17T07:39:18+0200',
                    rows_count=10,
                    data_size_bytes=10240,
                    links=[
                        Link(
                            type='ui-detail',
                            title='Table: users',
                            url='https://connection.test.keboola.com/admin/projects/69420/storage/in.c-foo/table/users',
                        )
                    ],
                ),
                TableDetail(
                    id='in.c-foo.emails',
                    name='emails',
                    display_name='All user emails.',
                    primary_key=['email_id'],
                    created='2025-08-17T07:39:18+0200',
                    rows_count=33,
                    data_size_bytes=332211,
                    links=[
                        Link(
                            type='ui-detail',
                            title='Table: emails',
                            url='https://connection.test.keboola.com/admin/projects/69420'
                            '/storage/in.c-foo/table/emails',
                        )
                    ],
                ),
            ],
        ),
        (
            '1246948',  # development branch
            'in.c-foo',
            [
                TableDetail(
                    id='in.c-foo.users',
                    name='users',
                    display_name='All system users.',
                    primary_key=['user_id'],
                    created='2025-08-17T07:39:18+0200',
                    rows_count=10,
                    data_size_bytes=10240,
                    links=[
                        Link(
                            type='ui-detail',
                            title='Table: users',
                            url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                            '/storage/in.c-foo/table/users',
                        )
                    ],
                ),
                # in.c-foo.emails comes from in.c-1246948-foo bucket
                TableDetail(
                    id='in.c-foo.emails',
                    name='emails',
                    display_name='All user emails.',
                    primary_key=['email_id'],
                    created='2025-08-21T01:02:03+0400',
                    rows_count=22,
                    data_size_bytes=2211,
                    links=[
                        Link(
                            type='ui-detail',
                            title='Table: emails',
                            url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                            '/storage/in.c-1246948-foo/table/emails',
                        )
                    ],
                ),
                TableDetail(
                    id='in.c-foo.assets',
                    name='assets',
                    display_name='Company assets.',
                    primary_key=['asset_id'],
                    created='2025-08-22T11:22:33+0200',
                    rows_count=123,
                    data_size_bytes=123456,
                    source_project='Source Project (ID: 1234)',
                    links=[
                        Link(
                            type='ui-detail',
                            title='Table: assets',
                            url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                            '/storage/in.c-1246948-foo/table/assets',
                        )
                    ],
                ),
            ],
        ),
    ],
)
async def test_get_tables(
    branch_id: str | None,
    bucket_id: str,
    expected_tables: list[TableDetail],
    mocker: MockerFixture,
    mcp_context_client: Context,
) -> None:
    """Test get_tables tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(side_effect=_bucket_table_list_side_effect)
    links_manager = await ProjectLinksManager.from_client(keboola_client)

    result = await get_tables(mcp_context_client, [bucket_id])
    assert isinstance(result, GetTablesOutput)

    expected_result = GetTablesOutput(
        tables=expected_tables, links=[links_manager.get_bucket_dashboard_link()]
    ).pack_links()
    assert result == expected_result

    if branch_id:
        keboola_client.storage_client.bucket_detail.assert_has_calls(
            [call(bucket_id), call(bucket_id.replace('c-', f'c-{branch_id}-'))]
        )
        keboola_client.storage_client.bucket_table_list.assert_has_calls(
            [
                call(bucket_id, include=['metadata']),
                call(bucket_id.replace('c-', f'c-{branch_id}-'), include=['metadata']),
            ]
        )
    else:
        keboola_client.storage_client.bucket_detail.assert_called_once_with(bucket_id)
        keboola_client.storage_client.bucket_table_list.assert_called_once_with(bucket_id, include=['metadata'])


@pytest.mark.asyncio
async def test_update_descriptions_bucket_success(
    mocker: MockerFixture, mcp_context_client, mock_update_bucket_description_response
) -> None:
    """Test successful update of bucket description using update_descriptions."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_metadata_update = mocker.AsyncMock(
        return_value=mock_update_bucket_description_response,
    )

    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[DescriptionUpdate(item_id='in.c-test-bucket', description='Updated bucket description')],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 1
    assert result.failed == 0
    assert len(result.results) == 1

    bucket_result = result.results[0]
    assert bucket_result.item_id == 'in.c-test-bucket'
    assert bucket_result.success is True
    assert bucket_result.error is None
    assert bucket_result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')

    keboola_client.storage_client.bucket_metadata_update.assert_called_once_with(
        bucket_id='in.c-test-bucket',
        metadata={MetadataField.DESCRIPTION: 'Updated bucket description'},
    )


@pytest.mark.asyncio
async def test_update_descriptions_table_success(
    mocker: MockerFixture, mcp_context_client, mock_update_table_description_response
) -> None:
    """Test successful update of table description using update_descriptions."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.table_metadata_update = mocker.AsyncMock(
        return_value=mock_update_table_description_response,
    )

    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[DescriptionUpdate(item_id='in.c-test.test-table', description='Updated table description')],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 1
    assert result.failed == 0
    assert len(result.results) == 1

    table_result = result.results[0]
    assert table_result.item_id == 'in.c-test.test-table'
    assert table_result.success is True
    assert table_result.error is None
    assert table_result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')

    keboola_client.storage_client.table_metadata_update.assert_called_once_with(
        table_id='in.c-test.test-table',
        metadata={MetadataField.DESCRIPTION: 'Updated table description'},
        columns_metadata={},
    )


@pytest.mark.asyncio
async def test_update_descriptions_column_success(
    mocker: MockerFixture, mcp_context_client, mock_update_column_description_response
) -> None:
    """Test successful update of column description using update_descriptions."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.table_metadata_update = mocker.AsyncMock(
        return_value=mock_update_column_description_response,
    )

    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[
            DescriptionUpdate(item_id='in.c-test.test-table.column_name', description='Updated column description')
        ],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 1
    assert result.failed == 0
    assert len(result.results) == 1

    column_result = result.results[0]
    assert column_result.item_id == 'in.c-test.test-table.column_name'
    assert column_result.success is True
    assert column_result.error is None
    assert column_result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')

    keboola_client.storage_client.table_metadata_update.assert_called_once_with(
        table_id='in.c-test.test-table',
        columns_metadata={
            'column_name': [
                {'key': MetadataField.DESCRIPTION, 'value': 'Updated column description', 'columnName': 'column_name'}
            ]
        },
    )


@pytest.mark.asyncio
async def test_update_descriptions_mixed_types_success(
    mocker: MockerFixture,
    mcp_context_client,
    mock_update_bucket_description_response,
    mock_update_table_description_response,
) -> None:
    """Test successful update of mixed types using update_descriptions."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_metadata_update = mocker.AsyncMock(
        return_value=mock_update_bucket_description_response,
    )
    keboola_client.storage_client.table_metadata_update = mocker.AsyncMock(
        return_value=mock_update_table_description_response,
    )

    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[
            DescriptionUpdate(item_id='in.c-test-bucket', description='Updated bucket description'),
            DescriptionUpdate(item_id='in.c-test.test-table', description='Updated table description'),
        ],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 2
    assert result.successful == 2
    assert result.failed == 0
    assert len(result.results) == 2

    # Check bucket result
    bucket_result = next(r for r in result.results if r.item_id == 'in.c-test-bucket')
    assert bucket_result.success is True
    assert bucket_result.error is None

    # Check table result
    table_result = next(r for r in result.results if r.item_id == 'in.c-test.test-table')
    assert table_result.success is True
    assert table_result.error is None

    # Verify API calls
    keboola_client.storage_client.bucket_metadata_update.assert_called_once_with(
        bucket_id='in.c-test-bucket',
        metadata={MetadataField.DESCRIPTION: 'Updated bucket description'},
    )
    keboola_client.storage_client.table_metadata_update.assert_called_once_with(
        table_id='in.c-test.test-table',
        metadata={MetadataField.DESCRIPTION: 'Updated table description'},
        columns_metadata={},
    )


@pytest.mark.asyncio
async def test_update_descriptions_invalid_path_error(mcp_context_client) -> None:
    """Test that invalid paths are handled gracefully."""
    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[DescriptionUpdate(item_id='invalid-path', description='This should fail')],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 0
    assert result.failed == 1
    assert len(result.results) == 1

    error_result = result.results[0]
    assert error_result.item_id == 'invalid-path'
    assert error_result.success is False
    assert error_result.error is not None
    assert 'Invalid item_id format' in error_result.error
    assert error_result.timestamp is None


@pytest.mark.asyncio
async def test_update_descriptions_api_error_handling(mocker: MockerFixture, mcp_context_client) -> None:
    """Test that API errors are handled gracefully."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_metadata_update = mocker.AsyncMock()
    keboola_client.storage_client.bucket_metadata_update.side_effect = httpx.HTTPStatusError(
        message='API Error', request=AsyncMock(), response=httpx.Response(status_code=500)
    )

    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[DescriptionUpdate(item_id='in.c-test-bucket', description='This will fail')],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 1
    assert result.successful == 0
    assert result.failed == 1
    assert len(result.results) == 1

    error_result = result.results[0]
    assert error_result.item_id == 'in.c-test-bucket'
    assert error_result.success is False
    assert error_result.error is not None
    assert error_result.timestamp is None


@pytest.mark.asyncio
async def test_update_descriptions_empty_updates(mcp_context_client) -> None:
    """Test that empty updates dictionary is handled."""
    result = await update_descriptions(
        ctx=mcp_context_client,
        updates=[],
    )

    assert isinstance(result, UpdateDescriptionsOutput)
    assert result.total_processed == 0
    assert result.successful == 0
    assert result.failed == 0
    assert len(result.results) == 0

    @pytest.mark.asyncio
    async def test_get_buckets_use_serializer(mocker):
        # Ideally, we'd test the output of every tool, but the required mocking would be excessive.
        # Here, we test only the 'get_buckets' tool.
        # The test_server.TestServer.test_tools_have_serializer() test verifies that the same serializer is used
        # for all tools.
        # Therefore, all tools should produce compact JSON in their unstructured output.
        cfg_dict = {
            'storage_token': '123-test-storage-token',
            'storage_api_url': 'https://connection.keboola.com',
            'transport': 'stdio',
        }
        config = Config.from_dict(cfg_dict)

        mocker.patch(
            'keboola_mcp_server.clients.base.KeboolaServiceClient.get',
            return_value={'owner': {'id': '123'}},
        )
        mocker.patch(
            'keboola_mcp_server.clients.client.AsyncStorageClient.trigger_event',
            return_value={},
        )
        mocker.patch(
            'keboola_mcp_server.clients.client.AsyncStorageClient.bucket_list',
            return_value=[
                {
                    'uri': 'https://connection.keboola.com/v2/storage/buckets/in.c-foo',
                    'id': 'in.c-foo',
                    'name': 'c-foo',
                    'displayName': 'foo',
                    'idBranch': 202,
                    'stage': 'in',
                    'description': '',
                    'tables': 'https://connection.keboola.com/v2/storage/buckets/in.c-foo',
                    'created': '2025-06-05T08:16:36+0200',
                    'lastChangeDate': '2025-06-05T08:17:12+0200',
                    'updated': None,
                    'isReadOnly': False,
                    'dataSizeBytes': 112233,
                    'rowsCount': 987,
                    'isMaintenance': False,
                    'backend': 'snowflake',
                    'sharing': None,
                    'hasExternalSchema': False,
                    'databaseName': '',
                    'path': 'in.c-foo',
                    'isSnowflakeSharedDatabase': False,
                    'color': None,
                    'owner': None,
                    'backendPath': ['KEBOOLA_123', 'in.c-foo'],
                    'attributes': [],
                }
            ],
        )
        expected = GetBucketsOutput(
            buckets=[
                BucketDetail(
                    id='in.c-foo',
                    name='c-foo',
                    display_name='foo',
                    description='',
                    stage='in',
                    created='2025-06-05T08:16:36+0200',
                    data_size_bytes=112233,
                )
            ],
            bucket_counts=BucketCounts(total_buckets=1, input_buckets=1, output_buckets=0),
            links=[
                Link(
                    type='ui-dashboard',
                    title='Buckets in the project',
                    url='https://connection.keboola.com/admin/projects/123/storage',
                )
            ],
        )

        server = create_server(config, runtime_info=ServerRuntimeInfo(transport='stdio'))
        assert isinstance(server, FastMCP)

        async with Client(server) as client:
            result = await client.call_tool('get_buckets')
            # check the structured output
            assert GetBucketsOutput.model_validate(result.structured_content) == expected
            # check the unstructured output
            assert len(result.content) == 1
            assert result.content[0] == TextContent(
                type='text',
                # no fields with None values, no indentation, no whitespace
                text=json.dumps(expected.model_dump(exclude_none=True), ensure_ascii=False, separators=(',', ':')),
            )
