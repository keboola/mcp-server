from datetime import datetime
from typing import Any, Mapping, Sequence
from unittest.mock import AsyncMock, call

import httpx
import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    ListBucketsOutput,
    ListTablesOutput,
    TableColumnInfo,
    TableDetail,
    UpdateDescriptionOutput,
    get_bucket,
    get_table,
    list_buckets,
    list_tables,
    update_description,
)
from keboola_mcp_server.workspace import TableFqn, WorkspaceManager


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
                    Link(
                        type='ui-dashboard',
                        title='Buckets in the project',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage',
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
                    Link(
                        type='ui-dashboard',
                        title='Buckets in the project',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage',
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
                    Link(
                        type='ui-dashboard',
                        title='Buckets in the project',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage',
                    ),
                ],
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
                    Link(
                        type='ui-dashboard',
                        title='Buckets in the project',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage',
                    ),
                ],
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
                    Link(
                        type='ui-dashboard',
                        title='Buckets in the project',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage',
                    ),
                ],
            ),
        ),
    ],
)
async def test_get_bucket(
    branch_id: str | None,
    bucket_id: str,
    expected_bucket: BucketDetail,
    mocker: MockerFixture,
    mcp_context_client: Context,
):
    """Test get_bucket tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)

    result = await get_bucket(bucket_id, mcp_context_client)

    assert isinstance(result, BucketDetail)
    assert result == expected_bucket
    if branch_id:
        keboola_client.storage_client.bucket_detail.assert_has_calls(
            [call(bucket_id), call(bucket_id.replace('c-', f'c-{branch_id}-'))]
        )
    else:
        keboola_client.storage_client.bucket_detail.assert_called_once_with(bucket_id)


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
async def test_list_buckets(
    branch_id: str | None, expected_buckets: list[BucketDetail], mocker: MockerFixture, mcp_context_client: Context
) -> None:
    """Test the list_buckets tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=_get_sapi_buckets())

    result = await list_buckets(mcp_context_client)

    assert isinstance(result, ListBucketsOutput)
    assert result.buckets == expected_buckets
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
                    TableColumnInfo(name='user_id', quoted_name='#user_id#', native_type='INT', nullable=False),
                    TableColumnInfo(name='name', quoted_name='#name#', native_type='VARCHAR', nullable=False),
                    TableColumnInfo(name='surname', quoted_name='#surname#', native_type='VARCHAR', nullable=False),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-foo#.#users#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: users',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage/in.c-foo/table/users',
                    ),
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
                    TableColumnInfo(name='user_id', quoted_name='#user_id#', native_type='INT', nullable=False),
                    TableColumnInfo(name='name', quoted_name='#name#', native_type='VARCHAR', nullable=False),
                    TableColumnInfo(name='surname', quoted_name='#surname#', native_type='VARCHAR', nullable=False),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-foo#.#users#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: users',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage/in.c-foo'
                        '/table/users',
                    ),
                    Link(
                        type='ui-detail',
                        title='Bucket: c-foo',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948/storage/in.c-foo',
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
                    TableColumnInfo(name='email_id', quoted_name='#email_id#', native_type='INT', nullable=False),
                    TableColumnInfo(name='address', quoted_name='#address#', native_type='VARCHAR', nullable=False),
                    TableColumnInfo(name='user_id', quoted_name='#user_id#', native_type='INT', nullable=False),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-foo#.#emails#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: emails',
                        url='https://connection.test.keboola.com/admin/projects/69420/storage/in.c-foo/table/emails',
                    ),
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
                    TableColumnInfo(name='email_id', quoted_name='#email_id#', native_type='INT', nullable=False),
                    TableColumnInfo(name='address', quoted_name='#address#', native_type='VARCHAR', nullable=False),
                    TableColumnInfo(name='user_id', quoted_name='#user_id#', native_type='INT', nullable=False),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-1246948-foo#.#emails#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: emails',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo/table/emails',
                    ),
                    Link(
                        type='ui-detail',
                        title='Bucket: c-foo',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo',
                    ),
                ],
            ),
        ),
        (None, 'in.c-foo.assets', None),
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
                    TableColumnInfo(name='asset_id', quoted_name='#asset_id#', native_type='INT', nullable=False),
                    TableColumnInfo(name='name', quoted_name='#name#', native_type='VARCHAR', nullable=False),
                    TableColumnInfo(name='value', quoted_name='#value#', native_type='INT', nullable=True),
                ],
                fully_qualified_name='#SAPI_TEST#.#in.c-1246948-foo#.#assets#',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: assets',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo/table/assets',
                    ),
                    Link(
                        type='ui-detail',
                        title='Bucket: c-foo',
                        url='https://connection.test.keboola.com/admin/projects/69420/branch/1246948'
                        '/storage/in.c-1246948-foo',
                    ),
                ],
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
    workspace_manager.get_table_fqn = mocker.AsyncMock(
        side_effect=lambda sapi_table: TableFqn(
            db_name='SAPI_TEST',
            schema_name=sapi_table['bucket']['id'],
            table_name=sapi_table['id'].rsplit('.')[-1],
            quote_char='#',
        )
    )
    workspace_manager.get_quoted_name = mocker.AsyncMock(side_effect=lambda name: f'#{name}#')
    workspace_manager.get_sql_dialect = mocker.AsyncMock(return_value='test-sql-dialect')

    if expected_table:
        result = await get_table(table_id, mcp_context_client)

        assert isinstance(result, TableDetail)
        assert result == expected_table
        workspace_manager.get_sql_dialect.assert_called_once()
        workspace_manager.get_table_fqn.assert_called_once()
        workspace_manager.get_quoted_name.assert_has_calls([call(col_info.name) for col_info in expected_table.columns])

    else:
        with pytest.raises(ValueError, match=f'Table not found: {table_id}'):
            await get_table(table_id, mcp_context_client)

    if branch_id:
        keboola_client.storage_client.table_detail.assert_has_calls(
            [call(table_id), call(table_id.replace('c-', f'c-{branch_id}-'))]
        )
    else:
        keboola_client.storage_client.table_detail.assert_called_once_with(table_id)


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
                ),
                TableDetail(
                    id='in.c-foo.emails',
                    name='emails',
                    display_name='All user emails.',
                    primary_key=['email_id'],
                    created='2025-08-17T07:39:18+0200',
                    rows_count=33,
                    data_size_bytes=332211,
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
                ),
                TableDetail(
                    id='in.c-foo.assets',
                    name='assets',
                    display_name='Company assets.',
                    primary_key=['asset_id'],
                    created='2025-08-22T11:22:33+0200',
                    rows_count=123,
                    data_size_bytes=123456,
                ),
            ],
        ),
    ],
)
async def test_list_tables(
    branch_id: str | None,
    bucket_id: str,
    expected_tables: list[TableDetail],
    mocker: MockerFixture,
    mcp_context_client: Context,
) -> None:
    """Test list_tables tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.branch_id = branch_id
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(side_effect=_bucket_detail_side_effect)
    keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(side_effect=_bucket_table_list_side_effect)

    result = await list_tables(bucket_id, mcp_context_client)

    assert isinstance(result, ListTablesOutput)
    assert result.tables == expected_tables
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
async def test_update_bucket_description_success(
    mocker: MockerFixture, mcp_context_client, mock_update_bucket_description_response
) -> None:
    """Test successful update of bucket description."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_metadata_update = mocker.AsyncMock(
        return_value=mock_update_bucket_description_response,
    )

    result = await update_description(
        ctx=mcp_context_client,
        item_type='bucket',
        description='Updated bucket description',
        bucket_id='in.c-test.bucket-id',
    )

    assert isinstance(result, UpdateDescriptionOutput)
    assert result.success is True
    assert result.description == 'Updated bucket description'
    assert result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')
    keboola_client.storage_client.bucket_metadata_update.assert_called_once_with(
        bucket_id='in.c-test.bucket-id',
        metadata={MetadataField.DESCRIPTION: 'Updated bucket description'},
    )


@pytest.mark.asyncio
async def test_update_table_description_success(
    mocker: MockerFixture, mcp_context_client, mock_update_table_description_response
) -> None:
    """Test successful update of table description."""

    # Mock the Keboola client post method
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.table_metadata_update = mocker.AsyncMock(
        return_value=mock_update_table_description_response,
    )

    result = await update_description(
        ctx=mcp_context_client,
        item_type='table',
        description='Updated table description',
        table_id='in.c-test.test-table',
    )

    assert isinstance(result, UpdateDescriptionOutput)
    assert result.success is True
    assert result.description == 'Updated table description'
    assert result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')
    keboola_client.storage_client.table_metadata_update.assert_called_once_with(
        table_id='in.c-test.test-table',
        metadata={MetadataField.DESCRIPTION: 'Updated table description'},
        columns_metadata={},
    )


@pytest.mark.asyncio
async def test_update_column_description_success(
    mocker: MockerFixture, mcp_context_client, mock_update_column_description_response
) -> None:
    """Test successful update of column description."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.table_metadata_update = mocker.AsyncMock(
        return_value=mock_update_column_description_response,
    )

    result = await update_description(
        ctx=mcp_context_client,
        item_type='column',
        description='Updated column description',
        table_id='in.c-test.test-table',
        column_name='text',
    )

    assert isinstance(result, UpdateDescriptionOutput)
    assert result.success is True
    assert result.description == 'Updated column description'
    assert result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')
    keboola_client.storage_client.table_metadata_update.assert_called_once_with(
        table_id='in.c-test.test-table',
        columns_metadata={
            'text': [{'key': MetadataField.DESCRIPTION, 'value': 'Updated column description', 'columnName': 'text'}]
        },
    )
