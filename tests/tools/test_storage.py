from datetime import datetime
from typing import Any, Mapping, Sequence

import pytest
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import Config, MetadataField
from keboola_mcp_server.tools.sql import TableFqn, WorkspaceManager
from keboola_mcp_server.tools.storage import (
    BucketDetail,
    TableColumnInfo,
    TableDetail,
    UpdateDescriptionResponse,
    get_bucket_detail,
    get_table_detail,
    retrieve_bucket_tables,
    retrieve_buckets,
    update_bucket_description,
    update_column_description,
    update_table_description,
)


def parse_iso_timestamp(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace('Z', '+00:00'))


@pytest.fixture
def test_config() -> Config:
    """Create a test configuration."""
    return Config(
        storage_token='test-token',
        storage_api_url='https://connection.test.keboola.com',
    )


@pytest.fixture
def mock_table_data() -> Mapping[str, Any]:
    """Create a combined fixture for table testing.

    Contains both the raw API response data and the expected transformed data.
    """
    raw_table_data = {
        'id': 'in.c-test.test-table',
        'name': 'test-table',
        'primary_key': ['id'],
        'created': '2024-01-01T00:00:00Z',
        'rows_count': 100,
        'data_size_bytes': 1000,
        'columns': ['id', 'name', 'value'],
    }

    return {
        'raw_table_data': raw_table_data,  # What the client returns
        'additional_data': {
            # What workspace_manager should return
            'table_fqn': TableFqn('SAPI_TEST', 'in.c-test', 'test-table', quote_char='#'),
            # Expected transformed columns
            'columns': [
                TableColumnInfo(name='id', quoted_name='#id#'),
                TableColumnInfo(name='name', quoted_name='#name#'),
                TableColumnInfo(name='value', quoted_name='#value#'),
            ],
        },
    }


@pytest.fixture
def mock_buckets() -> Sequence[Mapping[str, Any]]:
    """Fixture for mock bucket data."""
    return [
        {
            'id': 'bucket1',
            'name': 'Test Bucket 1',
            'description': 'A test bucket',
            'stage': 'production',
            'created': '2024-01-01T00:00:00Z',
            'table_count': 5,
            'data_size_bytes': 1024,
        },
        {
            'id': 'bucket2',
            'name': 'Test Bucket 2',
            'description': 'Another test bucket',
            'created': '2025-01-01T00:00:00Z',
            'table_count': 3,
            'data_size_bytes': 2048,
        },
    ]


@pytest.fixture
def mock_update_bucket_description_response() -> Sequence[Mapping[str, Any]]:
    """Mock valid response list for updating a bucket description."""
    return [
        {
            'id': '999',
            'key': MetadataField.DESCRIPTION.value,
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
@pytest.mark.parametrize('bucket_id', ['bucket1', 'bucket2'])
async def test_get_bucket_detail(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_buckets: Sequence[Mapping[str, Any]],
    bucket_id: str,
):
    """Test get_bucket_detail tool."""

    expected_bucket = next(b for b in mock_buckets if b['id'] == bucket_id)

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_detail = mocker.AsyncMock(return_value=expected_bucket)

    result = await get_bucket_detail(bucket_id, mcp_context_client)

    assert isinstance(result, BucketDetail)
    assert result.id == expected_bucket['id']
    assert result.name == expected_bucket['name']

    # Check optional fields only if they are present in the expected bucket
    if 'description' in expected_bucket:
        assert result.description == expected_bucket['description']
    if 'stage' in expected_bucket:
        assert result.stage == expected_bucket['stage']
    if 'created' in expected_bucket:
        assert result.created == expected_bucket['created']
    if 'tables_count' in expected_bucket:
        assert result.tables_count == expected_bucket['tables_count']
    if 'data_size_bytes' in expected_bucket:
        assert result.data_size_bytes == expected_bucket['data_size_bytes']


@pytest.mark.asyncio
async def test_retrieve_buckets_in_project(
    mocker: MockerFixture, mcp_context_client: Context, mock_buckets: Sequence[Mapping[str, Any]]
) -> None:
    """Test the retrieve_buckets_in_project tool."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=mock_buckets)

    result = await retrieve_buckets(mcp_context_client)

    assert isinstance(result, list)
    assert len(result) == len(mock_buckets)
    assert all(isinstance(bucket, BucketDetail) for bucket in result)

    # Assert that the returned BucketDetail objects match the mock data
    for expected_bucket, result_bucket in zip(mock_buckets, result):
        assert result_bucket.id == expected_bucket['id']
        assert result_bucket.name == expected_bucket['name']
        if 'description' in expected_bucket:
            assert result_bucket.description == expected_bucket['description']
        if 'stage' in expected_bucket:
            assert result_bucket.stage == expected_bucket['stage']
        if 'created' in expected_bucket:
            assert result_bucket.created == expected_bucket['created']
        if 'tables_count' in expected_bucket:
            assert result_bucket.tables_count == expected_bucket['tables_count']
        if 'data_size_bytes' in expected_bucket:
            assert result_bucket.data_size_bytes == expected_bucket['data_size_bytes']

    keboola_client.storage_client.bucket_list.assert_called_once()


@pytest.mark.asyncio
async def test_get_table_detail(
    mocker: MockerFixture, mcp_context_client: Context, mock_table_data: Mapping[str, Any]
) -> None:
    """Test get_table_detail tool."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.table_detail = mocker.AsyncMock(return_value=mock_table_data['raw_table_data'])

    workspace_manager = WorkspaceManager.from_state(mcp_context_client.session.state)
    workspace_manager.get_table_fqn = mocker.AsyncMock(return_value=mock_table_data['additional_data']['table_fqn'])
    workspace_manager.get_quoted_name.side_effect = lambda name: f'#{name}#'
    result = await get_table_detail(mock_table_data['raw_table_data']['id'], mcp_context_client)

    assert isinstance(result, TableDetail)
    assert result.id == mock_table_data['raw_table_data']['id']
    assert result.name == mock_table_data['raw_table_data']['name']
    assert result.primary_key == mock_table_data['raw_table_data']['primary_key']
    assert result.rows_count == mock_table_data['raw_table_data']['rows_count']
    assert result.data_size_bytes == mock_table_data['raw_table_data']['data_size_bytes']
    assert result.fully_qualified_name == str(mock_table_data['additional_data']['table_fqn'])
    assert result.columns == mock_table_data['additional_data']['columns']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('sapi_response', 'expected'),
    [
        (
            [{'id': 'in.c-bucket.foo', 'name': 'foo'}],
            [TableDetail(id='in.c-bucket.foo', name='foo')],
        ),
        (
            [
                {
                    'id': 'in.c-bucket.bar',
                    'name': 'bar',
                    'metadata': [{'key': 'KBC.description', 'value': 'Nice Bar'}],
                }
            ],
            [TableDetail(id='in.c-bucket.bar', name='bar', description='Nice Bar')],
        ),
    ],
)
async def test_retrieve_bucket_tables_in_project(
    mocker: MockerFixture,
    sapi_response: dict[str, Any],
    expected: list[TableDetail],
    mcp_context_client: Context,
) -> None:
    """Test retrieve_bucket_tables_in_project tool."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=sapi_response)
    result = await retrieve_bucket_tables('bucket-id', mcp_context_client)
    assert result == expected
    keboola_client.storage_client.bucket_table_list.assert_called_once_with('bucket-id', include=['metadata'])


@pytest.mark.asyncio
async def test_update_bucket_description_success(
    mocker: MockerFixture, mcp_context_client, mock_update_bucket_description_response
) -> None:
    """Test successful update of bucket description."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.post = mocker.AsyncMock(return_value=mock_update_bucket_description_response)

    result = await update_bucket_description(
        bucket_id='in.c-test.bucket-id',
        description='Updated bucket description',
        ctx=mcp_context_client,
    )

    assert isinstance(result, UpdateDescriptionResponse)
    assert result.success is True
    assert result.description == 'Updated bucket description'
    assert result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')
    keboola_client.storage_client.post.assert_called_once_with(
        endpoint='buckets/in.c-test.bucket-id/metadata',
        data={
            'provider': 'user',
            'metadata': [{'key': MetadataField.DESCRIPTION.value, 'value': 'Updated bucket description'}],
        },
    )


@pytest.mark.asyncio
async def test_update_table_description_success(
    mocker: MockerFixture, mcp_context_client, mock_update_table_description_response
) -> None:
    """Test successful update of table description."""

    # Mock the Keboola client post method
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.post = mocker.AsyncMock(return_value=mock_update_table_description_response)

    result = await update_table_description(
        table_id='in.c-test.test-table',
        description='Updated table description',
        ctx=mcp_context_client,
    )

    assert isinstance(result, UpdateDescriptionResponse)
    assert result.success is True
    assert result.description == 'Updated table description'
    assert result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')
    keboola_client.storage_client.post.assert_called_once_with(
        endpoint='tables/in.c-test.test-table/metadata',
        data={
            'provider': 'user',
            'metadata': [{'key': MetadataField.DESCRIPTION.value, 'value': 'Updated table description'}],
        },
    )


@pytest.mark.asyncio
async def test_update_column_description_success(
    mocker: MockerFixture, mcp_context_client, mock_update_column_description_response
) -> None:
    """Test successful update of column description."""

    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    keboola_client.storage_client.post = mocker.AsyncMock(return_value=mock_update_column_description_response)

    result = await update_column_description(
        table_id='in.c-test.test-table',
        column_name='text',
        description='Updated column description',
        ctx=mcp_context_client,
    )

    assert isinstance(result, UpdateDescriptionResponse)
    assert result.success is True
    assert result.description == 'Updated column description'
    assert result.timestamp == parse_iso_timestamp('2024-01-01T00:00:00Z')
    keboola_client.storage_client.post.assert_called_once_with(
        endpoint='tables/in.c-test.test-table/metadata',
        data={
            'columnsMetadata': {
                'text': [
                    {
                        'columnName': 'text',
                        'key': 'KBC.description',
                        'value': 'Updated column description',
                    }
                ]
            },
            'provider': 'user',
        },
    )
