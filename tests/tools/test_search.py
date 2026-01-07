from typing import Any, cast
from unittest.mock import call

import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.ai_service import ComponentSuggestionResponse, SuggestedComponent
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import ItemType
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.search import (
    SearchHit,
    SuggestedComponentOutput,
    find_component_id,
    search,
)


class TestSearch:
    """Test cases for the search tool function."""

    @pytest.mark.asyncio
    async def test_search_no_patterns(self, mcp_context_client: Context):
        with pytest.raises(ValueError, match='At least one search pattern must be provided.'):
            await search(ctx=mcp_context_client, patterns=[])

        with pytest.raises(ValueError, match='At least one search pattern must be provided.'):
            await search(ctx=mcp_context_client, patterns=[''])

    @pytest.mark.asyncio
    async def test_search_success(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test successful search with regex patterns."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        project_id = await keboola_client.storage_client.project_id()

        # Mock bucket_list
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(
            return_value=[
                {'id': 'in.c-test-bucket', 'name': 'test-bucket', 'created': '2024-01-01T00:00:00Z'},
            ]
        )

        # Mock bucket_table_list
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(
            return_value=[
                {
                    'id': 'in.c-test-bucket.test-table',
                    'name': 'test-table',
                    'created': '2024-01-01T00:00:00Z',
                }
            ]
        )

        # Mock component_list - return different results based on component type
        def component_list_side_effect(component_type, include=None):
            if component_type == 'extractor':
                return [
                    {
                        'id': 'keboola.ex-db-mysql',
                        'name': 'MySQL Extractor',
                        'configurations': [
                            {
                                'id': 'test-config',
                                'name': 'Test MySQL Config',
                                'created': '2024-01-02T00:00:00Z',
                                'rows': [],
                            }
                        ],
                    }
                ]
            return []

        keboola_client.storage_client.component_list = mocker.AsyncMock(side_effect=component_list_side_effect)

        # Mock workspace_list
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(
            ctx=mcp_context_client,
            patterns=['test'],
            item_types=(cast(ItemType, 'table'), cast(ItemType, 'configuration')),
            limit=20,
            offset=0,
        )

        assert isinstance(result, list)
        assert result == [
            SearchHit(
                component_id='keboola.ex-db-mysql',
                configuration_id='test-config',
                item_type='configuration',
                updated='2024-01-02T00:00:00Z',
                name='Test MySQL Config',
                links=[
                    Link(
                        type='ui-detail',
                        title='Configuration: Test MySQL Config',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/components/keboola.ex-db-mysql/test-config'
                        ),
                    )
                ],
            ),
            SearchHit(
                table_id='in.c-test-bucket.test-table',
                item_type='table',
                updated='2024-01-01T00:00:00Z',
                name='test-table',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: test-table',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket/table/test-table'
                        ),
                    )
                ],
            ),
        ]

    @pytest.mark.asyncio
    async def test_search_with_regex_pattern(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with regex patterns."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        project_id = await keboola_client.storage_client.project_id()

        # Mock bucket_list
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(
            return_value=[
                {'id': 'in.c-customer-data', 'name': 'customer-data', 'created': '2024-01-01T00:00:00Z'},
                {'id': 'in.c-product-data', 'name': 'product-data', 'created': '2024-01-02T00:00:00Z'},
            ]
        )

        # Mock other endpoints
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(ctx=mcp_context_client, patterns=['customer.*'], item_types=(cast(ItemType, 'bucket'),))

        assert isinstance(result, list)
        assert result == [
            SearchHit(
                bucket_id='in.c-customer-data',
                item_type='bucket',
                updated='2024-01-01T00:00:00Z',
                name='customer-data',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: customer-data',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-customer-data'
                        ),
                    )
                ],
            ),
        ]

    @pytest.mark.asyncio
    async def test_search_default_parameters(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with default parameters (limit=50, offset=0, all item types)."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Create 60 buckets to verify default limit of 50 is applied
        # Use lastChangeDate to ensure predictable sorting (most recent = bucket-059)
        buckets = [
            {
                'id': f'in.c-test-bucket-{i:03d}',
                'name': f'test-bucket-{i:03d}',
                'created': '2024-01-01T00:00:00Z',
                'lastChangeDate': f'2024-01-01T{i:02d}:00:00Z',
            }
            for i in range(60)
        ]
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=buckets)

        # Mock other endpoints
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        # Call without specifying limit, offset, or item_types
        result = await search(ctx=mcp_context_client, patterns=['test'])

        assert isinstance(result, list)
        # Should return exactly 50 items (default limit), not all 60
        assert len(result) == 50, f'Expected default limit of 50, got {len(result)}'
        # The first item should be the most recently updated
        assert result[0].bucket_id == 'in.c-test-bucket-059'

    @pytest.mark.asyncio
    async def test_search_limit_out_of_range(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with limit out of range gets clamped to default (50)."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Create 60 buckets to verify limit clamping
        # Use lastChangeDate to ensure predictable sorting
        buckets = [
            {
                'id': f'in.c-test-bucket-{i:03d}',
                'name': f'test-bucket-{i:03d}',
                'created': '2024-01-01T00:00:00Z',
                'lastChangeDate': f'2024-01-01T{i:02d}:00:00Z',
            }
            for i in range(60)
        ]
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=buckets)

        # Mock other endpoints
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        # Test with limit too high (> MAX_GLOBAL_SEARCH_LIMIT = 100)
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=200)
        assert isinstance(result, list)
        # Should be overridden to DEFAULT_GLOBAL_SEARCH_LIMIT = 50
        assert len(result) == 50, f'Expected limit to be overridden to 50, got {len(result)}'

        # Test with limit too low (<= 0)
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=0)
        assert isinstance(result, list)
        assert len(result) == 50, f'Expected limit to be overridden to 50, got {len(result)}'

        # Test with negative limit
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=-5)
        assert isinstance(result, list)
        assert len(result) == 50, f'Expected limit to be overridden to 50, got {len(result)}'

    @pytest.mark.asyncio
    async def test_search_negative_offset(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with negative offset gets clamped to 0."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Create buckets with predictable order
        # Use lastChangeDate to ensure bucket-009 is the most recent
        buckets = [
            {
                'id': f'in.c-test-bucket-{i:03d}',
                'name': f'test-bucket-{i:03d}',
                'created': '2024-01-01T00:00:00Z',
                'lastChangeDate': f'2024-01-01T{i:02d}:00:00Z',
            }
            for i in range(10)
        ]
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=buckets)

        # Mock other endpoints
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        # Test with negative offset
        result = await search(ctx=mcp_context_client, patterns=['test'], offset=-10, limit=5)
        assert isinstance(result, list)
        # Should be overridden to offset=0, returning first 5 items
        assert len(result) == 5
        # First item should be the most recently updated (bucket-009)
        assert result[0].bucket_id == 'in.c-test-bucket-009'

        # Verify it matches the result with offset=0
        result_with_zero_offset = await search(ctx=mcp_context_client, patterns=['test'], offset=0, limit=5)
        assert result == result_with_zero_offset, 'Negative offset should behave the same as offset=0'

    @pytest.mark.asyncio
    async def test_search_pagination(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with pagination."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        project_id = await keboola_client.storage_client.project_id()

        # Mock bucket_list with multiple items
        buckets = [
            {'id': f'in.c-bucket-{i}', 'name': f'test-bucket-{i}', 'created': f'2024-01-{i:02d}T00:00:00Z'}
            for i in range(1, 11)
        ]
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=buckets)

        # Mock other endpoints
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        # Test pagination
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=2, offset=0)
        assert result == [
            SearchHit(
                bucket_id='in.c-bucket-10',
                item_type='bucket',
                updated='2024-01-10T00:00:00Z',
                name='test-bucket-10',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: test-bucket-10',
                        url=(f'https://connection.test.keboola.com/admin/projects/{project_id}/storage/in.c-bucket-10'),
                    )
                ],
            ),
            SearchHit(
                bucket_id='in.c-bucket-9',
                item_type='bucket',
                updated='2024-01-09T00:00:00Z',
                name='test-bucket-9',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: test-bucket-9',
                        url=(f'https://connection.test.keboola.com/admin/projects/{project_id}/storage/in.c-bucket-9'),
                    )
                ],
            ),
        ]

        result = await search(ctx=mcp_context_client, patterns=['test'], limit=1, offset=2)
        assert result == [
            SearchHit(
                bucket_id='in.c-bucket-8',
                item_type='bucket',
                updated='2024-01-08T00:00:00Z',
                name='test-bucket-8',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: test-bucket-8',
                        url=(f'https://connection.test.keboola.com/admin/projects/{project_id}/storage/in.c-bucket-8'),
                    )
                ],
            )
        ]

    @pytest.mark.asyncio
    async def test_search_matches_description(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search matches description field."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        project_id = await keboola_client.storage_client.project_id()

        # Mock bucket_list with description
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(
            return_value=[
                {
                    'id': 'in.c-my-bucket',
                    'name': 'my-bucket',
                    'created': '2024-01-01T00:00:00Z',
                    'metadata': [{'key': MetadataField.DESCRIPTION, 'value': 'This contains test data'}],
                }
            ]
        )

        # Mock other endpoints
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(ctx=mcp_context_client, patterns=['test'], item_types=(cast(ItemType, 'bucket'),))

        assert isinstance(result, list)
        assert result == [
            SearchHit(
                bucket_id='in.c-my-bucket',
                item_type='bucket',
                updated='2024-01-01T00:00:00Z',
                name='my-bucket',
                description='This contains test data',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: my-bucket',
                        url=(f'https://connection.test.keboola.com/admin/projects/{project_id}/storage/in.c-my-bucket'),
                    )
                ],
            )
        ]

    @pytest.mark.asyncio
    async def test_search_hits_sorting(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search hits sorting."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        project_id = await keboola_client.storage_client.project_id()

        keboola_client.storage_client.bucket_list = mocker.AsyncMock(
            return_value=[
                {'id': 'in.c-test-bucket-a', 'name': 'test-bucket-a', 'created': '2024-01-01T00:00:00Z'},
                {
                    'id': 'in.c-test-bucket-b',
                    'name': 'test-bucket-b',
                    'created': '2024-01-01T00:00:00Z',
                    'lastChangeDate': '2024-01-02T00:00:00Z',
                },
                {'id': 'in.c-test-bucket-c', 'name': 'test-bucket-c'},
            ]
        )

        def _bucket_table_list_side_effect(bucket_id: str, include: Any = None) -> list[JsonDict]:
            if bucket_id == 'in.c-test-bucket-a':
                return [
                    {'id': 'in.c-test-bucket-a.test-table', 'name': 'test-table', 'created': '2024-01-01T00:00:00Z'}
                ]
            elif bucket_id == 'in.c-test-bucket-b':
                return [
                    {
                        'id': 'in.c-test-bucket-b.test-table',
                        'name': 'test-table',
                        'created': '2024-01-01T00:00:00Z',
                        'lastChangeDate': '2024-01-02T00:00:00Z',
                    }
                ]
            else:
                return []

        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(side_effect=_bucket_table_list_side_effect)

        def _component_list_side_effect(
            component_type: str | None = None, include: Any | None = None
        ) -> list[JsonDict]:
            if not component_type:
                return [
                    {
                        'id': 'keboola.ex-db-mysql',
                        'name': 'MySQL Extractor',
                        'configurations': [
                            {
                                'id': 'test-config-a',
                                'name': 'Test MySQL Config A',
                                'created': '2024-01-03T00:00:00Z',
                                'rows': [],
                            },
                            {
                                'id': 'test-config-b',
                                'name': 'Test MySQL Config B',
                                'created': '2024-01-03T00:00:00Z',
                                'currentVersion': {
                                    'created': '2024-01-04T00:00:00Z',
                                },
                                'rows': [],
                            },
                        ],
                    }
                ]
            else:
                return []

        keboola_client.storage_client.component_list = mocker.AsyncMock(side_effect=_component_list_side_effect)
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(ctx=mcp_context_client, patterns=['test'], limit=20, offset=0)

        assert isinstance(result, list)
        assert result == [
            SearchHit(
                component_id='keboola.ex-db-mysql',
                configuration_id='test-config-b',
                item_type='configuration',
                updated='2024-01-04T00:00:00Z',
                name='Test MySQL Config B',
                links=[
                    Link(
                        type='ui-detail',
                        title='Configuration: Test MySQL Config B',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/components/keboola.ex-db-mysql/test-config-b'
                        ),
                    )
                ],
            ),
            SearchHit(
                component_id='keboola.ex-db-mysql',
                configuration_id='test-config-a',
                item_type='configuration',
                updated='2024-01-03T00:00:00Z',
                name='Test MySQL Config A',
                links=[
                    Link(
                        type='ui-detail',
                        title='Configuration: Test MySQL Config A',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/components/keboola.ex-db-mysql/test-config-a'
                        ),
                    )
                ],
            ),
            SearchHit(
                table_id='in.c-test-bucket-b.test-table',
                item_type='table',
                updated='2024-01-02T00:00:00Z',
                name='test-table',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: test-table',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket-b/table/test-table'
                        ),
                    )
                ],
            ),
            SearchHit(
                bucket_id='in.c-test-bucket-b',
                item_type='bucket',
                updated='2024-01-02T00:00:00Z',
                name='test-bucket-b',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: test-bucket-b',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket-b'
                        ),
                    )
                ],
            ),
            SearchHit(
                table_id='in.c-test-bucket-a.test-table',
                item_type='table',
                updated='2024-01-01T00:00:00Z',
                name='test-table',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: test-table',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket-a/table/test-table'
                        ),
                    )
                ],
            ),
            SearchHit(
                bucket_id='in.c-test-bucket-a',
                item_type='bucket',
                updated='2024-01-01T00:00:00Z',
                name='test-bucket-a',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: test-bucket-a',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket-a'
                        ),
                    )
                ],
            ),
            SearchHit(
                bucket_id='in.c-test-bucket-c',
                item_type='bucket',
                updated='',
                name='test-bucket-c',
                links=[
                    Link(
                        type='ui-detail',
                        title='Bucket: test-bucket-c',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket-c'
                        ),
                    )
                ],
            ),
        ]

        keboola_client.storage_client.bucket_list.assert_has_calls([call(), call()])
        keboola_client.storage_client.bucket_table_list.assert_has_calls(
            [
                call('in.c-test-bucket-a', include=['columns', 'columnMetadata']),
                call('in.c-test-bucket-b', include=['columns', 'columnMetadata']),
                call('in.c-test-bucket-c', include=['columns', 'columnMetadata']),
            ]
        )
        keboola_client.storage_client.component_list.assert_called_once_with(None, include=['configuration', 'rows'])
        keboola_client.storage_client.workspace_list.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('tables_data', 'search_pattern', 'expected_count', 'expected_first_table_id'),
        [
            # Test: search finds table by matching column name
            (
                [
                    {
                        'id': 'in.c-test-bucket.users',
                        'name': 'users',
                        'created': '2024-01-01T00:00:00Z',
                        'columns': ['id', 'email', 'name'],
                        'columnMetadata': {},
                    }
                ],
                'email',
                1,
                'in.c-test-bucket.users',
            ),
            # Test: search finds table by matching column description
            (
                [
                    {
                        'id': 'in.c-test-bucket.customers',
                        'name': 'customers',
                        'created': '2024-01-01T00:00:00Z',
                        'columns': ['id', 'contact_info'],
                        'columnMetadata': {
                            'contact_info': [{'key': MetadataField.DESCRIPTION, 'value': 'Customer email address'}]
                        },
                    }
                ],
                'email',
                1,
                'in.c-test-bucket.customers',
            ),
            # Test: table appears only once when both table name and column match
            (
                [
                    {
                        'id': 'in.c-test-bucket.customer_data',
                        'name': 'customer_data',
                        'created': '2024-01-01T00:00:00Z',
                        'columns': ['customer_id', 'name', 'email'],
                        'columnMetadata': {},
                    }
                ],
                'customer',
                1,
                'in.c-test-bucket.customer_data',
            ),
            # Test: handles tables without columns or columnMetadata gracefully
            (
                [
                    {
                        'id': 'in.c-test-bucket.table1',
                        'name': 'table1',
                        'created': '2024-01-01T00:00:00Z',
                        # No 'columns' field
                        # No 'columnMetadata' field
                    },
                    {
                        'id': 'in.c-test-bucket.table2',
                        'name': 'table2',
                        'created': '2024-01-01T00:00:00Z',
                        'columns': [],  # Empty columns
                        'columnMetadata': {},  # Empty metadata
                    },
                ],
                'test',
                2,
                'in.c-test-bucket.table2',  # table2 comes first due to reverse sorting by ID
            ),
        ],
    )
    async def test_search_table_by_columns(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        tables_data: list[JsonDict],
        search_pattern: str,
        expected_count: int,
        expected_first_table_id: str,
    ):
        """Test search functionality with table columns and metadata."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock bucket_list
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(
            return_value=[
                {'id': 'in.c-test-bucket', 'name': 'test-bucket', 'created': '2024-01-01T00:00:00Z'},
            ]
        )

        # Mock bucket_table_list with provided test data
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=tables_data)

        result = await search(ctx=mcp_context_client, patterns=[search_pattern], item_types=(cast(ItemType, 'table'),))

        assert isinstance(result, list)
        assert len(result) == expected_count
        if expected_count > 0:
            assert result[0].table_id == expected_first_table_id


@pytest.mark.asyncio
async def test_find_component_id(mocker: MockerFixture, mcp_context_client: Context):
    """Test find_component_id returns suggested components."""
    keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
    project_id = await keboola_client.storage_client.project_id()

    # Mock suggest_component to return a list of suggested components
    expected_component_1 = SuggestedComponent(component_id='keboola.ex-salesforce', score=0.95, source='ai')
    expected_component_2 = SuggestedComponent(component_id='keboola.ex-db-mysql', score=0.85, source='ai')
    mock_response = ComponentSuggestionResponse(components=[expected_component_1, expected_component_2])
    keboola_client.ai_service_client.suggest_component = mocker.AsyncMock(return_value=mock_response)

    query = 'I am looking for a salesforce extractor component'
    result = await find_component_id(ctx=mcp_context_client, query=query)

    assert isinstance(result, list)
    assert result == [
        SuggestedComponentOutput(
            component_id='keboola.ex-salesforce',
            score=0.95,
            links=[
                Link(
                    type='ui-dashboard',
                    title='Component "keboola.ex-salesforce" Configurations Dashboard',
                    url=(
                        f'https://connection.test.keboola.com/admin/projects/{project_id}'
                        '/components/keboola.ex-salesforce'
                    ),
                )
            ],
        ),
        SuggestedComponentOutput(
            component_id='keboola.ex-db-mysql',
            score=0.85,
            links=[
                Link(
                    type='ui-dashboard',
                    title='Component "keboola.ex-db-mysql" Configurations Dashboard',
                    url=(
                        f'https://connection.test.keboola.com/admin/projects/{project_id}'
                        '/components/keboola.ex-db-mysql'
                    ),
                )
            ],
        ),
    ]
    keboola_client.ai_service_client.suggest_component.assert_called_once_with(query)
