from typing import Any, cast
from unittest.mock import call

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.ai_service import ComponentSuggestionResponse, SuggestedComponent
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import DATA_APP_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.clients.storage import GlobalSearchResponse
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.search import (
    SearchHit,
    SearchItemType,
    SearchOutput,
    SearchSpec,
    SuggestedComponentOutput,
    find_component_id,
    search,
)


class TestSearch:
    """Test cases for the search tool function."""

    @pytest.fixture(autouse=True)
    def _mock_features(self, mocker: MockerFixture, mcp_context_client: Context):
        """Disable storage-branches (no dual-fetch) and global-search (legacy textual path) features."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.has_feature = mocker.AsyncMock(return_value=False)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=False)

    @pytest.mark.asyncio
    async def test_search_no_patterns(self, mcp_context_client: Context):
        with pytest.raises(ToolError, match='At least one search pattern must be provided.'):
            await search(ctx=mcp_context_client, patterns=[])

        with pytest.raises(ToolError, match='At least one search pattern must be provided.'):
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
            item_types=(cast(SearchItemType, 'table'), cast(SearchItemType, 'configuration')),
            limit=20,
            offset=0,
        )

        assert isinstance(result, SearchOutput)
        assert result.total == 2
        assert result.branch_scope == 'current-branch'
        assert result.hits == [
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
    async def test_enumeration_filters_to_requested_item_types(
        self, mocker: MockerFixture, mcp_context_client: Context
    ):
        """The legacy enumeration path must not leak configuration hits when only configuration-row is requested."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        def component_list_side_effect(component_type, include=None):
            if component_type == 'extractor':
                return [
                    {
                        'id': 'keboola.ex-db-mysql',
                        'name': 'MySQL Extractor',
                        'configurations': [
                            {
                                'id': 'test-config',
                                'name': 'test config',
                                'created': '2024-01-02T00:00:00Z',
                                'rows': [{'id': 'test-row', 'name': 'test row', 'created': '2024-01-03T00:00:00Z'}],
                            }
                        ],
                    }
                ]
            return []

        keboola_client.storage_client.component_list = mocker.AsyncMock(side_effect=component_list_side_effect)

        result = await search(
            ctx=mcp_context_client,
            patterns=['test'],
            item_types=(cast(SearchItemType, 'configuration-row'),),
        )

        assert {hit.item_type for hit in result.hits} == {'configuration-row'}
        assert result.total == 1
        assert result.by_type == {'configuration-row': 1}

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

        result = await search(
            ctx=mcp_context_client,
            patterns=['customer.*'],
            item_types=(cast(SearchItemType, 'bucket'),),
            mode='regex',
        )

        assert result.hits == [
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

        # Should return exactly 50 items (default limit), not all 60
        assert len(result.hits) == 50, f'Expected default limit of 50, got {len(result.hits)}'
        assert result.total == 60
        # The first item should be the most recently updated
        assert result.hits[0].bucket_id == 'in.c-test-bucket-059'

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
        # Should be overridden to DEFAULT_GLOBAL_SEARCH_LIMIT = 50
        assert len(result.hits) == 50, f'Expected limit to be overridden to 50, got {len(result.hits)}'

        # Test with limit too low (<= 0)
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=0)
        assert len(result.hits) == 50, f'Expected limit to be overridden to 50, got {len(result.hits)}'

        # Test with negative limit
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=-5)
        assert len(result.hits) == 50, f'Expected limit to be overridden to 50, got {len(result.hits)}'

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
        # Should be overridden to offset=0, returning first 5 items
        assert len(result.hits) == 5
        # First item should be the most recently updated (bucket-009)
        assert result.hits[0].bucket_id == 'in.c-test-bucket-009'

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
        assert result.hits == [
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
        assert result.hits == [
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

        result = await search(ctx=mcp_context_client, patterns=['test'], item_types=(cast(SearchItemType, 'bucket'),))

        assert result.hits == [
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

        def _bucket_table_list_side_effect(bucket_id: str, include: Any = None, **kwargs: Any) -> list[JsonDict]:
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

        assert result.hits == [
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

        keboola_client.storage_client.bucket_list.assert_has_calls(
            [call(branch_id='default'), call(branch_id='default')]
        )
        keboola_client.storage_client.bucket_table_list.assert_has_calls(
            [
                call('in.c-test-bucket-a', include=['columns', 'columnMetadata'], branch_id='default'),
                call('in.c-test-bucket-b', include=['columns', 'columnMetadata'], branch_id='default'),
                call('in.c-test-bucket-c', include=['columns', 'columnMetadata'], branch_id='default'),
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

        result = await search(
            ctx=mcp_context_client, patterns=[search_pattern], item_types=(cast(SearchItemType, 'table'),)
        )

        assert len(result.hits) == expected_count
        if expected_count > 0:
            assert result.hits[0].table_id == expected_first_table_id

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        (
            'patterns',
            'scopes',
            'component_configurations',
            'expected_hits',
        ),
        [
            (
                ['alpha', 'beta'],
                ('parameters', 'storage.input'),
                [
                    {
                        'id': 'test-config',
                        'name': 'Test Config',
                        'created': '2024-01-02T00:00:00Z',
                        'configuration': {
                            'parameters': {'query': 'alpha'},
                            'storage': {'input': [{'source': 'beta'}]},
                        },
                        'rows': [],
                    }
                ],
                [
                    (
                        'test-config',
                        [
                            {'scope': 'parameters.query', 'patterns': ['alpha']},
                            {'scope': 'storage.input[0].source', 'patterns': ['beta']},
                        ],
                    )
                ],
            ),
            (
                ['gamma'],
                tuple(),
                [
                    {
                        'id': 'test-config',
                        'name': 'Test Config',
                        'created': '2024-01-02T00:00:00Z',
                        'configuration': {
                            'parameters': {'query': 'alpha'},
                            'storage': {
                                'input': [{'source': 'beta'}, {'source': 'gamma'}],
                                'output': [{'destination': 'gamma'}],
                            },
                        },
                        'rows': [],
                    }
                ],
                [
                    (
                        'test-config',
                        [
                            {'scope': 'storage.input[1].source', 'patterns': ['gamma']},
                            {'scope': 'storage.output[0].destination', 'patterns': ['gamma']},
                        ],
                    )
                ],
            ),
            (
                ['alpha'],
                ('parameters',),
                [
                    {
                        'id': 'test-config',
                        'name': 'Test Config',
                        'created': '2024-01-02T00:00:00Z',
                        'configuration': {
                            'parameters': {'query': 'alpha'},
                            'storage': {'input': [{'source': 'alpha'}]},
                        },
                        'rows': [],
                    }
                ],
                [('test-config', [{'scope': 'parameters.query', 'patterns': ['alpha']}])],
            ),
            (
                ['alpha'],
                ('authorization.#apiKey',),
                [
                    {
                        'id': 'test-config',
                        'name': 'Test Config',
                        'created': '2024-01-02T00:00:00Z',
                        'configuration': {
                            'authorization': {'#apiKey': 'alpha'},
                            'parameters': {'query': 'nomatch'},
                        },
                        'rows': [],
                    }
                ],
                [('test-config', [{'scope': 'authorization.#apiKey', 'patterns': ['alpha']}])],
            ),
            (
                ['alpha', 'beta'],
                ('parameters',),
                [
                    {
                        'id': 'test-config',
                        'name': 'Test Config',
                        'created': '2024-01-02T00:00:00Z',
                        'configuration': {
                            'parameters': {'query': 'alpha beta', 'query2': 'beta'},
                        },
                        'rows': [],
                    }
                ],
                [
                    (
                        'test-config',
                        [
                            {'scope': 'parameters.query', 'patterns': ['alpha', 'beta']},
                            {'scope': 'parameters.query2', 'patterns': ['beta']},
                        ],
                    )
                ],
            ),
            (
                ['alpha', 'gamma'],
                tuple(),
                [
                    {
                        'id': 'test-config-a',
                        'name': 'Test Config A',
                        'created': '2024-01-02T00:00:00Z',
                        'configuration': {
                            'parameters': {'query': 'alpha'},
                            'storage': {'input': [{'source': 'beta'}]},
                        },
                        'rows': [],
                    },
                    {
                        'id': 'test-config-b',
                        'name': 'Test Config B',
                        'created': '2024-01-03T00:00:00Z',
                        'configuration': {
                            'storage': {'output': [{'destination': 'gamma'}]},
                        },
                        'rows': [],
                    },
                    {
                        'id': 'test-config-c',
                        'name': 'Test Config C',
                        'created': '2024-01-01T00:00:00Z',
                        'configuration': {
                            'parameters': {'query': 'nomatch'},
                        },
                        'rows': [],
                    },
                ],
                [
                    ('test-config-b', [{'scope': 'storage.output[0].destination', 'patterns': ['gamma']}]),
                    ('test-config-a', [{'scope': 'parameters.query', 'patterns': ['alpha']}]),
                ],
            ),
        ],
        ids=[
            'all_matches_in_scopes',
            'most_specific_scope_only',
            'scope_constrains_same_value_in_other_path',
            'hash_prefixed_scope_key_in_search_tool',
            'group_two_patterns_in_one_scope',
            'multiple_configurations_returned',
        ],
    )
    async def test_search_config_based_match_scopes(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        patterns: list[str],
        scopes: tuple[str, ...],
        component_configurations: list[dict[str, Any]],
        expected_hits: list[tuple[str, list[dict[str, Any]]]],
    ):
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.bucket_table_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(
            side_effect=lambda component_type, include=None: (
                [
                    {
                        'id': 'keboola.ex-db-mysql',
                        'type': 'extractor',
                        'configurations': component_configurations,
                    }
                ]
                if component_type == 'extractor'
                else []
            )
        )
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(
            ctx=mcp_context_client,
            patterns=patterns,
            item_types=(cast(SearchItemType, 'configuration'),),
            search_type='config-based',
            scopes=scopes,
        )

        normalized_actual = [
            (
                hit.configuration_id,
                sorted(
                    ({'scope': m.scope, 'patterns': sorted(m.patterns)} for m in hit.matches),
                    key=lambda x: x['scope'] or '',
                ),
            )
            for hit in result.hits
        ]
        normalized_expected = [
            (
                config_id,
                sorted(
                    ({'scope': m['scope'], 'patterns': sorted(m['patterns'])} for m in matches),
                    key=lambda x: x['scope'] or '',
                ),
            )
            for config_id, matches in expected_hits
        ]
        assert normalized_actual == normalized_expected


def _global_search_item(
    item_id: str,
    name: str,
    item_type: str,
    *,
    component_id: str | None = None,
    full_path: dict[str, Any] | None = None,
    created: str = '2024-01-01T00:00:00+00:00',
) -> JsonDict:
    return {
        'id': item_id,
        'name': name,
        'type': item_type,
        'fullPath': full_path or {},
        'componentId': component_id,
        'organizationId': 1,
        'projectId': 69420,
        'projectName': 'Test Project',
        'created': created,
    }


def _global_search_response(*items: JsonDict) -> GlobalSearchResponse:
    by_type: dict[str, int] = {}
    for item in items:
        item_type = cast(str, item['type'])
        by_type[item_type] = by_type.get(item_type, 0) + 1
    return GlobalSearchResponse.model_validate(
        {'all': len(items), 'items': list(items), 'byType': by_type, 'byProject': {'69420': 'Test Project'}}
    )


class TestGlobalTextualSearch:
    """Test cases for the textual search backed by the SAPI global-search endpoint."""

    @pytest.fixture(autouse=True)
    def _enable_global_search(self, mocker: MockerFixture, mcp_context_client: Context):
        """Enable the global-search feature so that textual search uses the server-side endpoint."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

    @pytest.mark.asyncio
    async def test_search_maps_global_search_items(self, mocker: MockerFixture, mcp_context_client: Context):
        """Items are mapped to SearchHits with IDs, branch info, re-typed flows and links."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        project_id = await keboola_client.storage_client.project_id()

        response = _global_search_response(
            _global_search_item(
                'in.c-test-bucket.users',
                'users',
                'table',
                full_path={'bucket': {'id': 'in.c-test-bucket'}, 'branch': {'id': 7, 'name': 'Main'}},
                created='2024-01-03T00:00:00+00:00',
            ),
            _global_search_item(
                'cfg-1',
                'Test MySQL Config',
                'configuration',
                component_id='keboola.ex-db-mysql',
                created='2024-01-02T00:00:00+00:00',
            ),
            _global_search_item(
                'flow-1',
                'My Flow',
                'configuration',
                component_id='keboola.orchestrator',
                created='2024-01-01T00:00:00+00:00',
            ),
        )
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=response)

        result = await search(ctx=mcp_context_client, patterns=['test'])

        keboola_client.storage_client.global_search.assert_called_once_with(
            query='test', types=[], limit=50, offset=0, branch_scope='current'
        )
        assert isinstance(result, SearchOutput)
        assert result.branch_scope == 'current-branch'
        assert result.total == 3
        assert result.by_type == {'table': 1, 'configuration': 2}
        assert result.hits == [
            SearchHit(
                table_id='in.c-test-bucket.users',
                bucket_id='in.c-test-bucket',
                item_type='table',
                updated='2024-01-03T00:00:00+00:00',
                name='users',
                branch_id='7',
                branch_name='Main',
                links=[
                    Link(
                        type='ui-detail',
                        title='Table: users',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/storage/in.c-test-bucket/table/users'
                        ),
                    )
                ],
            ),
            SearchHit(
                component_id='keboola.ex-db-mysql',
                configuration_id='cfg-1',
                item_type='configuration',
                updated='2024-01-02T00:00:00+00:00',
                name='Test MySQL Config',
                links=[
                    Link(
                        type='ui-detail',
                        title='Configuration: Test MySQL Config',
                        url=(
                            f'https://connection.test.keboola.com/admin/projects/{project_id}'
                            '/components/keboola.ex-db-mysql/cfg-1'
                        ),
                    )
                ],
            ),
            SearchHit(
                component_id='keboola.orchestrator',
                configuration_id='flow-1',
                item_type='flow',
                updated='2024-01-01T00:00:00+00:00',
                name='My Flow',
                links=[
                    Link(
                        type='ui-detail',
                        title='Flow: My Flow',
                        url=(f'https://connection.test.keboola.com/admin/projects/{project_id}' '/flows/flow-1'),
                    )
                ],
            ),
        ]

    @pytest.mark.asyncio
    async def test_search_widens_to_all_branches_on_zero_hits(self, mocker: MockerFixture, mcp_context_client: Context):
        """When the current branch context has no hits, the search retries across all branches."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        dev_branch_hit = _global_search_response(
            _global_search_item(
                'in.c-dev-bucket.users',
                'users',
                'table',
                full_path={'branch': {'id': 123, 'name': 'my-dev-branch'}},
            ),
        )
        keboola_client.storage_client.global_search = mocker.AsyncMock(
            side_effect=[_global_search_response(), dev_branch_hit]
        )

        result = await search(ctx=mcp_context_client, patterns=['users'], item_types=('table',))

        assert keboola_client.storage_client.global_search.call_args_list == [
            call(query='users', types=['table'], limit=50, offset=0, branch_scope='current'),
            call(query='users', types=['table'], limit=50, offset=0, branch_scope='all'),
        ]
        assert result.branch_scope == 'all-branches'
        assert len(result.hits) == 1
        assert result.hits[0].table_id == 'in.c-dev-bucket.users'
        assert result.hits[0].branch_id == '123'
        assert result.hits[0].branch_name == 'my-dev-branch'

    @pytest.mark.asyncio
    async def test_search_does_not_widen_when_paginating(self, mocker: MockerFixture, mcp_context_client: Context):
        """An empty page with non-zero offset must not trigger the all-branches retry."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=_global_search_response())

        result = await search(ctx=mcp_context_client, patterns=['users'], offset=10)

        keboola_client.storage_client.global_search.assert_called_once_with(
            query='users', types=[], limit=50, offset=10, branch_scope='current'
        )
        assert result.hits == []
        assert result.branch_scope == 'current-branch'

    @pytest.mark.asyncio
    async def test_search_falls_back_to_enumeration_on_zero_hits(
        self, mocker: MockerFixture, mcp_context_client: Context
    ):
        """Zero hits (even after the all-branches retry) means the index may not be back-filled —
        fall back to client-side enumeration so we never silently return nothing."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=_global_search_response())
        fallback = SearchOutput(
            hits=[SearchHit(table_id='in.c-main.users', item_type='table', updated='', name='users')],
            total=1,
            by_type={'table': 1},
            branch_scope='current-branch',
        )
        enum_mock = mocker.patch(
            'keboola_mcp_server.tools.search._enumeration_search',
            new=mocker.AsyncMock(return_value=fallback),
        )

        result = await search(ctx=mcp_context_client, patterns=['users'], item_types=('table',))

        enum_mock.assert_awaited_once()
        assert [h.table_id for h in result.hits] == ['in.c-main.users']

    @pytest.mark.asyncio
    async def test_search_falls_back_to_enumeration_on_error(self, mocker: MockerFixture, mcp_context_client: Context):
        """A failing global-search request (e.g. a transient 5xx) falls back to enumeration."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.global_search = mocker.AsyncMock(
            side_effect=RuntimeError('global-search exploded')
        )
        fallback = SearchOutput(
            hits=[SearchHit(table_id='in.c-main.users', item_type='table', updated='', name='users')],
            total=1,
            by_type={'table': 1},
            branch_scope='current-branch',
        )
        enum_mock = mocker.patch(
            'keboola_mcp_server.tools.search._enumeration_search',
            new=mocker.AsyncMock(return_value=fallback),
        )

        result = await search(ctx=mcp_context_client, patterns=['users'], item_types=('table',))

        enum_mock.assert_awaited_once()
        assert [h.table_id for h in result.hits] == ['in.c-main.users']

    @pytest.mark.asyncio
    async def test_search_multiple_patterns_merge_and_dedupe(self, mocker: MockerFixture, mcp_context_client: Context):
        """Each pattern issues its own request; results are OR-merged and deduplicated by item."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        shared_item = _global_search_item('cfg-1', 'Shared Config', 'configuration', component_id='keboola.ex-db-mysql')
        unique_item = _global_search_item('cfg-2', 'Unique Config', 'configuration', component_id='keboola.ex-db-mysql')
        keboola_client.storage_client.global_search = mocker.AsyncMock(
            side_effect=[
                _global_search_response(shared_item, unique_item),
                _global_search_response(shared_item),
            ]
        )

        result = await search(ctx=mcp_context_client, patterns=['shared', 'config'])

        assert keboola_client.storage_client.global_search.call_count == 2
        assert sorted(hit.configuration_id for hit in result.hits) == ['cfg-1', 'cfg-2']
        # The shared item is counted once per pattern in the server-side totals.
        assert result.total == 3

    @pytest.mark.asyncio
    async def test_search_data_app_narrowing(self, mocker: MockerFixture, mcp_context_client: Context):
        """Searching for data-apps over-fetches configurations and narrows them by component ID."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        response = _global_search_response(
            _global_search_item('app-1', 'My Data App', 'configuration', component_id=DATA_APP_COMPONENT_ID),
            _global_search_item('cfg-1', 'Regular Config', 'configuration', component_id='keboola.ex-db-mysql'),
        )
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=response)

        result = await search(ctx=mcp_context_client, patterns=['app'], item_types=('data-app',))

        # Narrowing 'configuration' to data-apps is lossy, so the page is over-fetched up to the server max
        # to avoid under-filling once the non-matching configurations are dropped client-side.
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='app', types=['configuration'], limit=100, offset=0, branch_scope='current'
        )
        assert len(result.hits) == 1
        assert result.hits[0].configuration_id == 'app-1'
        assert result.hits[0].item_type == 'data-app'

    @pytest.mark.asyncio
    async def test_search_overfetch_fills_page_after_narrowing(
        self, mocker: MockerFixture, mcp_context_client: Context
    ):
        """A small user limit still over-fetches to the server max, then caps the narrowed page to that limit.

        Regular configurations preceding the data-apps would under-fill the page if the user limit were sent
        verbatim; over-fetching ensures the page reaches the requested limit when enough data-apps exist.
        """
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        items = [
            _global_search_item('cfg-1', 'Regular One', 'configuration', component_id='keboola.ex-db-mysql'),
            _global_search_item('cfg-2', 'Regular Two', 'configuration', component_id='keboola.ex-db-mysql'),
            _global_search_item('app-1', 'Data App One', 'configuration', component_id=DATA_APP_COMPONENT_ID),
            _global_search_item('app-2', 'Data App Two', 'configuration', component_id=DATA_APP_COMPONENT_ID),
            _global_search_item('app-3', 'Data App Three', 'configuration', component_id=DATA_APP_COMPONENT_ID),
        ]
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=_global_search_response(*items))

        result = await search(ctx=mcp_context_client, patterns=['app'], item_types=('data-app',), limit=2)

        # The user limit (2) is below the server max, so the request over-fetches up to 100...
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='app', types=['configuration'], limit=100, offset=0, branch_scope='current'
        )
        # ...and the narrowed page is capped to the user limit, containing only data-apps.
        assert len(result.hits) == 2
        assert all(hit.item_type == 'data-app' for hit in result.hits)

    @pytest.mark.asyncio
    async def test_search_configuration_row_mapping(self, mocker: MockerFixture, mcp_context_client: Context):
        """Row hits resolve their parent configuration from fullPath; unresolvable rows are skipped."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        response = _global_search_response(
            _global_search_item(
                'row-1',
                'My Row',
                'configuration-row',
                component_id='keboola.ex-db-mysql',
                full_path={'configuration': {'id': 'cfg-1', 'name': 'Parent Config'}},
            ),
            _global_search_item('row-2', 'Orphan Row', 'configuration-row', component_id='keboola.ex-db-mysql'),
        )
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=response)

        result = await search(ctx=mcp_context_client, patterns=['row'], item_types=('configuration-row',))

        assert len(result.hits) == 1
        assert result.hits[0] == SearchHit(
            component_id='keboola.ex-db-mysql',
            configuration_id='cfg-1',
            configuration_row_id='row-1',
            item_type='configuration-row',
            updated='2024-01-01T00:00:00+00:00',
            name='My Row',
            links=result.hits[0].links,
        )

    @pytest.mark.asyncio
    async def test_search_regex_mode_rejected(self, mocker: MockerFixture, mcp_context_client: Context):
        """Regex patterns are not supported by the server-side textual search."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.global_search = mocker.AsyncMock()

        with pytest.raises(ToolError, match='Regex patterns are not supported for textual search'):
            await search(ctx=mcp_context_client, patterns=['customer.*'], mode='regex')

        keboola_client.storage_client.global_search.assert_not_called()


@pytest.mark.parametrize(
    ('spec_kwargs', 'texts', 'expected'),
    [
        (
            {
                'patterns': ['foo.*', 'fo.*', 'olala'],
                'item_types': ('bucket',),
                'pattern_mode': 'regex',
                'return_all_matched_patterns': True,
            },
            ['foo.*', 'foobar', 'olala'],
            [
                {'scope': None, 'patterns': ['foo.*', 'fo.*']},
                {'scope': None, 'patterns': ['foo.*', 'fo.*']},
                {'scope': None, 'patterns': ['olala']},
            ],
        ),
        (
            {
                'patterns': ['foo.*', 'fo.*', 'olala'],
                'item_types': ('bucket',),
                'pattern_mode': 'regex',
                'return_all_matched_patterns': False,
            },
            ['foo.*', 'foobar', 'olala'],
            [{'scope': None, 'patterns': ['foo.*']}],
        ),
        (
            {
                'patterns': ['nomatch'],
                'item_types': ('bucket',),
                'return_all_matched_patterns': True,
            },
            ['Foo baz', 'BAR qux'],
            [],
        ),
        (
            {
                'patterns': ['bar'],
                'item_types': ('bucket',),
                'pattern_mode': 'literal',
                'case_sensitive': False,
                'return_all_matched_patterns': False,
            },
            ['Foo baz', 'BAR qux', 'BARAndSomething'],
            [
                {'scope': None, 'patterns': ['bar']},
            ],
        ),
        (
            {
                'patterns': ['bar'],
                'item_types': ('bucket',),
                'pattern_mode': 'literal',
                'case_sensitive': True,
                'return_all_matched_patterns': False,
            },
            ['Foo baz', 'BAR qux', 'BARrAndSomething'],
            [],
        ),
    ],
    ids=[
        'regex_all_matches',
        'regex_any_match',
        'regex_no_match',
        'literal_match_case_insensitive',
        'literal_match_case_sensitive',
    ],
)
def test_match_texts(spec_kwargs: dict[str, Any], texts: list[str], expected: list[dict]):
    spec = SearchSpec(**spec_kwargs)
    matches = spec.match_texts(texts)
    assert [match.model_dump() for match in matches] == expected


@pytest.mark.parametrize(
    ('spec_kwargs', 'configuration', 'expected'),
    [
        (
            # Scopes provided; each scope has one matching leaf – returns the exact leaf path.
            {
                'patterns': ['alpha', 'beta'],
                'item_types': ('configuration',),
                'search_scopes': ('parameters', 'storage.input'),
                'return_all_matched_patterns': True,
            },
            {
                'parameters': {'query': 'alpha'},
                'storage': {'input': [{'source': 'beta'}], 'output': [{'destination': 'gamma'}]},
            },
            [
                {'scope': 'parameters.query', 'patterns': ['alpha']},
                {'scope': 'storage.input[0].source', 'patterns': ['beta']},
            ],
        ),
        (
            # Both patterns match across two leaves inside the same scope; each leaf gets its own entry.
            {
                'patterns': ['alpha', 'beta'],
                'item_types': ('configuration',),
                'search_scopes': ('parameters', 'storage.input'),
                'return_all_matched_patterns': True,
            },
            {
                'parameters': {'query': 'alpha'},
                'storage': {'input': [{'source': 'beta'}, {'source': 'alpha'}], 'output': [{'destination': 'gamma'}]},
            },
            [
                {'scope': 'parameters.query', 'patterns': ['alpha']},
                {'scope': 'storage.input[0].source', 'patterns': ['beta']},
                {'scope': 'storage.input[1].source', 'patterns': ['alpha']},
            ],
        ),
        (
            # Pattern not present in any of the specified scopes → empty result.
            {
                'patterns': ['gamma'],
                'item_types': ('configuration',),
                'search_scopes': ('parameters', 'storage.input'),
                'return_all_matched_patterns': True,
            },
            {
                'parameters': {'query': 'alpha'},
                'storage': {'input': [{'source': 'beta'}], 'output': [{'destination': 'gamma'}]},
            },
            [],
        ),
        (
            # No scopes → walk the whole config; can match parent nodes containing the searched fragment.
            {
                'patterns': ['gamma'],
                'item_types': ('configuration',),
                'return_all_matched_patterns': True,
            },
            {
                'parameters': {'query': 'alpha'},
                'storage': {'input': [{'source': 'beta'}], 'output': [{'destination': 'gamma'}]},
            },
            [
                {'scope': 'storage', 'patterns': ['gamma']},
                {'scope': 'storage.output', 'patterns': ['gamma']},
                {'scope': 'storage.output[0].destination', 'patterns': ['gamma']},
            ],
        ),
        (
            # return_all_matched_patterns=False → stop after first matching leaf.
            {
                'patterns': ['alpha', 'beta'],
                'item_types': ('configuration',),
                'search_scopes': ('parameters', 'storage.input'),
                'return_all_matched_patterns': False,
            },
            {
                'parameters': {'query': 'alpha'},
                'storage': {'input': [{'source': 'beta'}], 'output': [{'destination': 'gamma'}]},
            },
            [{'scope': 'parameters.query', 'patterns': ['alpha']}],
        ),
        (
            # Overlapping scopes should not return duplicate leaf hits.
            {
                'patterns': ['alpha'],
                'item_types': ('configuration',),
                'search_scopes': ('parameters', 'parameters.query'),
                'return_all_matched_patterns': True,
            },
            {'parameters': {'query': 'alpha'}},
            [{'scope': 'parameters.query', 'patterns': ['alpha']}],
        ),
        (
            # Scope pointing directly to scalar should still match (self-scope fallback).
            {
                'patterns': ['wttr.in'],
                'item_types': ('configuration',),
                'search_scopes': ('parameters.api.baseUrl',),
                'return_all_matched_patterns': True,
            },
            {'parameters': {'api': {'baseUrl': 'https://wttr.in'}}},
            [{'scope': 'parameters.api.baseUrl', 'patterns': ['wttr.in']}],
        ),
        (
            # Scope with #-prefixed key should be normalized and parsed correctly.
            {
                'patterns': ['alpha'],
                'item_types': ('configuration',),
                'search_scopes': ('authorization.#apiKey',),
                'return_all_matched_patterns': True,
            },
            {'authorization': {'#apiKey': 'alpha'}},
            [{'scope': 'authorization.#apiKey', 'patterns': ['alpha']}],
        ),
    ],
    ids=[
        'all_patterns_many_scopes',
        'two_patterns_in_one_scope',
        'no_patterns_in_scope',
        'all_patterns_no_scope',
        'any_patterns_return_first_match',
        'overlapping_scopes_deduplicated',
        'scalar_scope_matches_self',
        'hash_prefixed_scope_key_matches',
    ],
)
def test_match_configuration_scopes(spec_kwargs: dict[str, Any], configuration: dict[str, Any], expected: list[dict]):
    spec = SearchSpec(**spec_kwargs)
    matches = spec.match_configuration_scopes(configuration)
    assert [match.model_dump() for match in matches] == expected


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
