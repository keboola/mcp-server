from typing import Any, cast
from unittest.mock import call

import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import ItemType
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.search import SearchHit, search


class TestSearchTool:
    """Test cases for the search tool function."""

    @pytest.mark.asyncio
    async def test_search_success(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test successful search with regex patterns."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

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
            ),
            SearchHit(
                table_id='in.c-test-bucket.test-table',
                item_type='table',
                updated='2024-01-01T00:00:00Z',
                name='test-table',
            ),
        ]

    @pytest.mark.asyncio
    async def test_search_with_regex_pattern(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with regex patterns."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

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
            ),
        ]

    @pytest.mark.asyncio
    async def test_search_default_parameters(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with default parameters."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock all endpoints to return empty results
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(ctx=mcp_context_client, patterns=['test'])

        assert isinstance(result, list)
        assert result == []

    @pytest.mark.asyncio
    async def test_search_limit_out_of_range(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with limit out of range gets clamped to default."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock all endpoints
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        # Test with limit too high
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=200)
        assert isinstance(result, list)

        # Test with limit too low
        result = await search(ctx=mcp_context_client, patterns=['test'], limit=0)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_search_negative_offset(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with negative offset gets clamped to 0."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

        # Mock all endpoints
        keboola_client.storage_client.bucket_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.component_list = mocker.AsyncMock(return_value=[])
        keboola_client.storage_client.workspace_list = mocker.AsyncMock(return_value=[])

        result = await search(ctx=mcp_context_client, patterns=['test'], offset=-10)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_search_pagination(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search with pagination."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

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
                bucket_id='in.c-bucket-10', item_type='bucket', updated='2024-01-10T00:00:00Z', name='test-bucket-10'
            ),
            SearchHit(
                bucket_id='in.c-bucket-9', item_type='bucket', updated='2024-01-09T00:00:00Z', name='test-bucket-9'
            ),
        ]

        result = await search(ctx=mcp_context_client, patterns=['test'], limit=1, offset=2)
        assert result == [
            SearchHit(
                bucket_id='in.c-bucket-8', item_type='bucket', updated='2024-01-08T00:00:00Z', name='test-bucket-8'
            )
        ]

    @pytest.mark.asyncio
    async def test_search_matches_description(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test search matches description field."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

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
            )
        ]

    @pytest.mark.asyncio
    async def test_search_hits_sorting(self, mocker: MockerFixture, mcp_context_client: Context):
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)

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
            ),
            SearchHit(
                component_id='keboola.ex-db-mysql',
                configuration_id='test-config-a',
                item_type='configuration',
                updated='2024-01-03T00:00:00Z',
                name='Test MySQL Config A',
            ),
            SearchHit(
                table_id='in.c-test-bucket-b.test-table',
                item_type='table',
                updated='2024-01-02T00:00:00Z',
                name='test-table',
            ),
            SearchHit(
                bucket_id='in.c-test-bucket-b',
                item_type='bucket',
                updated='2024-01-02T00:00:00Z',
                name='test-bucket-b',
            ),
            SearchHit(
                table_id='in.c-test-bucket-a.test-table',
                item_type='table',
                updated='2024-01-01T00:00:00Z',
                name='test-table',
            ),
            SearchHit(
                bucket_id='in.c-test-bucket-a',
                item_type='bucket',
                updated='2024-01-01T00:00:00Z',
                name='test-bucket-a',
            ),
            SearchHit(
                bucket_id='in.c-test-bucket-c',
                item_type='bucket',
                updated='',
                name='test-bucket-c',
            ),
        ]

        keboola_client.storage_client.bucket_list.assert_has_calls([call(), call()])
        keboola_client.storage_client.bucket_table_list.assert_has_calls(
            [
                call('in.c-test-bucket-a'),
                call('in.c-test-bucket-b'),
                call('in.c-test-bucket-c'),
            ]
        )
        keboola_client.storage_client.component_list.assert_called_once_with(None, include=['configuration', 'rows'])
        keboola_client.storage_client.workspace_list.assert_not_called()
