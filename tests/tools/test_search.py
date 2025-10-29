from typing import Any, cast

import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import GlobalSearchResponse, ItemType
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.tools.search import SearchHit, search


@pytest.fixture
def mock_global_search_items() -> list[dict[str, Any]]:
    """Mock GlobalSearchResponse.Item data."""
    return [
        {
            'id': 'in.c-bucket.table1',
            'name': 'table1',
            'type': 'table',
            'fullPath': {'bucket': {'id': 'in.c-bucket', 'name': 'bucket'}},
            'componentId': None,
            'organizationId': 123,
            'projectId': 456,
            'projectName': 'Test Project',
            'created': '2024-01-01T00:00:00Z',
        },
        {
            'id': 'keboola.ex-db-mysql.config1',
            'name': 'MySQL Config',
            'type': 'configuration',
            'fullPath': {'component': {'id': 'keboola.ex-db-mysql', 'name': 'MySQL Extractor'}},
            'componentId': 'keboola.ex-db-mysql',
            'organizationId': 123,
            'projectId': 456,
            'projectName': 'Test Project',
            'created': '2024-01-02T00:00:00Z',
        },
        {
            'id': 'keboola.ex-db-mysql.config1.row1',
            'name': 'Row Config',
            'type': 'configuration-row',
            'fullPath': {
                'component': {'id': 'keboola.ex-db-mysql', 'name': 'MySQL Extractor'},
                'configuration': {'id': 'config1', 'name': 'MySQL Config'},
            },
            'componentId': 'keboola.ex-db-mysql',
            'organizationId': 123,
            'projectId': 456,
            'projectName': 'Test Project',
            'created': '2024-01-03T00:00:00Z',
        },
        {
            'id': 'flow123',
            'name': 'Test Flow',
            'type': 'flow',
            'fullPath': {'component': {'id': 'keboola.orchestrator', 'name': 'Orchestrator'}},
            'componentId': 'keboola.orchestrator',
            'organizationId': 123,
            'projectId': 456,
            'projectName': 'Test Project',
            'created': '2024-01-04T00:00:00Z',
        },
    ]


@pytest.fixture
def mock_global_search_response(mock_global_search_items: list[dict[str, Any]]) -> dict[str, Any]:
    """Mock GlobalSearchResponse data."""
    return {
        'all': 4,
        'items': mock_global_search_items,
        'byType': {
            'table': 1,
            'configuration': 1,
            'configuration-row': 1,
            'flow': 1,
        },
        'byProject': {'456': 'Test Project'},
    }


@pytest.fixture
def parsed_global_search_response(mock_global_search_response: dict[str, Any]) -> GlobalSearchResponse:
    """Parsed GlobalSearchResponse object."""
    return GlobalSearchResponse.model_validate(mock_global_search_response)


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
                created='2024-01-02T00:00:00Z',
                name='Test MySQL Config',
            ),
            SearchHit(
                table_id='in.c-test-bucket.test-table',
                item_type='table',
                created='2024-01-01T00:00:00Z',
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
                created='2024-01-01T00:00:00Z',
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
                bucket_id='in.c-bucket-10', item_type='bucket', created='2024-01-10T00:00:00Z', name='test-bucket-10'
            ),
            SearchHit(
                bucket_id='in.c-bucket-9', item_type='bucket', created='2024-01-09T00:00:00Z', name='test-bucket-9'
            ),
        ]

        result = await search(ctx=mcp_context_client, patterns=['test'], limit=1, offset=2)
        assert result == [
            SearchHit(
                bucket_id='in.c-bucket-8', item_type='bucket', created='2024-01-08T00:00:00Z', name='test-bucket-8'
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
                created='2024-01-01T00:00:00Z',
                name='my-bucket',
                description='This contains test data',
            )
        ]
