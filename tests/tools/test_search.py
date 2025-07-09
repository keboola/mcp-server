from datetime import datetime
from typing import Any

import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.client import GlobalSearchResponse, KeboolaClient
from keboola_mcp_server.tools.search import (
    DEFAULT_GLOBAL_SEARCH_LIMIT,
    GlobalSearchItemsGroup,
    GlobalSearchOutput,
    global_search,
)


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


class TestGlobalSearchGroupItemsFromApiResponse:
    """Test cases for GlobalSearchGroupItems.from_api_response method."""

    def test_from_api_response_table_type(self, parsed_global_search_response):
        """Test creating group items for table type."""
        table_items = [item for item in parsed_global_search_response.items if item.type == 'table']

        result = GlobalSearchItemsGroup.from_api_response('table', table_items)

        assert result.group_type == 'table'
        assert result.group_count == 1
        assert len(result.group_items) == 1

        item = result.group_items[0]
        assert item.id == 'in.c-bucket.table1'
        assert item.name == 'table1'
        assert item.created == datetime.fromisoformat('2024-01-01T00:00:00+00:00')
        assert item.additional_info['bucket_id'] == 'in.c-bucket'
        assert item.additional_info['bucket_name'] == 'bucket'

    def test_from_api_response_configuration_type(self, parsed_global_search_response):
        """Test creating group items for configuration type."""
        config_items = [item for item in parsed_global_search_response.items if item.type == 'configuration']

        result = GlobalSearchItemsGroup.from_api_response('configuration', config_items)

        assert result.group_type == 'configuration'
        assert result.group_count == 1
        assert len(result.group_items) == 1

        item = result.group_items[0]
        assert item.id == 'keboola.ex-db-mysql.config1'
        assert item.name == 'MySQL Config'
        assert item.additional_info['component_id'] == 'keboola.ex-db-mysql'
        assert item.additional_info['component_name'] == 'MySQL Extractor'

    def test_from_api_response_configuration_row_type(self, parsed_global_search_response):
        """Test creating group items for configuration-row type."""
        row_items = [item for item in parsed_global_search_response.items if item.type == 'configuration-row']

        result = GlobalSearchItemsGroup.from_api_response('configuration-row', row_items)

        assert result.group_type == 'configuration-row'
        assert result.group_count == 1
        assert len(result.group_items) == 1

        item = result.group_items[0]
        assert item.id == 'keboola.ex-db-mysql.config1.row1'
        assert item.name == 'Row Config'
        assert item.additional_info['component_id'] == 'keboola.ex-db-mysql'
        assert item.additional_info['component_name'] == 'MySQL Extractor'
        assert item.additional_info['configuration_id'] == 'config1'
        assert item.additional_info['configuration_name'] == 'MySQL Config'

    def test_from_api_response_flow_type(self, parsed_global_search_response):
        """Test creating group items for flow type."""
        flow_items = [item for item in parsed_global_search_response.items if item.type == 'flow']

        result = GlobalSearchItemsGroup.from_api_response('flow', flow_items)

        assert result.group_type == 'flow'
        assert result.group_count == 1
        assert len(result.group_items) == 1

        item = result.group_items[0]
        assert item.id == 'flow123'
        assert item.name == 'Test Flow'
        assert item.additional_info['component_id'] == 'keboola.orchestrator'
        assert item.additional_info['component_name'] == 'Orchestrator'

    def test_from_api_response_filters_by_type(self, parsed_global_search_response):
        """Test that from_api_response filters items by the specified type."""
        all_items = parsed_global_search_response.items

        result = GlobalSearchItemsGroup.from_api_response('table', all_items)

        assert result.group_count == 1
        assert len(result.group_items) == 1
        assert result.group_items[0].id == 'in.c-bucket.table1'

    def test_from_api_response_empty_items(self):
        """Test from_api_response with empty items list."""
        result = GlobalSearchItemsGroup.from_api_response('table', [])

        assert result.group_type == 'table'
        assert result.group_count == 0
        assert len(result.group_items) == 0


class TestGlobalSearchGroupItemsGroupTypeItemFromApiResponse:
    """Test cases for GlobalSearchGroupItems.GroupTypeItem.from_api_response method."""

    def test_from_api_response_table_item(self, parsed_global_search_response):
        """Test creating item from table API response."""
        table_item = next(item for item in parsed_global_search_response.items if item.type == 'table')

        result = GlobalSearchItemsGroup.GroupItem.from_api_response(table_item)

        assert result.name == 'table1'
        assert result.id == 'in.c-bucket.table1'
        assert result.created == datetime.fromisoformat('2024-01-01T00:00:00+00:00')
        assert result.additional_info['bucket_id'] == 'in.c-bucket'
        assert result.additional_info['bucket_name'] == 'bucket'

    def test_from_api_response_missing_bucket_info(self):
        """Test creating item when bucket info is missing it should fail with KeyError."""
        item_data = {
            'id': 'table_without_bucket',
            'name': 'Table',
            'type': 'table',
            'fullPath': {},
            'componentId': None,
            'organizationId': 123,
            'projectId': 456,
            'projectName': 'Test Project',
            'created': '2024-01-01T00:00:00Z',
        }
        item = GlobalSearchResponse.Item.model_validate(item_data)
        with pytest.raises(KeyError):
            GlobalSearchItemsGroup.GroupItem.from_api_response(item)


class TestGlobalSearchAnswerFromApiResponse:
    """Test cases for GlobalSearchAnswer.from_api_responses method."""

    def test_from_api_responses(self, parsed_global_search_response):
        """Test creating answer from API response."""
        result = GlobalSearchOutput.from_api_responses(parsed_global_search_response)

        assert result.counts == {
            'table': 1,
            'configuration': 1,
            'configuration-row': 1,
            'flow': 1,
        }

        # Should be sorted by type name
        expected_types = ['configuration', 'configuration-row', 'flow', 'table']
        actual_types = [group.group_type for group in result.type_groups]
        assert actual_types == expected_types

        # Check group counts
        for group in result.type_groups:
            assert group.group_count == 1

    def test_from_api_responses_empty(self):
        """Test creating answer from empty API response."""
        empty_response = GlobalSearchResponse(all=0, items=[], byType={}, byProject={})

        result = GlobalSearchOutput.from_api_responses(empty_response)

        assert result.counts == {}
        assert len(result.type_groups) == 0


class TestGlobalSearchTool:
    """Test cases for the global_search tool function."""

    @pytest.mark.asyncio
    async def test_global_search_success(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_global_search_response
    ):
        """Test successful global search."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

        mock_response = GlobalSearchResponse.model_validate(mock_global_search_response)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=mock_response)

        result = await global_search(
            ctx=mcp_context_client,
            name_prefixes=['test', 'table'],
            entity_types=('table', 'configuration'),
            limit=20,
            offset=0,
        )

        assert isinstance(result, GlobalSearchOutput)
        assert result.counts['table'] == 1
        assert result.counts['configuration'] == 1

        # Verify the storage client was called with correct parameters
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='test table', types=('table', 'configuration'), limit=20, offset=0
        )

    @pytest.mark.asyncio
    async def test_global_search_default_parameters(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_global_search_response
    ):
        """Test global search with default parameters."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

        mock_response = GlobalSearchResponse.model_validate(mock_global_search_response)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=mock_response)

        result = await global_search(ctx=mcp_context_client, name_prefixes=['test'])

        assert isinstance(result, GlobalSearchOutput)

        # Verify the storage client was called with default parameters
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='test', types=tuple(), limit=DEFAULT_GLOBAL_SEARCH_LIMIT, offset=0
        )

    @pytest.mark.asyncio
    async def test_global_search_limit_out_of_range(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_global_search_response
    ):
        """Test global search with limit out of range gets clamped to default."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

        mock_response = GlobalSearchResponse.model_validate(mock_global_search_response)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=mock_response)

        # Test with limit too high
        await global_search(ctx=mcp_context_client, name_prefixes=['test'], limit=200)

        # Should use default limit
        keboola_client.storage_client.global_search.assert_called_with(
            query='test', types=tuple(), limit=DEFAULT_GLOBAL_SEARCH_LIMIT, offset=0
        )

        # Test with limit too low
        keboola_client.storage_client.global_search.reset_mock()
        await global_search(ctx=mcp_context_client, name_prefixes=['test'], limit=0)

        # Should use default limit
        keboola_client.storage_client.global_search.assert_called_with(
            query='test', types=tuple(), limit=DEFAULT_GLOBAL_SEARCH_LIMIT, offset=0
        )

    @pytest.mark.asyncio
    async def test_global_search_negative_offset(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_global_search_response
    ):
        """Test global search with negative offset gets clamped to 0."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

        mock_response = GlobalSearchResponse.model_validate(mock_global_search_response)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=mock_response)

        await global_search(ctx=mcp_context_client, name_prefixes=['test'], offset=-10)

        # Should use offset 0
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='test', types=tuple(), limit=DEFAULT_GLOBAL_SEARCH_LIMIT, offset=0
        )

    @pytest.mark.asyncio
    async def test_global_search_feature_not_enabled(self, mocker: MockerFixture, mcp_context_client: Context):
        """Test global search when feature is not enabled."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=False)

        with pytest.raises(ValueError, match='Global search is not enabled'):
            await global_search(ctx=mcp_context_client, name_prefixes=['test'])

    @pytest.mark.asyncio
    async def test_global_search_joins_prefixes(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_global_search_response
    ):
        """Test that global search joins name prefixes with spaces."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

        mock_response = GlobalSearchResponse.model_validate(mock_global_search_response)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=mock_response)

        await global_search(ctx=mcp_context_client, name_prefixes=['test', 'table', 'data'])

        # Should join with spaces
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='test table data', types=tuple(), limit=DEFAULT_GLOBAL_SEARCH_LIMIT, offset=0
        )

    @pytest.mark.asyncio
    async def test_global_search_with_valid_limit(
        self, mocker: MockerFixture, mcp_context_client: Context, mock_global_search_response
    ):
        """Test global search with valid limit in range."""
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.is_enabled = mocker.AsyncMock(return_value=True)

        mock_response = GlobalSearchResponse.model_validate(mock_global_search_response)
        keboola_client.storage_client.global_search = mocker.AsyncMock(return_value=mock_response)

        await global_search(ctx=mcp_context_client, name_prefixes=['test'], limit=75)

        # Should use the provided limit
        keboola_client.storage_client.global_search.assert_called_once_with(
            query='test', types=tuple(), limit=75, offset=0
        )
