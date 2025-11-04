import logging

import pytest
from fastmcp import Context

from integtests.conftest import BucketDef, ConfigDef, TableDef
from keboola_mcp_server.clients.ai_service import SuggestedComponent
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.search import find_component_id, search

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.parametrize('item_type', [None, 'bucket', 'table', 'configuration', 'transformation'])
async def test_search_end_to_end(
    item_type: str | None,
    keboola_client: KeboolaClient,
    mcp_context: Context,
    buckets: list[BucketDef],
    tables: list[TableDef],
    configs: list[ConfigDef],
) -> None:
    """
    Test the search tool end-to-end by searching for items that exist in the test project.
    This verifies that the search returns expected results for buckets, tables, and configurations.
    """
    item_types = (item_type,) if item_type else tuple()

    # Search for test items by name pattern 'test' which should match our test data
    result = await search(
        ctx=mcp_context, patterns=['test'], item_types=item_types, limit=50, offset=0  # Search all types
    )

    # filter out data apps that seem to often be left behind in the testing project
    result = [hit for hit in result if hit.item_type != 'configuration' or hit.configuration_id != 'keboola.data-apps']

    # Verify the result structure
    assert isinstance(result, list)

    # Verify we found some results
    assert len(result) > 0, 'Should find at least some test items'

    # Create sets of expected IDs for verification
    if not item_type or item_type == 'bucket':
        expected_bucket_ids = {bucket.bucket_id for bucket in buckets}
        actual_bucket_ids = {hit.bucket_id for hit in result if hit.item_type == 'bucket'}
        assert actual_bucket_ids == expected_bucket_ids, f'Should find all test buckets. Found: {result}'

    if not item_type or item_type == 'table':
        expected_table_ids = {table.table_id for table in tables}
        actual_table_ids = {hit.table_id for hit in result if hit.item_type == 'table'}
        assert actual_table_ids == expected_table_ids, f'Should find all test tables. Found: {result}'

    if not item_type:
        expected_config_ids = {config.configuration_id for config in configs}
        actual_config_ids = {
            hit.configuration_id for hit in result if hit.item_type in ['configuration', 'transformation']
        }
        assert actual_config_ids == expected_config_ids, f'Should find all test configurations. Found: {result}'

    elif item_type == 'configuration':
        expected_config_ids = {config.configuration_id for config in configs if config.component_id == 'ex-generic-v2'}
        actual_config_ids = {hit.configuration_id for hit in result if hit.item_type == 'configuration'}
        assert actual_config_ids == expected_config_ids, f'Should find all test configurations. Found: {result}'

    elif item_type == 'transformation':
        expected_config_ids = {
            config.configuration_id for config in configs if config.component_id == 'keboola.snowflake-transformation'
        }
        actual_config_ids = {hit.configuration_id for hit in result if hit.item_type == 'transformation'}
        assert actual_config_ids == expected_config_ids, f'Should find all test transformations. Found: {result}'


@pytest.mark.asyncio
async def test_find_component_id(mcp_context: Context):
    """Tests that `find_component_id` returns relevant component IDs for a query."""
    query = 'generic extractor - extract data from many APIs'
    generic_extractor_id = 'ex-generic-v2'

    result = await find_component_id(query=query, ctx=mcp_context)

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(component, SuggestedComponent) for component in result)
    assert generic_extractor_id in [component.component_id for component in result]
