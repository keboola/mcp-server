import logging
import time

import pytest
from fastmcp import Context

from integtests.conftest import BucketDef, ConfigDef, TableDef
from keboola_mcp_server.clients.ai_service import SuggestedComponent
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.search import GlobalSearchOutput, find_component_id, search

LOG = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip('The global searching in Keboola platform is unstable and makes this test fail randomly.')
async def test_global_search_end_to_end(
    keboola_client: KeboolaClient,
    mcp_context: Context,
    buckets: list[BucketDef],
    tables: list[TableDef],
    configs: list[ConfigDef],
) -> None:
    """
    Test the global_search tool end-to-end by searching for items that exist in the test project.
    This verifies that the search returns expected results for buckets, tables, and configurations.
    """

    # skip this test if the global search is not available
    if not await keboola_client.storage_client.is_enabled('global-search'):
        LOG.warning('Global search is not available. Please enable it in the project settings.')
        pytest.skip('Global search is not available. Please enable it in the project settings.')

    # Search for test items by name prefix 'test' which should match our test data
    # searching is flaky, so we retry a few times
    await search(ctx=mcp_context, name_prefixes=['test'], item_types=tuple(), limit=50, offset=0)  # Search all types
    time.sleep(5)
    result = await search(
        ctx=mcp_context, name_prefixes=['test'], item_types=tuple(), limit=50, offset=0  # Search all types
    )

    # Verify the result structure
    assert isinstance(result, GlobalSearchOutput)
    assert isinstance(result.counts, dict)
    assert isinstance(result.groups, dict)
    assert 'total' in result.counts

    # Verify we found some results
    assert result.counts['total'] > 0, 'Should find at least some test items'

    # Create sets of expected IDs for verification
    expected_bucket_ids = {bucket.bucket_id for bucket in buckets}
    expected_table_ids = {table.table_id for table in tables}
    expected_config_ids = {config.configuration_id for config in configs if config.configuration_id}

    # Check that we can find test buckets
    bucket_groups = [group for group in result.groups.values() if group.type == 'bucket']
    assert len(bucket_groups) == 1
    bucket_group = bucket_groups[0]
    found_bucket_ids = {item.id for item in bucket_group.items}
    # At least some test buckets should be found
    assert found_bucket_ids.intersection(expected_bucket_ids), 'Should find at least one test bucket'

    # Check that we can find test tables
    table_groups = [group for group in result.groups.values() if group.type == 'table']
    assert len(table_groups) == 1
    table_group = table_groups[0]
    found_table_ids = {item.id for item in table_group.items}
    # At least some test tables should be found
    assert found_table_ids.intersection(expected_table_ids), 'Should find at least one test table'

    # Check that we can find test configurations
    config_groups = [group for group in result.groups.values() if group.type == 'configuration']
    assert len(config_groups) == 1
    config_group = config_groups[0]
    found_config_ids = {item.id for item in config_group.items}
    # At least some test configurations should be found
    assert found_config_ids.intersection(expected_config_ids), 'Should find at least one test configuration'


@pytest.mark.asyncio
async def test_find_component_id(mcp_context: Context):
    """Tests that `find_component_id` returns relevant component IDs for a query."""
    query = 'generic extractor'
    generic_extractor_id = 'ex-generic-v2'

    result = await find_component_id(query=query, ctx=mcp_context)

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(isinstance(component, SuggestedComponent) for component in result)
    assert generic_extractor_id in [component.component_id for component in result]
