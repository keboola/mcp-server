import logging
import pytest

from keboola_mcp_server.client import AsyncStorageClient, GlobalSearchResponse, KeboolaClient, ProjectFeature
from integtests.conftest import ConfigDef, ProjectDef, TableDef

LOG = logging.getLogger(__name__)


class TestAsyncStorageClient:

    @pytest.fixture
    def storage_client(self, keboola_client: KeboolaClient, keboola_project: ProjectDef) -> AsyncStorageClient:
        return keboola_client.storage_client

    @pytest.mark.asyncio
    async def test_global_search(self, storage_client: AsyncStorageClient):
        NOT_EXISTING_ID = 'not-existing-id'
        ret = await storage_client.global_search(query=NOT_EXISTING_ID)
        assert isinstance(ret, GlobalSearchResponse)
        assert ret.all == 0
        assert ret.items == []
        assert ret.by_type == {'total': 0}
        assert ret.by_project == {}
    
    @pytest.mark.asyncio
    async def test_global_search_with_results(self, storage_client: AsyncStorageClient, tables: list[TableDef]):
        search_for_name = 'test'
        is_global_search_enabled = await storage_client.is_enabled('global-search')
        if not is_global_search_enabled:
            LOG.warning('Global search is not enabled in the project. Skipping test. Please enable it in the project.')
            pytest.skip('Global search is not enabled in the project. Skipping test.')
        
        ret = await storage_client.global_search(query=search_for_name, types = ['table'])
        assert isinstance(ret, GlobalSearchResponse)
        assert ret.all == len(tables)

