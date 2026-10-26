import logging

import pytest

from integtests.conftest import ProjectDef, TableDef
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.storage import AsyncStorageClient, GlobalSearchResponse

LOG = logging.getLogger(__name__)

# Any component works for the folder test; a transformation needs no credentials to be created.
_TEST_COMPONENT_ID = 'keboola.python-transformation-v2'


class TestAsyncStorageClient:
    @pytest.fixture
    def storage_client(self, keboola_client: KeboolaClient, keboola_project: ProjectDef) -> AsyncStorageClient:
        return keboola_client.storage_client

    @pytest.mark.asyncio
    @pytest.mark.skip(reason='Unstable')
    async def test_global_search(self, storage_client: AsyncStorageClient):
        not_existing_id = 'not-existing-id'
        ret = await storage_client.global_search(query=not_existing_id)
        assert isinstance(ret, GlobalSearchResponse)
        assert ret.all == 0
        assert ret.items == []
        assert ret.by_type == {'total': 0}
        assert ret.by_project == {}

    @pytest.mark.asyncio
    @pytest.mark.skip(reason='Unstable')
    async def test_global_search_with_results(self, storage_client: AsyncStorageClient, tables: list[TableDef]):
        search_for_name = 'test'
        is_global_search_enabled = await storage_client.is_enabled('global-search')
        if not is_global_search_enabled:
            LOG.warning('Global search is not enabled in the project. Skipping test. Please enable it in the project.')
            pytest.skip('Global search is not enabled in the project. Skipping test.')

        ret = await storage_client.global_search(query=search_for_name, types=['table'])
        assert isinstance(ret, GlobalSearchResponse)
        assert ret.all == len(tables)
        assert len(ret.items) == len(tables)
        assert all(item.type == 'table' for item in ret.items)

    @pytest.mark.asyncio
    async def test_configuration_folder_round_trip(self, storage_client: AsyncStorageClient, unique_id: str):
        """
        Tests that the configuration `folder` field is written by a folder-only update and read back by the
        configuration detail and list endpoints, and that an empty folder clears it.
        """
        folder_name = f'Test Folder {unique_id}'
        created = await storage_client.configuration_create(
            component_id=_TEST_COMPONENT_ID,
            name=f'folder-test-{unique_id}',
            description='Configuration created by an automated test of the configuration folder',
            configuration={},
        )
        configuration_id = str(created['id'])
        try:
            await storage_client.configuration_folder_update(_TEST_COMPONENT_ID, configuration_id, f'  {folder_name}  ')

            detail = await storage_client.configuration_detail(_TEST_COMPONENT_ID, configuration_id)
            assert detail['folder'] == folder_name
            assert detail['version'] == created['version'], 'A folder-only update must not create a version.'
            listed = await storage_client.configuration_list(_TEST_COMPONENT_ID)
            assert [c['folder'] for c in listed if str(c['id']) == configuration_id] == [folder_name]

            await storage_client.configuration_folder_update(_TEST_COMPONENT_ID, configuration_id, '')
            detail = await storage_client.configuration_detail(_TEST_COMPONENT_ID, configuration_id)
            assert detail['folder'] is None
        finally:
            await storage_client.configuration_delete(
                component_id=_TEST_COMPONENT_ID,
                configuration_id=configuration_id,
                skip_trash=True,
            )
