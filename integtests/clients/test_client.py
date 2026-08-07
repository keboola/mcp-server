import logging
from collections.abc import Mapping
from typing import Any

import pytest

from integtests.conftest import ProjectDef, TableDef
from keboola_mcp_server.clients.client import KeboolaClient, get_metadata_property
from keboola_mcp_server.clients.storage import AsyncStorageClient, GlobalSearchResponse
from keboola_mcp_server.config import MetadataField

LOG = logging.getLogger(__name__)

# Any component works for the metadata search; a transformation needs no credentials to be created.
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
    async def test_component_configurations_search_returns_metadata(
        self, storage_client: AsyncStorageClient, unique_id: str
    ):
        """
        Tests the search endpoint against the live SAPI contract.

        This guards two failure modes that unit tests with a mocked client cannot see, and that
        together silently disabled the whole folder-hint feature:
        - a wrong filter name (`componentId` instead of `idComponent`) fails the request with 400;
        - omitting `include=filteredMetadata` returns rows with no `metadata` key at all, so no
          folder name is ever found even though the request succeeds.
        """
        folder_name = f'Test Folder {unique_id}'
        created = await storage_client.configuration_create(
            component_id=_TEST_COMPONENT_ID,
            name=f'search-metadata-test-{unique_id}',
            description='Configuration created by an automated test of the metadata search endpoint',
            configuration={},
        )
        configuration_id = str(created['id'])
        try:
            await storage_client.configuration_metadata_update(
                component_id=_TEST_COMPONENT_ID,
                configuration_id=configuration_id,
                metadata={MetadataField.CONFIGURATION_FOLDER_NAME: folder_name},
            )

            results = await storage_client.component_configurations_search(
                component_id=_TEST_COMPONENT_ID,
                metadata_keys=[MetadataField.CONFIGURATION_FOLDER_NAME],
            )

            matching = [r for r in results if r.get('configurationId') == configuration_id]
            assert len(matching) == 1, f'Configuration {configuration_id} not found in search results: {results}'
            assert matching[0].get('idComponent') == _TEST_COMPONENT_ID
            raw_metadata = matching[0].get('metadata')
            assert isinstance(raw_metadata, list), f'Expected a "metadata" list, got: {raw_metadata!r}'
            metadata: list[Mapping[str, Any]] = [md for md in raw_metadata if isinstance(md, dict)]
            assert get_metadata_property(metadata, MetadataField.CONFIGURATION_FOLDER_NAME) == folder_name
        finally:
            await storage_client.configuration_delete(
                component_id=_TEST_COMPONENT_ID,
                configuration_id=configuration_id,
                skip_trash=True,
            )
