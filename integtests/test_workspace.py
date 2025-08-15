import logging
from typing import Any, Generator, Mapping

import pytest
import requests
from kbcstorage.client import Client as SyncStorageClient

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@pytest.fixture
def dynamic_manager(
    keboola_client: KeboolaClient, sync_storage_client: SyncStorageClient, workspace_schema: str
) -> Generator[WorkspaceManager, Any, None]:
    storage_client = sync_storage_client
    token_info = storage_client.tokens.verify()
    project_id: str = token_info['owner']['id']

    def _get_workspace_meta() -> list[Mapping[str, Any]]:
        metadata: list[Mapping[str, Any]] = []
        for m in storage_client.branches.metadata('default'):
            if m.get('key') == WorkspaceManager.MCP_META_KEY:
                metadata.append(m)
        return metadata

    metas = _get_workspace_meta()
    if metas:
        pytest.fail(f'Expecting empty Keboola project {project_id}, but found {metas} in the default branch')

    workspaces = storage_client.workspaces.list()
    # ignore the static workspaces
    workspaces = [
        w
        for w in workspaces
        if all(
            [
                w['connection']['schema'] != workspace_schema,
                w.get('creatorToken', {}).get('description') != 'Background Indexing Token',
            ]
        )
    ]
    if workspaces:
        pytest.fail(
            f'Expecting empty Keboola project {project_id}, but found {len(workspaces)} extra workspaces: '
            f'{[{"id": w["id"], "name": w["name"]} for w in workspaces]}'
        )

    yield WorkspaceManager(keboola_client)

    LOG.info(f'Cleaning up workspaces in Keboola project with ID={project_id}')
    metas = _get_workspace_meta()
    if len(metas) > 1:
        LOG.info(f'Multiple metadata entries found: {metas}')
    for meta in metas:
        try:
            storage_client.workspaces.delete(meta['value'])
            LOG.info(f'Deleted workspaces: {meta["value"]}')
        except requests.HTTPError:
            LOG.exception(f'Failed to delete workspace {meta["value"]}')
        try:
            storage_client.branches._delete(f'{storage_client.branches.base_url}branch/default/metadata/{meta["id"]}')
            LOG.info(f'Deleted workspaces metadata: {meta["id"]}')
        except requests.HTTPError as e:
            LOG.exception(f'Failed to delete workspace metadata {meta["id"]}: {e}')


class TestWorkspaceManager:

    @pytest.mark.asyncio
    async def test_static_workspace(self, workspace_manager: WorkspaceManager, workspace_schema: str):
        assert workspace_manager._workspace_schema == workspace_schema

        info = await workspace_manager._find_ws_by_schema(workspace_schema)
        assert info is not None
        assert info.schema == workspace_schema
        assert info.backend in ['snowflake', 'bigquery']

        workspace = await workspace_manager._get_workspace()
        assert workspace is not None
        assert workspace.id == info.id

    @pytest.mark.asyncio
    async def test_dynamic_workspace(self, dynamic_manager: WorkspaceManager):
        assert dynamic_manager._workspace_schema is None

        # check that there is no workspace in the branch
        info = await dynamic_manager._find_ws_in_branch()
        assert info is None

        # create workspace
        workspace = await dynamic_manager._get_workspace()
        assert workspace is not None

        # check that the new workspace is recorded in the branch
        info = await dynamic_manager._find_ws_in_branch()
        assert info is not None
        assert workspace.id == info.id
