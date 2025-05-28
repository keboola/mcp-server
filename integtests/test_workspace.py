from typing import Any, Generator, Mapping

import pytest
from kbcstorage.client import Client as SyncStorageClient

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.tools.workspace import WorkspaceManager


def _storage_client(storage_api_url: str, storage_api_token: str) -> SyncStorageClient:
    return SyncStorageClient(storage_api_url, storage_api_token)


@pytest.fixture
def dynamic_manager(
        keboola_client: KeboolaClient, storage_api_token: str, storage_api_url: str, workspace_schema: str
) -> Generator[WorkspaceManager, Any, None]:
    branch_id = keboola_client.storage_client.branch_id
    storage_client = _storage_client(storage_api_url, storage_api_token)

    def _get_workspace_meta() -> Mapping[str, Any] | None:
        metadata = storage_client.branches.metadata(branch_id)
        for m in metadata:
            if m.get('key') == WorkspaceManager.MCP_META_KEY:
                return m
        return None

    meta = _get_workspace_meta()
    if meta:
        pytest.fail(f'Expecting empty Keboola project, but found {meta} in {branch_id} branch')

    workspaces = storage_client.workspaces.list()
    # ignore the static workspace
    workspaces = [w for w in workspaces if w['connection']['schema'] != workspace_schema]
    if workspaces:
        pytest.fail(f'Expecting empty Keboola project, but found {len(workspaces)} extra workspaces')

    yield WorkspaceManager(keboola_client)

    meta = _get_workspace_meta()
    if meta:
        storage_client.workspaces.delete(meta['value'])
        storage_client.branches._delete(f'{storage_client.branches.base_url}branch/{branch_id}/metadata/{meta["id"]}')


class TestWorkspaceManager:

    @pytest.mark.asyncio
    async def test_static_workspace(self, workspace_manager: WorkspaceManager, workspace_schema: str):
        assert workspace_manager._workspace_schema == workspace_schema

        info = await workspace_manager._find_info_by_schema(workspace_schema)
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
        info = await dynamic_manager._find_info_in_branch()
        assert info is None

        # create workspace
        workspace = await dynamic_manager._get_workspace()
        assert workspace is not None

        # check that the new workspace is recorded in the branch
        info = await dynamic_manager._find_info_in_branch()
        assert info is not None
        assert workspace.id == info.id
