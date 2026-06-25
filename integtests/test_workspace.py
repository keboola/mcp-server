import logging
from collections.abc import AsyncGenerator, Mapping
from typing import Any

import pytest
import pytest_asyncio
import requests
from kbcstorage.client import Client as SyncStorageClient

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@pytest_asyncio.fixture
async def dynamic_manager(
    keboola_client: KeboolaClient, sync_storage_client: SyncStorageClient, workspace_schema: str
) -> AsyncGenerator[WorkspaceManager, Any]:
    storage_client = sync_storage_client
    token_info = storage_client.tokens.verify()
    project_id: str = token_info['owner']['id']
    component_id = WorkspaceManager.MCP_WORKSPACE_COMPONENT_ID

    def _mcp_workspaces() -> list[Mapping[str, Any]]:
        """MCP workspaces, discovered by their component id (no branch-metadata pointer)."""
        return [w for w in storage_client.workspaces.list() if w.get('component') == component_id]

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

    existing_configs = list(storage_client.configurations.list(component_id=component_id))
    if existing_configs:
        pytest.fail(
            f'Expecting no MCP workspace configs in project {project_id}, '
            f'but found: {[c.get("id") for c in existing_configs]}'
        )

    yield await WorkspaceManager.create(keboola_client)

    LOG.info(f'Cleaning up workspaces in Keboola project with ID={project_id}')
    # The MCP server no longer stamps a branch-metadata pointer; its workspaces are discovered
    # (and cleaned up) by their component id.
    for ws in _mcp_workspaces():
        try:
            storage_client.workspaces.delete(ws['id'])
            LOG.info(f'Deleted workspace: {ws["id"]}')
        except requests.HTTPError:
            LOG.exception(f'Failed to delete workspace {ws["id"]}')

    # Clean up configurations created under the MCP workspace component
    try:
        configs = storage_client.configurations.list(component_id=component_id)
        for cfg in configs:
            cfg_id = cfg.get('id')
            if cfg_id:
                try:
                    storage_client.configurations.delete(component_id, cfg_id)
                    # Double delete to skip trash
                    storage_client.configurations.delete(component_id, cfg_id)
                    LOG.info(f'Deleted component config: {component_id}/{cfg_id}')
                except requests.HTTPError:
                    LOG.exception(f'Failed to delete component config {component_id}/{cfg_id}')
    except requests.HTTPError:
        LOG.exception(f'Failed to list configs for {component_id}')


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
