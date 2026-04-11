from unittest.mock import MagicMock

import pytest

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.project_registry import ProjectContext, ProjectRegistry
from keboola_mcp_server.workspace import WorkspaceManager


def _make_context(project_id: str, alias: str | None = None, forbid_writes: bool = False) -> ProjectContext:
    return ProjectContext(
        project_id=project_id,
        client=MagicMock(spec=KeboolaClient),
        workspace_manager=MagicMock(spec=WorkspaceManager),
        alias=alias,
        forbid_main_branch_writes=forbid_writes,
    )


class TestProjectRegistry:
    def test_single_project_default(self) -> None:
        ctx = _make_context('123')
        registry = ProjectRegistry(projects={'123': ctx})

        result = registry.get_project()
        assert result is ctx

    def test_single_project_by_id(self) -> None:
        ctx = _make_context('123')
        registry = ProjectRegistry(projects={'123': ctx})

        result = registry.get_project('123')
        assert result is ctx

    def test_multi_project_by_id(self) -> None:
        ctx1 = _make_context('123')
        ctx2 = _make_context('456')
        registry = ProjectRegistry(projects={'123': ctx1, '456': ctx2})

        assert registry.get_project('123') is ctx1
        assert registry.get_project('456') is ctx2

    def test_multi_project_no_default_raises(self) -> None:
        ctx1 = _make_context('123')
        ctx2 = _make_context('456')
        registry = ProjectRegistry(projects={'123': ctx1, '456': ctx2})

        with pytest.raises(ValueError, match='no project_id specified'):
            registry.get_project()

    def test_multi_project_with_default(self) -> None:
        ctx1 = _make_context('123')
        ctx2 = _make_context('456')
        registry = ProjectRegistry(
            projects={'123': ctx1, '456': ctx2},
            default_project_id='456',
        )

        assert registry.get_project() is ctx2

    def test_resolve_by_alias(self) -> None:
        ctx = _make_context('123', alias='my-project')
        registry = ProjectRegistry(projects={'123': ctx})

        assert registry.get_project('my-project') is ctx

    def test_unknown_project_raises(self) -> None:
        ctx = _make_context('123')
        registry = ProjectRegistry(projects={'123': ctx})

        with pytest.raises(ValueError, match='not found'):
            registry.get_project('999')

    def test_empty_projects_raises(self) -> None:
        with pytest.raises(ValueError, match='at least one project'):
            ProjectRegistry(projects={})

    def test_inject_into_state(self) -> None:
        ctx = _make_context('123')
        registry = ProjectRegistry(projects={'123': ctx})
        state: dict = {}

        result = registry.inject_into_state(state, '123')
        assert result is ctx
        assert state[KeboolaClient.STATE_KEY] is ctx.client
        assert state[WorkspaceManager.STATE_KEY] is ctx.workspace_manager

    def test_list_projects(self) -> None:
        ctx1 = _make_context('123')
        ctx2 = _make_context('456')
        registry = ProjectRegistry(projects={'123': ctx1, '456': ctx2})

        projects = registry.list_projects()
        assert len(projects) == 2

    def test_from_state(self) -> None:
        ctx = _make_context('123')
        registry = ProjectRegistry(projects={'123': ctx})
        state = {ProjectRegistry.STATE_KEY: registry}

        assert ProjectRegistry.from_state(state) is registry

    def test_from_state_missing_raises(self) -> None:
        with pytest.raises(ValueError, match='not available'):
            ProjectRegistry.from_state({})
