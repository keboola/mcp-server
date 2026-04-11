"""Multi-project registry for MPA (Multi-Project Architecture) mode.

Holds per-project KeboolaClient and WorkspaceManager instances and provides
project resolution logic used by ProjectResolutionMiddleware.
"""

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


@dataclass
class ProjectContext:
    """Holds all per-project runtime state."""

    project_id: str
    client: KeboolaClient
    workspace_manager: WorkspaceManager
    alias: str | None
    forbid_main_branch_writes: bool


class ProjectRegistry:
    """Registry of all projects available in the current MPA session.

    Provides lookup by project_id or alias, and injects the resolved project's
    client/workspace into session state under the legacy keys so that existing
    tool functions work without modification.
    """

    STATE_KEY = 'project_registry'

    def __init__(
        self,
        projects: dict[str, ProjectContext],
        default_project_id: str | None = None,
    ) -> None:
        if not projects:
            raise ValueError('ProjectRegistry requires at least one project.')
        self._projects = projects  # keyed by project_id
        self._default_project_id = default_project_id

        # Build alias -> project_id lookup
        self._alias_to_id: dict[str, str] = {}
        for pid, ctx in projects.items():
            if ctx.alias:
                self._alias_to_id[ctx.alias] = pid

    @property
    def default_project_id(self) -> str | None:
        return self._default_project_id

    @property
    def projects(self) -> dict[str, ProjectContext]:
        return dict(self._projects)

    def get_project(self, project_id: str | None = None) -> ProjectContext:
        """Resolve a project by ID, alias, or default.

        :param project_id: Project ID or alias. If None, returns the default project.
        :raises ValueError: If the project cannot be resolved.
        """
        if project_id is None:
            return self._get_default_project()

        # Try direct project_id lookup
        if project_id in self._projects:
            return self._projects[project_id]

        # Try alias lookup
        if project_id in self._alias_to_id:
            resolved_id = self._alias_to_id[project_id]
            return self._projects[resolved_id]

        available = self._format_available_projects()
        raise ValueError(f'Project "{project_id}" not found. Available projects: {available}')

    def _get_default_project(self) -> ProjectContext:
        """Get the default project or the only project if there's just one."""
        if self._default_project_id and self._default_project_id in self._projects:
            return self._projects[self._default_project_id]

        if len(self._projects) == 1:
            return next(iter(self._projects.values()))

        available = self._format_available_projects()
        raise ValueError(
            f'Multiple projects available but no project_id specified and no default set. '
            f'Available projects: {available}'
        )

    def _format_available_projects(self) -> str:
        parts = []
        for pid, ctx in self._projects.items():
            if ctx.alias:
                parts.append(f'{pid} ({ctx.alias})')
            else:
                parts.append(pid)
        return ', '.join(parts)

    def list_projects(self) -> list[ProjectContext]:
        return list(self._projects.values())

    def inject_into_state(self, state: dict[str, Any], project_id: str | None = None) -> ProjectContext:
        """Resolve a project and inject its client/workspace into session state under legacy keys.

        This allows existing tool functions to work without modification via
        KeboolaClient.from_state() and WorkspaceManager.from_state().

        :param state: The session state dict to inject into.
        :param project_id: Project ID or alias. If None, uses default.
        :returns: The resolved ProjectContext.
        """
        project = self.get_project(project_id)
        state[KeboolaClient.STATE_KEY] = project.client
        state[WorkspaceManager.STATE_KEY] = project.workspace_manager
        return project

    @classmethod
    def from_state(cls, state: Mapping[str, Any]) -> 'ProjectRegistry':
        instance = state.get(cls.STATE_KEY)
        if not isinstance(instance, ProjectRegistry):
            raise ValueError('ProjectRegistry is not available in the session state.')
        return instance
