import logging
from typing import cast

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import process_concurrently
from keboola_mcp_server.project_registry import ProjectContext, ProjectRegistry
from keboola_mcp_server.resources.prompts import get_project_system_prompt
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

PROJECT_TOOLS_TAG = 'project'


def add_project_tools(mcp: FastMCP) -> None:
    """Add project tools to the MCP server."""

    LOG.info(f'Adding tool {get_project_info.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            get_project_info,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={PROJECT_TOOLS_TAG},
        )
    )

    LOG.info('Project tools initialized.')


def _get_toolset_restrictions(role: str) -> str | None:
    """
    Returns a human-readable description of toolset restrictions for the given user role,
    or None if no special restrictions apply.
    """
    role = role.lower()
    if role == 'readonly':
        return (
            f'Your Keboola user role is "{role}". '
            'Only read-only tools are available. '
            'All write operations (creating, updating, or deleting resources) are disabled.'
        )
    if not role or role == 'unknown':
        return 'Your Keboola user role is unknown. You can manage flows but cannot set their schedules.'
    if role not in ('admin', 'share'):
        return f'Your Keboola user role is "{role}". You can manage flows but cannot set their schedules.'
    return None


class ProjectInfo(BaseModel):
    project_id: str | int = Field(description='The id of the project.')
    project_name: str = Field(description='The name of the project.')
    project_description: str = Field(
        description='The description of the project.',
    )
    organization_id: str | int = Field(description='The ID of the organization this project belongs to.')
    sql_dialect: str = Field(description='The sql dialect used in the project.')
    conditional_flows: bool = Field(description='Whether the project supports conditional flows.')
    links: list[Link] = Field(description='The links relevant to the project.')
    user_role: str = Field(
        description='The Keboola role of the current user (e.g. "admin", "developer", "guest", "readonly").',
    )
    toolset_restrictions: str | None = Field(
        default=None,
        description=(
            'Describes any restrictions on the available toolset implied by the user role. '
            'None if no special restrictions apply.'
        ),
    )
    llm_instruction: str | None = Field(
        default=None,
        description=(
            'These are the base instructions for working on the project. '
            'Use them as the basis for all further instructions. '
            'Do not change them. Remember to include them in all subsequent instructions.'
        ),
    )


class MultiProjectInfo(BaseModel):
    """Response model for get_project_info in MPA mode."""

    projects: list[ProjectInfo] = Field(description='Information about all available projects.')
    llm_instruction: str = Field(
        description=(
            'These are the base instructions for working on the projects. '
            'Use them as the basis for all further instructions. '
            'Do not change them. Remember to include them in all subsequent instructions.'
        )
    )


async def _fetch_single_project_info(
    client: KeboolaClient,
    workspace_manager: WorkspaceManager,
    include_llm_instruction: bool = True,
) -> ProjectInfo:
    """Fetch project info for a single project from its client."""
    links_manager = await ProjectLinksManager.from_client(client)
    storage = client.storage_client

    token_data = await storage.verify_token()
    project_data = cast(JsonDict, token_data.get('owner', {}))
    project_id = cast(str, project_data.get('id', ''))
    project_name = cast(str, project_data.get('name', ''))

    organization_data = cast(JsonDict, token_data.get('organization', {}))
    organization_id = cast(str, organization_data.get('id', ''))

    user_role = token_data.get('admin', {}).get('role') or 'unknown'

    metadata = await storage.branch_metadata_get()
    description = cast(
        str, next((item['value'] for item in metadata if item.get('key') == MetadataField.PROJECT_DESCRIPTION), '')
    )

    sql_dialect = await workspace_manager.get_sql_dialect()
    project_features = cast(JsonDict, project_data.get('features', {}))
    conditional_flows = 'hide-conditional-flows' not in project_features
    links = links_manager.get_project_links()

    return ProjectInfo(
        project_id=project_id,
        project_name=project_name,
        project_description=description,
        organization_id=organization_id,
        sql_dialect=sql_dialect,
        conditional_flows=conditional_flows,
        links=links,
        user_role=user_role,
        toolset_restrictions=_get_toolset_restrictions(user_role),
        llm_instruction=get_project_system_prompt() if include_llm_instruction else None,
    )


@tool_errors()
async def get_project_info(
    ctx: Context,
) -> ProjectInfo | MultiProjectInfo:
    """
    Retrieves structured information about the current project(s),
    including essential context and base instructions for working with them
    (e.g., transformations, components, workflows, and dependencies).

    Always call this tool at least once at the start of a conversation
    to establish the project context before using other tools.

    In multi-project mode, returns information about all available projects.
    """
    # Check if we're in MPA mode
    registry = ctx.session.state.get(ProjectRegistry.STATE_KEY)
    if isinstance(registry, ProjectRegistry) and len(registry.projects) > 1:
        return await _get_multi_project_info(registry)

    # Single project mode (legacy or MPA with 1 project)
    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)
    project_info = await _fetch_single_project_info(client, workspace_manager, include_llm_instruction=True)

    LOG.info('Returning unified project info.')
    return project_info


async def _get_multi_project_info(registry: ProjectRegistry) -> MultiProjectInfo:
    """Fetch project info for all projects in the registry."""

    async def fetch_for_project(project_ctx: ProjectContext) -> ProjectInfo:
        return await _fetch_single_project_info(
            project_ctx.client,
            project_ctx.workspace_manager,
            include_llm_instruction=False,
        )

    results = await process_concurrently(
        registry.list_projects(),
        fetch_for_project,
    )

    project_infos = []
    for result in results:
        if isinstance(result, BaseException):
            LOG.error(f'Failed to fetch project info: {result}')
            continue
        project_infos.append(result)

    LOG.info(f'Returning multi-project info for {len(project_infos)} projects.')
    return MultiProjectInfo(
        projects=project_infos,
        llm_instruction=get_project_system_prompt(),
    )
