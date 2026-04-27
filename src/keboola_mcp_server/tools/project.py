import logging
from typing import Annotated, cast

from fastmcp import Context, FastMCP
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
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

    LOG.info(f'Adding tool {update_project_description.__name__} to the MCP server.')
    mcp.add_tool(
        FunctionTool.from_function(
            update_project_description,
            annotations=ToolAnnotations(destructiveHint=True),
            tags={PROJECT_TOOLS_TAG},
        )
    )

    LOG.info('Project tools initialized.')


async def _resolve_branch_context(client: KeboolaClient) -> tuple[str | int, str, bool]:
    """
    Resolves the current branch's id, name, and dev-branch flag from the storage API.

    `client.branch_id` is None on the default/production branch (normalized by
    `KeboolaClient.with_branch_id`), so we look up the branches list and pick either
    the entry matching the client's branch_id or the one with `isDefault=True`.
    """
    target_branch_id = client.branch_id
    branches = await client.storage_client.branches_list()

    selected: JsonDict | None = None
    for branch in branches:
        if target_branch_id is None:
            if branch.get('isDefault') is True:
                selected = branch
                break
        else:
            if str(branch.get('id')) == str(target_branch_id):
                selected = branch
                break

    if selected is None:
        # Should not happen in a healthy project, but stay defensive.
        return target_branch_id or 'default', 'unknown', target_branch_id is not None

    branch_id = cast('str | int', selected.get('id', target_branch_id or 'default'))
    branch_name = cast(str, selected.get('name', 'unknown'))
    is_development_branch = selected.get('isDefault') is not True
    return branch_id, branch_name, is_development_branch


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
    branch_id: str | int = Field(
        description='The ID of the branch this call is operating on (default/production or a development branch).'
    )
    branch_name: str = Field(description='The name of the branch this call is operating on.')
    is_development_branch: bool = Field(
        description=(
            'True if this call is operating on a development branch, False if on the default/production branch. '
            'Use this to apply branch-specific guidance (e.g., FQN handling in transformations, '
            'unsupported tools in development branches).'
        )
    )
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
    llm_instruction: str = Field(
        description=(
            'These are the base instructions for working on the project. '
            'Use them as the basis for all further instructions. '
            'Do not change them. Remember to include them in all subsequent instructions.'
        )
    )


@tool_errors()
async def update_project_description(
    ctx: Context,
    description: Annotated[
        str,
        Field(description='The new project description text.'),
    ],
) -> None:
    """Updates the description of the current Keboola project."""
    client = KeboolaClient.from_state(ctx.session.state)
    storage = client.storage_client

    await storage.branch_metadata_update({MetadataField.PROJECT_DESCRIPTION: description})

    LOG.info('Project description updated successfully.')


@tool_errors()
async def get_project_info(
    ctx: Context,
) -> ProjectInfo:
    """
    Retrieves structured information about the current project,
    including essential context and base instructions for working with it
    (e.g., transformations, components, workflows, and dependencies).

    Always call this tool at least once at the start of a conversation
    to establish the project context before using other tools.
    """
    client = KeboolaClient.from_state(ctx.session.state)
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

    sql_dialect = await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()
    project_features = cast(JsonDict, project_data.get('features', {}))
    conditional_flows = 'hide-conditional-flows' not in project_features
    links = links_manager.get_project_links()

    branch_id, branch_name, is_development_branch = await _resolve_branch_context(client)

    project_info = ProjectInfo(
        project_id=project_id,
        project_name=project_name,
        project_description=description,
        organization_id=organization_id,
        sql_dialect=sql_dialect,
        conditional_flows=conditional_flows,
        links=links,
        branch_id=branch_id,
        branch_name=branch_name,
        is_development_branch=is_development_branch,
        user_role=user_role,
        toolset_restrictions=_get_toolset_restrictions(user_role),
        llm_instruction=get_project_system_prompt(sql_dialect),
    )

    LOG.info('Returning unified project info.')
    return project_info
