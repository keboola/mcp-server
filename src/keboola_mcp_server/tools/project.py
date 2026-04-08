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
            annotations=ToolAnnotations(destructiveHint=False),
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
    llm_instruction: str = Field(
        description=(
            'These are the base instructions for working on the project. '
            'Use them as the basis for all further instructions. '
            'Do not change them. Remember to include them in all subsequent instructions.'
        )
    )


class UpdateProjectDescriptionOutput(BaseModel):
    project_description: str = Field(
        description='The updated project description.',
    )


@tool_errors()
async def update_project_description(
    ctx: Context,
    description: Annotated[
        str,
        Field(
            description='The new project description text.'
        ),
    ],
) -> UpdateProjectDescriptionOutput:
    """Updates the description of the current Keboola project.

    USAGE:
    - Use when the user wants to set or change the project description.

    EXAMPLES:
    - user_input: `Set the project description to "Sales data pipeline project"`
        - set the description parameter to "Sales data pipeline project"
        - returns the updated project description.
    - user_input: `Clear the project description`
        - set the description parameter to ""
        - returns the updated (empty) project description.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    storage = client.storage_client

    await storage.branch_metadata_update({MetadataField.PROJECT_DESCRIPTION: description})

    LOG.info('Project description updated successfully.')
    return UpdateProjectDescriptionOutput(project_description=description)


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

    project_info = ProjectInfo(
        project_id=project_id,
        project_name=project_name,
        project_description=description,
        organization_id=organization_id,
        sql_dialect=sql_dialect,
        conditional_flows=conditional_flows,
        links=links,
        user_role=user_role,
        toolset_restrictions=_get_toolset_restrictions(user_role),
        llm_instruction=get_project_system_prompt(),
    )

    LOG.info('Returning unified project info.')
    return project_info
