import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import with_session_state
from keboola_mcp_server.tools.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)


def add_project_tools(mcp: FastMCP) -> None:
    """Add project tools to the MCP server."""
    project_tools = [get_project_info]

    for tool in project_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(tool)

    LOG.info('Project tools initialized.')


class ProjectInfo(BaseModel):
    project_id: str | int = Field(
        ...,
        description='The id of the project.'
    )
    project_name: str = Field(
        ...,
        description='The name of the project.'
    )
    project_description: str = Field(
        ...,
        description='The description of the project.',
    )
    organization_id: str | int = Field(
        ...,
        description='The ID of the organization this project belongs to.'
    )
    sql_dialect: str = Field(
        ...,
        description='The sql dialect used in the project.'
    )
    links: list[Link] = Field(..., description='The links relevant to the project.')


@tool_errors()
@with_session_state()
async def get_project_info(
    ctx: Context,
) -> Annotated[ProjectInfo, Field(description='Structured project info.')]:
    """Return structured project information pulled from multiple endpoints."""
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    storage = client.storage_client

    token_data = await storage.verify_token()
    project_data = token_data.get('owner', {})
    organization_id = token_data.get('organization', {}).get('id', '')

    metadata = await storage.get('branch/default/metadata')
    description = next((item['value'] for item in metadata if item.get('key') == MetadataField.PROJECT_DESCRIPTION), '')

    sql_dialect = await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()
    links = links_manager.get_project_links()

    project_info = ProjectInfo(project_id=project_data['id'],
                               project_name=project_data['name'],
                               project_description=description,
                               organization_id=organization_id,
                               sql_dialect=sql_dialect,
                               links=links)
    LOG.info('Returning unified project info.')
    return project_info
