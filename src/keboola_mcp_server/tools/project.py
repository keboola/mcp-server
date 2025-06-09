import logging
from typing import Annotated

from fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.config import MetadataField
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, LinksManager
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
        description='The id of the project.',
        validation_alias=AliasChoices('id', 'project_id', 'projectId', 'project-id'),
    )
    project_name: str = Field(
        ...,
        description='The name of the project.',
        validation_alias=AliasChoices('name', 'project_name', 'projectName', 'project-name'),
    )
    project_description: str = Field(
        ...,
        description='The description of the project.',
        validation_alias=AliasChoices(
            'description', 'project_description', 'projectDescription', 'project-description'
        ),
    )
    organization_id: str | int = Field(
        ...,
        description='The ID of the organization this project belongs to.',
        validation_alias=AliasChoices('organization_id', 'organizationId', 'organization-id'),
    )
    sql_dialect: str = Field(
        ...,
        description='The sql dialect used in the project.',
        validation_alias=AliasChoices('sql_dialect', 'sqlDialect', 'sql-dialect'),
    )
    links: list[Link] = Field(..., description='The links relevant to the tool call.')


@tool_errors()
@with_session_state()
async def get_project_info(
    ctx: Context,
) -> Annotated[dict, Field(description='Structured project info including ID, name, description, and SQL dialect.')]:
    """Return structured project information pulled from multiple endpoints."""
    client = KeboolaClient.from_state(ctx.session.state)
    storage = client.storage_client

    token_data = await storage.verify_token()
    project_data = token_data.get('owner', {})
    organization_id = token_data.get('organization', {}).get('id', '')

    metadata = await storage.get('branch/default/metadata')
    description = next((item['value'] for item in metadata if item.get('key') == MetadataField.PROJECT_DESCRIPTION), '')

    sql_dialect = await WorkspaceManager.from_state(ctx.session.state).get_sql_dialect()
    base_url = storage.base_api_url
    project_id = project_data['id']

    links = LinksManager(base_url).get_project_links(project_id)

    combined = {
        **project_data,
        'organizationId': organization_id,
        'projectDescription': description,
        'sqlDialect': sql_dialect,
        'links': links,
    }

    validated = ProjectInfo.model_validate(combined)
    LOG.info('Returning unified project info.')
    return validated
