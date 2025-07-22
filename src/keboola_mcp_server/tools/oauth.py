"""OAuth URL generation tools for the MCP server."""

import logging
from typing import Annotated

from fastmcp import Context
from fastmcp.tools import FunctionTool
from pydantic import Field

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer, with_session_state

LOG = logging.getLogger(__name__)

TOOL_GROUP_NAME = 'OAUTH'


def add_oauth_tools(mcp: KeboolaMcpServer) -> None:
    """Adds OAuth tools to the MCP server."""
    mcp.add_tool(FunctionTool.from_function(create_oauth_url))
    LOG.info('OAuth tools added to the MCP server.')


@tool_errors()
@with_session_state()
async def create_oauth_url(
    component_id: Annotated[
        str, Field(description='The component ID to grant access to (e.g., "keboola.ex-google-analytics-v4").')
    ],
    config_id: Annotated[str, Field(description='The configuration ID for the component.')],
    ctx: Context,
) -> str:
    """
    Generates an OAuth authorization URL for a Keboola component configuration.

    When using this tool, be very concise in your response. Just guide the user to click the
    authorization link.

    Note that this tool should be called specifically for these OAuth-requiring components after their
    configuration is created e.g. keboola.ex-google-analytics-v4 and keboola.ex-gmail.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    # Create short-lived SAPI token
    token_data = {
        'description': f'Short-lived token for OAuth URL - {component_id}/{config_id}',
        'componentAccess': [component_id],
        'expiresIn': 3600,  # 1 hour expiration
    }

    # Create the token using the storage client
    token_response = await client.storage_client.post(endpoint='tokens', data=token_data)

    # Extract the token from response
    sapi_token = token_response['token']

    # Get the storage API URL from client
    storage_api_url = client.storage_client.base_api_url

    # Generate OAuth URL
    oauth_url = (
        f'https://external.keboola.com/oauth/index.html?token={sapi_token}'
        f'&sapiUrl={storage_api_url}#/{component_id}/{config_id}'
    )

    return oauth_url
