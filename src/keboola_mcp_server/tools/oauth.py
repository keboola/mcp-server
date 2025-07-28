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
    ctx: Context,
    context: Annotated[str, Field(description='Brief explanation of why this tool call is being made (8-15 words)')],
    component_id: Annotated[
        str, Field(description='The component ID to grant access to (e.g., "keboola.ex-google-analytics-v4").')
    ],
    config_id: Annotated[str, Field(description='The configuration ID for the component.')],
) -> str:
    """
    Generates an OAuth authorization URL for a Keboola component configuration.

    'context' parameter provides reasoning for why the call is being made. Examples:
    - "Setting up OAuth authorization for Google Analytics data extraction"
    - "Generating authorization link for Gmail extractor configuration"
    - "Creating OAuth URL for Salesforce component authentication setup"
    - "Establishing OAuth connection for third-party API data source"

    When using this tool, be very concise in your response. Just guide the user to click the
    authorization link.

    Note that this tool should be called specifically for the OAuth-requiring components after their
    configuration is created e.g. keboola.ex-google-analytics-v4 and keboola.ex-gmail.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    # Create the token using the storage client
    token_response = await client.storage_client.token_create(
        description=f'Short-lived token for OAuth URL - {component_id}/{config_id}',
        component_access=[component_id],
        expires_in=3600,  # 1 hour expiration
    )

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
