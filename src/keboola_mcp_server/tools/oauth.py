"""OAuth URL generation tools for the MCP server."""

import logging
from typing import Annotated
from urllib.parse import urlencode, urlunsplit

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import Field

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer

LOG = logging.getLogger(__name__)

OAUTH_TOOLS_TAG = 'oauth'


def add_oauth_tools(mcp: KeboolaMcpServer) -> None:
    """Adds OAuth tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            create_oauth_url,
            annotations=ToolAnnotations(destructiveHint=True),
            tags={OAUTH_TOOLS_TAG},
        )
    )
    LOG.info('OAuth tools added to the MCP server.')


@tool_errors()
async def create_oauth_url(
    component_id: Annotated[
        str, Field(description='The component ID to grant access to (e.g., "keboola.ex-google-analytics-v4").')
    ],
    config_id: Annotated[str, Field(description='The configuration ID for the component.')],
    ctx: Context,
) -> Annotated[str, Field(description='The OAuth authorization URL.')]:
    """
    Generates an OAuth authorization URL for a Keboola component configuration.

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

    # Generate OAuth URL
    query_params = urlencode({'token': sapi_token, 'sapiUrl': client.storage_api_url})
    fragment = f'/{component_id}/{config_id}'

    oauth_url = urlunsplit(
        (
            'https',  # scheme
            'external.keboola.com',  # netloc
            '/oauth/index.html',  # path
            query_params,  # query
            fragment,  # fragment
        )
    )

    return oauth_url
