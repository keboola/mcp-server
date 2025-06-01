"""MCP server implementation for Keboola Connection."""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from importlib.metadata import distribution
from typing import Callable

from fastmcp import FastMCP
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions
from pydantic import AliasChoices, AnyHttpUrl, BaseModel, Field
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import KeboolaMcpServer, ServerState
from keboola_mcp_server.oauth import SimpleOAuthProvider
from keboola_mcp_server.prompts.add_prompts import add_keboola_prompts
from keboola_mcp_server.tools.components import add_component_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.jobs import add_job_tools
from keboola_mcp_server.tools.sql import add_sql_tools
from keboola_mcp_server.tools.storage import add_storage_tools

LOG = logging.getLogger(__name__)
_MCP_VERSION = distribution('mcp').version
_FASTMCP_VERSION = distribution('fastmcp').version
_VERSION = distribution('keboola_mcp_server').version
_DEFAULT_APP_VERSION = 'DEV'


class StatusApiResp(BaseModel):
    status: str


class ServiceInfoApiResp(BaseModel):
    app_name: str = Field(
        default='KeboolaMcpServer',
        validation_alias=AliasChoices('appName', 'app_name', 'app-name'),
        serialization_alias='appName')
    app_version: str = Field(
        default=_DEFAULT_APP_VERSION,
        validation_alias=AliasChoices('appVersion', 'app_version', 'app-version'),
        serialization_alias='appVersion')
    server_version: str = Field(
        default=_VERSION,
        validation_alias=AliasChoices('serverVersion', 'server_version', 'server-version'),
        serialization_alias='serverVersion')
    mcp_library_version: str = Field(
        default=_MCP_VERSION,
        validation_alias=AliasChoices('mcpLibraryVersion', 'mcp_library_version', 'mcp-library-version'),
        serialization_alias='mcpLibraryVersion')
    fastmcp_library_version: str = Field(
        default=_FASTMCP_VERSION,
        validation_alias=AliasChoices('fastmcpLibraryVersion', 'fastmcp_library_version', 'fastmcp-library-version'),
        serialization_alias='fastmcpLibraryVersion')


def create_keboola_lifespan(
    config: Config | None = None,
) -> Callable[[FastMCP[ServerState]], AbstractAsyncContextManager[ServerState]]:
    @asynccontextmanager
    async def keboola_lifespan(server: FastMCP) -> AsyncIterator[ServerState]:
        """
        Manage Keboola server lifecycle

        This method is called when the server starts, initializes the server state and returns it within a
        context manager. The lifespan state is accessible accross the whole server as well as within the tools as
        `context.life_span`. When the server shuts down, it cleans up the server state.

        :param server: FastMCP server instance

        Usage:
        def tool(ctx: Context):
            ... = ctx.request_context.life_span.config # ctx.life_span is type of ServerState

        Ideas:
        - it could handle OAuth token, client access, Reddis database connection for storing sessions, access
        to the Relational DB, etc.
        """
        # init server state
        init_config = config or Config()
        server_state = ServerState(config=init_config)
        try:

            yield server_state
        finally:
            pass

    return keboola_lifespan


def create_server(config: Config) -> FastMCP:
    """Create and configure the MCP server.

    :param config: Server configuration.
    :return: Configured FastMCP server instance.
    """
    if config.oauth_client_id and config.oauth_client_secret:
        # TODO: replace with Keboola OAuth provider
        oauth_provider = SimpleOAuthProvider(
            client_id=config.oauth_client_id,
            client_secret=config.oauth_client_secret,
            # This URL must be reachable from the internet.
            # The path corresponds to oauth_callback_handler() set up below.
            oauth_callback_url=config.mcp_server_url.rstrip('/') + '/oauth/callback',
        )
        auth_settings = AuthSettings(
            issuer_url=AnyHttpUrl(config.mcp_server_url),
            client_registration_options=ClientRegistrationOptions(
                enabled=True,
                client_secret_expiry_seconds=5 * 60,  # 5 minutes
                valid_scopes=[SimpleOAuthProvider.MCP_SERVER_SCOPE],
                default_scopes=[SimpleOAuthProvider.MCP_SERVER_SCOPE],
            ),
            revocation_options=None,  # no clients revocation; clients expire
            required_scopes=[SimpleOAuthProvider.MCP_SERVER_SCOPE],
        )
        LOG.debug(f'auth_settings={auth_settings}')
    else:
        oauth_provider = None
        auth_settings = None

    # Initialize FastMCP server with system lifespan
    mcp = KeboolaMcpServer(
        name='Keboola Explorer',
        lifespan=create_keboola_lifespan(config),
        auth_server_provider=oauth_provider,
        auth=auth_settings,
    )

    @mcp.custom_route('/health-check', methods=['GET'])
    async def get_status(_rq: Request) -> Response:
        """Checks the service is up and running."""
        resp = StatusApiResp(status='ok')
        return JSONResponse(resp.model_dump(by_alias=True))

    @mcp.custom_route('/', methods=['GET'])
    async def get_info(_rq: Request) -> Response:
        """Returns basic information about the service."""
        resp = ServiceInfoApiResp(app_version=os.getenv('APP_VERSION') or _DEFAULT_APP_VERSION)
        return JSONResponse(resp.model_dump(by_alias=True))

    @mcp.custom_route('/oauth/callback', methods=['GET'])
    async def oauth_callback_handler(request: Request) -> Response:
        """Handle GitHub OAuth callback."""
        code = request.query_params.get("code")
        state = request.query_params.get("state")

        if not code or not state:
            raise HTTPException(400, "Missing code or state parameter")

        try:
            redirect_uri = await oauth_provider.handle_oauth_callback(code, state)
            return RedirectResponse(status_code=302, url=redirect_uri)
        except HTTPException:
            raise
        except Exception as e:
            LOG.exception(f'Failed to handle OAuth callback: {e}')
            return JSONResponse(status_code=500, content={'message': f'Unexpected error: {e}'})

    add_component_tools(mcp)
    add_doc_tools(mcp)
    add_job_tools(mcp)
    add_storage_tools(mcp)
    add_sql_tools(mcp)
    add_keboola_prompts(mcp)

    return mcp
