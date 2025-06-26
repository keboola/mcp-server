"""MCP server implementation for Keboola Connection."""
import dataclasses
import logging
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from importlib.metadata import distribution
from typing import Callable

from fastmcp import FastMCP
from pydantic import AliasChoices, BaseModel, Field
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import KeboolaMcpServer, ServerState
from keboola_mcp_server.oauth import SimpleOAuthProvider
from keboola_mcp_server.prompts.add_prompts import add_keboola_prompts
from keboola_mcp_server.tools.components import add_component_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.flow.tools import add_flow_tools
from keboola_mcp_server.tools.jobs import add_job_tools
from keboola_mcp_server.tools.project import add_project_tools
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
    config = config.replace_by(os.environ)

    hostname_suffix = os.environ.get('HOSTNAME_SUFFIX')
    if not config.storage_api_url and hostname_suffix:
        config = dataclasses.replace(config, storage_api_url=f'https://connection.{hostname_suffix}')

    if config.oauth_client_id and config.oauth_client_secret:
        # fall back to HOSTNAME_SUFFIX if no URLs are specified for the OAUth server or the MCP server itself
        if not config.oauth_server_url and hostname_suffix:
            config = dataclasses.replace(config, oauth_server_url=f'https://connection.{hostname_suffix}')
        if not config.mcp_server_url and hostname_suffix:
            config = dataclasses.replace(config, mcp_server_url=f'https://mcp.{hostname_suffix}')
        if not config.oauth_scope:
            config = dataclasses.replace(config, oauth_scope='email')

        oauth_provider = SimpleOAuthProvider(
            storage_api_url=config.storage_api_url,
            client_id=config.oauth_client_id,
            client_secret=config.oauth_client_secret,
            server_url=config.oauth_server_url,
            scope=config.oauth_scope,
            # This URL must be reachable from the internet.
            mcp_server_url=config.mcp_server_url,
            # The path corresponds to oauth_callback_handler() set up below.
            callback_endpoint='/oauth/callback',
            jwt_secret=config.jwt_secret,
        )
    else:
        oauth_provider = None

    # Initialize FastMCP server with system lifespan
    LOG.info(f'Creating server with config: {config}')
    mcp = KeboolaMcpServer(
        name='Keboola Explorer',
        lifespan=create_keboola_lifespan(config),
        auth=oauth_provider,
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
        code = request.query_params.get('code')
        state = request.query_params.get('state')

        if not code or not state:
            raise HTTPException(400, 'Missing code or state parameter')

        try:
            assert oauth_provider  # this must have been set if we are handling OAuth callbacks
            redirect_uri = await oauth_provider.handle_oauth_callback(code, state)
            return RedirectResponse(status_code=302, url=redirect_uri)
        except HTTPException:
            raise
        except Exception as e:
            LOG.exception(f'Failed to handle OAuth callback: {e}')
            return JSONResponse(status_code=500, content={'message': f'Unexpected error: {e}'})

    add_component_tools(mcp)
    add_doc_tools(mcp)
    add_flow_tools(mcp)
    add_job_tools(mcp)
    add_project_tools(mcp)
    add_sql_tools(mcp)
    add_storage_tools(mcp)
    add_keboola_prompts(mcp)

    return mcp
