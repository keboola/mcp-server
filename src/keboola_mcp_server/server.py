"""MCP server implementation for Keboola Connection."""

import dataclasses
import logging
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Callable, Literal

from fastmcp import FastMCP
from mcp.server.auth.routes import create_auth_routes
from pydantic import AliasChoices, BaseModel, Field
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from keboola_mcp_server.config import Config, ServerRuntimeInfo, Transport
from keboola_mcp_server.mcp import KeboolaMcpServer, ServerState, SessionStateMiddleware, ToolsFilteringMiddleware
from keboola_mcp_server.oauth import SimpleOAuthProvider
from keboola_mcp_server.prompts.add_prompts import add_keboola_prompts
from keboola_mcp_server.tools.components import add_component_tools
from keboola_mcp_server.tools.data_apps import add_data_app_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.flow.tools import add_flow_tools
from keboola_mcp_server.tools.jobs import add_job_tools
from keboola_mcp_server.tools.oauth import add_oauth_tools
from keboola_mcp_server.tools.project import add_project_tools
from keboola_mcp_server.tools.search import add_search_tools
from keboola_mcp_server.tools.sql import add_sql_tools
from keboola_mcp_server.tools.storage import add_storage_tools

LOG = logging.getLogger(__name__)


class StatusApiResp(BaseModel):
    status: str


class ServiceInfoApiResp(BaseModel):
    app_name: str = Field(
        default='KeboolaMcpServer',
        validation_alias=AliasChoices('appName', 'app_name', 'app-name'),
        serialization_alias='appName',
    )
    app_version: str = Field(
        validation_alias=AliasChoices('appVersion', 'app_version', 'app-version'), serialization_alias='appVersion'
    )
    server_version: str = Field(
        validation_alias=AliasChoices('serverVersion', 'server_version', 'server-version'),
        serialization_alias='serverVersion',
    )
    mcp_library_version: str = Field(
        validation_alias=AliasChoices('mcpLibraryVersion', 'mcp_library_version', 'mcp-library-version'),
        serialization_alias='mcpLibraryVersion',
    )
    fastmcp_library_version: str = Field(
        validation_alias=AliasChoices('fastmcpLibraryVersion', 'fastmcp_library_version', 'fastmcp-library-version'),
        serialization_alias='fastmcpLibraryVersion',
    )
    server_transport: Transport | None = Field(
        validation_alias=AliasChoices('serverTransport', 'server_transport', 'server-transport'),
        serialization_alias='serverTransport',
        default=None,
    )


def create_keboola_lifespan(
    server_state: ServerState,
) -> Callable[[FastMCP[ServerState]], AbstractAsyncContextManager[ServerState]]:
    @asynccontextmanager
    async def keboola_lifespan(server: FastMCP) -> AsyncIterator[ServerState]:
        """
        Manage Keboola server lifecycle

        This method is called when the server starts, initializes the server state and returns it within a
        context manager. The lifespan state is accessible across the whole server as well as within the tools as
        `context.life_span`. When the server shuts down, it cleans up the server state.

        :param server: FastMCP server instance

        Usage:
        def tool(ctx: Context):
            ... = ctx.request_context.life_span.config # ctx.life_span is type of ServerState

        Ideas:
        - it could handle OAuth token, client access, Redis database connection for storing sessions, access
        to the Relational DB, etc.
        """
        yield server_state

    return keboola_lifespan


class CustomRoutes:
    """Routes which are not part of the MCP protocol."""

    def __init__(self, server_state: ServerState, oauth_provider: SimpleOAuthProvider | None = None) -> None:
        self.server_state = server_state
        self.oauth_provider = oauth_provider

    async def get_status(self, _rq: Request) -> Response:
        """Checks the service is up and running."""
        resp = StatusApiResp(status='ok')
        return JSONResponse(resp.model_dump(by_alias=True))

    async def get_info(self, _rq: Request) -> Response:
        """Returns basic information about the service."""
        resp = ServiceInfoApiResp(
            app_version=self.server_state.runtime_info.app_version,
            server_version=self.server_state.runtime_info.server_version,
            mcp_library_version=self.server_state.runtime_info.mcp_library_version,
            fastmcp_library_version=self.server_state.runtime_info.fastmcp_library_version,
            server_transport=self.server_state.runtime_info.transport,
        )
        return JSONResponse(resp.model_dump(by_alias=True))

    async def oauth_callback_handler(self, request: Request) -> Response:
        """Handle GitHub OAuth callback."""
        code = request.query_params.get('code')
        state = request.query_params.get('state')

        if not code or not state:
            raise HTTPException(400, 'Missing code or state parameter')

        try:
            assert self.oauth_provider  # this must have been set if we are handling OAuth callbacks
            redirect_uri = await self.oauth_provider.handle_oauth_callback(code, state)
            return RedirectResponse(status_code=302, url=redirect_uri)
        except HTTPException:
            raise
        except Exception as e:
            LOG.exception(f'Failed to handle OAuth callback: {e}')
            return JSONResponse(status_code=500, content={'message': f'Unexpected error: {e}'})

    def add_to_mcp(self, mcp: FastMCP) -> None:
        """Add custom routes to an MCP server.

        :param mcp: MCP server instance.
        """
        mcp.custom_route('/', methods=['GET'])(self.get_info)
        mcp.custom_route('/health-check', methods=['GET'])(self.get_status)
        if self.oauth_provider:
            mcp.custom_route('/oauth/callback', methods=['GET'])(self.oauth_callback_handler)

    def add_to_starlette(self, app: Starlette) -> None:
        """Add custom routes to a Starlette app.

        :param app: Starlette app instance.
        """
        app.add_route('/', self.get_info, methods=['GET'])
        app.add_route('/health-check', self.get_status, methods=['GET'])
        if self.oauth_provider:
            app.add_route('/oauth/callback', self.oauth_callback_handler, methods=['GET'])
            auth_routes = create_auth_routes(
                self.oauth_provider,
                self.oauth_provider.issuer_url,
                self.oauth_provider.service_documentation_url,
                self.oauth_provider.client_registration_options,
                self.oauth_provider.revocation_options,
            )
            for route in auth_routes:
                app.add_route(route.path, route.endpoint, methods=route.methods)


def create_server(
    config: Config,
    *,
    runtime_info: ServerRuntimeInfo,
    custom_routes_handling: Literal['add', 'return'] | None = 'add',
) -> FastMCP | tuple[FastMCP, CustomRoutes]:
    """Create and configure the MCP server.

    :param config: Server configuration.
    :param runtime_info: Server runtime information holding the server versions, transport, etc.
    :param custom_routes_handling: Add custom routes (health check etc.) to the server. If 'add',
        the routes are added to the MCP server instance. If 'return', the routes are returned as a CustomRoutes
        instance. If None, no custom routes are added. The 'return' mode is a workaround for the 'http-compat'
        mode, where we need to add the custom routes to the parent app.
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
    server_state = ServerState(config=config, runtime_info=runtime_info)
    mcp = KeboolaMcpServer(
        name='Keboola MCP Server',
        lifespan=create_keboola_lifespan(server_state),
        auth=oauth_provider,
        middleware=[SessionStateMiddleware(), ToolsFilteringMiddleware()],
    )

    if custom_routes_handling:
        custom_routes = CustomRoutes(server_state=server_state, oauth_provider=oauth_provider)
        if custom_routes_handling == 'add':
            custom_routes.add_to_mcp(mcp)

    add_component_tools(mcp)
    add_data_app_tools(mcp)
    add_doc_tools(mcp)
    add_flow_tools(mcp)
    add_job_tools(mcp)
    add_oauth_tools(mcp)
    add_project_tools(mcp)
    add_search_tools(mcp)
    add_sql_tools(mcp)
    add_storage_tools(mcp)
    add_keboola_prompts(mcp)

    if custom_routes_handling != 'return':
        return mcp
    else:
        return mcp, custom_routes
