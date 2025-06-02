"""MCP server implementation for Keboola Connection."""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from importlib.metadata import distribution
from typing import Callable, Optional

from fastmcp import FastMCP
from pydantic import AliasChoices, BaseModel, Field
from starlette.requests import Request
from starlette.responses import Response

from keboola_mcp_server.config import Config
from keboola_mcp_server.mcp import KeboolaMcpServer, ServerState
from keboola_mcp_server.prompts.add_prompts import add_keboola_prompts
from keboola_mcp_server.tools.components import add_component_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.flow import add_flow_tools
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


def create_server(config: Optional[Config] = None) -> FastMCP:
    """Create and configure the MCP server.

    Args:
        config: Server configuration. If None, loads from environment.

    Returns:
        Configured FastMCP server instance
    """
    # Initialize FastMCP server with system lifespan
    mcp = KeboolaMcpServer(name='Keboola Explorer', lifespan=create_keboola_lifespan(config))

    @mcp.custom_route('/health-check', methods=['GET'])
    async def get_status(_rq: Request) -> Response:
        """Checks the service is up and running."""
        resp = StatusApiResp(status='ok')
        return Response(resp.model_dump_json(by_alias=True), media_type='application/json')

    @mcp.custom_route('/', methods=['GET'])
    async def get_info(_rq: Request) -> Response:
        """Returns basic information about the service."""
        resp = ServiceInfoApiResp(app_version=os.getenv('APP_VERSION') or _DEFAULT_APP_VERSION)
        return Response(resp.model_dump_json(by_alias=True), media_type='application/json')

    add_component_tools(mcp)
    add_doc_tools(mcp)
    add_job_tools(mcp)
    add_storage_tools(mcp)
    add_sql_tools(mcp)
    add_flow_tools(mcp)
    add_keboola_prompts(mcp)

    return mcp
