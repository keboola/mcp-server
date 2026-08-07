"""MCP server implementation for Keboola Connection."""

import dataclasses
import logging
import os
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Literal

from fastmcp import FastMCP
from fastmcp.server.middleware.logging import LoggingMiddleware
from pydantic import AliasChoices, BaseModel, Field
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response

from keboola_mcp_server.authorization import ToolAuthorizationMiddleware
from keboola_mcp_server.config import Config, ServerRuntimeInfo, Transport, get_env_storage_api_url
from keboola_mcp_server.errors import ValidationErrorMiddleware
from keboola_mcp_server.mcp import KeboolaMcpServer, ServerState, SessionStateMiddleware, ToolsFilteringMiddleware
from keboola_mcp_server.multiproject import MultiProjectMiddleware
from keboola_mcp_server.oauth import SimpleOAuthProvider
from keboola_mcp_server.preview import preview_config_diff
from keboola_mcp_server.prompts.add_prompts import add_keboola_prompts
from keboola_mcp_server.session_store.crypto import resolve_encryption_key
from keboola_mcp_server.session_store.kai_scope import PostgresKaiScopeStore
from keboola_mcp_server.session_store.repository import PostgresSessionStore
from keboola_mcp_server.tools.components.tools import add_component_tools
from keboola_mcp_server.tools.data_apps import add_data_app_tools
from keboola_mcp_server.tools.doc import add_doc_tools
from keboola_mcp_server.tools.flow.tools import add_flow_tools
from keboola_mcp_server.tools.jobs import add_job_tools
from keboola_mcp_server.tools.oauth import add_oauth_tools
from keboola_mcp_server.tools.project import add_project_tools
from keboola_mcp_server.tools.search import add_search_tools
from keboola_mcp_server.tools.semantic import add_semantic_tools
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
    server_id: str = Field(
        validation_alias=AliasChoices('serverId', 'server_id', 'server-id'),
        serialization_alias='serverId',
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
            server_id=self.server_state.runtime_info.server_id,
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
            LOG.exception('Failed to handle OAuth callback')
            return JSONResponse(status_code=500, content={'message': f'Unexpected error: {e}'})

    def add_to_mcp(self, mcp: FastMCP) -> None:
        """Add custom routes to an MCP server.

        :param mcp: MCP server instance.
        """
        mcp.custom_route('/', methods=['GET'])(self.get_info)
        mcp.custom_route('/health-check', methods=['GET'])(self.get_status)
        mcp.custom_route('/preview/configuration', methods=['POST'])(preview_config_diff)
        if self.oauth_provider:
            mcp.custom_route('/oauth/callback', methods=['GET'])(self.oauth_callback_handler)

    def add_to_starlette(self, app: Starlette) -> None:
        """Add custom routes to a Starlette app.

        :param app: Starlette app instance.
        """
        app.state.server_state = self.server_state
        app.add_route('/', self.get_info, methods=['GET'])
        app.add_route('/health-check', self.get_status, methods=['GET'])
        app.add_route('/preview/configuration', preview_config_diff, methods=['POST'])
        if self.oauth_provider:
            app.add_route('/oauth/callback', self.oauth_callback_handler, methods=['GET'])
            for route in self.oauth_provider.get_routes():
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
    # This is where the server's own stack is resolved, once: the '--api-url' CLI parameter (already
    # in `config`) wins over the environment. The resulting `config.storage_api_url` is the single
    # value that every "is this my stack?" check uses afterwards — the per-request Storage API URL
    # pinning and the Kubernetes ServiceAccount step-up alike (`ServerState.own_stack_storage_api_url`).
    if not config.storage_api_url and (env_storage_api_url := get_env_storage_api_url()):
        config = dataclasses.replace(config, storage_api_url=env_storage_api_url)

    if config.oauth_client_id and config.oauth_client_secret:
        # fall back to HOSTNAME_SUFFIX if no URLs are specified for the OAUth server or the MCP server itself
        if not config.oauth_server_url and hostname_suffix:
            config = dataclasses.replace(config, oauth_server_url=f'https://connection.{hostname_suffix}')
        if not config.mcp_server_url and hostname_suffix:
            config = dataclasses.replace(config, mcp_server_url=f'https://mcp.{hostname_suffix}')
        if not config.oauth_scope:
            config = dataclasses.replace(config, oauth_scope='email')

        # OAuth sessions (the real Keboola access/refresh tokens) live in Postgres, not in a
        # self-contained JWT (oauth_session_persistence RFC) -- revocation and server-managed
        # refresh both need a durable, deletable row. No silent in-memory fallback for this
        # production auth path: refuse to start rather than accept OAuth logins nothing can revoke.
        if not config.postgres_dsn:
            raise RuntimeError(
                'OAuth is configured (oauth_client_id/oauth_client_secret) but no Postgres DSN is set. '
                'Set MCP_DB_URL (or KBC_POSTGRES_DSN) so OAuth sessions can be stored.'
            )
        # Without an explicit key, resolve_encryption_key() falls back to a process-local one --
        # fine for local dev/tests, but in production it would silently make persisted sessions
        # undecryptable after every restart (same "refuse to start" reasoning as the DSN check above).
        if not config.session_encryption_key:
            raise RuntimeError(
                'OAuth is configured (oauth_client_id/oauth_client_secret) but no session encryption key is '
                'set. Set KBC_SESSION_ENCRYPTION_KEY so persisted OAuth sessions survive a process restart.'
            )
        session_store = PostgresSessionStore(
            config.postgres_dsn, encryption_key=resolve_encryption_key(config.session_encryption_key)
        )

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
            session_store=session_store,
        )
    else:
        oauth_provider = None
        session_store = None

    # Kai session-scope persistence (pat_token_support/RFC.md, increment 6) needs only a Postgres
    # DSN -- unlike OAuth sessions it stores no credential material, so no encryption key or
    # oauth_client_id/secret is required. Independent of whether OAuth is configured above.
    kai_scope_store = PostgresKaiScopeStore(config.postgres_dsn) if config.postgres_dsn else None

    # Initialize FastMCP server with system lifespan
    LOG.info(f'Creating server with config: {config}')
    server_state = ServerState(
        config=config, runtime_info=runtime_info, session_store=session_store, kai_scope_store=kai_scope_store
    )
    mcp = KeboolaMcpServer(
        name='Keboola MCP Server',
        instructions=(
            'This server supports multi-project mode for stack-wide Keboola programmatic tokens '
            '(kbc_at_/kbc_pat_). When the session uses such a token, data tools are BLOCKED until a '
            'project scope is confirmed. So at the very START of the conversation, before doing anything '
            'else: call "get_accessible_projects", show the user their projects, and ASK whether to work '
            'across ALL of them or a subset. Do not decide for them. Then call "set_project_scope" with '
            'their answer (no arguments = all projects, or the chosen project ids, optionally '
            'read_only=true). Both tools return a "scope_token" -- the server does not remember the '
            'scope between calls, so resend that value as the "scope_token" argument on every '
            'subsequent tool call in this conversation. After that, read-only tools return results per '
            'project. Never write to more than one project without explicit user confirmation — write '
            'operations target the active (first-scoped) project only. If instead the session uses a '
            'legacy project-scoped Storage API token, it is already bound to a single project: use the '
            'tools directly — "get_accessible_projects" / "set_project_scope" do not apply (they will '
            'report that no programmatic token is present). Note: outside the Storage API, some tools '
            'may need per-project token support not yet available on every stack; surface such errors '
            'plainly rather than retrying.'
        ),
        lifespan=create_keboola_lifespan(server_state),
        auth=oauth_provider,
        middleware=[
            LoggingMiddleware(log_level=logging.DEBUG),
            SessionStateMiddleware(),
            ToolAuthorizationMiddleware(),
            # MultiProjectMiddleware must wrap ToolsFilteringMiddleware (run first in this list =
            # outer), not the reverse: it swaps the active KeboolaClient per project during fan-out,
            # and ToolsFilteringMiddleware's per-project feature/role/branch checks must be
            # re-evaluated against each swapped client — not just once against the pre-fan-out client.
            MultiProjectMiddleware(),
            ToolsFilteringMiddleware(),
            ValidationErrorMiddleware(),
        ],
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
    add_semantic_tools(mcp)
    add_sql_tools(mcp)
    add_storage_tools(mcp)
    add_keboola_prompts(mcp)

    if custom_routes_handling != 'return':
        return mcp
    else:
        return mcp, custom_routes
