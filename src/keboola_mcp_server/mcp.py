"""
This module overrides FastMCP.add_tool() to improve conversion of tool function docstrings
into tool descriptions.
It also provides a decorator that MCP tool functions can use to inject session state into their Context parameter
and other utilities for the MCP server.
"""

import asyncio
import dataclasses
import logging
import os
import textwrap
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, TypeVar
from unittest.mock import MagicMock

import toon_format
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import Tool
from fastmcp.tools.tool import ToolResult
from mcp import types as mt
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from pydantic import BaseModel
from pydantic_core import to_json
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.auth_login import exchange_scoped_token, get_access_token, introspect_token, load_tokens
from keboola_mcp_server.clients.auth_bridge import StorageTokenResolver, is_programmatic_token
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.oauth import ProxyAccessToken
from keboola_mcp_server.tools.constants import MODIFY_FLOW_TOOL_NAME, SEMANTIC_TOOLS_TAG, UPDATE_FLOW_TOOL_NAME
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)
CONVERSATION_ID = 'conversation_id'
SCOPE_KEY = 'project_scope'

# Tools that must not be fanned out across multiple projects, even when a multi-project scope is
# active and they are read-only: the scope/auth tools operate on the whole-stack token (not a single
# project), and get_project_info resolves through the active project's WorkspaceManager (workspace id
# / sql dialect), so it reports the active project only.
# query_data is intentionally NOT here: the fan-out swaps a per-project WorkspaceManager (see
# MultiProjectMiddleware._swap_project) so a query runs against the workspace of each targeted
# project — narrow to one with the project_ids filter, or run across all scoped projects.
_NO_FANOUT_TOOLS = {'get_accessible_projects', 'set_project_scope', 'get_project_info'}

# Tools allowed before the user has confirmed a project scope. Everything else is blocked with a
# message telling the assistant to ask the user which projects to work on first (ask-first UX).
_BOOTSTRAP_TOOLS = {'get_accessible_projects', 'set_project_scope'}

# Optional per-call argument injected on fan-out-eligible read tools to restrict a single call to a
# subset of the scoped projects (consumed and stripped by MultiProjectMiddleware.on_call_tool).
_PROJECT_FILTER_ARG = 'project_ids'


@dataclasses.dataclass(frozen=True)
class SessionScope:
    """In-conversation multi-project scope (PSGO-261 increment 2).

    Persisted on the session across the per-request state rebuild. ``project_ids`` is the
    user-selected set; ``scoped_token`` is the child access token minted by /v1/auth/pat/exchange
    and narrowed to those projects (re-minted from the parent when near expiry).
    """

    project_ids: list[int]
    read_only: bool = False
    scoped_token: str | None = None
    scoped_expires_at: float | None = None
    confirmed: bool = False
    """True once the user has explicitly chosen a scope via ``set_project_scope``. The default
    auto-leased scope is unconfirmed, which gates data tools until the user decides."""

    @property
    def active_project_id(self) -> int | None:
        return self.project_ids[0] if self.project_ids else None

    @property
    def is_near_expiry(self) -> bool:
        if self.scoped_expires_at is None:
            return False
        return time.time() >= (self.scoped_expires_at - 60)


R = TypeVar('R')
T = TypeVar('T')

DEFAULT_CONCURRENCY = 10

SEMANTIC_TOOLING_FEATURE = 'mcp-semantic-tooling'
SEMANTIC_TOOL_NAMES = {
    'search_semantic_context',
    'get_semantic_context',
    'get_semantic_schema',
    'validate_semantic_query',
}
# Data app tools are supported only in the main/production branch. This single set is the source of
# truth for both the on_list_tools filter and the on_call_tool guard — keeping them in sync is what
# prevents a new (possibly destructive) data app tool from leaking onto non-main branches.
DATA_APP_BRANCH_GATED_TOOLS = {
    'modify_streamlit_data_app',
    'modify_python_js_data_app',
    'create_python_js_data_app_git_credential',
    'get_data_apps',
    'deploy_data_app',
    'delete_python_js_data_app_draft',
}


def is_read_only_tool(tool: Tool) -> bool:
    """Check if a tool has readOnlyHint=True annotation."""
    if tool.annotations is None:
        return False
    return tool.annotations.readOnlyHint is True


def is_semantic_tool(tool: Tool) -> bool:
    """Check whether a tool belongs to semantic tooling."""
    return SEMANTIC_TOOLS_TAG in (tool.tags or set()) or tool.name in SEMANTIC_TOOL_NAMES


@dataclasses.dataclass(frozen=True)
class ServerState:
    config: Config
    runtime_info: ServerRuntimeInfo

    @classmethod
    def from_context(cls, ctx: Context) -> 'ServerState':
        server_state = ctx.request_context.lifespan_context
        if not isinstance(server_state, ServerState):
            raise ValueError('ServerState is not available in the context.')
        return server_state

    @classmethod
    def from_starlette(cls, app: Starlette) -> 'ServerState':
        server_state = app.state.server_state
        if not isinstance(server_state, ServerState):
            raise ValueError('ServerState is not available in the Starlette app.')
        return server_state


class ForwardSlashMiddleware:
    def __init__(self, app: ASGIApp):
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        LOG.debug(f'ForwardSlashMiddleware: scope={scope}')

        if scope['type'] == 'http':
            path = scope['path']
            if path in ['/mcp']:
                scope = dict(scope)
                scope['path'] = f'{path}/'

        await self._app(scope, receive, send)


class KeboolaMcpServer(FastMCP):
    def add_tool(self, tool: Tool) -> None:
        """Applies `textwrap.dedent()` function to the tool's docstring, if no explicit description is provided."""
        update = {}
        if tool.description:
            description = textwrap.dedent(tool.description).strip()
            if description != tool.description:
                update['description'] = description
        if not tool.serializer:
            update['serializer'] = _exclude_none_serializer

        if update:
            tool = tool.model_copy(update=update)

        super().add_tool(tool)


def get_http_request_or_none() -> Request | None:
    try:
        return get_http_request()
    except RuntimeError:
        return None


class SessionStateMiddleware(fmw.Middleware):
    """
    FastMCP middleware that manages session state in the Context parameter.

    This middleware sets up the session state containing instances of `KeboolaClient` and `WorkspaceManager`
    in the tool function's Context. These are initialized using the MCP server configuration, which is
    composed of the following parameter sources:

    * Initial configuration obtained from CLI parameters when starting the server
    * Environment variables
    * HTTP headers
    * URL query parameters

    Note: HTTP headers and URL query parameters are only used when the server runs on HTTP-based transport.
    """

    async def on_request(
        self,
        context: fmw.MiddlewareContext[mt.Request[Any, Any]],
        call_next: fmw.CallNext[mt.Request[Any, Any], Any],
    ) -> Any:
        """
        Manages session state in the Context parameter. This middleware sets up the session state for all the other
        MCP functions down the chain. It is called for each tool, prompt, resource, etc. calls.

        In fastmcp 2.13.0+, this must run in on_request rather than on_message because ctx.session
        requires the request context to be available.

        :param context: Middleware context containing FastMCP context.
        :param call_next: Next middleware in the chain to call.
        :returns: Result from executing the middleware chain.
        """
        # Skip session setup for initialize request - session state is only needed for actual operations
        if context.method == 'initialize':
            return await call_next(context)

        ctx = context.fastmcp_context
        assert isinstance(ctx, Context), f'Expecting Context, got {type(ctx)}.'

        if not isinstance(ctx.session, MagicMock):
            server_state = ServerState.from_context(ctx)
            config: Config = server_state.config
            runtime_info: ServerRuntimeInfo = server_state.runtime_info

            # IMPORTANT: Since mcp 1.12.4 and fastmcp 2.11 the fastmcp.server.dependencies.get_http_request()
            #   returns the same object as ctx.request_context.request.

            if http_rq := get_http_request_or_none():
                config = self.apply_request_config(http_rq, config)

            # Capability-discovery requests (tools/list, prompts/list, resources/list) MUST be fast and
            # network-free: a client fetches all three on connect, so any Connection round-trip here
            # (token introspect, refresh, or scoped-exchange) makes connecting hang until the client's
            # 30s timeout. For /list we do zero network in on_request — no auto-lease, no token refresh,
            # no scoped re-mint — and use the stored session token as-is (no refresh). The scope and
            # fresh tokens are established on the first real (non-list) tool call.
            is_list = context.method.endswith('/list')

            # Local streamable-HTTP with no token supplied (no header / env): fall back to the stored
            # PKCE session. For non-list requests keep it fresh (refresh + persist rotation); for /list
            # read it without a network refresh. No-op when a token is provided or on the deployed
            # server (KBC_KUBERNETES_TOKEN_PATH set).
            config = await self._maybe_use_stored_session(config, refresh=not is_list)

            # In-conversation multi-project scope persists on the session across this per-request
            # state rebuild. With no scope and no preset project, auto-lease ALL accessible projects
            # (multi-project mode) so read tools fan out across everything — but never on /list.
            scope = self._read_persisted_scope(ctx.session)
            if scope is None and not config.project_id and not is_list:
                scope = await self._autolease_default_scope(config)
            if not is_list:
                config, scope = await self._resolve_local_tokens(config, scope)

            # TODO: We could probably get rid of the 'state' attribute set on ctx.session and just
            #  pass KeboolaClient and WorkspaceManager instances to a tool as extra parameters.

            # Skip branch validation for /list requests (tools/list, resources/list, prompts/list, etc.)
            # so that clients can discover available tools even when the configured branch ID doesn't
            # exist yet. For these requests the client is created without a branch ID. Otherwise, the branch is
            # validated via a SAPI call.
            if is_list:
                if config.branch_id:
                    LOG.info(f'Skipping branch validation for {context.method} request.')
                config = dataclasses.replace(config, branch_id=None)

            state = await self.create_session_state(config, runtime_info)
            if scope is not None:
                state[SCOPE_KEY] = scope
            ctx.session.state = state

        try:
            return await call_next(context)
        finally:
            # NOTE: This line is commented following a bug related to session state clearance in Claude client
            # ctx.session.state = {}
            pass

    @classmethod
    def _get_headers(cls, runtime_info: ServerRuntimeInfo) -> dict[str, Any]:
        """
        :param runtime_info: Runtime information
        :return: Additional headers for the requests used for tracing the MCP server
        """
        return {
            'User-Agent': (
                f'Keboola MCP Server/{runtime_info.server_version} app_env={runtime_info.app_env} '
                f'transport={runtime_info.transport}'
            ),
            'MCP-Server-Transport': runtime_info.transport or 'NA',
            'MCP-Server-Versions': (
                f'keboola-mcp-server/{runtime_info.server_version} mcp/{runtime_info.mcp_library_version} '
                f'fastmcp/{runtime_info.fastmcp_library_version}'
            ),
        }

    @classmethod
    def apply_request_config(cls, http_rq: Request, config: Config) -> Config:
        LOG.debug(f'Injecting headers: http_rq={http_rq}, headers={http_rq.headers}')
        config = config.replace_by(http_rq.headers)

        if user := http_rq.scope.get('user'):
            LOG.debug(f'Injecting bearer and SAPI tokens: user={user}, access_token={user.access_token}')
            assert isinstance(user, AuthenticatedUser), f'Expecting AuthenticatedUser, got: {type(user)}'
            assert isinstance(
                user.access_token, ProxyAccessToken
            ), f'Expecting ProxyAccessToken, got: {type(user.access_token)}'
            config = dataclasses.replace(
                config,
                storage_token=user.access_token.sapi_token,
                bearer_token=user.access_token.delegate.token,
            )

        return config

    @staticmethod
    def _read_persisted_scope(session: Any) -> 'SessionScope | None':
        """Reads the multi-project scope stashed in the prior request's session state, if any."""
        prior = getattr(session, 'state', None)
        if isinstance(prior, dict):
            scope = prior.get(SCOPE_KEY)
            if isinstance(scope, SessionScope):
                return scope
        return None

    @classmethod
    def _is_local_programmatic(cls, config: Config) -> bool:
        """True for a local (non-deployed) session carrying a Keboola programmatic token."""
        return (
            not os.environ.get('KBC_KUBERNETES_TOKEN_PATH')
            and bool(config.storage_token)
            and bool(config.storage_api_url)
            and is_programmatic_token(config.storage_token)
        )

    @classmethod
    async def _maybe_use_stored_session(cls, config: Config, *, refresh: bool = True) -> Config:
        """Populate the token from the stored PKCE session for a local, tokenless request.

        Only when: no token is set, a stack URL is known, and this is not the deployed server. With
        ``refresh=True`` reads (and refreshes + persists) the session leased by
        ``keboola-mcp-server login``. With ``refresh=False`` (capability-discovery /list requests)
        reads the stored token WITHOUT any network refresh, so listing never blocks on Connection.
        If there is no stored session, leaves the config unchanged.
        """
        if config.storage_token or not config.storage_api_url:
            return config
        if os.environ.get('KBC_KUBERNETES_TOKEN_PATH'):
            return config
        if refresh:
            try:
                access_token = await get_access_token(config.storage_api_url)
            except RuntimeError:
                return config
        else:
            tokens = load_tokens(config.storage_api_url)
            if not tokens:
                return config
            access_token = tokens.access_token
        return dataclasses.replace(config, storage_token=access_token)

    @classmethod
    async def _autolease_default_scope(cls, config: Config) -> 'SessionScope | None':
        """
        Default to multi-project mode: scope the session to ALL accessible projects.

        Introspects the programmatic token once to enumerate the projects it can reach and returns a
        scope covering all of them (no minted token — the whole-stack parent token is used, narrowed
        per request only by the ``X-KBC-ProjectId`` header). Returns None when introspection is
        unavailable (deployed server, legacy token, or no reachable projects) so the caller falls
        back to the existing single-project behavior.
        """
        if not cls._is_local_programmatic(config):
            return None
        try:
            parent = await get_access_token(config.storage_api_url)
        except RuntimeError:
            parent = config.storage_token
        try:
            introspection = await introspect_token(config.storage_api_url, subject_token=parent)
        except Exception as e:
            LOG.warning(f'Could not auto-lease projects from token introspection: {e}')
            return None
        project_ids = [p.id for p in introspection.projects]
        if not project_ids:
            return None
        LOG.info(f'Multi-project mode: auto-leased {len(project_ids)} accessible project(s) as the default scope.')
        return SessionScope(project_ids=project_ids)

    @classmethod
    async def _resolve_local_tokens(
        cls, config: Config, scope: 'SessionScope | None'
    ) -> 'tuple[Config, SessionScope | None]':
        """
        For local (non-deployed) programmatic-token sessions, keep tokens fresh during usage.

        Refreshes the stored whole-stack (parent) token via the PKCE credential store. When the user
        has explicitly narrowed scope (a minted scoped token is present), that token is re-minted from
        the parent when it nears expiry. The default (auto-leased) multi-project scope carries no
        minted token and simply uses the parent token, narrowed per request by ``X-KBC-ProjectId``.
        On the deployed server (``KBC_KUBERNETES_TOKEN_PATH`` set) the per-request resolver exchange
        already handles freshness, so this is a no-op there.
        """
        if not cls._is_local_programmatic(config):
            return config, scope

        parent = config.storage_token
        try:
            # Refreshes (and persists the rotated pair) when near expiry; raises if no stored creds.
            parent = await get_access_token(config.storage_api_url)
        except RuntimeError:
            pass  # token supplied directly (no PKCE login) — use it as-is

        token = parent
        project_id = config.project_id
        if scope and scope.project_ids:
            project_id = str(scope.active_project_id)
            if scope.scoped_token is not None:
                if scope.is_near_expiry:
                    try:
                        minted = await exchange_scoped_token(
                            config.storage_api_url,
                            subject_token=parent,
                            project_ids=scope.project_ids,
                            read_only=scope.read_only,
                        )
                        scope = dataclasses.replace(
                            scope, scoped_token=minted.access_token, scoped_expires_at=minted.expires_at
                        )
                    except Exception as e:
                        # Don't break the session if re-minting fails; fall back to the parent token.
                        LOG.warning(f'Could not refresh the scoped token; using the parent token: {e}')
                        scope = dataclasses.replace(scope, scoped_token=None, scoped_expires_at=None)
                token = scope.scoped_token or parent

        config = dataclasses.replace(config, storage_token=token, project_id=project_id)
        return config, scope

    @classmethod
    async def _exchange_programmatic_token(cls, config: Config) -> str:
        """
        Exchanges a programmatic token (kbc_at_/kbc_pat_) for the project's legacy Storage token.

        The resolver is reached only on the deployed MCP server, which has a projected
        ServiceAccount token at ``KBC_KUBERNETES_TOKEN_PATH`` (read from the process
        environment only, never from per-request config). A project id is required because
        a programmatic token is not project-bound.
        """
        kubernetes_token_path = os.environ.get('KBC_KUBERNETES_TOKEN_PATH')
        if not kubernetes_token_path:
            raise ValueError(
                'Received a Keboola programmatic token (kbc_at_/kbc_pat_) but KBC_KUBERNETES_TOKEN_PATH '
                'is not configured. Programmatic-token exchange is available only on the deployed MCP server.'
            )
        if not config.project_id:
            raise ValueError(
                'A project id is required to exchange a programmatic token. '
                'Set the KBC_PROJECT_ID env var or the X-KBC-ProjectId header.'
            )
        try:
            project_id = int(config.project_id)
        except (TypeError, ValueError):
            raise ValueError(f'Invalid project id for programmatic-token exchange: {config.project_id!r}')

        resolver = StorageTokenResolver(
            storage_api_url=config.storage_api_url,
            kubernetes_token_path=kubernetes_token_path,
        )
        return await resolver.resolve(subject_token=config.storage_token, project_id=project_id)

    @classmethod
    async def create_session_state(
        cls,
        config: Config,
        runtime_info: ServerRuntimeInfo,
        readonly: bool | None = None,
    ) -> dict[str, Any]:
        """
        Creates `KeboolaClient` and `WorkspaceManager` instances and returns them in the session state.

        :param config: The MCP server configuration.
        :param runtime_info: The MCP server runtime information.
        :param readonly: If True, the `KeboolaClient` will only use HTTP GET, HEAD operations.
        :return: The session state dictionary containing the created client and workspace manager instances.
        """
        LOG.info(f'Creating SessionState from config: {config}.')

        state: dict[str, Any] = {}
        try:
            if not config.storage_token:
                raise ValueError('Storage API token is not provided.')
            if not config.storage_api_url:
                raise ValueError('Storage API URL is not provided.')

            storage_token = config.storage_token
            bearer_token = config.bearer_token
            extra_headers: dict[str, Any] = {}
            if is_programmatic_token(storage_token):
                if os.environ.get('KBC_KUBERNETES_TOKEN_PATH'):
                    # Deployed: exchange the programmatic token (kbc_at_/kbc_pat_) for the project's
                    # legacy Storage token via the auth-bridge resolver, then use it downstream unchanged.
                    storage_token = await cls._exchange_programmatic_token(config)
                    bearer_token = None
                else:
                    # Local: no projected SA token to reach the resolver. Forward the programmatic token
                    # downstream as a Bearer and let PAT-aware services exchange it; name the target
                    # project when one has been selected.
                    bearer_token = storage_token
                    if config.project_id:
                        extra_headers['X-KBC-ProjectId'] = config.project_id

            client = await KeboolaClient(
                storage_api_url=config.storage_api_url,
                storage_api_token=storage_token,
                bearer_token=bearer_token,
                headers={**cls._get_headers(runtime_info), **extra_headers},
                readonly=readonly,
            ).with_branch_id(config.branch_id)

            state[KeboolaClient.STATE_KEY] = client
            LOG.info('Successfully initialized Storage API client.')
        except Exception as e:
            LOG.error(f'Failed to initialize Keboola client: {e}')
            raise

        try:
            # The Kubernetes ServiceAccount token path is read from the process environment
            # only (KBC_KUBERNETES_TOKEN_PATH), never from `Config`/HTTP headers — it is a
            # deployment-level credential of the MCP server itself and must not be
            # overridable per request.
            kubernetes_token_path = os.environ.get('KBC_KUBERNETES_TOKEN_PATH')
            workspace_manager = await WorkspaceManager.create(
                client, config.workspace_schema, kubernetes_token_path=kubernetes_token_path
            )
            state[WorkspaceManager.STATE_KEY] = workspace_manager
            LOG.info('Successfully initialized Storage API Workspace manager.')
        except Exception as e:
            LOG.error(f'Failed to initialize Storage API Workspace manager: {e}')
            raise

        state[CONVERSATION_ID] = config.conversation_id
        return state


class ToolsFilteringMiddleware(fmw.Middleware):
    """
    This middleware filters out tools that are not available in the current project. The filtering is based on the
    project features.

    The middleware intercepts the `on_list_tools()` call and removes the unavailable tools
    from the list. The AI assistants should not even see the tools that are not available in the current project.

    The middleware also intercepts the `on_call_tool()` call and raises an exception if a call is attempted to a tool
    that is not available in the current project.

    Tool visibility for modify_flow and update_flow:

    | Token Type       | Role        | modify_flow | update_flow | Read-Only Tools |
    |-----------------|-------------|-------------|-------------|-----------------|
    | OAuth (any)     | any         | ✅          | ❌          | ✅              |
    | SAPI            | admin/share | ✅          | ❌          | ✅              |
    | SAPI            | ''/guest    | ❌          | ✅          | ✅              |
    | SAPI/OAuth      | readOnly    | ❌          | ❌          | ✅              |
    """

    @staticmethod
    def _is_oauth_authenticated(ctx: Context) -> bool:
        """
        Detect if the user is authenticated via OAuth.

        Returns True if bearer token is present, False otherwise.
        """
        keboola_client = KeboolaClient.from_state(ctx.session.state)
        return bool(keboola_client.bearer_token)

    @staticmethod
    async def get_token_info(ctx: Context) -> JsonDict:
        assert isinstance(ctx, Context), f'Expecting Context, got {type(ctx)}.'
        client = KeboolaClient.from_state(ctx.session.state)
        return await client.storage_client.verify_token()

    @staticmethod
    def get_project_features(token_info: JsonDict) -> set[str]:
        owner_data = token_info.get('owner', {})
        if not isinstance(owner_data, dict):
            return set()
        return set(filter(None, owner_data.get('features', [])))

    @staticmethod
    def get_token_role(token_info: JsonDict) -> str:
        admin_data = token_info.get('admin', {})
        if isinstance(admin_data, dict):
            role = admin_data.get('role')
            if isinstance(role, str):
                return role
        return ''

    @staticmethod
    def is_client_using_main_branch(ctx: Context) -> bool:
        """
        Checks if the current branch is the main/production branch.
        """
        client = KeboolaClient.from_state(ctx.session.state)
        branch_id = client.branch_id

        # We use None for the branch id referring to the main/production branch in the KeboolaClient.
        return branch_id is None

    async def on_list_tools(
        self, context: MiddlewareContext[mt.ListToolsRequest], call_next: CallNext[mt.ListToolsRequest, list[Tool]]
    ) -> list[Tool]:
        tools = await call_next(context)
        token_info = await self.get_token_info(context.fastmcp_context)
        features = self.get_project_features(token_info)
        token_role = self.get_token_role(token_info).lower()

        if 'hide-conditional-flows' in features:
            tools = [t for t in tools if t.name != 'create_conditional_flow']
        else:
            tools = [t for t in tools if t.name != 'create_flow']

        # Show modify_flow to: admin, share, OR OAuth users
        # Show update_flow to: everyone else (except readOnly, handled below)
        is_oauth = self._is_oauth_authenticated(context.fastmcp_context)
        if token_role in ('admin', 'share') or is_oauth:
            tools = [t for t in tools if t.name != UPDATE_FLOW_TOOL_NAME]
        else:
            tools = [t for t in tools if t.name != MODIFY_FLOW_TOOL_NAME]

        if not self.is_client_using_main_branch(context.fastmcp_context):
            # Filter out data app tools when the client is not using the main/production branch
            tools = [t for t in tools if t.name not in DATA_APP_BRANCH_GATED_TOOLS]

        if token_role == 'readonly':
            tools = [t for t in tools if is_read_only_tool(t)]
            LOG.debug(f'Read-only access: filtered to {len(tools)} read-only tools for role={token_role}')

        if SEMANTIC_TOOLING_FEATURE not in features:
            tools = [t for t in tools if not is_semantic_tool(t)]

        return tools

    @staticmethod
    def authorize_tool_call(
        *,
        tool_name: str,
        is_read_only: bool,
        is_semantic: bool,
        token_role: str,
        features: set[str],
        is_oauth: bool,
        is_main_branch: bool,
    ) -> str | None:
        """
        Decide whether a call to ``tool_name`` is allowed given the project features, the token role,
        the authentication mode and the branch.

        This is the single source of truth for the project-feature / token-role / branch gating.
        :meth:`on_call_tool` applies it to MCP tool calls; the raw ``/preview/configuration`` Starlette
        route reuses it (see ``preview.py``) so the preview path enforces exactly the same rules.

        :return: A denial message if the call is not allowed, or ``None`` if it is allowed.
        """
        token_role = token_role.lower()

        if token_role == 'readonly' and not is_read_only:
            return (
                f'Access denied: The tool "{tool_name}" requires write permissions. '
                f'Your current role ({token_role}) only allows read-only operations. '
                f'Contact your administrator to request write access.'
            )

        if SEMANTIC_TOOLING_FEATURE not in features and is_semantic:
            return (
                f'The tool "{tool_name}" is not available in this project. '
                'Please ask Keboola support to enable "Semantic Layer Tooling" feature.'
            )

        if 'hide-conditional-flows' in features:
            if tool_name == 'create_conditional_flow':
                return (
                    'The "create_conditional_flow" tool is not available in this project. '
                    'Please ask Keboola support to enable "Conditional Flows" feature '
                    'or use "create_flow" tool instead.'
                )
        else:
            if tool_name == 'create_flow':
                return (
                    'The "create_flow" tool is not available in this project. '
                    'This project uses "Conditional Flows", '
                    'please use "create_conditional_flow" tool instead.'
                )

        if token_role in ('admin', 'share') or is_oauth:
            if tool_name == UPDATE_FLOW_TOOL_NAME:
                return (
                    'The "update_flow" tool is not available for admin/OAuth tokens. '
                    f'Use "{MODIFY_FLOW_TOOL_NAME}" to manage schedules instead.'
                )
        else:
            if tool_name == MODIFY_FLOW_TOOL_NAME:
                return (
                    f'The "{MODIFY_FLOW_TOOL_NAME}" tool is not available for this token. '
                    f'Use "{UPDATE_FLOW_TOOL_NAME}" to update flow configuration instead.'
                )

        if tool_name in DATA_APP_BRANCH_GATED_TOOLS and not is_main_branch:
            return 'Data apps are supported only in the main production branch.'

        return None

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        tool = await context.fastmcp_context.fastmcp.get_tool(context.message.name)
        token_info = await self.get_token_info(context.fastmcp_context)

        denial = self.authorize_tool_call(
            tool_name=tool.name,
            is_read_only=is_read_only_tool(tool),
            is_semantic=is_semantic_tool(tool),
            token_role=self.get_token_role(token_info),
            features=self.get_project_features(token_info),
            is_oauth=self._is_oauth_authenticated(context.fastmcp_context),
            is_main_branch=self.is_client_using_main_branch(context.fastmcp_context),
        )
        if denial:
            raise ToolError(denial)

        return await call_next(context)


class MultiProjectMiddleware(fmw.Middleware):
    """Fans a read-only tool call out across every project in the active multi-project scope.

    Single-project (or no) scope is an unchanged passthrough. With >1 project selected, a read-only
    tool runs once per project — the active ``KeboolaClient`` in session state is swapped to each
    project's client and the per-project results are labelled and concatenated (no structured-content
    merge, so each tool keeps its native output shape). Write tools never fan out: they target the
    active project only, so the agent can never write to multiple projects without the user explicitly
    re-scoping (PSGO-261 decision D8).
    """

    async def on_call_tool(
        self,
        context: MiddlewareContext[mt.CallToolRequestParams],
        call_next: CallNext[mt.CallToolRequestParams, mt.CallToolResult],
    ) -> mt.CallToolResult:
        ctx = context.fastmcp_context
        state = ctx.session.state
        scope = state.get(SCOPE_KEY) if isinstance(state, dict) else None
        name = context.message.name

        # Ask-first gate: until the user confirms a scope via set_project_scope, block data tools and
        # tell the assistant to ask the user which projects to work on. Only applies when a scope has
        # been auto-leased (local programmatic session); deployed/legacy sessions have no scope.
        if isinstance(scope, SessionScope) and not scope.confirmed and name not in _BOOTSTRAP_TOOLS:
            raise ToolError(
                f'This session can access {len(scope.project_ids)} Keboola project(s), but no scope has '
                'been confirmed yet. Call "get_accessible_projects", show the user their projects, and ask '
                'whether to work across ALL of them or a subset. Then call "set_project_scope" '
                '(no arguments = all projects, or pass the chosen project ids, optionally read_only=true). '
                'This confirmation is required once per session.'
            )

        # No auto-leased scope (deployed / legacy) or a bootstrap/scope tool: pass through untouched.
        # Bootstrap tools own a real `project_ids` argument, so we must not strip it.
        if not isinstance(scope, SessionScope) or name in _BOOTSTRAP_TOOLS:
            return await call_next(context)
        # Workspace-bound and write tools always target the active project (no fan-out, filter ignored).
        if name in _NO_FANOUT_TOOLS:
            return await call_next(context)
        tool = await ctx.fastmcp.get_tool(name)
        if not is_read_only_tool(tool):
            return await call_next(context)

        # Read tool: consume the optional per-call project filter (advertised via on_list_tools) so the
        # tool never receives it, then narrow this call's target projects to the requested subset.
        requested = None
        args = getattr(context.message, 'arguments', None)
        if isinstance(args, dict):
            requested = args.pop(_PROJECT_FILTER_ARG, None)

        targets = list(scope.project_ids)
        if requested:
            outside = [p for p in requested if p not in scope.project_ids]
            if outside:
                raise ToolError(
                    f'Project(s) {outside} are outside the current scope {scope.project_ids}. '
                    'Call "set_project_scope" to change the scope first.'
                )
            targets = [p for p in scope.project_ids if p in requested]
        if not targets:
            return await call_next(context)

        server_state = ServerState.from_context(ctx)
        original_client = state.get(KeboolaClient.STATE_KEY)
        original_workspace = state.get(WorkspaceManager.STATE_KEY)
        # Default (auto-leased) scope carries no minted token; fall back to the active client's token.
        base_token = scope.scoped_token or (original_client.token if isinstance(original_client, KeboolaClient) else '')

        # A single target (scope of one, or narrowed to one via the filter) runs once against that
        # project only — one call, that project's X-KBC-ProjectId, no per-project envelope.
        if len(targets) == 1:
            target = targets[0]
            if target == scope.active_project_id:
                return await call_next(context)
            try:
                await self._swap_project(state, server_state, base_token, target, scope.read_only)
                return await call_next(context)
            finally:
                state[KeboolaClient.STATE_KEY] = original_client
                state[WorkspaceManager.STATE_KEY] = original_workspace

        results: list[tuple[int, ToolResult]] = []
        errors: list[tuple[int, str]] = []
        try:
            for project_id in targets:
                await self._swap_project(state, server_state, base_token, project_id, scope.read_only)
                # Isolate per-project failures: one project's error (e.g. Queue 401, a transient 5xx)
                # must not discard the other projects' good results. Collect it and keep going, so the
                # agent gets a partial response plus a retry hint. CancelledError is BaseException, so
                # `except Exception` lets client cancellation propagate.
                try:
                    results.append((project_id, await call_next(context)))
                except Exception as e:
                    LOG.warning(f'Fan-out call failed for project {project_id}: {e}')
                    errors.append((project_id, str(e)))
        finally:
            state[KeboolaClient.STATE_KEY] = original_client
            state[WorkspaceManager.STATE_KEY] = original_workspace

        # Every project failed → nothing partial to return; surface a single aggregate error.
        if not results and errors:
            detail = '; '.join(f'project {pid}: {msg}' for pid, msg in errors)
            raise ToolError(f'The tool failed for all {len(errors)} scoped project(s): {detail}')

        return self._merge(results, errors)

    async def on_list_tools(
        self,
        context: MiddlewareContext[mt.ListToolsRequest],
        call_next: CallNext[mt.ListToolsRequest, list[Tool]],
    ) -> list[Tool]:
        # Advertise the optional per-call `project_ids` filter on fan-out-eligible read tools while a
        # multi-project scope is active, so the assistant can target a subset (e.g. a single project)
        # without changing the session scope. The value is consumed and stripped in on_call_tool.
        tools = await call_next(context)
        ctx = context.fastmcp_context
        state = getattr(ctx.session, 'state', None)
        scope = state.get(SCOPE_KEY) if isinstance(state, dict) else None

        # NOTE: we intentionally do NOT hide data tools before a scope is confirmed. Hiding relied on
        # the client re-fetching the tool list after notifications/tools/list_changed, which Claude Code
        # (and others) don't do mid-session — that left the newly-unlocked tools invisible until a
        # reconnect. Instead every tool stays listed and the call-time ask-first gate (on_call_tool)
        # steers the user to set_project_scope first; once scoped, the already-listed tools just work.
        if not (isinstance(scope, SessionScope) and scope.confirmed and len(scope.project_ids) > 1):
            return tools

        patched: list[Tool] = []
        for tool in tools:
            if tool.name in _BOOTSTRAP_TOOLS or tool.name in _NO_FANOUT_TOOLS or not is_read_only_tool(tool):
                patched.append(tool)
                continue
            params = dict(tool.parameters or {})
            props = dict(params.get('properties') or {})
            if _PROJECT_FILTER_ARG in props:
                patched.append(tool)
                continue
            props[_PROJECT_FILTER_ARG] = {
                'type': 'array',
                'items': {'type': 'integer'},
                'description': (
                    'Optional. Restrict this call to these project ids (a subset of the confirmed '
                    'multi-project scope). Omit to run across all scoped projects.'
                ),
            }
            params['properties'] = props
            patched.append(tool.model_copy(update={'parameters': params}))
        return patched

    @classmethod
    async def _swap_project(
        cls,
        state: dict[str, Any],
        server_state: ServerState,
        base_token: str,
        project_id: int,
        read_only: bool,
    ) -> None:
        """Points the session state at `project_id` for the duration of one fanned-out tool call.

        Swaps in a per-project `KeboolaClient` AND a `WorkspaceManager` built on it, so
        workspace-bound reads (query_data) run against *this* project's workspace rather than the
        active project's. The workspace is provisioned lazily on first use per project.
        ponytail: rebuilt per call; caching across calls would need a store that survives the
        per-request state rebuild — add if provisioning latency shows up in practice.
        """
        client = await cls._client_for_project(server_state, base_token, project_id, read_only)
        state[KeboolaClient.STATE_KEY] = client
        state[WorkspaceManager.STATE_KEY] = await WorkspaceManager.create(client, server_state.config.workspace_schema)

    @staticmethod
    async def _client_for_project(
        server_state: ServerState, token: str, project_id: int, read_only: bool
    ) -> KeboolaClient:
        return await KeboolaClient(
            storage_api_url=server_state.config.storage_api_url,
            storage_api_token=token,
            bearer_token=token,
            headers={
                **SessionStateMiddleware._get_headers(server_state.runtime_info),
                'X-KBC-ProjectId': str(project_id),
            },
            readonly=read_only or None,
        ).with_branch_id(None)

    @staticmethod
    def _deep_merge(a: Any, b: Any) -> Any:
        """Merges two per-project structured outputs so the result still validates the tool's schema.

        Lists are concatenated (the combined slice across projects), nested objects merged key by key,
        and numeric counters summed; any other scalar keeps the first project's value. This keeps every
        required field present with its declared type, so the merged object validates against the
        single-project output schema.
        """
        if isinstance(a, list) and isinstance(b, list):
            return a + b
        if isinstance(a, dict) and isinstance(b, dict):
            merged = dict(a)
            for key, value in b.items():
                merged[key] = MultiProjectMiddleware._deep_merge(a[key], value) if key in a else value
            return merged
        if isinstance(a, bool) or isinstance(b, bool):
            return a
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return a + b  # counters like search "total"
        return a

    # Total list items across projects before a fanned-out result degrades to count-first: instead of
    # dumping every project's full listing (which, on big projects, overflows the context window in a
    # single tool result), return per-project counts + a truncated sample + guidance to narrow. Small
    # multi-project results stay fully detailed. Class attribute so tests can lower it.
    _FANOUT_MAX_ITEMS = 200

    @staticmethod
    def _largest_list_len(sc: Any) -> int:
        """Item count of a structured payload = the length of its largest top-level list (buckets/tables/hits)."""
        if isinstance(sc, dict):
            return max((len(v) for v in sc.values() if isinstance(v, list)), default=0)
        if isinstance(sc, list):
            return len(sc)
        return 0

    @staticmethod
    def _truncate_lists(sc: Any, limit: int) -> Any:
        """Truncate every top-level list to `limit` (schema-safe: a shorter list still validates)."""
        if isinstance(sc, dict):
            return {k: (v[:limit] if isinstance(v, list) else v) for k, v in sc.items()}
        if isinstance(sc, list):
            return sc[:limit]
        return sc

    @staticmethod
    def _merge(results: list[tuple[int, 'ToolResult']], errors: 'list[tuple[int, str]] | None' = None) -> 'ToolResult':
        # Deep-merge the per-project structured payloads into one schema-valid object (lists concatenated
        # across projects). Counters (e.g. bucket_counts, search total) are summed by _deep_merge, so they
        # keep reflecting the true totals even if the item lists get truncated below.
        # Per-project failures (partial success) are surfaced as retry-hint notes in the text content,
        # so the model can re-run just the failed project(s) via the project_ids filter.
        error_notes = [
            mt.TextContent(
                type='text',
                text=f'project {pid} failed (retry with project_ids=[{pid}]): {msg}',
            )
            for pid, msg in (errors or [])
        ]
        merged_structured: Any = None
        per_project_counts: list[tuple[int, int]] = []
        total_items = 0
        for project_id, result in results:
            sc = result.structured_content
            per_project_counts.append((project_id, MultiProjectMiddleware._largest_list_len(sc)))
            total_items += MultiProjectMiddleware._largest_list_len(sc)
            if sc is not None:
                merged_structured = (
                    sc if merged_structured is None else MultiProjectMiddleware._deep_merge(merged_structured, sc)
                )

        # Small enough: full detail with per-project text envelopes (attribution the model can read).
        if total_items <= MultiProjectMiddleware._FANOUT_MAX_ITEMS:
            content: list[Any] = list(error_notes)
            for project_id, result in results:
                content.append(mt.TextContent(type='text', text=f'=== project {project_id} ==='))
                content.extend(result.content or [])
            return ToolResult(content=content, structured_content=merged_structured)

        # Count-first: the combined listing is too large for one result. Return per-project counts, a
        # truncated sample (first _FANOUT_MAX_ITEMS), and guidance — instead of every project's full dump.
        summary = ', '.join(f'project {pid}: {n}' for pid, n in per_project_counts)
        note = (
            f'Multi-project result is large — {total_items} items across {len(results)} project(s) '
            f'({summary}). Showing the first {MultiProjectMiddleware._FANOUT_MAX_ITEMS} in structured_content; '
            f'counters reflect the true totals. Narrow with project_ids=[...] on this tool, or use the '
            f'search tool to find specific items.'
        )
        truncated = MultiProjectMiddleware._truncate_lists(merged_structured, MultiProjectMiddleware._FANOUT_MAX_ITEMS)
        return ToolResult(content=error_notes + [mt.TextContent(type='text', text=note)], structured_content=truncated)


def _to_python(data: Any, exclude_none: bool = True) -> Any | None:
    if isinstance(data, BaseModel):
        return data.model_dump(exclude_none=exclude_none, by_alias=False)
    elif isinstance(data, (list, tuple)):
        # Handle sequences of BaseModels
        cleaned = []
        for item in data:
            if isinstance(item, BaseModel):
                cleaned.append(item.model_dump(exclude_none=exclude_none, by_alias=False))
            elif item is not None:
                cleaned.append(_to_python(item, exclude_none=exclude_none))
            elif not exclude_none:
                cleaned.append(None)
        return cleaned
    elif isinstance(data, dict):
        # Handle dictionaries that might contain BaseModels
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, BaseModel):
                cleaned[key] = value.model_dump(exclude_none=exclude_none, by_alias=False)
            elif value is not None:
                cleaned[key] = _to_python(value, exclude_none=exclude_none)
            elif not exclude_none:
                cleaned[key] = None
        return cleaned
    elif data is not None:
        return data
    else:
        return None


def _filter_toon_nulls(data: Any) -> Any:
    """
    Drops None fields while keeping TOON's list-of-dicts alignment.
    Single-item lists drop keys that have None assigned.
    Multi-item lists drop keys that have None assigned in all items.
    """
    if isinstance(data, list):
        if not data:
            return data

        elif all(isinstance(item, dict) for item in data):
            if len(data) == 1:
                return [_filter_toon_nulls(data[0])]

            ordered_keys_with_values: list[str] = []
            seen_keys_with_values: set[str] = set()
            for item in data:
                for key, value in item.items():
                    if value is not None and key not in seen_keys_with_values:
                        seen_keys_with_values.add(key)
                        ordered_keys_with_values.append(key)

            cleaned_items: list[dict[str, Any]] = []
            for item in data:
                cleaned_item: dict[str, Any] = {}
                for key in ordered_keys_with_values:
                    value = item.get(key)
                    if value is None:
                        cleaned_item[key] = None
                    else:
                        cleaned_item[key] = _filter_toon_nulls(value)
                cleaned_items.append(cleaned_item)

            return cleaned_items

        else:
            return [_filter_toon_nulls(item) if item is not None else None for item in data]

    if isinstance(data, dict):
        cleaned: dict[str, Any] = {}
        for key, value in data.items():
            if value is None:
                continue
            cleaned[key] = _filter_toon_nulls(value)
        return cleaned

    return data


def _exclude_none_serializer(data: Any) -> str:
    if (cleaned := _to_python(data)) is not None:
        return to_json(cleaned, fallback=str).decode('utf-8')
    else:
        return ''


def toon_serializer(data: Any) -> str:
    return toon_format.encode(_to_python(data, exclude_none=False))


def toon_serializer_compact(data: Any) -> str:
    return toon_format.encode(_filter_toon_nulls(_to_python(data, exclude_none=False)))


async def process_concurrently(
    items: Iterable[T],
    afunc: Callable[[T], Awaitable[R]],
    max_concurrency: int = DEFAULT_CONCURRENCY,
) -> list[R | BaseException]:
    """
    Asynchronously process a collection of items with a specified concurrency limit.

    :param items: The collection of items to process.
    :param afunc: An asynchronous function to apply to each item.
    :param max_concurrency: The maximum number of concurrent executions allowed.
    :return: A list of results or exceptions from processing each item.
             The order of results corresponds to the order of the input items.
    """
    if max_concurrency <= 0:
        raise ValueError('max_concurrency must be a positive integer.')

    semaphore = asyncio.Semaphore(max_concurrency)

    async def process_item_with_semaphore(item: T) -> R:
        async with semaphore:
            return await afunc(item)

    tasks = [asyncio.create_task(process_item_with_semaphore(item)) for item in items]

    return await asyncio.gather(*tasks, return_exceptions=True)


class AggregateError(Exception):
    """Exception that aggregates multiple exceptions (Python 3.10 compatible alternative to ExceptionGroup)."""

    def __init__(self, message: str, exceptions: Iterable[BaseException]):
        self.message = message
        self.exceptions = list(exceptions)
        super().__init__(message, self.exceptions)

    def __str__(self) -> str:
        error_details = '; '.join(f'{type(e).__name__}: {e}' for e in self.exceptions)
        return f'{self.message} ({len(self.exceptions)} errors): {error_details}'


def unwrap_results(results: Iterable[R | BaseException], message: str = 'Multiple errors occurred') -> list[R]:
    """
    Unwrap results from process_concurrently, raising an AggregateError if any exceptions occurred.

    :param results: List of results or exceptions from process_concurrently.
    :param message: Message for the AggregateError if exceptions are present.
    :return: List of successful results.
    :raises AggregateError: If any results are exceptions.
    """
    successes: list[R] = []
    exceptions: list[BaseException] = []

    for result in results:
        if isinstance(result, BaseException):
            exceptions.append(result)
        else:
            successes.append(result)

    if exceptions:
        raise AggregateError(message, exceptions)

    return successes
