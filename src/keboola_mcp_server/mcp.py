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
from mcp import types as mt
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from pydantic import BaseModel
from pydantic_core import to_json
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.clients.auth_bridge import StorageTokenResolver, is_programmatic_token, strip_bearer
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import Config, ServerRuntimeInfo, is_same_stack
from keboola_mcp_server.oauth import ProxyAccessToken
from keboola_mcp_server.tools.constants import MODIFY_FLOW_TOOL_NAME, SEMANTIC_TOOLS_TAG, UPDATE_FLOW_TOOL_NAME
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)
CONVERSATION_ID = 'conversation_id'

R = TypeVar('R')
T = TypeVar('T')

DEFAULT_CONCURRENCY = 10

# Metastore object type used to detect whether a project already has any semantic models.
SEMANTIC_MODEL_OBJECT_TYPE = 'semantic-model'
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


async def project_has_semantic_models(client: KeboolaClient) -> bool:
    """
    Detect whether the project has at least one semantic model, so the semantic tools can be
    shown/allowed dynamically instead of behind a project feature flag.

    Fails closed: if the metastore call raises (e.g. the project is not provisioned for the
    semantic layer, or the API returns a 5xx), we treat it as "no semantic models" so the tools
    stay hidden. This preserves the previous default-off behavior.
    """
    try:
        objects = await client.metastore_client.list_objects(SEMANTIC_MODEL_OBJECT_TYPE, limit=1)
        return bool(objects)
    except Exception as e:
        LOG.debug(f'Failed to detect semantic models, assuming none are available: {e}')
        return False


@dataclasses.dataclass(frozen=True)
class ServerState:
    config: Config
    runtime_info: ServerRuntimeInfo

    @property
    def own_stack_storage_api_url(self) -> str | None:
        """
        The Storage API URL of the Keboola stack that this server instance belongs to, or None when
        it has no stack of its own (a locally run server that only learns the stack from the caller).

        This is the single, trusted answer to "which stack is mine?" — every check that compares a
        session's stack against the server's own stack must use this value, so that the checks cannot
        drift apart. It is resolved once, when the server starts: `create_server()` builds
        `self.config` from the '--api-url' CLI parameter, the process environment and the
        'HOSTNAME_SUFFIX' fallback (see `get_env_storage_api_url()`). Per-request HTTP headers can
        never influence it — `ServerState` is frozen and `SessionStateMiddleware.apply_request_config()`
        returns a new `Config` for each request instead of mutating `self.config`.
        """
        return self.config.storage_api_url

    @classmethod
    def from_context(cls, ctx: Context) -> 'ServerState':
        server_state = ctx.request_context.lifespan_context
        if not isinstance(server_state, ServerState):
            raise TypeError('ServerState is not available in the context.')
        return server_state

    @classmethod
    def from_starlette(cls, app: Starlette) -> 'ServerState':
        server_state = app.state.server_state
        if not isinstance(server_state, ServerState):
            raise TypeError('ServerState is not available in the Starlette app.')
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

            # Resolved once here and threaded through both stack checks (the per-request Storage
            # API URL pinning below and the Kubernetes step-up in `KeboolaClient`), so the two
            # cannot disagree about which stack is ours.
            own_stack_storage_api_url = server_state.own_stack_storage_api_url

            if http_rq := get_http_request_or_none():
                config = self.apply_request_config(http_rq, config, own_stack_storage_api_url=own_stack_storage_api_url)

            # TODO: We could probably get rid of the 'state' attribute set on ctx.session and just
            #  pass KeboolaClient and WorkspaceManager instances to a tool as extra parameters.

            # Skip branch validation for /list requests (tools/list, resources/list, prompts/list, etc.)
            # so that clients can discover available tools even when the configured branch ID doesn't
            # exist yet. For these requests the client is created without a branch ID. Otherwise, the branch is
            # validated via a SAPI call.
            is_list = context.method.endswith('/list')
            if is_list:
                if config.branch_id:
                    LOG.info(f'Skipping branch validation for {context.method} request.')
                config = dataclasses.replace(config, branch_id=None)

            state = await self.create_session_state(
                config, runtime_info, own_stack_storage_api_url=own_stack_storage_api_url, skip_token_exchange=is_list
            )
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
    def apply_request_config(cls, http_rq: Request, config: Config, *, own_stack_storage_api_url: str | None) -> Config:
        """
        Builds the configuration for a single HTTP request by applying the request's headers
        on top of the server's own configuration.

        The Storage API URL is treated specially: it selects the Keboola stack that the server
        talks to on the caller's behalf, using the caller's credentials and, on the deployed
        server, the server's own ServiceAccount identity. When the server has a stack of its own,
        a request asking for a different stack is not honoured — the server's own Storage API URL
        is kept. The check is an exact host match against that single known URL (see
        `is_same_stack()`); no prefix or pattern matching is used.

        A programmatic bearer token (`kbc_at_`/`kbc_pat_`) is likewise treated specially: it
        arrives only as `Authorization: Bearer <token>`, which `Config.replace_by` cannot route
        into `storage_token` (no header/alias maps to it), so it is read directly off the request
        here into an otherwise-empty `storage_token` slot for `create_session_state`'s exchange to
        pick up.

        :param http_rq: The incoming HTTP request whose headers are applied.
        :param config: The server's own configuration (from CLI parameters and environment).
        :param own_stack_storage_api_url: The Storage API URL of the server's own stack
            (`ServerState.own_stack_storage_api_url`), or None when it has no stack of its own and
            therefore takes the URL from the request. It must never come from a request header.
        :return: The configuration to use for this request.
        """
        # Header names only, never values -- Authorization and the X-Storage-(Api-)Token variants
        # carry live credentials, and this server now also routes a programmatic bearer token out
        # of Authorization (see below), so the set of sensitive header names logging could sweep up
        # here is no longer just Storage tokens.
        LOG.debug(f'Injecting headers: http_rq={http_rq}, header_names={list(http_rq.headers.keys())}')
        config = config.replace_by(http_rq.headers)

        # A programmatic bearer token (kbc_at_/kbc_pat_) is sent as `Authorization: Bearer <token>`,
        # never as a Storage-token header/alias -- `Config.replace_by` above has no way to route it
        # into `storage_token` (`Authorization` matches no field name or alias). Read it here, but
        # only into an otherwise-empty slot and only when it actually looks like one of these
        # tokens, so this never overrides an explicit X-Storage-(Api-)Token or forwards an unrelated
        # bearer scheme downstream as if it were a Storage token.
        if not config.storage_token and is_programmatic_token(auth_header := http_rq.headers.get('Authorization')):
            config = dataclasses.replace(config, storage_token=strip_bearer(auth_header))

        if own_stack_storage_api_url and not is_same_stack(config.storage_api_url, own_stack_storage_api_url):
            LOG.warning(
                f'Ignoring the requested Storage API URL "{config.storage_api_url}"; '
                f'this server only serves "{own_stack_storage_api_url}".'
            )
            config = dataclasses.replace(config, storage_api_url=own_stack_storage_api_url)

        if user := http_rq.scope.get('user'):
            LOG.debug(f'Injecting bearer and SAPI tokens: user={user}, access_token={user.access_token}')
            assert isinstance(user, AuthenticatedUser), f'Expecting AuthenticatedUser, got: {type(user)}'
            assert isinstance(user.access_token, ProxyAccessToken), (
                f'Expecting ProxyAccessToken, got: {type(user.access_token)}'
            )
            config = dataclasses.replace(
                config,
                storage_token=user.access_token.sapi_token,
                bearer_token=user.access_token.delegate.token,
            )

        return config

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
        *,
        own_stack_storage_api_url: str | None,
        skip_token_exchange: bool = False,
    ) -> dict[str, Any]:
        """
        Creates `KeboolaClient` and `WorkspaceManager` instances and returns them in the session state.

        :param config: The MCP server configuration, already amended with the request's headers
            (see `apply_request_config()`).
        :param runtime_info: The MCP server runtime information.
        :param readonly: If True, the `KeboolaClient` will only use HTTP GET, HEAD operations.
        :param own_stack_storage_api_url: The Storage API URL of the server's own stack
            (`ServerState.own_stack_storage_api_url`), or None when it has no stack of its own. Passed
            to the `KeboolaClient`, which sends the Kubernetes ServiceAccount step-up header only when
            the session talks to that stack. It must never come from a request header, so it is passed
            separately instead of being read off `config`, whose Storage API URL a header can set.
        :param skip_token_exchange: Skip the programmatic-token exchange (used for capability-discovery
            `/list` requests -- see `on_request`). The resolver call has up to a ~35s timeout; a client's
            initial `tools/list` fetch must be fast, so `/list` builds its `KeboolaClient` with the raw,
            unexchanged token instead. Storage/metastore calls made with it fail fast (a normal 401/403,
            not a hang) and are already handled as a soft failure (e.g. `project_has_semantic_models`
            fails closed) -- the same trade-off this method already makes for branch validation on `/list`.
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
            if is_programmatic_token(storage_token) and not skip_token_exchange:
                # A Keboola programmatic token (kbc_at_/kbc_pat_) is not a Storage token; exchange
                # it for the project's legacy Storage token and use that downstream unchanged.
                storage_token = await cls._exchange_programmatic_token(config)
                bearer_token = None

            client = await KeboolaClient(
                storage_api_url=config.storage_api_url,
                storage_api_token=storage_token,
                bearer_token=bearer_token,
                headers=cls._get_headers(runtime_info),
                readonly=readonly,
                own_stack_storage_api_url=own_stack_storage_api_url,
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
            # overridable per request. An unforgeable path is not enough on its own, because the
            # destination can come from a header — `KeboolaClient.step_up_storage_client()`
            # therefore attaches the JWT only when the target is this server's own stack.
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

        client = KeboolaClient.from_state(context.fastmcp_context.session.state)
        has_semantic_models = await project_has_semantic_models(client)
        if not has_semantic_models:
            tools = [t for t in tools if not is_semantic_tool(t)]

        return tools

    @staticmethod
    def authorize_tool_call(
        *,
        tool_name: str,
        is_read_only: bool,
        is_semantic: bool,
        has_semantic_models: bool,
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

        if is_semantic and not has_semantic_models:
            return (
                f'The tool "{tool_name}" is not available in this project. '
                'This project has no semantic models, so semantic tools are unavailable.'
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

        has_semantic_models = False
        if is_semantic_tool(tool):
            client = KeboolaClient.from_state(context.fastmcp_context.session.state)
            has_semantic_models = await project_has_semantic_models(client)

        denial = self.authorize_tool_call(
            tool_name=tool.name,
            is_read_only=is_read_only_tool(tool),
            is_semantic=is_semantic_tool(tool),
            has_semantic_models=has_semantic_models,
            token_role=self.get_token_role(token_info),
            features=self.get_project_features(token_info),
            is_oauth=self._is_oauth_authenticated(context.fastmcp_context),
            is_main_branch=self.is_client_using_main_branch(context.fastmcp_context),
        )
        if denial:
            raise ToolError(denial)

        return await call_next(context)


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
