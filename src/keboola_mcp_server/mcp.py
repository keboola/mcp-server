"""
This module overrides FastMCP.add_tool() to improve conversion of tool function docstrings
into tool descriptions.
It also provides a decorator that MCP tool functions can use to inject session state into their Context parameter
and other utilities for the MCP server.
"""

import asyncio
import dataclasses
import logging
import textwrap
from collections.abc import Awaitable, Callable, Iterable, Sequence
from contextvars import ContextVar
from http import HTTPStatus
from typing import Any, TypeVar, cast
from unittest.mock import MagicMock

import httpx
import toon_format
from fastmcp import Context, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server import middleware as fmw
from fastmcp.server.dependencies import get_http_request
from fastmcp.server.middleware import CallNext, MiddlewareContext
from fastmcp.tools import FunctionTool, Tool, ToolResult
from mcp import types as mt
from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from pydantic import BaseModel
from pydantic_core import to_json
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.anonymous_entry import (
    credential_unchanged,
    is_anonymous_entry,
    only_provisioned_credential,
    strip_credentials,
)
from keboola_mcp_server.auth_login import (
    exchange_scoped_token,
    forget_rejected_access_token,
    get_access_token,
    introspect_token,
    load_tokens,
)
from keboola_mcp_server.clients.auth_bridge import (
    StorageTokenExchangeError,
    StorageTokenResolver,
    is_programmatic_token,
    strip_bearer,
)
from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.config import (
    Config,
    ServerRuntimeInfo,
    build_tracing_headers,
    deployed_sa_token_path,
    is_same_stack,
)
from keboola_mcp_server.oauth import ProxyAccessToken
from keboola_mcp_server.provisioned_session import (
    ProvisionedSessionEnded,
    ProvisionedSessionRotationLost,
)
from keboola_mcp_server.provisioned_session import (
    handle_unauthorized as handle_provisioned_session_unauthorized,
)
from keboola_mcp_server.provisioned_session import (
    resolve_session as resolve_provisioned_session,
)
from keboola_mcp_server.scope import (
    OAUTH_SESSION_ID_KEY,
    SCOPE_KEY,
    SCOPE_TOKEN_ARG,
    ScopeTokenUnreadable,
    SessionScope,
    persist_scope,
    resolve_provisioned_binding_aad,
    resolve_scope_binding_aad,
    resolve_scope_key,
)
from keboola_mcp_server.session_store.kai_scope import KaiScopeStore
from keboola_mcp_server.session_store.repository import SessionStore
from keboola_mcp_server.tools.constants import (
    BOOTSTRAP_TOOLS,
    MODIFY_FLOW_TOOL_NAME,
    SEMANTIC_TOOLS_TAG,
    UPDATE_FLOW_TOOL_NAME,
)
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)
CONVERSATION_ID = 'conversation_id'
# The branch a tools/list request was configured for. Its session client is always built on the main
# branch (see `SessionStateMiddleware.on_request`), so `ToolsFilteringMiddleware` reads this to apply
# the branch-gated filters.
LISTED_BRANCH_ID = 'listed_branch_id'
# The stack URL this session resolved to (header, env or CLI). Kept in the session state because a
# bootstrap session (no credential yet, see `create_session_state`) has no KeboolaClient to read it
# from, and `ServerState.config` holds only the server-level value, not the request's.
STORAGE_API_URL = 'storage_api_url'

# The project the current request runs against, stamped on every log record (see
# `install_project_id_log_field`) so logs can be filtered per project/organization in Datadog.
_PROJECT_ID_LOG_CTX: ContextVar[str | None] = ContextVar('project_id_log_ctx', default=None)


def install_project_id_log_field() -> None:
    """Adds `project_id` to every log record; the JSON formatter emits it as a top-level attribute."""
    factory = logging.getLogRecordFactory()
    if getattr(factory, '_stamps_project_id', False):
        return

    def stamping_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = factory(*args, **kwargs)
        if project_id := _PROJECT_ID_LOG_CTX.get():
            record.project_id = project_id
        return record

    stamping_factory._stamps_project_id = True
    logging.setLogRecordFactory(stamping_factory)


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
    'get_data_app_preview_link',
}

# The tools a session created through the ChatGPT plugin (bound to the global router's resource) may see and call: an
# explicit allow-list, so that a tool added later stays hidden there until it is reviewed for the plugin directory
# (OpenAI scans the plugin's tool list and holds new tools until approved). Not listed: `run_sync_action` (an arbitrary
# action on any component, a generic executor by OpenAI's rules), `create_project` (an unowned PAYG project, next to
# the commerce rules), and the tools that hand out a secret, which OpenAI forbids in responses:
# `create_python_js_data_app_git_credential` and `modify_python_js_data_app` (git URLs with an embedded secret),
# `delete_python_js_data_app_draft` (part of that git workflow) and `get_data_app_preview_link` (a URL that carries a
# secret and skips the app login).
DIRECTORY_TOOLS = frozenset(
    {
        'add_config_row',
        'create_conditional_flow',
        'create_config',
        'create_flow',
        'create_oauth_url',
        'create_sql_transformation',
        'create_stream',
        'deploy_data_app',
        'docs_query',
        'find_component_id',
        'get_accessible_projects',
        'get_buckets',
        'get_components',
        'get_config_examples',
        'get_configs',
        'get_data_apps',
        'get_flow_examples',
        'get_flow_schema',
        'get_flows',
        'get_jobs',
        'get_project_info',
        'get_semantic_context',
        'get_semantic_schema',
        'get_shared_buckets',
        'get_streams',
        'get_tables',
        'link_shared_bucket',
        'modify_flow',
        'modify_streamlit_data_app',
        'query_data',
        'run_job',
        'search',
        'search_semantic_context',
        'set_project_scope',
        'update_config',
        'update_config_row',
        'update_descriptions',
        'update_flow',
        'update_project_description',
        'update_sql_transformation',
        'validate_semantic_query',
    }
)


def _is_unauthorized(error: BaseException) -> bool:
    """True when `error` is, or was caused by, a 401 from a Keboola API.

    The chain matters: `tool_errors()` re-raises most failures as a `ToolError` carrying the
    original as its `__cause__`, so checking only the outermost exception would miss exactly the
    tool calls this is meant to notice.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, httpx.HTTPStatusError) and current.response.status_code == HTTPStatus.UNAUTHORIZED:
            return True
        current = current.__cause__ or current.__context__
    return False


def is_read_only_tool(tool: Tool) -> bool:
    """Check if a tool has read_only_hint=True annotation."""
    if tool.annotations is None:
        return False
    return tool.annotations.read_only_hint is True


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
    session_store: SessionStore | None = None
    kai_scope_store: KaiScopeStore | None = None
    _token_resolvers: dict[str, StorageTokenResolver] = dataclasses.field(
        default_factory=dict, compare=False, repr=False
    )

    def storage_token_resolver(self, storage_api_url: str | None) -> StorageTokenResolver | None:
        """The auth-bridge resolver for `storage_api_url`, or None when this process is not the
        deployed server.

        One instance per stack URL, reused for the life of the server -- each holds its own pooled
        `httpx.AsyncClient` (see `StorageTokenResolver`), so a fanned-out multi-project call, and
        every request after it, reuse one connection instead of a TLS handshake per resolve. This
        is also the single place that answers "am I the deployed server?" for token exchange --
        `KeboolaClient.create` just uses the resolver it is handed, or skips the exchange when
        handed None.
        """
        path = deployed_sa_token_path()
        if not path or not storage_api_url:
            return None
        # `path` is not part of the cache key: it comes from the process environment only
        # (KBC_KUBERNETES_TOKEN_PATH), so it is a constant for the life of this ServerState.
        if (resolver := self._token_resolvers.get(storage_api_url)) is None:
            resolver = self._token_resolvers[storage_api_url] = StorageTokenResolver(
                storage_api_url=storage_api_url, kubernetes_token_path=path
            )
        return resolver

    async def aclose(self) -> None:
        """Closes every pooled connection held by `storage_token_resolver`. Call once, from the
        server's lifespan teardown."""
        for resolver in self._token_resolvers.values():
            await resolver.aclose()
        self._token_resolvers.clear()

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
    """Lets a client address a mounted app without the trailing slash it is mounted at.

    :param mount_paths: The paths to fix up. Defaults to the streamable-HTTP endpoint alone; the
        unauthenticated one (`ANONYMOUS_MOUNT_PATH`) is added when that endpoint is served, since a
        newcomer pasting that URL into a client will leave the slash off exactly as often.
    """

    def __init__(self, app: ASGIApp, mount_paths: Sequence[str] = ('/mcp',)):
        self._app = app
        self._mount_paths = frozenset(mount_paths)

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        LOG.debug(f'ForwardSlashMiddleware: scope={scope}')

        if scope['type'] == 'http':
            path = scope['path']
            if path in self._mount_paths:
                scope = dict(scope)
                scope['path'] = f'{path}/'

        await self._app(scope, receive, send)


LEGACY_SSE_PATHS = frozenset({'/sse', '/sse/'})
STREAMABLE_HTTP_PATH = '/mcp/'


class LegacySsePathMiddleware:
    """
    Serves connector URLs configured before the SSE transport was removed (AI-4033).

    Requests to `/sse` and `/sse/` are routed to the streamable-HTTP endpoint (`/mcp/`), so clients
    that POST `initialize` first keep working with the same auth and middleware. A legacy SSE client
    opening its event stream (a GET on `/sse` carrying neither `Mcp-Session-Id` nor `MCP-Protocol-Version`)
    gets `410 Gone` naming the new URL instead of a standalone stream that never delivers an `endpoint`
    event.

    Legacy SSE clients cannot be told apart from others by their headers, so this is a trade-off: a
    streamable-HTTP client that sends neither header also gets the 410. That is possible with the default
    stateless mode (no session id) and a 2025-03-26-era client, as `MCP-Protocol-Version` was only mandated
    in 2025-06-18. Its optional GET stream on `/sse` is answered with 410, where `/mcp` answers 405 (the
    spec's "no GET stream" signal); its POSTs are unaffected.
    """

    def __init__(self, app: ASGIApp, public_url: str | None = None):
        self._app = app
        self._public_url = public_url.rstrip('/') if public_url else None

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope['type'] == 'http' and scope['path'] in LEGACY_SSE_PATHS:
            if self._is_legacy_sse_handshake(scope):
                await self._send_gone(scope, receive, send)
                return
            scope = dict(scope)
            scope['path'] = STREAMABLE_HTTP_PATH
            scope['raw_path'] = STREAMABLE_HTTP_PATH.encode()

        await self._app(scope, receive, send)

    @staticmethod
    def _is_legacy_sse_handshake(scope: Scope) -> bool:
        if scope['method'] != 'GET':
            return False
        header_names = {name.lower() for name, _ in scope.get('headers', [])}
        return b'mcp-session-id' not in header_names and b'mcp-protocol-version' not in header_names

    async def _send_gone(self, scope: Scope, receive: Receive, send: Send) -> None:
        request = Request(scope)
        if self._public_url:
            mcp_url = f'{self._public_url}/mcp'
        else:
            mcp_url = str(request.url.replace(path=f'{scope.get("root_path", "")}/mcp', query=''))
        LOG.info(f'Rejecting legacy SSE handshake on {scope["path"]}; user-agent={request.headers.get("user-agent")}')
        response = JSONResponse(
            status_code=HTTPStatus.GONE,
            content={
                'error': 'sse_transport_removed',
                'message': (
                    'The SSE transport is no longer supported. '
                    f'Reconfigure your MCP client to use the Streamable HTTP endpoint: {mcp_url}'
                ),
                'mcp_url': mcp_url,
            },
        )
        await response(scope, receive, send)


class KeboolaMcpServer(FastMCP):
    def add_tool(self, tool: Tool) -> None:
        """Applies `textwrap.dedent()` function to the tool's docstring, if no explicit description is provided."""
        if tool.description:
            description = textwrap.dedent(tool.description).strip()
            if description != tool.description:
                tool = tool.model_copy(update={'description': description})

        super().add_tool(tool)


def get_http_request_or_none() -> Request | None:
    try:
        return get_http_request()
    except RuntimeError:
        return None


def is_directory_session() -> bool:
    """`is_directory_request` for the request of the MCP call being handled."""
    return is_directory_request(get_http_request_or_none())


def is_directory_request(http_rq: Request | None) -> bool:
    """Whether the request comes from a session bound to the global router's resource (`ProxyAccessToken.
    directory_profile`), which gets only the `DIRECTORY_TOOLS`. Read from the session row through the bearer token,
    before and independently of any Keboola token verification, so no fallback path can widen it."""
    user = http_rq.scope.get('user') if http_rq is not None else None
    return (
        isinstance(user, AuthenticatedUser)
        and isinstance(user.access_token, ProxyAccessToken)
        and user.access_token.directory_profile
    )


def directory_denial(tool_name: str) -> str:
    return (
        f'The tool "{tool_name}" is not available in the Keboola plugin. '
        'Use Keboola through its own MCP server URL to reach every tool.'
    )


#: Exceptions this middleware raises whose message *is* the answer the caller needs, rather than a
#: failure to be reported as one. They have to leave as `ToolError`, because anything else raised
#: from a middleware is not a tool result: FastMCP masks it and the caller is handed "Internal
#: server error" instead of the explanation -- which is precisely backwards for these three.
#:
#: Found on canary-orion: claiming a provisioned project did end the session exactly as designed
#: (Connection refused the refresh token, the row was revoked, `session_ended` was raised with the
#: right text) and the caller still saw only "Internal server error" -- the one outcome requirement
#: 5 of the RFC exists to prevent.
_CALLER_FACING_ERRORS = (ProvisionedSessionEnded, ProvisionedSessionRotationLost, ScopeTokenUnreadable)


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
        """Sets up the session state (see `_on_request`), turning the few failures that are really
        *answers* into ones the caller can read.

        See `_CALLER_FACING_ERRORS`: their messages are written for whoever made the call -- the
        project was claimed, the scope_token cannot be read, the refreshed credentials could not be
        stored -- and each tells them what to do next. Raised from a middleware they would reach an
        HTTP caller as "Internal server error" instead.
        """
        try:
            return await self._on_request(context, call_next)
        except _CALLER_FACING_ERRORS as e:
            raise ToolError(str(e)) from e

    async def _on_request(
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

        config: Config | None = None
        scope: SessionScope | None = None
        server_state: ServerState | None = None
        if not isinstance(ctx.session, MagicMock):
            server_state = ServerState.from_context(ctx)
            config = server_state.config
            runtime_info: ServerRuntimeInfo = server_state.runtime_info

            # IMPORTANT: Since mcp 1.12.4 and fastmcp 2.11 the fastmcp.server.dependencies.get_http_request()
            #   returns the same object as ctx.request_context.request.

            # Resolved once here and threaded through both stack checks (the per-request Storage
            # API URL pinning below and the Kubernetes step-up in `KeboolaClient`), so the two
            # cannot disagree about which stack is ours.
            own_stack_storage_api_url = server_state.own_stack_storage_api_url

            if http_rq := get_http_request_or_none():
                config = self.apply_request_config(http_rq, config, own_stack_storage_api_url=own_stack_storage_api_url)
            # Whether this request came in on the unauthenticated endpoint. Every "find this session
            # a credential" path below is skipped for one, because each of them would lend it an
            # identity belonging to somebody else: the operator's own token, or the local user's
            # login. See `anonymous_entry`.
            anonymous = is_anonymous_entry(http_rq)

            # Capability-discovery requests (tools/list, prompts/list, resources/list) MUST be fast: a
            # client fetches all three on connect, so any Connection AUTH round-trip here (token
            # introspect, refresh, or scoped-exchange) makes connecting hang until the client's 30s
            # timeout. For /list we skip all that extra auth work — no auto-lease, no token refresh, no
            # scoped re-mint — and use the stored session token as-is. (create_session_state below may
            # still make ordinary Storage calls, e.g. WorkspaceManager.create; the point is /list adds
            # none of the introspect/refresh/exchange round-trips.) Scope and fresh tokens are
            # established on the first real (non-list) tool call.
            is_list = context.method.endswith('/list')

            # Local streamable-HTTP with no token supplied (no header / env): fall back to the stored
            # PKCE session. For non-list requests keep it fresh (refresh + persist rotation); for /list
            # read it without a network refresh. No-op when a token is provided or on the deployed
            # server (KBC_KUBERNETES_TOKEN_PATH set).
            if not anonymous:
                config = await self._maybe_use_stored_session(config, refresh=not is_list)

            # In-conversation multi-project scope is carried by the caller as the `scope_token` tool
            # argument (see SessionScope.to_token/from_token) rather than read back from
            # ctx.session.state, which is rebuilt empty on every request under this server's default
            # stateless-HTTP transport. With no scope and no preset project, auto-lease ALL accessible
            # projects (multi-project mode) so read tools fan out across everything — but never on /list.
            scope = self._read_scope_from_request(context, config, self._read_oauth_session_id(http_rq))
            # OAuth-authenticated sessions don't need scope_token at all: the opaque OAuth access
            # token is already resent on every call and resolves through the Postgres session store
            # (load_access_token), so a confirmed scope persisted there (via set_project_scope ->
            # SessionStore.update_scope) is read back here instead of round-tripping it as an argument.
            if scope is None:
                scope = self._read_persisted_oauth_scope(http_rq)
            # stdio and --no-stateless-http streamable-http reuse the same ctx.session object (and
            # its .state dict) across every request in the conversation, unlike the stateless-http
            # default where FastMCP hands out a fresh session per request. On those transports, a
            # scope already confirmed by an earlier set_project_scope call is still sitting in
            # ctx.session.state -- reuse it instead of falling back to scope_token/auto-lease, so
            # the caller never needs to resend scope_token at all.
            if scope is None and runtime_info.session_state_persists:
                scope = self._read_persisted_local_scope(ctx)
            # Deployed, non-OAuth, programmatic-token sessions (Kai) carry no MCP-minted
            # identifier and no persistent ctx.session -- kai_session_scope RFC persists their
            # confirmed scope server-side instead, keyed by (conversation_id, token user id).
            if (
                scope is None
                and not is_list
                and config.conversation_id
                and deployed_sa_token_path()
                and self._oauth_access_token(http_rq) is None
                and is_programmatic_token(config.storage_token)
                and server_state.kai_scope_store is not None
            ):
                scope = await self._read_persisted_kai_scope(config, server_state.kai_scope_store)
            # Local sessions are scoped at `login` time now (see the "Security hardening" RFC
            # increment) -- a persisted choice, once one exists, is used as a confirmed scope with
            # no ask-first gate needed. Only a credential predating this choice (or a token
            # supplied directly, never run through `login`) falls through to the old
            # auto-lease-then-ask-first default below.
            if scope is None and not config.project_id and not is_list and not anonymous:
                scope = await self._read_persisted_login_scope(config)
            if scope is None and not config.project_id and not is_list and not anonymous:
                scope = await self._autolease_default_scope(config)
            # A project provisioned by `create_project` on this (deployed) server keeps its
            # credentials in the session store, not in the caller's hands -- the caller only echoes
            # the row's handle inside `scope_token`. Resolve it into a usable token before anything
            # else needs one. Done for /list too: unlike the login-store paths above this is a cheap
            # Postgres read, and without it a listing for such a session has no credentials at all.
            config = await resolve_provisioned_session(config, scope, server_state.session_store)
            # Belt and braces for the one endpoint where a mistake is not recoverable: the call above
            # is the only thing that can put a token into an anonymous request's config, and only for
            # a session whose handle the caller already holds. Checked here and again after the
            # token-resolution block below, which does not know who it is serving -- see
            # `credential_unchanged`.
            anonymous_credential = (
                only_provisioned_credential(config, scope.provisioned_session_id if scope else None)
                if anonymous
                else None
            )
            if not is_list:
                scoped_token_before = scope.scoped_token if scope is not None else None
                config, scope = await self._resolve_local_tokens(config, scope, anonymous=anonymous)
                # _resolve_local_tokens re-mints a near-expiry scoped_token for OAuth/deployed
                # sessions too (not just local ones) -- persist the refresh to the OAuth session row
                # immediately, so it's not silently re-attempted (and re-written) on every single
                # request for the rest of this token's lifetime, only once per actual expiry.
                if (
                    scope is not None
                    and scope.scoped_token != scoped_token_before
                    and (oauth_session_id := self._read_oauth_session_id(http_rq)) is not None
                    and server_state.session_store is not None
                ):
                    await persist_scope(server_state.session_store, oauth_session_id, scope)

            if anonymous:
                credential_unchanged(config, anonymous_credential)

            # TODO: We could probably get rid of the 'state' attribute set on ctx.session and just
            #  pass KeboolaClient and WorkspaceManager instances to a tool as extra parameters.

            # Skip branch validation for /list requests (tools/list, resources/list, prompts/list, etc.)
            # so that clients can discover available tools even when the configured branch ID doesn't
            # exist yet. For these requests the client is created without a branch ID. Otherwise, the branch is
            # validated via a SAPI call. tools/list keeps the configured ID aside for its branch filters.
            listed_branch_id = config.branch_id if context.method == 'tools/list' else None
            if is_list:
                if config.branch_id:
                    LOG.info(f'Skipping branch validation for {context.method} request.')
                config = dataclasses.replace(config, branch_id=None)

            # A read-only confirmed scope is enforced locally too (not just by the remote scoped
            # token, which may not exist -- see set_project_scope's exchange-failure fallback and
            # the "Security hardening" RFC increment): the base session client itself is built
            # read-only whenever the scope requests it, success or failure of the token exchange.
            readonly = True if scope is not None and scope.read_only else None
            state = await self.create_session_state(
                config,
                runtime_info,
                readonly=readonly,
                own_stack_storage_api_url=own_stack_storage_api_url,
                # /list must add no Connection auth round-trips (see the is_list comment above):
                # no resolver means no auth-bridge exchange. Mirrors `refresh=not is_list` above.
                token_resolver=None if is_list else server_state.storage_token_resolver(config.storage_api_url),
            )
            if scope is not None:
                state[SCOPE_KEY] = scope
            if oauth_session_id := self._read_oauth_session_id(http_rq):
                state[OAUTH_SESSION_ID_KEY] = oauth_session_id
            if listed_branch_id:
                state[LISTED_BRANCH_ID] = listed_branch_id
            ctx.session.state = state

        try:
            return await call_next(context)
        except Exception as e:
            if config is not None and _is_unauthorized(e):
                await self._handle_unauthorized(config, scope, server_state)
            raise
        finally:
            # NOTE: This line is commented following a bug related to session state clearance in Claude client
            # ctx.session.state = {}
            pass

    @classmethod
    async def _handle_unauthorized(
        cls,
        config: Config,
        scope: 'SessionScope | None' = None,
        server_state: ServerState | None = None,
    ) -> None:
        """Drops a locally stored session whose credential Connection no longer accepts.

        An expired session already self-heals: the refresh fails and `get_access_token` forgets it.
        A *revoked* one does not -- an agent-provisioned session is revoked the moment the human
        confirms the project (agent_provisioning RFC), so its access token starts 401ing while it
        still looks fresh and no refresh is ever attempted. Without this, every remaining call in
        that hour 401s with no way out.

        Deleting a credential is destructive (the next session has to log in again), so all three
        of these must hold before anything is removed:

        * The request used the **stored** token. A token supplied per request (a header on a
          streamable-http call) is not this session's stored credential, and its 401 says nothing
          about the stored one.
        * Re-checking the token with `introspect_token` fails **with an authentication error**.
          A 401 alone is not proof the credential is dead: an unconfirmed or stale project scope
          produces one too (see `RawKeboolaClient._raise_for_status`). A timeout, a connection
          error or a 5xx proves even less -- the credential is kept and the caller still sees the
          original error.
        * The stored entry is *still* that same token when the delete happens -- see
          `forget_rejected_access_token`, which compares under the credential-store locks, so a
          concurrent refresh or a fresh `login` is never thrown away.
        """
        if (
            scope is not None
            and scope.provisioned_session_id is not None
            and server_state is not None
            and server_state.session_store is not None
        ):
            await handle_provisioned_session_unauthorized(config, scope, server_state.session_store)
            return
        if not cls._is_local_programmatic(config):
            return
        storage_api_url = cast(str, config.storage_api_url)
        access_token = strip_bearer(cast(str, config.storage_token))
        tokens = load_tokens(storage_api_url)
        if tokens is None or tokens.access_token != access_token:
            return
        try:
            await introspect_token(storage_api_url, subject_token=access_token)
            return  # the credential itself is fine; the 401 was about something else
        except httpx.HTTPStatusError as e:
            if e.response.status_code not in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
                LOG.warning(f'Could not verify the stored session for {storage_api_url}; keeping it: {e}')
                return
        except Exception as e:
            # Transport error, timeout, DNS -- says nothing about the credential.
            LOG.warning(f'Could not verify the stored session for {storage_api_url}; keeping it: {e}')
            return
        if await forget_rejected_access_token(storage_api_url, access_token):
            LOG.info(f'The stored session for {storage_api_url} was rejected by Connection; dropped it.')

    async def on_list_tools(
        self,
        context: fmw.MiddlewareContext[mt.ListToolsRequest],
        call_next: fmw.CallNext[mt.ListToolsRequest, list[Tool]],
    ) -> list[Tool]:
        """Advertises the optional `scope_token` argument on every tool.

        Unconditional (unlike MultiProjectMiddleware's `_PROJECT_FILTER_ARG` patch, which is gated on
        an active multi-project scope): a `tools/list` request cannot itself carry `scope_token`, so
        whether a scope is currently confirmed can't be known while building this response. Showing
        the parameter always costs nothing when unused and is what lets the caller learn about it
        before ever calling `set_project_scope`.

        Skipped entirely when this session's transport persists `ctx.session.state` across requests
        (stdio, or streamable-http with `--no-stateless-http`) -- there, `on_request` reuses the
        already-confirmed scope straight from that state, so `scope_token` is dead weight.
        """
        tools = await call_next(context)
        ctx = getattr(context, 'fastmcp_context', None)
        if (
            ctx is not None
            and isinstance(ctx, Context)
            and ServerState.from_context(ctx).runtime_info.session_state_persists
        ):
            return tools
        patched: list[Tool] = []
        for tool in tools:
            params = dict(tool.parameters or {})
            props = dict(params.get('properties') or {})
            if SCOPE_TOKEN_ARG in props:
                patched.append(tool)
                continue
            props[SCOPE_TOKEN_ARG] = {
                'type': 'string',
                'description': (
                    'Opaque token returned by "set_project_scope" (also echoed by '
                    '"get_accessible_projects" once a scope is confirmed). The server does not '
                    'remember the scope between calls -- resend this value on every tool call in '
                    'this conversation once you have it.'
                ),
            }
            params['properties'] = props
            patched.append(tool.model_copy(update={'parameters': params}))
        return patched

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
        server_config = config
        # Only fields meant to vary per request are settable from a header -- see
        # Config._HEADER_ELIGIBLE_FIELDS / the "Security hardening" RFC increment. In particular
        # this keeps `jwt_secret` (which would otherwise let a caller forge their own scope_token)
        # and the other deployment-level fields permanently unreachable from a request.
        config = config.replace_by_headers(http_rq.headers)

        # A programmatic bearer token (kbc_at_/kbc_pat_) is sent as `Authorization: Bearer <token>`,
        # never as a Storage-token header/alias -- `Config.replace_by` above has no way to route it
        # into `storage_token` (`Authorization` matches no field name or alias). Read it here, but
        # only into an otherwise-empty slot and only when it actually looks like one of these
        # tokens, so this never overrides an explicit X-Storage-(Api-)Token or forwards an unrelated
        # bearer scheme downstream as if it were a Storage token.
        if not config.storage_token and is_programmatic_token(auth_header := http_rq.headers.get('Authorization')):
            config = dataclasses.replace(config, storage_token=strip_bearer(auth_header))

        # A workspace pin (`workspace_id`/`workspace_schema`) configured on the server fences
        # off which project data a request can reach -- mirror the `own_stack_storage_api_url`
        # reasoning above: a deployment that pinned itself must not be overridable by a header.
        # A server with no pin of its own (the shared multi-tenant case, e.g. AJDA-3052's Data
        # App flow) keeps taking the pin from the request, which is the only source it has.
        # Checked/restored together (not per-field): the same silent-override risk that justifies
        # this for an id-based server pin applies just as much to a schema-based one, and a
        # schema-pinned server is a single-project deployment that isn't a target for per-request
        # `X-Workspace-Id` headers in the first place -- see the mcp.py:320 review thread.
        #
        # `workspace_schema` has no `empty_means_absent` (unlike `workspace_id`), so an explicit
        # `X-Workspace-Schema: ''` opt-out (the multi-user pattern the README describes) survives
        # `replace_by` as `''`, not `None` -- comparing it directly against the server's pin here
        # would treat that falsy-but-set value as an override and restore the pin right back,
        # defeating the opt-out. Only a genuinely non-empty, different value counts as a request
        # to override.
        if (server_config.workspace_id or server_config.workspace_schema) and (
            config.workspace_id != server_config.workspace_id
            or (config.workspace_schema and config.workspace_schema != server_config.workspace_schema)
        ):
            LOG.warning(
                f'Ignoring the requested workspace pin (workspace_id={config.workspace_id!r}, '
                f'workspace_schema={config.workspace_schema!r}); this server is pinned to '
                f'workspace_id={server_config.workspace_id!r}, workspace_schema={server_config.workspace_schema!r}.'
            )
            config = dataclasses.replace(
                config,
                workspace_id=server_config.workspace_id,
                workspace_schema=server_config.workspace_schema,
            )

        if own_stack_storage_api_url and not is_same_stack(config.storage_api_url, own_stack_storage_api_url):
            LOG.warning(
                f'Ignoring the requested Storage API URL "{config.storage_api_url}"; '
                f'this server only serves "{own_stack_storage_api_url}".'
            )
            config = dataclasses.replace(config, storage_api_url=own_stack_storage_api_url)

        if user := http_rq.scope.get('user'):
            assert isinstance(user, AuthenticatedUser), f'Expecting AuthenticatedUser, got: {type(user)}'
            assert isinstance(user.access_token, ProxyAccessToken), (
                f'Expecting ProxyAccessToken, got: {type(user.access_token)}'
            )
            # Log only non-sensitive identifiers; ProxyAccessToken's default repr includes the raw
            # kbc_access_token/kbc_refresh_token, which must never be logged.
            LOG.debug(
                f'Injecting exchanged session token: client_id={user.access_token.client_id}, '
                f'session_id={user.access_token.session_id}'
            )
            # The exchanged kbc_at_ token is a Keboola programmatic token; is_programmatic_token()
            # detects it downstream and the full PSGO-261 multi-project machinery applies unchanged.
            config = dataclasses.replace(config, storage_token=user.access_token.kbc_access_token)

        if is_anonymous_entry(http_rq):
            # The unauthenticated endpoint serves exactly one shape of caller: one who has no
            # credential of their own (see `anonymous_entry`). The caller's own headers were already
            # refused at the ASGI layer; what is dropped here is everything the *server* would
            # otherwise lend them -- a `KBC_STORAGE_TOKEN` in the pod's environment, a project pin --
            # so that no request on this path can act under an identity it did not provision for
            # itself. From here the only token it can ever acquire is `resolve_provisioned_session`'s,
            # for a session whose handle it already holds. Last in this method on purpose: whatever
            # is added above, the anonymous endpoint ends up with nothing.
            config = strip_credentials(config)

        return config

    @classmethod
    def _read_scope_from_request(
        cls, context: fmw.MiddlewareContext[Any], config: Config, oauth_session_id: str | None = None
    ) -> 'SessionScope | None':
        """Decodes the ``scope_token`` tool-call argument (if any) back into a SessionScope.

        Pops the argument so it never reaches the tool function, matching how
        MultiProjectMiddleware.on_call_tool consumes _PROJECT_FILTER_ARG. *No* scope_token means
        "no scope yet" -- the ask-first gate in MultiProjectMiddleware then steers the caller back
        through get_accessible_projects / set_project_scope. One that is present but unreadable is
        an error instead (`ScopeTokenUnreadable`): silently continuing would run the call under a
        different scope than the caller believes it is in -- see that exception.

        A token can carry either of two bindings, so both are tried: `set_project_scope` binds to
        the caller's presented credential, while a provisioned project's handle binds to the OAuth
        session id, which (unlike that credential) does not rotate underneath it. Trying both is not
        a weakening -- each candidate is a key the caller already demonstrably holds, and a token
        sealed for anyone else decrypts under neither.
        """
        # context.message always exists (a required MiddlewareContext field); only whether it HAS
        # .arguments varies by request type (a ListToolsRequest has none, a CallToolRequestParams
        # does), hence the single getattr here.
        args = getattr(context.message, 'arguments', None)
        if not isinstance(args, dict):
            return None
        token = args.pop(SCOPE_TOKEN_ARG, None)
        if not token:
            return None
        key = resolve_scope_key(config)
        candidates: list[bytes | None] = []
        for aad in (
            resolve_scope_binding_aad(config.storage_token),
            resolve_provisioned_binding_aad(oauth_session_id),
            None,  # minted for a caller that held no credential to bind to
        ):
            if aad not in candidates:
                candidates.append(aad)
        for aad in candidates:
            try:
                return SessionScope.from_token(token, key, aad=aad)
            except Exception as e:
                # Expected for every candidate but the right one; only all of them failing is news.
                last_error = e
        LOG.warning(f'Received a scope_token that cannot be read under any binding for this session: {last_error}')
        raise ScopeTokenUnreadable(
            'The "scope_token" sent with this call cannot be read: it was issued for a different session, '
            'or by a server that no longer has the key it was sealed with. Stop resending it. If it came '
            'from "set_project_scope", call "get_accessible_projects" and "set_project_scope" again to pick '
            'a project; if it came from "create_project", that temporary session cannot be reached from '
            'this one any more and the project has to be claimed through its confirm_url.'
        )

    @staticmethod
    def _oauth_access_token(http_rq: Request | None) -> ProxyAccessToken | None:
        if http_rq is None:
            return None
        user = http_rq.scope.get('user')
        if not isinstance(user, AuthenticatedUser) or not isinstance(user.access_token, ProxyAccessToken):
            return None
        return user.access_token

    @classmethod
    def _read_persisted_oauth_scope(cls, http_rq: Request | None) -> 'SessionScope | None':
        """The multi-project scope persisted on the OAuth session row, if any.

        Only used as a fallback when the caller sent no ``scope_token`` -- an explicit scope_token
        (e.g. a fresher re-scope from the same request) always takes precedence.
        """
        access_token = cls._oauth_access_token(http_rq)
        if access_token is None or not access_token.scope_confirmed or access_token.scope_project_ids is None:
            return None
        return SessionScope(
            project_ids=access_token.scope_project_ids,
            read_only=access_token.scope_read_only,
            scoped_token=access_token.scope_scoped_token,
            scoped_expires_at=(
                access_token.scope_scoped_expires_at.timestamp()
                if access_token.scope_scoped_expires_at is not None
                else None
            ),
            confirmed=True,
        )

    @classmethod
    def _read_oauth_session_id(cls, http_rq: Request | None) -> str | None:
        access_token = cls._oauth_access_token(http_rq)
        return access_token.session_id if access_token is not None else None

    @staticmethod
    def _read_persisted_local_scope(ctx: Context) -> 'SessionScope | None':
        """The scope confirmed by an earlier ``set_project_scope`` call on this same, still-live
        ``ctx.session`` -- only ever meaningful when the transport pins one session object across
        requests (see ``ServerRuntimeInfo.session_state_persists``); callers must check that first.
        """
        # Real session objects (e.g. MiddlewareServerSession) have no `.state` attribute at all
        # until this middleware sets one on a prior request -- getattr, not direct access.
        state = getattr(ctx.session, 'state', None)
        if not isinstance(state, dict):
            return None
        scope = state.get(SCOPE_KEY)
        return scope if isinstance(scope, SessionScope) else None

    @classmethod
    async def _read_persisted_kai_scope(cls, config: Config, store: KaiScopeStore) -> 'SessionScope | None':
        """The scope confirmed by an earlier `set_project_scope` call on this Kai conversation,
        looked up by `sha256(conversation_id:user_id)` (kai_session_scope RFC) rather than by
        token hash, since Kai refreshes its raw token independently and its value isn't stable
        across that refresh. Drops (and forgets) the stored scope -- rather than auto-narrowing or
        trusting stale access -- if a previously scoped project is no longer reachable by the
        current token; callers see this as "no scope yet" and are steered back through
        get_accessible_projects / set_project_scope.

        Best-effort end to end -- a Postgres outage (`DatabaseUnavailableError`) degrades the same
        way an introspection failure already does: "no scope yet" rather than a crashed request.
        This is the one Postgres-touching path outside the OAuth bearer-auth flow (see
        `DatabaseUnavailableMiddleware`), reachable even without OAuth configured.
        """
        try:
            introspection = await introspect_token(
                config.storage_api_url, subject_token=strip_bearer(config.storage_token)
            )
            if introspection.user_id is None:
                return None
            stored = await store.get(config.conversation_id, introspection.user_id)
            if stored is None:
                return None
            current_project_ids = {p.id for p in introspection.projects}
            if not set(stored.project_ids).issubset(current_project_ids):
                LOG.info('Persisted Kai scope references a project no longer reachable; dropping it.')
                await store.drop(config.conversation_id, introspection.user_id)
                return None
            return SessionScope(project_ids=stored.project_ids, read_only=stored.read_only, confirmed=stored.confirmed)
        except Exception as e:
            LOG.warning(f'Could not resolve persisted Kai scope: {e}', exc_info=True)
            return None

    @classmethod
    def _is_local_programmatic(cls, config: Config) -> bool:
        """True for a local (non-deployed) session carrying a Keboola programmatic token."""
        return (
            not deployed_sa_token_path()
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
        if deployed_sa_token_path():
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
            if tokens.is_near_expiry:
                # The stored access token is (near) expired; using it as-is would make the /list
                # session-state build fail its Storage calls. Refresh only this case via the network —
                # valid tokens still take the network-free fast path so /list never blocks on connect.
                try:
                    access_token = await get_access_token(config.storage_api_url)
                except RuntimeError:
                    return config
            else:
                access_token = tokens.access_token
        return dataclasses.replace(config, storage_token=access_token)

    @classmethod
    async def _read_persisted_login_scope(cls, config: Config) -> 'SessionScope | None':
        """The project scope chosen at `login` time (see `auth_login.TokenSet.project_ids`), if
        any -- "Security hardening" RFC increment. Returns None (falls back to the old
        auto-lease-all default) when this isn't a local programmatic session, or the stored
        credential predates this choice / was never run through `login`'s prompt.
        """
        if not cls._is_local_programmatic(config):
            return None
        tokens = load_tokens(config.storage_api_url)
        if tokens is None or tokens.project_ids is None:
            return None
        return SessionScope(project_ids=tokens.project_ids, read_only=tokens.read_only, confirmed=True)

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
            parent = strip_bearer(config.storage_token)
        try:
            introspection = await introspect_token(config.storage_api_url, subject_token=parent)
        except Exception as e:
            LOG.warning(f'Could not auto-lease projects from token introspection: {e}', exc_info=True)
            return None
        project_ids = [p.id for p in introspection.projects]
        if not project_ids:
            return None
        LOG.info(f'Multi-project mode: auto-leased {len(project_ids)} accessible project(s) as the default scope.')
        return SessionScope(project_ids=project_ids)

    @classmethod
    async def _resolve_local_tokens(
        cls, config: Config, scope: 'SessionScope | None', *, anonymous: bool = False
    ) -> 'tuple[Config, SessionScope | None]':
        """
        For local (non-deployed) programmatic-token sessions, keep tokens fresh during usage.

        Refreshes the stored whole-stack (parent) token via the PKCE credential store. When the user
        has explicitly narrowed scope (a minted scoped token is present), that token is re-minted from
        the parent when it nears expiry. The default (auto-leased) multi-project scope carries no
        minted token and simply uses the parent token, narrowed per request by ``X-KBC-ProjectId``.
        On the deployed server (``KBC_KUBERNETES_TOKEN_PATH`` set), ``config.storage_token`` is
        already the freshly-refreshed OAuth ``kbc_access_token`` (refreshed by
        ``SimpleOAuthProvider.load_access_token``'s lazy refresh before this ever runs) -- that part
        needs no help here. Nothing else threads a confirmed scope's active project id into
        ``config`` for a deployed session, though: without it, ``create_session_state`` keeps
        building the active client from the unscoped whole-stack token with no ``X-KBC-ProjectId``,
        so every call after ``set_project_scope`` 401s even though scoping itself succeeded -- apply
        just the active project id here. The confirmed scope's own ``scoped_token`` (minted once by
        ``set_project_scope``, used by ``MultiProjectMiddleware`` for every fanned-out project once
        2+ are scoped -- including the first) *does* need the same near-expiry re-mint the local
        branch below does, or it silently starts 401ing mid-conversation once it expires, with no
        refresh ever attempted for the rest of the session (`on_request` persists the refreshed
        token back to the OAuth session row afterward, so this happens at most once per expiry, not
        every request).
        """
        # An anonymous request is never a local programmatic session, whatever its token looks
        # like: it has no login of its own to be kept fresh from. Without this it would take the
        # local branch below -- a provisioned `kbc_at_` token on a non-deployed server reads exactly
        # like one -- and `get_access_token` would replace it with whatever the *host* is logged in
        # as, handing an unauthenticated caller the operator's identity on any dev machine running
        # with `KBC_ALLOW_ANONYMOUS_PROVISIONING`. The branch it takes instead only narrows the
        # config to the scope's own project and re-mints a near-expiry scoped token, both of which
        # are the caller's own.
        if anonymous or not cls._is_local_programmatic(config):
            if scope and scope.project_ids:
                # Always the scope's own active project, never a caller-supplied X-KBC-ProjectId --
                # project_id is header-eligible (Config._HEADER_ELIGIBLE_FIELDS), and once a scope is
                # confirmed the header must not be able to silently redirect the base client to a
                # project outside (or merely different from) what the user confirmed. A tool wanting a
                # *different* one of the scoped projects still has its own project_id argument
                # (MultiProjectMiddleware._dispatch_single_target), validated against scope.project_ids
                # there -- this is only about which project the un-swapped base client targets.
                config = dataclasses.replace(config, project_id=str(scope.active_project_id))
            if scope is not None and scope.scoped_token is not None and scope.is_near_expiry:
                try:
                    minted = await exchange_scoped_token(
                        config.storage_api_url,
                        subject_token=strip_bearer(config.storage_token),
                        project_ids=scope.project_ids,
                        read_only=scope.read_only,
                    )
                    scope = dataclasses.replace(
                        scope, scoped_token=minted.access_token, scoped_expires_at=minted.expires_at
                    )
                except Exception as e:
                    # Don't break the session if re-minting fails -- the caller keeps using the
                    # (possibly already-expired) scoped_token, same failure mode as before this fix.
                    LOG.warning(f'Could not refresh the deployed session scoped token: {e}', exc_info=True)
            return config, scope

        # Strip any inbound `Bearer ` scheme; introspect/exchange helpers add the scheme themselves,
        # so a pre-prefixed token would produce an `Authorization: Bearer Bearer …` header.
        parent = strip_bearer(config.storage_token)
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
                        LOG.warning(f'Could not refresh the scoped token; using the parent token: {e}', exc_info=True)
                        scope = dataclasses.replace(scope, scoped_token=None, scoped_expires_at=None)
                token = scope.scoped_token or parent

        config = dataclasses.replace(config, storage_token=token, project_id=project_id)
        return config, scope

    @classmethod
    async def create_session_state(
        cls,
        config: Config,
        runtime_info: ServerRuntimeInfo,
        readonly: bool | None = None,
        *,
        own_stack_storage_api_url: str | None,
        token_resolver: StorageTokenResolver | None = None,
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
        :param token_resolver: The auth-bridge resolver `KeboolaClient.create` uses to exchange a
            programmatic token for a project's legacy Storage token, or None to skip that exchange
            (a local/non-deployed server, or a request -- `/list` -- that must add no auth
            round-trips; see the `on_request` call site).
        :return: The session state dictionary containing the created client and workspace manager instances.
        """
        _PROJECT_ID_LOG_CTX.set(config.project_id)
        LOG.info(f'Creating SessionState from config: {config}.')

        state: dict[str, Any] = {STORAGE_API_URL: config.storage_api_url}

        if not config.storage_token:
            # Bootstrap session (agent_provisioning RFC): no credential anywhere -- no header, no
            # env, no stored login. The session carries no KeboolaClient and no WorkspaceManager;
            # `KeboolaClient.from_state` tells any tool that needs one how to get a credential, and
            # `create_project` runs without one to obtain the first. Keeping the old hard failure
            # here would mean a stack URL alone could not even start a session, which is exactly
            # what the provisioning flow needs it to do.
            LOG.info('No Storage API token; creating a bootstrap session state without a client.')
            state[CONVERSATION_ID] = config.conversation_id
            return state

        try:
            if not config.storage_api_url:
                raise ValueError('Storage API URL is not provided.')

            # A programmatic token (kbc_at_/kbc_pat_) is narrowed to a specific project via
            # X-KBC-ProjectId when known (header, or a prior scope selection) -- unset
            # (whole-stack) is exactly what get_accessible_projects/set_project_scope need before
            # a project is chosen. A legacy project-bound Storage token derives its project from
            # the token itself and must not get the header.
            project_id = config.project_id if is_programmatic_token(config.storage_token) else None
            client = await KeboolaClient.create(
                storage_api_url=config.storage_api_url,
                storage_token=config.storage_token,
                bearer_token=config.bearer_token,
                branch_id=config.branch_id,
                project_id=project_id,
                token_resolver=token_resolver,
                headers={
                    **build_tracing_headers(runtime_info),
                    **({'X-KBC-ProjectId': project_id} if project_id else {}),
                },
                readonly=readonly,
                own_stack_storage_api_url=own_stack_storage_api_url,
            )

            state[KeboolaClient.STATE_KEY] = client
            LOG.info('Successfully initialized Storage API client.')
        except Exception as e:
            # A caller's own token being rejected (StorageTokenExchangeError with a pass-through
            # 400/401/403, already logged as such in StorageTokenResolver.resolve()) is not this
            # server's fault -- avoid double-logging the same routine failure at ERROR here too.
            is_caller_fault = isinstance(e, StorageTokenExchangeError) and e.status_code != int(HTTPStatus.BAD_GATEWAY)
            log = LOG.warning if is_caller_fault else LOG.error
            log(f'Failed to initialize Keboola client: {e}')
            raise

        try:
            # The Kubernetes ServiceAccount token path is read from the process environment
            # only (KBC_KUBERNETES_TOKEN_PATH), never from `Config`/HTTP headers — it is a
            # deployment-level credential of the MCP server itself and must not be
            # overridable per request. An unforgeable path is not enough on its own, because the
            # destination can come from a header — `KeboolaClient.step_up_storage_client()`
            # therefore attaches the JWT only when the target is this server's own stack.
            kubernetes_token_path = deployed_sa_token_path()
            workspace_manager = await WorkspaceManager.create(
                client,
                config.workspace_schema,
                kubernetes_token_path=kubernetes_token_path,
                workspace_id=config.workspace_id,
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
        if is_directory_session():
            # First, so that every path below (the bootstrap superset, the verify_token fallback) starts from it.
            tools = [t for t in tools if t.name in DIRECTORY_TOOLS]

        # A bootstrap session (no credential yet -- see `create_session_state`) has no client to
        # verify a token with, so there is nothing to filter on. Advertise the superset; the
        # on_call_tool guards still apply, and every tool but `create_project` fails with the "no
        # credentials" message from `KeboolaClient.from_state` if actually called.
        if KeboolaClient.STATE_KEY not in context.fastmcp_context.session.state:
            return tools

        # Feature/role filtering needs verify_token (a Connection round-trip with a single project
        # context). A programmatic (kbc_*) session has that context only once its client is narrowed
        # to a project via X-KBC-ProjectId: pre-scope there is no project (and the call would block
        # connecting on a slow stack), so advertise the superset. Post-scope, filter like any other
        # session so tools/list matches on_call_tool (AI-4075), but never let a verify_token failure
        # fail tools/list -- that 401 disconnected clients before (#605); fall back to the superset
        # minus the semantic tools, whose metastore check needs no verify_token (AI-4074).
        client = KeboolaClient.from_state(context.fastmcp_context.session.state)
        if is_programmatic_token(client.bearer_token):
            if not client.project_id:
                return tools
            token_info = await self._get_token_info_or_none(context.fastmcp_context)
            if token_info is None:
                return await self._filter_semantic_tools(tools, client)
        else:
            token_info = await self.get_token_info(context.fastmcp_context)

        is_main_branch = await self._is_listed_branch_main(context.fastmcp_context)
        tools = self._filter_by_token_info(tools, token_info, context.fastmcp_context, is_main_branch)
        return await self._filter_semantic_tools(tools, client)

    @staticmethod
    async def _is_listed_branch_main(ctx: Context) -> bool:
        branch_id = ctx.session.state.get(LISTED_BRANCH_ID)
        if not branch_id:
            return True
        client = KeboolaClient.from_state(ctx.session.state)
        try:
            detail = await client.storage_client.dev_branch_detail(branch_id)
        except Exception as e:
            LOG.info(f'Cannot resolve branch "{branch_id}" for tools/list, listing as the main branch: {e}')
            return True
        return not isinstance(detail, dict) or detail.get('isDefault') is not False

    @classmethod
    async def _get_token_info_or_none(cls, ctx: Context) -> JsonDict | None:
        try:
            return await cls.get_token_info(ctx)
        except Exception as e:
            LOG.warning(f'Failed to verify token for tools/list, listing all tools: {e}')
            return None

    @classmethod
    def _filter_by_token_info(
        cls, tools: list[Tool], token_info: JsonDict, ctx: Context, is_main_branch: bool
    ) -> list[Tool]:
        features = cls.get_project_features(token_info)
        token_role = cls.get_token_role(token_info).lower()

        if 'hide-conditional-flows' in features:
            tools = [t for t in tools if t.name != 'create_conditional_flow']
        else:
            tools = [t for t in tools if t.name != 'create_flow']

        # Show modify_flow to: admin, share, OR OAuth users
        # Show update_flow to: everyone else (except readOnly, handled below)
        is_oauth = cls._is_oauth_authenticated(ctx)
        if token_role in ('admin', 'share') or is_oauth:
            tools = [t for t in tools if t.name != UPDATE_FLOW_TOOL_NAME]
        else:
            tools = [t for t in tools if t.name != MODIFY_FLOW_TOOL_NAME]

        if not is_main_branch:
            # Filter out data app tools when the client is not using the main/production branch
            tools = [t for t in tools if t.name not in DATA_APP_BRANCH_GATED_TOOLS]

        if token_role == 'readonly':
            tools = [t for t in tools if is_read_only_tool(t)]
            LOG.debug(f'Read-only access: filtered to {len(tools)} read-only tools for role={token_role}')

        return tools

    @staticmethod
    async def _filter_semantic_tools(tools: list[Tool], client: KeboolaClient) -> list[Tool]:
        if await project_has_semantic_models(client):
            return tools
        return [t for t in tools if not is_semantic_tool(t)]

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
        directory_profile: bool,
    ) -> str | None:
        """
        Decide whether a call to ``tool_name`` is allowed given the project features, the token role,
        the authentication mode and the branch.

        This is the single source of truth for the project-feature / token-role / branch gating.
        :meth:`on_call_tool` applies it to MCP tool calls; the raw ``/preview/configuration`` Starlette
        route reuses it (see ``preview.py``) so the preview path enforces exactly the same rules.

        :param directory_profile: The session belongs to the ChatGPT plugin: only `DIRECTORY_TOOLS` may be called.
        :return: A denial message if the call is not allowed, or ``None`` if it is allowed.
        """
        if directory_profile and tool_name not in DIRECTORY_TOOLS:
            return directory_denial(tool_name)

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
        directory_profile = is_directory_session()
        if directory_profile and tool.name not in DIRECTORY_TOOLS:
            # Before everything else: the bootstrap exemption below and a failing verify_token must not reach the tool.
            raise ToolError(directory_denial(tool.name))

        # Bootstrap tools (get_accessible_projects/set_project_scope) must work before any project
        # is chosen -- that's their entire purpose. verify_token() needs a single-project context
        # (X-KBC-ProjectId); calling it here pre-scope would 401 before the tool's own body (which
        # establishes that context, e.g. via introspect_token) ever runs. Mirrors the same exemption
        # in on_list_tools and MultiProjectMiddleware.
        if tool.name in BOOTSTRAP_TOOLS:
            return await call_next(context)

        try:
            token_info = await self.get_token_info(context.fastmcp_context)
        except ValueError as e:
            if KeboolaClient.STATE_KEY in context.fastmcp_context.session.state:
                raise
            # A bootstrap session has no credential at all -- the normal state of a caller on the
            # anonymous endpoint before `create_project`, and of any session that has not signed in
            # yet. There is no project whose features could gate this call; what the caller needs is
            # the one sentence `KeboolaClient.from_state` raises, saying how to get a credential.
            # It has to leave as a `ToolError`: a bare `ValueError` raised from a middleware is not
            # a tool result, so an HTTP client is handed "Internal server error" instead of the
            # explanation -- which is exactly backwards for the one audience that needs it most.
            raise ToolError(str(e)) from e

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
            directory_profile=directory_profile,
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


class _SerializingFunctionTool(FunctionTool):
    """FunctionTool that renders its result with a fixed text encoder.

    fastmcp 4 dropped `Tool.serializer`; overriding `convert_result` is its documented
    replacement (https://gofastmcp.com/servers/tools#custom-serialization). Subclasses just
    plug in the desired encoder via `_serialize()`.

    Delegates to the base implementation first and only swaps in the custom-encoded text content:
    the base class is what derives `structured_content` from `output_schema` (the MCP client
    rejects a response with a schema but no `structured_content`), and that derivation is left
    untouched. A tool function that already returns a `ToolResult`/`CallToolResult`, or raw bytes,
    is passed through as-is -- those shapes carry meaning our text encoders aren't meant to touch.
    """

    def _serialize(self, data: Any) -> str:
        raise NotImplementedError

    def convert_result(self, raw_value: Any) -> ToolResult:
        result = super().convert_result(raw_value)
        if isinstance(raw_value, ToolResult | mt.CallToolResult | bytes):
            return result
        return result.model_copy(update={'content': [mt.TextContent(type='text', text=self._serialize(raw_value))]})


class PlainFunctionTool(_SerializingFunctionTool):
    """Default tool result encoding: null-stripped JSON text."""

    def _serialize(self, data: Any) -> str:
        return _exclude_none_serializer(data)


class ToonFunctionTool(_SerializingFunctionTool):
    """Tool result encoded as TOON, keeping null fields."""

    def _serialize(self, data: Any) -> str:
        return toon_serializer(data)


class ToonCompactFunctionTool(_SerializingFunctionTool):
    """Tool result encoded as TOON, with null fields dropped."""

    def _serialize(self, data: Any) -> str:
        return toon_serializer_compact(data)


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
