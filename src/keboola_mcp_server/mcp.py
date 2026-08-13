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

from keboola_mcp_server.auth_login import exchange_scoped_token, get_access_token, introspect_token, load_tokens
from keboola_mcp_server.clients.auth_bridge import is_programmatic_token, strip_bearer
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
from keboola_mcp_server.scope import (
    OAUTH_SESSION_ID_KEY,
    SCOPE_KEY,
    SCOPE_TOKEN_ARG,
    SessionScope,
    persist_scope,
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
    session_store: SessionStore | None = None
    kai_scope_store: KaiScopeStore | None = None

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
            config = await self._maybe_use_stored_session(config, refresh=not is_list)

            # In-conversation multi-project scope is carried by the caller as the `scope_token` tool
            # argument (see SessionScope.to_token/from_token) rather than read back from
            # ctx.session.state, which is rebuilt empty on every request under this server's default
            # stateless-HTTP transport. With no scope and no preset project, auto-lease ALL accessible
            # projects (multi-project mode) so read tools fan out across everything — but never on /list.
            scope = self._read_scope_from_request(context, config)
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
            if scope is None and not config.project_id and not is_list:
                scope = await self._read_persisted_login_scope(config)
            if scope is None and not config.project_id and not is_list:
                scope = await self._autolease_default_scope(config)
            if not is_list:
                scoped_token_before = scope.scoped_token if scope is not None else None
                config, scope = await self._resolve_local_tokens(config, scope)
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

            # A read-only confirmed scope is enforced locally too (not just by the remote scoped
            # token, which may not exist -- see set_project_scope's exchange-failure fallback and
            # the "Security hardening" RFC increment): the base session client itself is built
            # read-only whenever the scope requests it, success or failure of the token exchange.
            readonly = True if scope is not None and scope.read_only else None
            state = await self.create_session_state(
                config, runtime_info, readonly=readonly, own_stack_storage_api_url=own_stack_storage_api_url
            )
            if scope is not None:
                state[SCOPE_KEY] = scope
            if oauth_session_id := self._read_oauth_session_id(http_rq):
                state[OAUTH_SESSION_ID_KEY] = oauth_session_id
            ctx.session.state = state

        try:
            return await call_next(context)
        finally:
            # NOTE: This line is commented following a bug related to session state clearance in Claude client
            # ctx.session.state = {}
            pass

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

        :param http_rq: The incoming HTTP request whose headers are applied.
        :param config: The server's own configuration (from CLI parameters and environment).
        :param own_stack_storage_api_url: The Storage API URL of the server's own stack
            (`ServerState.own_stack_storage_api_url`), or None when it has no stack of its own and
            therefore takes the URL from the request. It must never come from a request header.
        :return: The configuration to use for this request.
        """
        LOG.debug(f'Injecting headers: http_rq={http_rq}, headers={http_rq.headers}')
        # Only fields meant to vary per request are settable from a header -- see
        # Config._HEADER_ELIGIBLE_FIELDS / the "Security hardening" RFC increment. In particular
        # this keeps `jwt_secret` (which would otherwise let a caller forge their own scope_token)
        # and the other deployment-level fields permanently unreachable from a request.
        config = config.replace_by_headers(http_rq.headers)

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

        return config

    @classmethod
    def _read_scope_from_request(cls, context: fmw.MiddlewareContext[Any], config: Config) -> 'SessionScope | None':
        """Decodes the ``scope_token`` tool-call argument (if any) back into a SessionScope.

        Pops the argument so it never reaches the tool function, matching how
        MultiProjectMiddleware.on_call_tool consumes _PROJECT_FILTER_ARG. Absent, malformed, or
        expired tokens are treated as "no scope yet" rather than an error -- the ask-first gate in
        MultiProjectMiddleware then steers the caller back through get_accessible_projects /
        set_project_scope.
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
        try:
            return SessionScope.from_token(token, resolve_scope_key(config))
        except Exception:
            LOG.warning('Ignoring invalid or expired scope_token.', exc_info=True)
            return None

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
        """
        try:
            introspection = await introspect_token(
                config.storage_api_url, subject_token=strip_bearer(config.storage_token)
            )
        except Exception as e:
            LOG.warning(f'Could not introspect Kai token for persisted scope lookup: {e}', exc_info=True)
            return None
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
        cls, config: Config, scope: 'SessionScope | None'
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
        if not cls._is_local_programmatic(config):
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
                # A programmatic token (kbc_at_/kbc_pat_) is forwarded downstream as a Bearer --
                # KeboolaClient already sends it that way to every service it wraps (Storage, Queue,
                # AI, etc.), so no legacy per-project Storage token needs to be minted for it. Strip
                # any inbound `Bearer ` scheme so the client's own `Bearer ` prefixing can't produce
                # `Bearer Bearer …`. Narrow to a specific project via X-KBC-ProjectId when known
                # (header, or a prior scope selection) -- unset (whole-stack) is exactly what
                # get_accessible_projects/set_project_scope need before a project is chosen.
                bearer_token = strip_bearer(storage_token)
                if config.project_id:
                    extra_headers['X-KBC-ProjectId'] = config.project_id

            client = await KeboolaClient(
                storage_api_url=config.storage_api_url,
                storage_api_token=storage_token,
                bearer_token=bearer_token,
                headers={**build_tracing_headers(runtime_info), **extra_headers},
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
            kubernetes_token_path = deployed_sa_token_path()
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

        # Feature/role filtering needs verify_token (a Connection round-trip with a single project
        # context). For a programmatic (kbc_*) session this doesn't work at list time: pre-scope there
        # is no project (and the call would block connecting on a slow stack); post-scope the session
        # holds a multi-project scoped token and verify without an X-KBC-ProjectId returns 401 — which
        # made every tools/list fail and the client disconnect. So skip list-time filtering for ALL
        # programmatic sessions and advertise the superset; the on_call_tool guards still enforce every
        # feature/role/branch rule per project (with the right project_id) when a tool is invoked.
        client = KeboolaClient.from_state(context.fastmcp_context.session.state)
        if is_programmatic_token(client.token):
            return tools

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

        # Bootstrap tools (get_accessible_projects/set_project_scope) must work before any project
        # is chosen -- that's their entire purpose. verify_token() needs a single-project context
        # (X-KBC-ProjectId); calling it here pre-scope would 401 before the tool's own body (which
        # establishes that context, e.g. via introspect_token) ever runs. Mirrors the same exemption
        # in on_list_tools and MultiProjectMiddleware.
        if tool.name in BOOTSTRAP_TOOLS:
            return await call_next(context)

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
