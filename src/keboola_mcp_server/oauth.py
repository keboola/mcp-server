import asyncio
import base64
import dataclasses
import hashlib
import hmac
import ipaddress
import json
import logging
import math
import os
import secrets
import time
from collections import OrderedDict, deque
from collections.abc import Mapping
from contextvars import ContextVar
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, cast
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import httpx
import jwt
from fastmcp.server.auth.auth import OAuthProvider
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    RefreshToken,
    TokenError,
    construct_redirect_uri,
)
from mcp.server.auth.settings import ClientRegistrationOptions
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull, OAuthToken
from pydantic import AnyHttpUrl, AnyUrl
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.routing import Route, get_route_path
from starlette.types import ASGIApp, Receive, Scope, Send

from keboola_mcp_server.auth_login import (
    TokenSet,
    exchange_scoped_token,
    introspect_token,
    parse_token_response,
    refresh_tokens,
)
from keboola_mcp_server.clients.auth_bridge import OAuthSessionExchanger, OAuthTokenExchangeError
from keboola_mcp_server.config import deployed_sa_token_path
from keboola_mcp_server.jwt_utils import decode_jwt, encode_jwt
from keboola_mcp_server.session_store import DatabaseUnavailableError
from keboola_mcp_server.session_store.rate_limit import RateLimitStore
from keboola_mcp_server.session_store.repository import SessionStore


# The OAuth scopes advertised on a session's tokens.
# - Every new session is 'claudai projectless' (authorize() requests it for every registered client).
# - 'claudai' only is a session persisted with oauth_projectless=False, before that was the case.
def _scopes_for_session(oauth_projectless: bool) -> list[str]:
    return ['claudai', 'projectless'] if oauth_projectless else ['claudai']


LOG = logging.getLogger(__name__)
_OAUTH_LOG_ALL = bool(os.getenv('KEBOOLA_MCP_SERVER_OAUTH_LOG_ALL'))

# The only hosts a plain http:// redirect_uri may target (RFC 8252 §7.3 loopback) -- see
# _OAuthClientInformationFull.validate_redirect_uri. pydantic's AnyUrl.host keeps the brackets on
# an IPv6 literal (e.g. '[::1]', not '::1'), verified against the installed pydantic version.
_LOOPBACK_HOSTS = frozenset({'localhost', '127.0.0.1', '[::1]'})

# The only hosts a cursor:// redirect_uri may target -- mirrors Connection's own
# PendingMcpClientDecoder::ALLOWED_CURSOR_HOSTS exactly (a custom scheme has no certificate
# authority backing it, so unlike https, "any host" is never a shape Connection will register).
_ALLOWED_CURSOR_HOSTS = frozenset({'anysphere.cursor-retrieval', 'anysphere.cursor-mcp'})

# redirect_uri -> the literal client_id Keboola pre-registered for it in Connection's oauth2_client
# table (a migration on the Connection side).
# Not a trust decision -- it only picks which Connection row to ask about; /oauth/clients/validate
# is still the sole authority on whether the pair is actually registered and active.
_WELL_KNOWN_CONNECTION_CLIENT_IDS: dict[str, str] = {
    'https://claude.ai/api/mcp/auth_callback': 'claude-ai',
}

# Cap on the in-process client_id -> client_name cache (see ConnectionClientRegistry.remember_client_name)
# so an unauthenticated caller spamming /register can't grow it unboundedly.
_MAX_CACHED_CLIENT_NAMES = 10_000

# ConnectionClientRegistry.check_registration's result cache -- see its __init__ docstring.
_MAX_CACHED_REGISTRATIONS = 10_000
_REGISTERED_CACHE_TTL_SECONDS = 300  # 5 min: a registered+active client's status rarely flips.

# Local cap on calls to Connection's POST /oauth/clients/validate, enforced in
# _check_registration_uncached before any network call. Connection's own ceiling for this route is 600
# calls/60s per egress IP, and every replica of this server shares that IP. The registration cache already
# absorbs the common case (repeat callers hit the cache, not Connection), so the only way to burn through the
# budget is a caller sending a distinct, never-cached redirect_uri on every request (Copilot review finding).
#
# This process-local limiter is the fallback: while the shared store answers, the budget is counted across every
# replica (`_VALIDATE_FLEET_LIMIT_PER_MINUTE`, AI-4005) and no replica arithmetic is needed. Without the store the bound
# that matters is still the fleet's: the per-process cap times the largest replica count we deploy must stay below
# Connection's ceiling, hence the default (100 x 5 = 500 < 600); a deployment with more replicas must lower it with
# `OAUTH_VALIDATE_RATE_LIMIT` rather than rely on this default.
_CONNECTION_VALIDATE_LIMIT_PER_WINDOW = 600
_ASSUMED_MAX_REPLICAS = 5
# The window of `_ApprovalBudget`: a day.
_APPROVAL_BUDGET_WINDOW_SECONDS = 24 * 60 * 60.0
_APPROVAL_BUDGET_WINDOW_SECONDS_INT = int(_APPROVAL_BUDGET_WINDOW_SECONDS)
_VALIDATE_RATE_LIMIT_MAX_CALLS = 100
_VALIDATE_RATE_LIMIT_WINDOW_SECONDS = 60.0
# The same call budget counted across every replica through the shared store (AI-4005): Connection's ceiling is per
# egress IP, so this is the number that matters; the per-process limit above only holds when the store is unreachable.
# Below Connection's 600 per minute with room for its other callers.
_VALIDATE_FLEET_LIMIT_PER_MINUTE = 500
# How long the last known REGISTERED answer for a well-known pair may be served when Connection cannot answer.
_WELL_KNOWN_STALE_GRACE_SECONDS = 3600.0
# After Connection fails to answer for a well-known pair (or answers "not registered"), it is not asked again for
# this long: the pair is exempt from the local limiter, so without a pause every sequential request would be one
# more call into a Connection that is already failing, rate-limiting this very egress IP, or has no such client.
_WELL_KNOWN_ERROR_BACKOFF_SECONDS = 15.0


class _SlidingWindowRateLimiter:
    """Caps calls to `max_calls` per `window_seconds`, process-local. Single-threaded asyncio means
    no lock is needed: `try_acquire` never awaits, so it always runs to completion uninterrupted."""

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        self._max_calls = max_calls
        self._window_seconds = window_seconds
        self._call_times: deque[float] = deque()

    def try_acquire(self) -> bool:
        now = time.monotonic()
        while self._call_times and now - self._call_times[0] > self._window_seconds:
            self._call_times.popleft()
        if len(self._call_times) >= self._max_calls:
            return False
        self._call_times.append(now)
        return True


class _ApprovalBudget:
    """Caps how many distinct clients per `window_seconds` this process sends to Connection's approval screen
    (`max_clients`), so a stack with dynamic approval turned on registers a few clients a day at most, not one per
    caller. The same client asking again inside the window costs nothing: a legitimate retry (the approval round trip
    is followed by a second login) must not use up the budget. Process-local, like `_SlidingWindowRateLimiter`, so N
    replicas allow up to N times the cap. The map is bounded by `max_clients`."""

    def __init__(self, max_clients: int, window_seconds: float = _APPROVAL_BUDGET_WINDOW_SECONDS) -> None:
        self._max_clients = max_clients
        self._window_seconds = window_seconds
        self._granted_at: dict[str, float] = {}

    def try_acquire(self, client_key: str) -> bool:
        now = time.monotonic()
        self._granted_at = {k: t for k, t in self._granted_at.items() if now - t <= self._window_seconds}
        if client_key in self._granted_at:
            return True
        if len(self._granted_at) >= self._max_clients:
            return False
        self._granted_at[client_key] = now
        return True


class _ClientRegistration(Enum):
    """Outcome of asking Connection whether a (client_id, redirect_uri) pair is registered."""

    REGISTERED = auto()
    NOT_REGISTERED = auto()
    # Connection could not be reached, or answered with something other than 200/404 -- never
    # treated as either of the above (fail closed, see AI-3792).
    ERROR = auto()


# Loopback hosts whose port does not take part in the derived Connection client_id (RFC 8252 §7.3's
# "any port" allowance for native/loopback clients). 'localhost' is included although Connection's own
# redirect-URI matching (LoopbackRedirectUri) and league still compare it port-exactly: a dynamically approved
# client is registered in Connection against this server's own callback URL, never the client's, so the client's
# port is not compared there. Only a client approved before AI-3995 (registered against its own redirect_uri)
# or a pre-registered one is, and neither is derived from this set. Claude Code sends
# http://localhost:<random port>/callback, so without 'localhost' every re-auth burned an approval.
_PORT_INSENSITIVE_LOOPBACK_HOSTS = frozenset({'localhost', '127.0.0.1', '::1'})


def _without_port_for_loopback(redirect_uri: str) -> str:
    """Strips the port from a loopback http:// URI (see _PORT_INSENSITIVE_LOOPBACK_HOSTS);
    returns every other URI unchanged."""
    parsed = urlparse(redirect_uri)
    if parsed.scheme != 'http' or (parsed.hostname or '') not in _PORT_INSENSITIVE_LOOPBACK_HOSTS:
        return redirect_uri
    host = f'[{parsed.hostname}]' if ':' in (parsed.hostname or '') else parsed.hostname
    return urlunparse(parsed._replace(netloc=host))


def _connection_client_id(redirect_uri: str, key: bytes) -> str:
    """
    Maps an AI assistant's own redirect_uri to the client_id used when talking to Connection's
    OAuth client registry.

    The id is an HMAC of the redirect_uri under a key only this deployment holds, so nobody else can compute it.
    Connection accepts an unsigned `pending_mcp_client` payload from any logged-in user at its public
    /oauth/authorize, so an id anyone could derive would let a user approve a pair out of band, skipping
    `OAUTH_DYNAMIC_CLIENT_APPROVAL`. The id only reaches a browser when this server sends the approval redirect
    (switch on), and the well-known pairs below keep their literal ids because only a Connection migration, not
    a user, registers them.

    Connection's oauth2_client.identifier column (and the pending_mcp_client payload's client_id
    field) are capped at 32 characters, but the mcp SDK mints a 36-character uuid4() as client_id
    for every client that dynamically registers via /register -- forwarding that verbatim would
    never fit. Deriving a short, stable id from redirect_uri instead means the same tool
    reconnecting (same callback URL) lands on the same Connection identity and reuses an earlier
    approval, even though the SDK hands it a fresh uuid on every /register call.

    The port is stripped first for localhost/127.0.0.1/[::1] (see _without_port_for_loopback) -- a loopback
    tool's ephemeral port changes on every reconnect (RFC 8252 §7.3; examples: Claude Code, VS
    Code, MCP Inspector, Codex, Gemini CLI), and without this every reconnect would derive a new
    id, need a fresh human approval, and leave behind a permanent, never-cleaned-up
    oauth2_client row on every stack (Vojtěch Biberle review, AI-2883/connection#8573).
    """
    if known := _WELL_KNOWN_CONNECTION_CLIENT_IDS.get(redirect_uri):
        return known
    digest = hmac.new(key, _without_port_for_loopback(redirect_uri).encode(), hashlib.sha256).hexdigest()[:24]
    return f'mcp-{digest}'


def _strip_unprintable(value: str) -> str:
    """Strips control/bidi/zero-width characters. `str.isprintable()` already excludes control
    (Cc), format (Cf -- covers zero-width and bidi-override characters), surrogate, private-use
    and separator categories, which is exactly what Connection's own DECEPTIVE_CHARS_PATTERN
    targets."""
    return ''.join(ch for ch in value if ch.isprintable())


def _sign_mcp_client_id(secret: str, mcp_client_id: str, state: str) -> str:
    """Authenticates the `mcp_client_id` this server puts on Connection's consent URL (AI-3995).

    HMAC-SHA256 over the id AND this request's `state`, keyed with this server's own OAuth client
    secret -- which Connection already holds for that client, so it can recompute it. Binding it to
    `state` (a JWT that carries the caller's redirect_uri) means a link cannot pair a trusted app's
    name with someone else's flow: the pair is only valid for the flow it was issued for."""
    # Length-prefixed so the (id, state) pair has exactly one encoding: with a plain separator, a
    # signature for ("a", "b.c") would also verify ("a.b", "c") because both fields may contain dots.
    message = f'{len(mcp_client_id.encode())}:{mcp_client_id}:{state}'
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


_MAX_CLIENT_NAME_BYTES = 128  # Connection's cap on the pending client's client_name: PHP strlen(), i.e. BYTES
_MIN_CLIENT_NAME_BYTES = 16  # always keep at least this much of the client's own name


def _truncate_utf8(text: str, max_bytes: int) -> str:
    """Cuts `text` to at most `max_bytes` UTF-8 bytes without splitting a character."""
    return text.encode('utf-8')[:max_bytes].decode('utf-8', 'ignore')


def _sanitize_client_name(name: str) -> str:
    """Strips unprintable characters Connection's pending_mcp_client decoder would otherwise
    reject outright (which would silently drop the whole approval payload -- see
    PendingMcpClientApprovalListener's catch-and-ignore on a malformed payload), and truncates to
    Connection's 128-BYTE cap (its decoder uses strlen(), so 200 emoji would not fit as 128 characters)."""
    return _truncate_utf8(_strip_unprintable(name), _MAX_CLIENT_NAME_BYTES)


def _approval_client_name(client_name: str, redirect_uri: str) -> str:
    """The name Connection's approval screen shows: the client's own name plus where it redirects,
    `<name> (redirects to https://host)`. Connection shows this server's callback as the redirect URI
    (RFC Decision §15), so this suffix is the only place the approver sees the caller's destination.

    Always within Connection's 128-byte cap. When it doesn't fit, the client's own name is shortened
    first, and a very long host loses its START (`...`) rather than its end: the final labels are the
    part that says who the destination is.
    """
    parsed = urlparse(redirect_uri)
    prefix = f'{parsed.scheme}://'
    host = parsed.netloc
    wrapper_bytes = len(b' (redirects to )') + len(prefix.encode())
    max_host_bytes = _MAX_CLIENT_NAME_BYTES - _MIN_CLIENT_NAME_BYTES - wrapper_bytes
    host_bytes = host.encode('utf-8')
    if len(host_bytes) > max_host_bytes:
        host = '...' + host_bytes[-(max_host_bytes - 3) :].decode('utf-8', 'ignore')
    suffix = f' (redirects to {prefix}{host})'
    return _truncate_utf8(client_name, _MAX_CLIENT_NAME_BYTES - len(suffix.encode())) + suffix


def _sanitize_for_log(value: str) -> str:
    """Same unprintable-character stripping as `_sanitize_client_name`, applied to any other
    caller-controlled value (e.g. `redirect_uri`) before it's interpolated into a log line --
    log injection via crafted control/bidi characters, same class of finding as the client_name
    fix above (Devin review, AI-2883). No length cap here: `redirect_uri` is already bounded to
    2048 characters by `validate_redirect_uri` before it can reach a log call site."""
    return _strip_unprintable(value)


def _sanitize_client_id_for_log(client_id: str) -> str:
    """`client_id` is a free-form query/body parameter on the unauthenticated `/authorize` and `/token`
    routes (the SDK's `get_client()` here accepts any value), so cap it and strip control/bidi
    characters before it reaches a log line -- the audit lines are the interim control for RFC
    Decision §10, and a crafted value must not forge records in them (Vojtěch Biberle review)."""
    return _sanitize_for_log(client_id)[:64]


def _create_http_client(*, follow_redirects: bool = True, timeout: httpx.Timeout | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(follow_redirects=follow_redirects, timeout=timeout or httpx.Timeout(30.0))


# 'claudai' satisfies the exchange endpoint's MissingClaudaiScopeException guard; 'projectless' makes the
# exchanged session whole-stack instead of project-pinned. Requested for every REGISTERED client (as before the
# AI-2883 registry): this server's own broker client, not the per-redirect client, is the one Connection checks
# for 'projectless'.
_CONNECTION_SCOPE = 'claudai projectless'


class ConnectionClientRegistry:
    """
    The MCP-server-side view of Connection's OAuth client registry (`oauth2_client` table):
    checking whether a dynamically-presented `(client_id, redirect_uri)` pair is registered, and
    building the pending-approval redirect for one that isn't. Kept separate from
    `SimpleOAuthProvider` -- an already-large class implementing the session/token-broker side of
    the OAuth-AS role -- because "is this client trusted" is a distinct concern from "how do we
    exchange/refresh/revoke a session for one that is". See the AI-2883 RFC
    (feature_spec/oauth_dynamic_client_registration/RFC.md) for the full design.
    """

    def __init__(
        self,
        server_url: str,
        *,
        validate_rate_limit: int | None = None,
        rate_limit_store: RateLimitStore | None = None,
        validate_fleet_limit: int | None = None,
    ) -> None:
        self._validate_url = urljoin(server_url, '/oauth/clients/validate')
        self._authorize_url = urljoin(server_url, '/oauth/authorize')
        # One pooled client for every validate call (created on first use, closed by `aclose()` from the server's
        # lifespan), instead of a new client -- and a new TLS handshake -- per call.
        self._http_client: httpx.AsyncClient | None = None
        # redirect_uri -> when a well-known pair was last seen REGISTERED (see `_check_and_cache`).
        self._last_known_registered: dict[str, float] = {}
        self._well_known_error_until: dict[str, float] = {}
        # redirect_uri -> until when a well-known pair that Connection answered "not registered" for is not asked
        # about again. The well-known pairs are exempt from the local limiter, so without this a missing or
        # deactivated pair would cost Connection one call per request.
        self._well_known_not_registered_until: dict[str, float] = {}

        # client_id -> client_name submitted at /register, so a dynamically-registered client's
        # approval screen can show a real name instead of just the derived Connection client_id
        # (/authorize never receives client_name itself -- see the RFC's Problem section). In-
        # process only, bounded -- see RFC Decisions §4 for why this is a deliberate, display-only
        # tradeoff and not a persistent store. The stored value is sanitized+capped at insertion,
        # not just when later read for display -- /register is unauthenticated, so an arbitrarily
        # long raw name per entry would let a caller inflate memory well past what the entry-count
        # cap alone bounds (Copilot review finding).
        self._client_names: OrderedDict[str, str] = OrderedDict()

        # (connection_client_id, redirect_uri) -> expires_at, REGISTERED only. Keyed on the pair: the
        # redirect_uri alone would let one client's approval admit every other one, since every
        # dynamically-approved client shares this server's own /oauth/callback (see
        # SimpleOAuthProvider._authorize); the client id alone would let a verdict for one of a client's
        # redirect_uris (its own, or ours) admit another one.
        # /authorize is unauthenticated, so every hit costs
        # Connection one call to /oauth/clients/validate -- which is itself IP-rate-limited, and
        # this server's whole egress IP shares that budget across every user of the stack. Caching
        # REGISTERED results means the common case (the same handful of already-registered clients
        # reconnecting with the same redirect_uri) never leaves this process.
        #
        # NOT_REGISTERED and ERROR are deliberately never cached:
        # - A cached NOT_REGISTERED would still show stale on the very next retry right after an
        #   admin clicks Allow, contradicting the "approve once, then retry" UX this flow depends
        #   on (Copilot review finding) -- and it buys little anyway: an attacker varying
        #   redirect_uri on every call misses this cache regardless (a fresh key each time), so it
        #   was never a real defense against that flood; local throttling below is what handles it.
        # - Caching ERROR would prolong an outage instead of retrying it -- fail-closed still
        #   applies on every uncached call (see AI-3792).
        # Keyed by the (connection_client_id, redirect_uri) pair, not the redirect_uri alone: a dynamically
        # approved client is registered with Connection against this server's own callback (RFC Decision
        # §15), so the redirect_uri sent to Connection no longer identifies the client by itself.
        self._registration_cache: OrderedDict[tuple[str, str], float] = OrderedDict()
        # One in-flight Connection call per pair: concurrent cache misses for the same pair share it instead of
        # each calling Connection (see `check_registration`).
        self._in_flight: dict[tuple[str, str], asyncio.Task[_ClientRegistration]] = {}

        self._validate_rate_limiter = _SlidingWindowRateLimiter(
            validate_rate_limit or _VALIDATE_RATE_LIMIT_MAX_CALLS, _VALIDATE_RATE_LIMIT_WINDOW_SECONDS
        )
        # The call budget shared by every replica, when there is a store to count it in (AI-4005); the per-process
        # limiter above is what holds without one, or while the store cannot be reached.
        self._rate_limit_store = rate_limit_store
        self._validate_fleet_limit = validate_fleet_limit or _VALIDATE_FLEET_LIMIT_PER_MINUTE

    def remember_client_name(self, client_id: str | None, client_name: str | None) -> None:
        if not client_id:
            return
        sanitized = _sanitize_client_name(client_name or '')
        if not sanitized:
            return
        self._client_names[client_id] = sanitized
        self._client_names.move_to_end(client_id)
        if len(self._client_names) > _MAX_CACHED_CLIENT_NAMES:
            self._client_names.popitem(last=False)

    def get_client_name(self, client_id: str | None) -> str | None:
        return self._client_names.get(client_id) if client_id else None

    async def check_registration(self, connection_client_id: str, redirect_uri: str) -> _ClientRegistration:
        """
        Checks whether (connection_client_id, redirect_uri) is registered on Connection via
        `POST /oauth/clients/validate` (docs/features/oauth-dynamic-client-registration.md in the
        connection repo). Stack-specific by construction -- it asks whichever Connection instance
        this registry was built for, so a client dynamically approved on one stack has no bearing
        on any other.

        Fails closed: any error talking to Connection (timeout, network error, unexpected status)
        returns ERROR, never REGISTERED and never silently NOT_REGISTERED -- see AI-3792.
        """
        # Keyed on the pair: a client can be registered against more than one redirect_uri (its own, from
        # before AI-3995, and this server's callback), and one pair's verdict must not admit another.
        key = (connection_client_id, redirect_uri)
        expires_at = self._registration_cache.get(key)
        if expires_at is not None:
            if time.monotonic() < expires_at:
                # Touch on read, not just on write -- otherwise a frequently-reused entry (e.g.
                # Claude.ai's own pair) never gets bumped and can still be the oldest-inserted
                # entry once enough unique, unrelated keys flood in, making it the first evicted
                # despite being the most valuable entry to keep (Copilot review finding).
                self._registration_cache.move_to_end(key)
                return _ClientRegistration.REGISTERED
            del self._registration_cache[key]

        # Single-flight: requests that all miss the cache for the same pair (process start, cache
        # expiry) share one call to Connection, so a burst cannot fan out 1:1 into Connection's shared
        # rate limit -- including for the well-known pairs, which are exempt from the local limiter.
        # Shielded, so one caller being cancelled doesn't cancel the call the others are waiting on.
        task = self._in_flight.get(key)
        if task is None:
            task = asyncio.ensure_future(self._check_and_cache(connection_client_id, redirect_uri))
            self._in_flight[key] = task
            task.add_done_callback(lambda t: self._in_flight.pop(key, None) if self._in_flight.get(key) is t else None)
        return await asyncio.shield(task)

    async def _check_and_cache(self, connection_client_id: str, redirect_uri: str) -> _ClientRegistration:
        key = (connection_client_id, redirect_uri)
        well_known = redirect_uri in _WELL_KNOWN_CONNECTION_CLIENT_IDS
        if well_known:
            now = time.monotonic()
            if now < self._well_known_not_registered_until.get(redirect_uri, -math.inf):
                # Connection said "not registered" a moment ago: repeat that instead of calling it again.
                return _ClientRegistration.NOT_REGISTERED
            if now < self._well_known_error_until.get(redirect_uri, -math.inf):
                # Connection failed to answer a moment ago: answer from memory instead of calling it again.
                return self._answer_without_connection(redirect_uri)

        result = await self._check_registration_uncached(connection_client_id, redirect_uri)
        now = time.monotonic()
        if result is _ClientRegistration.REGISTERED:
            self._registration_cache[key] = now + _REGISTERED_CACHE_TTL_SECONDS
            self._registration_cache.move_to_end(key)
            if len(self._registration_cache) > _MAX_CACHED_REGISTRATIONS:
                self._registration_cache.popitem(last=False)
            if well_known:
                self._last_known_registered[redirect_uri] = now
                self._well_known_error_until.pop(redirect_uri, None)
                self._well_known_not_registered_until.pop(redirect_uri, None)
        elif result is _ClientRegistration.NOT_REGISTERED and well_known:
            # A definite "not registered" (deactivated) must outlive a later error: forget the earlier success and
            # any error pause, or the next failure would serve that stale REGISTERED again and undo the deactivation.
            self._last_known_registered.pop(redirect_uri, None)
            self._well_known_error_until.pop(redirect_uri, None)
            # Bounded like the error pause: only the fixed set of well-known pairs ever gets an entry.
            self._well_known_not_registered_until[redirect_uri] = now + _WELL_KNOWN_ERROR_BACKOFF_SECONDS
        elif result is _ClientRegistration.ERROR and well_known:
            # Bounded: only the fixed set of well-known pairs ever gets an entry.
            self._well_known_error_until[redirect_uri] = now + _WELL_KNOWN_ERROR_BACKOFF_SECONDS
            return self._answer_without_connection(redirect_uri)
        return result

    def _answer_without_connection(self, redirect_uri: str) -> _ClientRegistration:
        """
        The answer for a well-known pair while Connection cannot give one (an outage, or its own rate limit used up
        by someone else's flood). A pair Keboola pre-registered itself that was registered a moment ago keeps
        logging in instead of failing together with the flood. Only the absence of an answer gets this: a definite
        NOT_REGISTERED (deactivated) is never overridden, and no other pair is ever served from memory.
        """
        if (
            time.monotonic() - self._last_known_registered.get(redirect_uri, -math.inf)
            < _WELL_KNOWN_STALE_GRACE_SECONDS
        ):
            LOG.warning(
                f'[check_registration] Connection unavailable; serving the last known registration for '
                f'redirect_uri={_sanitize_for_log(redirect_uri)}'
            )
            return _ClientRegistration.REGISTERED
        return _ClientRegistration.ERROR

    async def _validate_budget_allows(self) -> bool:
        """Whether one more call to Connection's validate endpoint fits the budget: the fleet-wide one when the shared
        store answers, else this process's own. A store that cannot answer (the database is down, or the migration that
        creates its table has not run yet) must not take logins down with it, so it falls back to the process limiter."""
        if self._rate_limit_store is not None:
            try:
                hits = await self._rate_limit_store.hit('validate', int(_VALIDATE_RATE_LIMIT_WINDOW_SECONDS))
            except Exception as e:
                LOG.warning(f'[check_registration] Shared rate limit unavailable, using the local one: {e!r}')
            else:
                return hits <= self._validate_fleet_limit
        return self._validate_rate_limiter.try_acquire()

    def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None or self._http_client.is_closed:
            # Explicit, tighter settings for this call, passed to the factory itself rather than overridden
            # per-request on top of its defaults. Never follow a redirect here: a misconfigured proxy/gateway
            # between here and Connection that redirects to something returning 200 (a login page, a catch-all
            # landing page, ...) must surface as an unexpected status (-> ERROR), never get silently interpreted
            # as "client is registered".
            self._http_client = _create_http_client(
                follow_redirects=False, timeout=httpx.Timeout(connect=3.0, read=5.0, write=3.0, pool=3.0)
            )
        return self._http_client

    async def aclose(self) -> None:
        """Closes the pooled client. Call once, from the server's lifespan teardown."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def _check_registration_uncached(self, connection_client_id: str, redirect_uri: str) -> _ClientRegistration:
        # A well-known pair (today: only claude.ai) is exempt from the local limiter -- a fixed,
        # tiny handful of redirect_uris, so letting them always through Connection's own (much
        # higher, IP-shared) limit costs nothing. Without this, an attacker flooding /authorize with
        # distinct, never-cached redirect_uris exhausts the shared limiter budget, and once claude.ai's
        # own 5-minute REGISTERED cache entry expires it gets ERROR'd out right along with the
        # attacker's traffic -- turning a limiter meant to protect Connection into the easier target
        # (Devin review finding, AI-2883).
        if redirect_uri not in _WELL_KNOWN_CONNECTION_CLIENT_IDS and not await self._validate_budget_allows():
            LOG.warning(
                f'[check_registration] Local rate limit exceeded, not calling Connection: '
                f'redirect_uri={_sanitize_for_log(redirect_uri)}'
            )
            return _ClientRegistration.ERROR

        try:
            response = await self._get_http_client().post(
                self._validate_url,
                json={'client_id': connection_client_id, 'redirect_uri': redirect_uri},
            )
        except (httpx.HTTPError, httpx.InvalidURL) as e:
            LOG.warning(f'[check_registration] Could not reach Connection: {e}', exc_info=True)
            return _ClientRegistration.ERROR

        if response.status_code == 200:
            # Connection's real 200 is always an empty JSON object -- checking the body
            # shape, not just the status code, catches a misconfigured intermediary (a health
            # check, an SSO/captive-portal page, a WAF challenge) answering 200 at this exact URL
            # without ever reaching Connection's real endpoint (Copilot review finding).
            try:
                body_is_empty_object = response.json() == {}
            except ValueError:
                body_is_empty_object = False
            if body_is_empty_object:
                return _ClientRegistration.REGISTERED
            LOG.warning(
                f'[check_registration] Unexpected 200 body from Connection (expected {{}}): '
                f'redirect_uri={_sanitize_for_log(redirect_uri)}, text={_sanitize_for_log(response.text[:200])!r}'
            )
            return _ClientRegistration.ERROR
        elif response.status_code == 404:
            return _ClientRegistration.NOT_REGISTERED
        else:
            LOG.warning(
                f'[check_registration] Unexpected response from Connection: '
                f'status={response.status_code}, text={_sanitize_for_log(response.text[:200])!r}'
            )
            return _ClientRegistration.ERROR

    def pending_approval_url(
        self, *, connection_client_id: str, redirect_uri: str, client_name: str | None, state: str
    ) -> str:
        """
        Builds the URL that sends the browser to Connection's own `/oauth/authorize` carrying a
        `pending_mcp_client` payload, so an authenticated Keboola user can approve this client
        (Connection's `PendingMcpClientApprovalListener` only gates that route -- not
        `/oauth/consent`, which `SimpleOAuthProvider` otherwise talks to directly; see the AI-2883
        RFC Decisions §3-4).

        `redirect_uri` is this server's own `/oauth/callback`, never the caller's: the Allow click
        issues a REAL Connection authorization code to it, and `state` brings the browser back
        with the original authorize request so `SimpleOAuthProvider.handle_oauth_callback()` can
        continue straight into `/oauth/consent` (RFC Decision §15, AI-3995). The code itself is
        never redeemed -- `code_challenge` below is a high-entropy random value with no known
        preimage, and Connection's league config requires a code challenge for public clients
        (`require_code_challenge_for_public_clients: true`,
        `connection/config/packages/league_oauth2_server.yaml`) -- so even though it never leaves
        this server's origin, nobody could redeem it. Keep that property: never replace the
        challenge with a value this server or its caller could recompute.
        """
        # Sanitize BEFORE falling back, not after: a client_name that is truthy but sanitizes to
        # '' (e.g. all control/zero-width characters) must still fall back to connection_client_id
        # -- Connection's decoder rejects an empty client_name outright, which would silently drop
        # the whole payload (PendingMcpClientApprovalListener's catch-and-ignore) and leave the
        # user with an opaque league "invalid_client" error instead of an approval screen.
        sanitized_name = _sanitize_client_name(client_name or '') or connection_client_id
        payload = json.dumps(
            {
                'client_id': connection_client_id,
                'client_name': sanitized_name,
                'redirect_uri': redirect_uri,
            },
            separators=(',', ':'),
        ).encode('utf-8')
        return construct_redirect_uri(
            self._authorize_url,
            client_id=connection_client_id,
            redirect_uri=redirect_uri,
            response_type='code',
            state=state,
            # Ask Connection to send the browser back to `redirect_uri` (this server's callback) right after
            # Allow, with `state` and no code, instead of continuing the request into a second consent screen
            # for the per-redirect client whose grant this server discards (AI-3995). A Connection that
            # doesn't know the parameter just keeps the old behaviour, which the callback also handles.
            return_to_client_after_approval='1',
            # MANDATORY, not defensive -- see this method's docstring. secrets.token_urlsafe(32) is
            # a random value presented AS IF it were a SHA-256 digest; no code_verifier can exist
            # for it, so Connection's PKCE check can never be satisfied by anyone, including the caller-controlled
            # redirect_uri that receives the resulting code. Never remove this parameter, and never
            # replace it with a value derived from anything this server or its caller could recompute.
            code_challenge=secrets.token_urlsafe(32),
            code_challenge_method='S256',
            pending_mcp_client=base64.urlsafe_b64encode(payload).decode('ascii'),
        )


def _log_debug(msg: str) -> None:
    """
    Logs the message at the DEBUG level if the environment variable KEBOOLA_MCP_SERVER_OAUTH_LOG_ALL is set.
    Use this function for logging sensitive information. It logs nothing by default.
    """
    if _OAUTH_LOG_ALL:
        LOG.debug(msg)


class _OAuthClientInformationFull(OAuthClientInformationFull):
    def validate_scope(self, requested_scope: str | None) -> list[str] | None:
        # This is supposed to verify that the requested scopes are a subset of the scopes that the client registered.
        # That, however, would require a persistent registry of clients.
        # So, instead we pretend that all the requested scopes have been registered.
        if requested_scope:
            return requested_scope.split(' ')
        else:
            return None

    def validate_redirect_uri(self, redirect_uri: AnyUrl | None) -> AnyUrl:
        # A synchronous SHAPE check only -- the real trust decision (is this exact client_id +
        # redirect_uri registered, or pending admin approval) happens against Connection's
        # oauth2_client table in SimpleOAuthProvider.authorize(), which is async and therefore
        # cannot run from this SDK hook (called synchronously, before authorize() -- see the
        # AI-2883 RFC, feature_spec/oauth_dynamic_client_registration/RFC.md, Resolution
        # Strategy §2).
        #
        # This is NOT a per-domain trust list (the removed hardcoded whitelist) -- nothing below grants
        # trust to any host, Connection's /oauth/clients/validate still does that exclusively for
        # https. It mirrors exactly the redirect_uri *shape* Connection will ever register (see
        # PendingMcpClientDecoder, cited in the RFC's "Redirect-URI shape" section): https with any
        # host, cursor:// restricted to Connection's own fixed host allowlist (a custom scheme has
        # no certificate authority backing it, so Connection never accepts "any" cursor host
        # either), or http:// restricted to loopback (RFC 8252). A shape outside that can never end
        # up REGISTERED anyway, so rejecting it here costs no legitimate flow -- but it matters for
        # a reason beyond tidiness: when authorize()'s Connection check errors, this hook has
        # *already run* and its accepted redirect_uri is what the mcp SDK's own error-response
        # fallback would use if anything else in authorize() raised unexpectedly. Bounding the
        # shape here bounds how bad that fallback can be (no file://, intent://, userinfo or
        # fragment tricks, no unlisted cursor host), even though authorize()'s own ERROR branch
        # avoids that fallback entirely by not raising (see its docstring).
        if not redirect_uri:
            LOG.warning('[validate_redirect_uri] No redirect_uri specified.')
            raise InvalidRedirectUriError('The redirect_uri must be specified.')

        stripped_uri = self._strip_redirect_uri(redirect_uri)
        # `is not None`, not truthiness: an empty-but-present component (e.g. the trailing '#' in
        # 'https://evil.example/cb#' parses to fragment='', not None) must still be rejected -- a
        # bare truthy check would silently let it through (Copilot review finding).
        if redirect_uri.username is not None or redirect_uri.password is not None or redirect_uri.fragment is not None:
            LOG.warning(f'[validate_redirect_uri] userinfo or fragment in redirect_uri: {stripped_uri}')
            raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')
        if len(str(redirect_uri)) > 2048:
            LOG.warning(f'[validate_redirect_uri] redirect_uri exceeds 2048 characters: {stripped_uri}')
            raise InvalidRedirectUriError('redirect_uri exceeds maximum length of 2048 characters.')

        scheme = redirect_uri.scheme
        if scheme == 'http':
            if (redirect_uri.host or '').lower() not in _LOOPBACK_HOSTS:
                LOG.warning(f'[validate_redirect_uri] non-loopback http redirect_uri: {stripped_uri}')
                raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')
        elif scheme == 'cursor':
            if (redirect_uri.host or '').lower() not in _ALLOWED_CURSOR_HOSTS:
                LOG.warning(f'[validate_redirect_uri] unlisted cursor host in redirect_uri: {stripped_uri}')
                raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')
        elif scheme != 'https':
            LOG.warning(f'[validate_redirect_uri] Rejected scheme in redirect_uri: {stripped_uri}')
            raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')

        LOG.info(f'[validate_redirect_uri] Accepted redirect_uri (pending Connection check): {stripped_uri}')
        return redirect_uri

    @staticmethod
    def _strip_redirect_uri(redirect_uri: AnyUrl) -> AnyUrl:
        return AnyUrl.build(scheme=redirect_uri.scheme or '', host=redirect_uri.host or '', port=redirect_uri.port)


class _ExtendedAuthorizationCode(AuthorizationCode):
    oauth_access_token: AccessToken
    oauth_refresh_token: RefreshToken
    # Whether the Connection OAuth scope requested in authorize() included 'projectless'.
    # - Always True for a code issued now; kept so the session row records what was requested.
    # - Defaults to True so an in-flight code whose state JWT predates this field still decodes.
    # Only a session persisted by an earlier build of AI-2883 can have False (a dynamically-approved
    # client that got 'claudai' alone, before RFC Decision §10); current write paths always set True.
    oauth_projectless: bool = True


class ProxyAccessToken(AccessToken):
    # The whole-stack Keboola programmatic session obtained by exchanging the league OAuth
    # access token (`oauth_session_exchange` RFC). `kbc_access_token` is forwarded downstream
    # as `config.storage_token`, exactly like a directly-supplied `kbc_at_*` token. The refresh
    # token is deliberately NOT carried here (only on `ProxyRefreshToken`, which is what
    # `exchange_refresh_token` actually receives) — access tokens are sent/handled far more often,
    # so duplicating the longer-lived refresh token onto them would needlessly widen its exposure.
    kbc_access_token: str
    session_id: str | None = None

    # The multi-project scope persisted on the oauth_sessions row (see SessionStore.update_scope),
    # carried here so mcp.py can rebuild a SessionScope without a second DB round-trip -- the row is
    # already fetched in load_access_token below. Same exposure-minimization reasoning as
    # kbc_access_token: only the fields mcp.py actually needs, not the whole OAuthSession.
    scope_project_ids: list[int] | None = None
    scope_read_only: bool = False
    scope_confirmed: bool = False
    scope_scoped_token: str | None = None
    scope_scoped_expires_at: datetime | None = None


class ProxyRefreshToken(RefreshToken):
    # The refresh side of the same exchanged session; used to refresh independently of the
    # (single-use, discarded) league OAuth token pair.
    kbc_refresh_token: str
    session_id: str | None = None


class DatabaseUnavailableMiddleware:
    """Translates a Postgres outage during bearer-token verification into a clean, retryable 503.

    Must wrap `AuthenticationMiddleware` from the outside (placed before it in
    `SimpleOAuthProvider.get_middleware()`'s returned list): a `DatabaseUnavailableError` raised
    inside `AuthenticationBackend.authenticate()` (this server's `load_access_token`, called once
    per request, before routing) is not a Starlette `AuthenticationError`, so
    `AuthenticationMiddleware` does not catch it -- it propagates past that middleware to whatever
    wraps it next. FastMCP's own Starlette app for this provider (`create_base_app`) registers no
    `exception_handlers` at all, and it is a separately *mounted* ASGI app, so the outer app's own
    `exception_handlers` (`cli.py`) never see it either -- verified empirically with
    `starlette.testclient.TestClient` against both FastMCP's actual `create_base_app` shape and a
    minimal repro. This middleware is the only place in the stack that can turn it into a response
    instead of Starlette's bare, unhelpful "Internal Server Error" default.
    """

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self._app(scope, receive, send)
            return
        try:
            await self._app(scope, receive, send)
        except DatabaseUnavailableError as e:
            LOG.error(f'Postgres is unavailable: {e}')
            response = JSONResponse(
                {'message': 'Service temporarily unavailable: the session database is unreachable.'},
                status_code=503,
            )
            await response(scope, receive, send)


# The OAuth routes a redirect to a caller-influenced host must never come out of. Matched without a trailing
# slash: Starlette answers `/authorize/` with a 307 whose Location is built from the request's Host header.
_GUARDED_OAUTH_PATHS = frozenset({'/authorize', '/register', '/token', '/revoke'})
# Every discovery document (`/.well-known/oauth-authorization-server`, `/.well-known/oauth-protected-resource[/...]`) is
# guarded too: their trailing-slash form gets the same Host-derived 307 as the OAuth routes.
_GUARDED_OAUTH_PATH_PREFIX = '/.well-known/'
_DEFAULT_PORTS = {'http': 80, 'https': 443}


def _origin(url: str) -> tuple[str, str, int] | None:
    """`(scheme, hostname, effective port)` of an absolute URL, or None for a relative or malformed one."""
    try:
        parsed = urlparse(url)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if not parsed.scheme or not host:
        return None
    scheme = parsed.scheme.lower()
    return scheme, host.lower(), port if port is not None else _DEFAULT_PORTS.get(scheme, 0)


class UntrustedAuthorizeRedirectMiddleware:
    """Blocks `/authorize` from ever redirecting to a host this server didn't intend.

    The mcp SDK's `AuthorizationHandler.handle` (pinned mcp==2.2.0,
    `mcp/server/auth/handlers/authorize.py`) validates the raw request against its
    `AuthorizationRequest` pydantic model *before* `provider.authorize()` ever runs. On failure
    (e.g. a request missing the required `code_challenge` field), its `error_response()` fallback
    loads a client via `get_client()` -- this provider's implementation is a no-op that returns a
    valid synthetic client for ANY `client_id`, no registry check -- and re-validates the raw
    `redirect_uri` via that client's `validate_redirect_uri()`, which accepts any HTTPS host by
    design (Connection is the real trust authority, not this hook -- see its docstring). If both
    succeed, the SDK redirects straight to that `redirect_uri` with the error params attached: an
    unauthenticated open redirect (CWE-601) that never reaches `_authorize()`'s Connection registry
    check at all. Human review finding (Vojtěch Biberle) on AI-2883, confirming an independent
    Claude review of the same PR.

    Every *legitimate* redirect this server's `/authorize` route issues targets only one of two
    known origins: Connection's own `server_url` (`_oauth_server_auth_url` and `ConnectionClientRegistry`'s
    own `/oauth/authorize`) or this server's own `mcp_server_url` (`_mcp_callback_url`) -- it never
    redirects straight to the caller-supplied `redirect_uri` (that only happens later, from
    `/oauth/callback`, after the real grant or a denied one -- see `SimpleOAuthProvider.access_denied_redirect`).
    So allowlisting the outgoing redirect's origin (scheme,
    host and effective port, not the hostname alone) to exactly those two closes the gap without
    touching the intentional "any https host" shape check. The same guard covers `/register`, `/token`,
    `/revoke` and the `/.well-known/*` discovery documents, and the trailing-slash form of each: Starlette's slash normalisation answers
    `/authorize/` with a 307 built from the request's Host header.

    Must be the outermost middleware (listed first in `get_middleware()`) so it inspects the final
    response after every inner layer -- including the SDK's own route handler -- has run.

    It must be applied in two places: `get_middleware()` only wraps FastMCP's mounted `/mcp` app,
    while the CLI attaches `get_routes()` (incl. the public root `/authorize`) to the outer Starlette
    app, so `CustomRoutes.add_to_starlette()` adds it there too.
    """

    def __init__(self, app: ASGIApp, trusted_origins: frozenset[tuple[str, str, int]]) -> None:
        self._app = app
        self._trusted_origins = trusted_origins

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Route-relative path: under the production `/mcp` mount `scope['path']` is `/mcp/authorize`
        # (root_path='/mcp'), which a raw comparison would miss (Vojtěch Biberle review, AI-2883).
        route_path = get_route_path(scope).rstrip('/') if scope['type'] == 'http' else ''
        if not route_path or not (
            route_path in _GUARDED_OAUTH_PATHS or route_path.startswith(_GUARDED_OAUTH_PATH_PREFIX)
        ):
            await self._app(scope, receive, send)
            return

        blocked = False

        async def guarded_send(message: dict) -> None:
            nonlocal blocked
            if blocked:
                # The original response was already replaced below; swallow its remaining
                # messages (e.g. the body chunk that would follow its start) instead of
                # forwarding them after our own complete response was sent.
                return
            if message['type'] == 'http.response.start' and 300 <= message['status'] < 400:
                location = Headers(raw=message['headers']).get('location')
                # A missing or relative Location has no origin, so it is blocked too: fail closed, since a
                # relative redirect can't be proven to stay on a trusted origin.
                origin = _origin(location) if location else None
                if origin is None or origin not in self._trusted_origins:
                    blocked = True
                    LOG.warning(
                        f'[authorize] Blocked redirect to untrusted origin: {_sanitize_for_log(location or "")}'
                    )
                    response = JSONResponse(
                        {'error': 'invalid_request', 'error_description': 'Invalid authorization request.'},
                        status_code=400,
                    )
                    await response(scope, receive, send)
                    return
            await send(message)

        await self._app(scope, receive, guarded_send)


_AUTHORIZE_RATE_LIMIT_WINDOW_SECONDS = 60


def _caller_address(scope: Scope, trusted_hops: int) -> str | None:
    """The caller's address as the nearest proxy we trust saw it, or None when there is none to count.

    Behind `trusted_hops` proxies that each append the address they received the connection from, the caller is the entry
    that many places from the END of `X-Forwarded-For`: everything before it was written by the caller or by hops
    nobody vouches for, so it is never used. Without that header the connection's own peer address is the best there is.

    The result is the key of a counter, so it is normalised to what one caller controls: an IPv4-mapped IPv6 address is
    the IPv4 one, and an IPv6 address is its /64 (a single subscriber is routinely given a whole /64, so counting the
    full address would let one caller dodge the limit by rotating through it). An address that cannot be told is None,
    never a shared catch-all key that one caller could fill for everybody else."""
    entries = [e.strip() for e in Headers(scope=scope).get('x-forwarded-for', '').split(',') if e.strip()]
    candidate: str | None = None
    if trusted_hops > 0 and len(entries) >= trusted_hops:
        candidate = entries[-trusted_hops]
    elif scope.get('client'):
        candidate = scope['client'][0]
    try:
        address = ipaddress.ip_address(candidate or '')
    except ValueError:
        return None
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return str(address.ipv4_mapped)
        return str(ipaddress.ip_network(f'{address}/64', strict=False))
    return str(address)


class AuthorizeRateLimitMiddleware:
    """Limits `/authorize` requests per caller address, across every replica (AI-4005).

    Every `/authorize` of an unknown client costs a call to Connection's unauthenticated validate endpoint, whose ceiling
    is shared by the whole stack, so one caller sending a fresh redirect_uri on every request could otherwise keep the
    stack-wide budget at capacity and lock every other uncached client out. The SDK's `authorize()` never sees the
    caller's address, hence a middleware ahead of the route. The count lives in the shared store so it holds across
    replicas; a store that cannot answer lets the request through (the database being down fails logins anyway)."""

    def __init__(self, app: ASGIApp, store: RateLimitStore, limit_per_minute: int, trusted_hops: int) -> None:
        self._app = app
        self._store = store
        self._limit = limit_per_minute
        self._trusted_hops = trusted_hops

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http' or get_route_path(scope).rstrip('/') != '/authorize':
            await self._app(scope, receive, send)
            return

        address = _caller_address(scope, self._trusted_hops)
        if address is None:  # nothing to tell the caller apart by: not counted, rather than pooled with everybody else
            await self._app(scope, receive, send)
            return
        try:
            hits = await self._store.hit(f'authorize:{address}', _AUTHORIZE_RATE_LIMIT_WINDOW_SECONDS)
        except Exception as e:
            LOG.warning(f'[authorize] Per-address rate limit unavailable, letting the request through: {e!r}')
            await self._app(scope, receive, send)
            return
        if hits > self._limit:
            LOG.warning(f'[authorize] Per-address rate limit reached: address={address}, limit={self._limit}/min')
            response = JSONResponse(
                {
                    'error': 'temporarily_unavailable',
                    'error_description': 'Too many authorization requests from this address. Try again in a minute.',
                },
                status_code=429,
                headers={'Retry-After': str(_AUTHORIZE_RATE_LIMIT_WINDOW_SECONDS)},
            )
            await response(scope, receive, send)
            return
        await self._app(scope, receive, send)


_METADATA_PATH_PREFIXES = ('/.well-known/oauth-authorization-server', '/.well-known/openid-configuration')


class _PatchAuthServerMetadata:
    """Wraps an authorization-server metadata endpoint so that it also advertises `none` as a token endpoint
    authentication method, first, and RFC 9207 issuer identification.

    The MCP SDK hardcodes `["client_secret_post", "client_secret_basic"]`, but this server's clients are public ones
    (PKCE, no secret) and its token endpoint accepts them: a client that picks its `token_endpoint_auth_method` from
    this document (Open WebUI does) would otherwise register as a confidential client. The SDK has no way to set
    `authorization_response_iss_parameter_supported` either, and ChatGPT uses its stable redirect URI only for an
    authorization server that announces it. The SDK bakes the metadata into the handler when it builds the route, so
    the document is patched on its way out instead. An object, not a function: Starlette would treat a function
    endpoint as a request handler rather than an ASGI app."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self._app(scope, receive, send)
            return

        start: dict | None = None
        body = b''

        async def capture(message: dict) -> None:
            nonlocal start, body
            if message['type'] == 'http.response.start':
                start = message  # held back: the length header has to match the patched body
                return
            if message['type'] != 'http.response.body':
                await send(message)
                return
            body += message.get('body', b'')
            if message.get('more_body'):
                return
            assert start is not None
            try:
                metadata = json.loads(body)
                if start['status'] == 200 and isinstance(metadata, dict):
                    methods = metadata.get('token_endpoint_auth_methods_supported')
                    if isinstance(methods, list) and 'none' not in methods:
                        metadata['token_endpoint_auth_methods_supported'] = ['none', *methods]
                    # Every authorization response to the client carries `iss`: see `SimpleOAuthProvider.issuer`.
                    metadata['authorization_response_iss_parameter_supported'] = True
                    body = json.dumps(metadata).encode()
            except ValueError:
                pass  # not JSON (an error page, a preflight answer): forwarded as it is
            headers = [(k, v) for k, v in start['headers'] if k.lower() != b'content-length']
            headers.append((b'content-length', str(len(body)).encode()))
            await send({**start, 'headers': headers})
            await send({'type': 'http.response.body', 'body': body})

        await self._app(scope, receive, capture)


# The RFC 8707 `resource` values of the /token request being handled, for `SimpleOAuthProvider`: the SDK's token handler
# passes them to the provider for no grant but the jwt-bearer one.
_TOKEN_REQUEST_RESOURCES: ContextVar[tuple[str, ...]] = ContextVar('token_request_resources', default=())


class _CaptureTokenRequestResource:
    """Wraps the /token endpoint so that `SimpleOAuthProvider` can read the request's `resource` parameters: the body is
    read here, put into `_TOKEN_REQUEST_RESOURCES`, and replayed to the wrapped handler unchanged."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self._app(scope, receive, send)
            return

        messages = []
        body = b''
        while True:
            message = await receive()
            messages.append(message)
            if message['type'] != 'http.request':
                break
            body += message.get('body', b'')
            if not message.get('more_body'):
                break

        async def replay() -> dict:
            return messages.pop(0) if messages else await receive()

        resources = tuple(parse_qs(body.decode('utf-8', 'replace')).get('resource', []))
        token = _TOKEN_REQUEST_RESOURCES.set(resources)
        try:
            await self._app(scope, replay, send)
        finally:
            _TOKEN_REQUEST_RESOURCES.reset(token)


def _resource_key(resource: str) -> tuple[str, str, int, str] | None:
    """What two spellings of one resource URL share (origin, and the path without a trailing slash), or None for a value
    that is not an absolute URL without query and fragment (RFC 8707 §2)."""
    origin = _origin(resource)
    if origin is None:
        return None
    parsed = urlparse(resource)  # cannot raise: `_origin` already parsed it
    if parsed.query or parsed.fragment:
        return None
    return *origin, parsed.path.rstrip('/')


class SimpleOAuthProvider(OAuthProvider):
    def __init__(
        self,
        *,
        storage_api_url: str,
        mcp_server_url: str,
        callback_endpoint: str,
        client_id: str,
        client_secret: str,
        server_url: str,
        scope: str,
        session_store: SessionStore,
        jwt_secret: str | None = None,
        validate_rate_limit: int | None = None,
        dynamic_client_approval: bool = False,
        approval_daily_limit: int | None = None,
        rate_limit_store: RateLimitStore | None = None,
        validate_fleet_limit: int | None = None,
        authorize_rate_limit_per_ip: int | None = None,
        trusted_proxy_hops: int = 1,
        encryption_key: bytes | None = None,
    ) -> None:
        """
        Creates OAuth provider implementation.

        :param storage_api_url: The URL of the Storage API service.
        :param mcp_server_url: The URL of the MCP server itself.
        :param callback_endpoint: The endpoint where the OAuth server redirects to after the user authorizes.
        :param client_id: The client ID registered with the OAuth server.
        :param client_secret: The client secret registered with the OAuth server
        :param server_url: The URL of the OAuth server that the MCP server should authenticate to.
        :param scope: The scope of access to request from the OAuth server.
        :param session_store: Postgres-backed store for the exchanged Keboola session (access/refresh
            token + multi-project scope) -- see oauth_session_persistence RFC. The short-lived,
            pre-authentication artifacts (authorize-state, authorization code) still use `jwt_secret`
            below; only the long-lived, real-credential-carrying tokens live in the store.
        :param jwt_secret: The secret key for encoding and decoding the pre-auth JWT artifacts.
        :param approval_daily_limit: With `dynamic_client_approval` on, how many distinct clients per day may be sent to
            Connection's approval screen; None means no cap. See `_ApprovalBudget`.
        :param rate_limit_store: Postgres-backed counters shared by every replica (AI-4005): the call budget for
            Connection's validate endpoint, the daily cap on new clients, and the per-address limit on `/authorize`.
            Without one (or while it cannot be reached) each of them is counted per process.
        :param validate_fleet_limit: Calls per minute, across every replica, to Connection's validate endpoint; None means
            the built-in default.
        :param authorize_rate_limit_per_ip: `/authorize` requests per minute allowed from one caller address, across
            every replica; None means no such limit.
        :param trusted_proxy_hops: How many proxies in front of this server append to `X-Forwarded-For`; the caller
            address is the entry that many places from the end (the ones before it are caller-supplied).
        :param encryption_key: The session encryption key; the Connection client ids are keyed with a key derived
            from it (see `_connection_client_id`), so every replica must get the same one. Without it a
            process-local key is used, which is fine for local runs and tests only.
        """
        super().__init__(
            base_url=mcp_server_url,
            client_registration_options=ClientRegistrationOptions(enabled=True),
        )
        self._session_store = session_store

        self._storage_api_url = storage_api_url
        self._mcp_callback_url = urljoin(mcp_server_url, callback_endpoint)
        self._oauth_client_id = client_id
        self._oauth_client_secret = client_secret
        self._oauth_server_auth_url = urljoin(server_url, '/oauth/consent')
        self._oauth_server_token_url = urljoin(server_url, '/oauth/token')
        self._oauth_scope = scope
        self._jwt_secret = jwt_secret or secrets.token_hex(32)
        # A sub-key, so the AES key itself is never used for anything but encrypting sessions.
        self._client_id_key = hmac.new(
            encryption_key or secrets.token_bytes(32), b'keboola-mcp-server/connection-client-id', hashlib.sha256
        ).digest()
        self._client_registry = ConnectionClientRegistry(
            server_url,
            validate_rate_limit=validate_rate_limit,
            rate_limit_store=rate_limit_store,
            validate_fleet_limit=validate_fleet_limit,
        )
        self._rate_limit_store = rate_limit_store
        self._authorize_rate_limit_per_ip = authorize_rate_limit_per_ip
        self._trusted_proxy_hops = trusted_proxy_hops
        # Whether an unregistered client is sent to Connection's approval screen. Off means only clients Connection
        # already knows can log in (see `Config.oauth_dynamic_client_approval`).
        self._dynamic_client_approval = dynamic_client_approval
        self._approval_daily_limit = approval_daily_limit
        self._approval_budget = _ApprovalBudget(approval_daily_limit) if approval_daily_limit else None
        # The only two origins the OAuth routes may ever legitimately redirect to -- see
        # UntrustedAuthorizeRedirectMiddleware's docstring.
        self.trusted_redirect_origins = frozenset(
            o for o in (_origin(mcp_server_url), _origin(server_url)) if o is not None
        )
        # The RFC 8707 resources this server's tokens may be issued for: the PRM `resource` (the server's own URL) and
        # the MCP endpoint a client was configured with (`/mcp`, or the legacy `/sse` that forwards to it).
        own_path = urlparse(mcp_server_url).path.rstrip('/')
        self._own_resource_keys = frozenset(
            key for path in ('', '/mcp', '/sse') if (key := _resource_key(urljoin(mcp_server_url, own_path + path)))
        )

    @property
    def issuer(self) -> str:
        """The issuer identifier, exactly as the metadata `issuer` and the PRM `authorization_servers` serialize it.

        RFC 9207: every authorization response this server sends to the client carries it as `iss`, and a client
        compares that with the issuer it discovered by exact string, so it must come from the same value."""
        return str(self.issuer_url)

    async def aclose(self) -> None:
        """Releases what the provider holds open (the pooled client of the client registry)."""
        await self._client_registry.aclose()
        if self._rate_limit_store is not None:
            await self._rate_limit_store.close()

    def authorize_rate_limit_middleware(self) -> Middleware | None:
        """The per-address limit on `/authorize`, or None when it is not configured or there is no shared store to
        count it in."""
        if self._rate_limit_store is None or not self._authorize_rate_limit_per_ip:
            return None
        return Middleware(
            AuthorizeRateLimitMiddleware,
            store=self._rate_limit_store,
            limit_per_minute=self._authorize_rate_limit_per_ip,
            trusted_hops=self._trusted_proxy_hops,
        )

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        """The SDK's routes, with the authorization-server metadata documents patched by `_PatchAuthServerMetadata` and
        the token endpoint wrapped by `_CaptureTokenRequestResource`."""
        routes: list[Route] = []
        for route in super().get_routes(mcp_path):
            if isinstance(route, Route) and route.path.startswith(_METADATA_PATH_PREFIXES):
                route = Route(route.path, endpoint=_PatchAuthServerMetadata(route.endpoint), methods=route.methods)
            elif isinstance(route, Route) and route.path == '/token':
                route = Route(route.path, endpoint=_CaptureTokenRequestResource(route.endpoint), methods=route.methods)
            routes.append(route)
        return routes

    def is_own_resource(self, resource: str) -> bool:
        """Whether `resource` names this server, in any spelling a client may use (see `_own_resource_keys`)."""
        return _resource_key(resource) in self._own_resource_keys

    def _token_resource(self, bound_resource: str | None, client_id: str) -> str | None:
        """The resource a /token request may issue tokens for, given the one the grant is already bound to (named at
        /authorize, or stored on the session): the request may repeat it or leave it out, never name another one.

        :raises TokenError: `invalid_target` (RFC 8707 §2) for more than one resource, a different one, or one that is
            not this server.
        """
        requested = _TOKEN_REQUEST_RESOURCES.get()
        resource = bound_resource or (requested[0] if requested else None)
        if len(requested) > 1:
            problem = 'Only one resource can be requested.'
        elif requested and _resource_key(requested[0]) != _resource_key(cast(str, resource)):
            problem = 'The resource differs from the one authorized.'
        elif resource is not None and not self.is_own_resource(resource):
            problem = 'The resource is not this MCP server.'
        else:
            return resource
        # WARNING on purpose: a client that names this server in a spelling nobody foresaw cannot log in.
        LOG.warning(
            f'[token] Refused with invalid_target: {problem} client_id={_sanitize_client_id_for_log(client_id)}, '
            f'bound={_sanitize_for_log(bound_resource or "")!r}, requested={[_sanitize_for_log(r) for r in requested]!r}'
        )
        raise TokenError(error='invalid_target', error_description=problem)

    def get_middleware(self) -> list[Middleware]:
        """Prepends `UntrustedAuthorizeRedirectMiddleware` and `DatabaseUnavailableMiddleware`
        ahead of the base class's `AuthenticationMiddleware`/`AuthContextMiddleware`.

        `UntrustedAuthorizeRedirectMiddleware` must be outermost (listed first) so it sees the
        final response after every inner layer, including `DatabaseUnavailableMiddleware` and the
        SDK's own route handler, has run -- see its docstring for why. `DatabaseUnavailableMiddleware`
        must sit outside `AuthenticationMiddleware` rather than rely on any `exception_handlers`
        dict -- see that middleware's own docstring.
        """
        return [
            Middleware(UntrustedAuthorizeRedirectMiddleware, trusted_origins=self.trusted_redirect_origins),
            *([limit] if (limit := self.authorize_rate_limit_middleware()) else []),
            Middleware(DatabaseUnavailableMiddleware),
            *super().get_middleware(),
        ]

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        """
        Gets the information about a registered OAuth client by its client ID.
        This specific implementation is a no-op to avoid having to persist the registered clients.

        :param client_id: A string representing the unique OAuth client identifier.
        :return: An `_OAuthClientInformationFull` instance which contains just the client ID
          and turns off all the client-based validations (e.g. redirect URI and scopes).
        """
        client = _OAuthClientInformationFull(
            # Use a fake redirect URI. Normally, we would retrieve the client from a persistent registry
            # and return the registered redirect URI.
            redirect_uris=[AnyHttpUrl('http://foo')],
            client_id=client_id,
            token_endpoint_auth_method='none',
        )
        LOG.debug(f'Client loaded: client_id={_sanitize_client_id_for_log(client_id)}')
        return client

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        """
        Registers an OAuth client. This grants the client no trust whatsoever -- that still comes
        entirely from Connection's oauth2_client registry, checked in authorize() -- it only
        remembers the client_name submitted here (if any) so Connection's dynamic-approval screen
        can show it later, since /authorize never receives client_name itself (see the AI-2883 RFC,
        Problem section, for why this cache exists at all).

        :param client_info: The full information of the OAuth client to be registered.
        """
        self._client_registry.remember_client_name(client_info.client_id, client_info.client_name)
        # Log the sanitized/capped name just stored, not the raw submission -- /register is
        # unauthenticated, so the raw client_name can carry control characters or be arbitrarily
        # long, and interpolating it directly would defeat the point of sanitizing it on the way
        # into the cache (Copilot review finding: log-injection / unbounded log message).
        sanitized_name = self._client_registry.get_client_name(client_info.client_id)
        LOG.debug(
            f'Client registered: client_id={_sanitize_client_id_for_log(client_info.client_id)}, client_name={sanitized_name}'
        )

    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        """
        Creates a URL that redirects to the OAuth server for authorization.

        First checks the requesting client + redirect_uri against Connection's OAuth client
        registry (`POST /oauth/clients/validate`, via `self._client_registry`) -- see the AI-2883
        RFC (feature_spec/oauth_dynamic_client_registration/RFC.md) for the full design and why
        this replaced a hardcoded domain whitelist. Every registered pair -- pre-registered
        (e.g. Claude.ai) or dynamically approved (Flow B, once approved) -- goes to Connection's
        `/oauth/consent` with `claudai projectless` scope, and each such request is logged at INFO
        (`Registered client proceeding to consent`). An unregistered pair is sent to Connection's
        own `/oauth/authorize` with a `pending_mcp_client` payload instead, so an authenticated
        Keboola user can approve it there. Connection being unreachable or erroring never falls
        through to any of these outcomes (fails closed, see AI-3792).

        The authorization URL's state parameter is an encrypted JWT that contains all the authorization parameters.
        The state expires after 5 minutes.

        :param client: The OAuth client details.
        :param params: The authorization parameters provided by the client, such as redirect URI, state, scopes, etc.

        :return: The authorization URL that redirects to the OAuth server.
        """
        try:
            return await self._authorize(client, params)
        except Exception:
            # Anything unexpected escaping this method reaches the mcp SDK's own generic handler
            # (AuthorizationHandler.handle's outer `except Exception`), which redirects to the
            # *caller-supplied* redirect_uri with error params -- an open redirect now that
            # validate_redirect_uri accepts any https host (Connection is the real authority, not
            # a domain list -- see its docstring). Route every unexpected failure through the same
            # own-origin fallback as the ERROR branch below, instead of relying on nothing else in
            # this method ever raising (Copilot review finding).
            LOG.exception(
                f'[authorize] Unexpected error building authorization URL: client_id={_sanitize_client_id_for_log(client.client_id)}'
            )
            return construct_redirect_uri(
                self._mcp_callback_url,
                error='temporarily_unavailable',
                error_description='Could not complete the authorization request.',
            )

    def _unregistered_client_page(self) -> str:
        """The same-origin error page for a client that is not registered, never the caller's redirect_uri (see the
        ERROR branch of `_authorize` for why)."""
        return construct_redirect_uri(
            self._mcp_callback_url,
            error='unregistered_client',
            error_description='The OAuth client is not registered.',
        )

    async def _approval_budget_allows(self, connection_client_id: str) -> bool:
        """Whether this client may be sent to the approval screen: counted across every replica when the shared store
        answers, else in this process (see `_ApprovalBudget`)."""
        if self._approval_daily_limit is None:
            return True
        if self._rate_limit_store is not None:
            try:
                return await self._rate_limit_store.claim(
                    'approval', connection_client_id, _APPROVAL_BUDGET_WINDOW_SECONDS_INT, self._approval_daily_limit
                )
            except Exception as e:
                LOG.warning(f'[authorize] Shared approval budget unavailable, using the local one: {e!r}')
        return self._approval_budget is None or self._approval_budget.try_acquire(connection_client_id)

    async def _authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        redirect_uri_str = str(params.redirect_uri)
        connection_client_id = _connection_client_id(redirect_uri_str, self._client_id_key)

        if params.resource is not None and not self.is_own_resource(params.resource):
            # RFC 8707 §2 invalid_target, before the user goes through consent for tokens /token would refuse. On this
            # server's own page, like every other error here (see the ERROR branch below for why).
            LOG.warning(
                f'[authorize] Refused with invalid_target: client_id={_sanitize_client_id_for_log(client.client_id)}, '
                f'resource={_sanitize_for_log(params.resource)!r}'
            )
            return construct_redirect_uri(
                self._mcp_callback_url,
                error='invalid_target',
                error_description='The requested resource is not this MCP server.',
            )

        registration = await self._check_registration(connection_client_id, redirect_uri_str)
        if registration is _ClientRegistration.ERROR:
            LOG.warning(
                f'[authorize] Could not verify client with Connection: client_id={_sanitize_client_id_for_log(client.client_id)}, '
                f'redirect_uri={_sanitize_for_log(redirect_uri_str)}'
            )
            # Deliberately NOT `raise AuthorizeError(...)` here: the mcp SDK's own handler catches
            # that and redirects to the *caller-supplied* redirect_uri with the error params
            # (AuthorizationHandler.error_response, which reuses the redirect_uri validate_redirect_uri
            # already accepted). Since that hook now accepts any https host (Connection is the real
            # authority, not a domain list -- see validate_redirect_uri's docstring), raising here
            # would make this server 302 to an attacker-chosen host on demand (e.g. by exhausting
            # Connection's rate limit) -- an open redirect. Redirecting to our own callback endpoint
            # instead keeps the browser on this server's own origin; the caller gets no callback at
            # all for this attempt (same shape as a Deny on Connection's approval screen) and must
            # time out and retry, same as any other undeliverable authorize attempt.
            return construct_redirect_uri(
                self._mcp_callback_url,
                error='temporarily_unavailable',
                error_description='Could not verify OAuth client with Connection.',
            )

        if registration is _ClientRegistration.NOT_REGISTERED and not self._dynamic_client_approval:
            LOG.info(
                f'[authorize] Unregistered client refused, dynamic client approval is off: client_id={_sanitize_client_id_for_log(client.client_id)}, '
                f'redirect_uri={_sanitize_for_log(redirect_uri_str)}'
            )
            return self._unregistered_client_page()

        if registration is _ClientRegistration.NOT_REGISTERED and not await self._approval_budget_allows(
            connection_client_id
        ):
            # WARNING on purpose: an operator alert keys on this line (more new clients wanting approval than the
            # daily cap allows is what abuse of the open approval looks like).
            LOG.warning(
                f'[authorize] Daily approval budget used up, client refused: client_id={_sanitize_client_id_for_log(client.client_id)}, '
                f'redirect_uri={_sanitize_for_log(redirect_uri_str)}'
            )
            return construct_redirect_uri(
                self._mcp_callback_url,
                error='approval_limit_reached',
                error_description='The daily limit of new OAuth clients was reached.',
            )

        # Create and encode the authorization state.
        # We don't store the authentication states that we create here to avoid having to persist them.
        # Instead, we encode them to JWT and pass them back to the client.
        # The states expire after 5 minutes.
        scopes = cast(list[str], params.scopes or [])
        state = {
            'redirect_uri': redirect_uri_str,
            'redirect_uri_provided_explicitly': str(params.redirect_uri_provided_explicitly),
            # the scopes sent by the MCP server's OAuth client (e.g. claude.ai)
            'scopes': scopes,
            'code_challenge': params.code_challenge,
            'state': params.state,
            'client_id': client.client_id,
            # RFC 8707: bound to the code, then to the session (see `exchange_authorization_code`)
            'resource': params.resource,
            'expires_at': time.time() + 5 * 60,  # 5 minutes from now
            # Carried through to exchange_authorization_code onto the session row. Always True
            # now: _CONNECTION_SCOPE below requests 'projectless' for every registered client.
            'projectless': True,
        }
        LOG.debug(
            f'[authorize] client_id={_sanitize_client_id_for_log(client.client_id)}, params={params}, state={state}'
        )

        if registration is _ClientRegistration.NOT_REGISTERED:
            LOG.info(
                f'[authorize] Unregistered client sent to Connection for approval: client_id={_sanitize_client_id_for_log(client.client_id)}, '
                f'redirect_uri={_sanitize_for_log(redirect_uri_str)}'
            )
            # The approval screen shows the payload's redirect_uri, which is now this server's own
            # callback (RFC Decision §15) -- so the client's real redirect target goes into the
            # displayed name instead, so the approver can still see where the client sends users.
            name = _sanitize_client_name(
                self._client_registry.get_client_name(client.client_id) or connection_client_id
            )
            return self._client_registry.pending_approval_url(
                connection_client_id=connection_client_id,
                redirect_uri=self._mcp_callback_url,
                client_name=_approval_client_name(name, redirect_uri_str),
                # longer than the usual 5 minutes: the user may have to log in to Connection first
                state=self._encode({**state, 'pending': True, 'expires_at': time.time() + 15 * 60}),
            )

        # registration is REGISTERED from here on.
        self._log_proceeding_to_consent(client.client_id, redirect_uri_str)
        auth_url = self._connection_consent_url(state)
        # The URL itself is not logged: it carries the derived Connection client id (`mcp_client_id`), which must stay
        # out of every log line, and the signed state.
        LOG.debug(f'[authorize] client_id={_sanitize_client_id_for_log(client.client_id)}, params={params}')
        return auth_url

    async def _check_registration(self, connection_client_id: str, redirect_uri: str) -> _ClientRegistration:
        """Whether Connection admits this client: registered against this server's callback (a client
        approved since AI-3995, or Claude.ai against its own URI), or -- for clients approved before that
        -- against its OWN redirect_uri. Such a client must keep working: Connection's approval gate skips
        an existing client id, so asking it to approve again would end in League rejecting the mismatching
        redirect_uri as `invalid_client`, a dead end the user cannot get out of."""
        connection_redirect_uri = self._connection_redirect_uri(redirect_uri)
        registration = await self._client_registry.check_registration(connection_client_id, connection_redirect_uri)
        if registration is _ClientRegistration.NOT_REGISTERED and connection_redirect_uri != redirect_uri:
            registration = await self._client_registry.check_registration(connection_client_id, redirect_uri)
        return registration

    def _log_proceeding_to_consent(self, client_id: str, redirect_uri: str) -> None:
        """INFO on purpose: Connection keeps no list of registered clients to inspect later, so this line
        is the record of which callback was sent to consent, and whether it was pre-registered. Written on
        both paths into /oauth/consent: straight from /authorize, and after a new client was approved."""
        connection_client_id = _connection_client_id(redirect_uri, self._client_id_key)
        LOG.info(
            f'[authorize] Registered client proceeding to consent: client_id={_sanitize_client_id_for_log(client_id)}, '
            f'redirect_uri={_sanitize_for_log(redirect_uri)}, '
            f'pre_registered={connection_client_id in _WELL_KNOWN_CONNECTION_CLIENT_IDS.values()}, '
            f'scope={_CONNECTION_SCOPE!r}'
        )

    def _connection_redirect_uri(self, redirect_uri: str) -> str:
        """The redirect_uri Connection has registered for this client: the client's own for a
        Keboola pre-registered one, this server's /oauth/callback for a dynamically approved one."""
        return redirect_uri if redirect_uri in _WELL_KNOWN_CONNECTION_CLIENT_IDS else self._mcp_callback_url

    def _connection_consent_url(self, state: dict[str, Any]) -> str:
        """Connection's /oauth/consent URL for an already-registered client; `state` comes back to
        handle_oauth_callback() with the authorization code."""
        state_jwt = self._encode(state)
        mcp_client_id = _connection_client_id(cast(str, state['redirect_uri']), self._client_id_key)
        return construct_redirect_uri(
            self._oauth_server_auth_url,
            client_id=self._oauth_client_id,
            response_type='code',
            redirect_uri=self._mcp_callback_url,
            state=state_jwt,
            scope=_CONNECTION_SCOPE,
            # The app that is actually asking, so Connection's consent screen can name it instead of the
            # broker (this server). Connection shows the registered client's own name, and only when the
            # signature below matches. Both are ignored by a Connection that doesn't know them yet.
            mcp_client_id=mcp_client_id,
            mcp_client_sig=_sign_mcp_client_id(self._oauth_client_secret, mcp_client_id, state_jwt),
        )

    async def _continue_after_approval(self, state_data: dict[str, Any]) -> str:
        """The user just approved a new client (Flow B): re-check the registration and carry on to
        /oauth/consent in the same browser session, instead of handing the client a dead code
        (AI-3995, RFC Decision §15). The Connection authorization code that came with the callback
        is ignored -- it is unredeemable by design, see ConnectionClientRegistry.pending_approval_url."""
        redirect_uri = cast(str, state_data['redirect_uri'])
        registration = await self._client_registry.check_registration(
            _connection_client_id(redirect_uri, self._client_id_key), self._connection_redirect_uri(redirect_uri)
        )
        if registration is not _ClientRegistration.REGISTERED:
            LOG.warning(f'[handle_oauth_callback] Pending client not registered after approval: {registration}')
            raise HTTPException(400, 'The OAuth client was not approved. Please try connecting again.')
        self._log_proceeding_to_consent(cast(str, state_data['client_id']), redirect_uri)
        state = {k: v for k, v in state_data.items() if k != 'pending'}
        state['expires_at'] = time.time() + 5 * 60
        return self._connection_consent_url(state)

    def _decode_consent_state(self, state: str, log_prefix: str) -> dict[str, Any] | None:
        """The live consent state `authorize()` encoded, or None. The one definition of a valid consent state for
        every path Connection sends it back on (the callback and the relayed denial).

        Neither the JWT nor its claims are logged in full: the same secret signs the MCP authorization codes, which
        carry the user's Connection tokens, and a JWS payload is readable without the key."""
        try:
            state_data = self._decode(state)
        except jwt.InvalidTokenError:
            LOG.debug(f'{log_prefix} Invalid state, length={len(state)}', exc_info=True)
            return None
        # Not a consent state: some other JWT signed with the same secret (an MCP authorization code).
        if not state_data or 'state' not in state_data:
            LOG.debug(f'{log_prefix} Invalid state, keys: {sorted(state_data or {})}')
            return None
        if state_data.get('expires_at', 0) < time.time():
            LOG.debug(
                f'{log_prefix} Expired state: redirect_uri={_sanitize_for_log(str(state_data.get("redirect_uri")))}, '
                f'expires_at={state_data.get("expires_at")}'
            )
            return None
        return state_data

    async def handle_oauth_callback(self, code: str | None, state: str) -> str:
        """
        Handles the callback from the OAuth server.

        :param code: The authorization code provided by the OAuth server; absent (or ignored) when the
            callback follows the approval of a new client, see `_continue_after_approval`.
        :param state: The state originally generated in the authorize() function.

        :return: The URL that redirects back to the AI assistant OAuth client.
        """
        # Validate the state first to prevent calling OAuth server with invalid authorization code.
        state_data = self._decode_consent_state(state, '[handle_oauth_callback]')
        if state_data is None:
            raise HTTPException(400, 'Invalid state parameter')

        if state_data.get('pending'):
            return await self._continue_after_approval(state_data)

        if not code:
            raise HTTPException(400, 'Missing code or state parameter')

        # Exchange the authorization code for the access token with the OAuth server.
        async with _create_http_client() as http_client:
            response = await http_client.post(
                self._oauth_server_token_url,
                data={
                    'client_id': self._oauth_client_id,
                    'client_secret': self._oauth_client_secret,
                    'code': code,
                    'grant_type': 'authorization_code',
                    # FYI: Some tutorials use the redirect_uri here, but it does not seem to be required.
                    # The Keboola OAuth server requires it, but the GitHub OAuth server does not.
                    'redirect_uri': self._mcp_callback_url,
                },
                headers={'Accept': 'application/json'},
            )

            if response.status_code != 200:
                LOG.error(
                    '[handle_oauth_callback] Failed to exchange code for token, '
                    f'OAuth server response: status={response.status_code}, text={response.text}'
                )
                raise HTTPException(
                    400, f'Failed to exchange code for token: status={response.status_code}, text={response.text}'
                )

            data = response.json()
            _log_debug(f'[handle_oauth_callback] OAuth server response: {data}')

            if 'error' in data:
                LOG.error(f'[handle_oauth_callback] Error when exchanging code for token: data={data}')
                raise HTTPException(400, data.get('error_description', data['error']))

        redirect_uri = cast(str, state_data['redirect_uri'])
        scopes = cast(list[str], state_data['scopes'])
        access_token, refresh_token = self._read_oauth_tokens(data, scopes)

        # Create MCP authorization code
        # This is deserialized into _ExtendedAuthorizationCode instance in load_authorization_code() function.
        # Never add a 'state' key: a consent state and a code are told apart by it.
        auth_code = {
            'code': f'mcp_{secrets.token_hex(16)}',
            'client_id': state_data['client_id'],
            'redirect_uri': redirect_uri,
            'redirect_uri_provided_explicitly': (state_data['redirect_uri_provided_explicitly'] == 'True'),
            'expires_at': int(time.time() + 5 * 60),  # 5 minutes from now
            'scopes': scopes,
            'code_challenge': state_data['code_challenge'],
            'resource': state_data.get('resource'),
            'oauth_access_token': access_token.model_dump(),
            'oauth_refresh_token': refresh_token.model_dump(),
            # Defaults to True (matching _ExtendedAuthorizationCode's own default) for a state JWT
            # encoded just before a deploy that predates this field.
            'oauth_projectless': bool(state_data.get('projectless', True)),
        }
        auth_code_jwt = self._encode(auth_code)

        mcp_redirect_uri = construct_redirect_uri(
            redirect_uri_base=redirect_uri,
            code=auth_code_jwt,
            state=state_data['state'],
            code_challenge=state_data['code_challenge'],
            iss=self.issuer,
        )
        # Only the base: the code carries the user's Connection tokens, readable without the key.
        LOG.debug(f'[handle_oauth_callback] Redirecting to the client: redirect_uri={_sanitize_for_log(redirect_uri)}')

        return mcp_redirect_uri

    def access_denied_redirect(self, state: str) -> str | None:
        """The client's own redirect_uri with `error=access_denied` (RFC 6749 §4.1.2.1) and `iss` (RFC 9207), for an
        authorization Connection denied (the user clicked Deny on the consent screen, or Connection refused the grant),
        so the client learns of the denial instead of timing out.

        Nothing proves that the error came from Connection, and a state can be replayed until it expires, so this is a
        redirect anyone can trigger to the state's redirect_uri. It is given only for a redirect_uri Keboola
        pre-registered itself (`_WELL_KNOWN_CONNECTION_CLIENT_IDS`): a dynamically approved one may have been approved
        by the very user who wants the redirect, and a pending one by nobody.

        :param state: The state Connection sent back with the error, the one `authorize()` put on the consent URL.
        :return: The redirect, or None when the state is not a live consent flow of a pre-registered client.
        """
        state_data = self._decode_consent_state(state, '[access_denied_redirect]')
        if state_data is None or state_data.get('pending'):
            return None
        redirect_uri = cast(str, state_data['redirect_uri'])
        if redirect_uri not in _WELL_KNOWN_CONNECTION_CLIENT_IDS:
            return None
        return construct_redirect_uri(redirect_uri, error='access_denied', state=state_data['state'], iss=self.issuer)

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        """
        Loads and validates the authorization code.
        This function decrypts a JWT authorization code and returns an `_ExtendedAuthorizationCode` object
        if the authorization code is valid. It returns `None` otherwise.

        :param client: The OAuth client details.
        :param authorization_code: The JWT authorization code to be loaded and validated.

        :return: An `_ExtendedAuthorizationCode` instance if the authorization code is valid, otherwise `None`.
        """
        try:
            auth_code_raw = self._decode(authorization_code)
        except jwt.InvalidTokenError:
            LOG.debug(
                f'[load_authorization_code] Invalid authorization_code, length={len(authorization_code)}', exc_info=True
            )
            return None
        # A consent state (the only JWT with a 'state' key) sent as the code is signed with the same secret but is no
        # code: /token answers invalid_grant.
        if 'state' in auth_code_raw:
            LOG.debug(f'[load_authorization_code] Not an authorization code, keys: {sorted(auth_code_raw)}')
            return None

        auth_code = _ExtendedAuthorizationCode.model_validate(
            auth_code_raw | {'redirect_uri': AnyUrl(auth_code_raw['redirect_uri'])}
        )
        _log_debug(
            f'[load_authorization_code] client_id={_sanitize_client_id_for_log(client.client_id)}, authorization_code={authorization_code}, '
            f'auth_code={auth_code}'
        )

        # Log the expired authorization code.
        # The mcp library itself performs the check and returns a proper response, but no logs.
        now = time.time()
        if auth_code.expires_at and auth_code.expires_at < now:
            LOG.info(
                f'[load_authorization_code] Expired authorization code: '
                f'auth_code.expires_at={auth_code.expires_at}, now={now}'
            )

        return auth_code

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        """
        Swaps the authorization code for a new access and refresh tokens from the OAuth server.
        The function also creates a new Storage API token for accessing the AI Service and Jobs Queue APIs.

        :param client: The OAuth client details.
        :param authorization_code: The authorization code issued earlier by the `authorize()` function.

        :return: A new OAuthToken containing the access and refresh tokens.

        :raises HTTPException: If the OAuth server response indicates an error.
        """
        _log_debug(
            f'[exchange_authorization_code] authorization_code={authorization_code}, client_id={_sanitize_client_id_for_log(client.client_id)}'
        )
        # Check that we get the instance loaded by load_authorization_code() function.
        assert isinstance(authorization_code, _ExtendedAuthorizationCode)
        # Before the exchange: a refused resource must not cost a Keboola session.
        resource = self._token_resource(authorization_code.resource, client.client_id)

        # Exchange the league OAuth access token for a whole-stack Keboola programmatic session.
        # The league token is used exactly once, here, and then never referenced again.
        token_set = await self._exchange_oauth_for_session(authorization_code.oauth_access_token.token)
        access_token, refresh_token, session = await self._session_store.create(
            client_id=client.client_id,
            user_email=None,
            kbc_access_token=token_set.access_token,
            kbc_refresh_token=token_set.refresh_token,
            kbc_access_expires_at=datetime.fromtimestamp(token_set.expires_at, tz=timezone.utc),
            oauth_projectless=authorization_code.oauth_projectless,
            resource=resource,
        )
        LOG.info(
            f'[exchange_authorization_code] OAuth session created: session_id={session.id}, '
            f'client_id={_sanitize_client_id_for_log(client.client_id)}, resource={_sanitize_for_log(resource or "")!r}'
        )
        await self._auto_confirm_project_scope(session.id, token_set.access_token)
        return self._oauth_token(access_token, refresh_token, authorization_code.scopes)

    async def _auto_confirm_project_scope(self, session_id: str, subject_token: str) -> None:
        """The league consent screen (`/oauth/consent`) already makes the user pick "all projects"
        or a specific subset before this code path ever runs -- there is no separate scoping
        decision left for the MCP session to defer to via ``set_project_scope``. Confirm the scope
        immediately to whatever the freshly-exchanged token can reach, so the session is usable
        right away (mirrors the local ``login``/``login --pat`` flow, which does the same for the
        same reason -- see the "Security hardening" RFC increment).

        This used to only auto-confirm when introspection returned exactly one project, on the
        assumption that this server's OAuth grant was always whole-stack (``claudai projectless``
        scope) and any other count was just the user's total org membership, not a deliberate
        choice. That assumption no longer holds now that the consent screen itself lets the user
        freeze access to a specific subset -- see the "increment 8" extension in the RFC and its
        follow-up note.

        Best-effort: introspection/exchange failures here just leave the session unconfirmed, same
        as before this method existed -- an explicit ``set_project_scope`` call still works.
        """
        try:
            introspection = await introspect_token(self._storage_api_url, subject_token=subject_token)
        except Exception as e:
            LOG.warning(f'Could not introspect new OAuth session for scope auto-confirm: {e}', exc_info=True)
            return
        if not introspection.projects:
            return
        project_ids = [p.id for p in introspection.projects]
        scoped_token: str | None = None
        scoped_expires_at: datetime | None = None
        try:
            minted = await exchange_scoped_token(
                self._storage_api_url, subject_token=subject_token, project_ids=project_ids, read_only=False
            )
            scoped_token = minted.access_token
            scoped_expires_at = datetime.fromtimestamp(minted.expires_at, tz=timezone.utc)
        except Exception as e:
            LOG.warning(f'Scoped-token exchange failed while auto-confirming project scope: {e}', exc_info=True)
        await self._session_store.update_scope(
            session_id,
            project_ids=project_ids,
            read_only=False,
            confirmed=True,
            scoped_token=scoped_token,
            scoped_expires_at=scoped_expires_at,
        )
        LOG.info(f'Session {session_id} auto-confirmed to its accessible project(s) {project_ids}.')

    async def load_access_token(self, token: str) -> AccessToken | None:
        """
        Loads an access token by looking up the opaque, randomly-generated token in the Postgres
        session store (oauth_session_persistence RFC) -- no signature to verify, the DB row's mere
        existence (and not being revoked) is the entire validity check.

        Refreshes the underlying Keboola credential transparently if it's near expiry, so a client
        that never proactively refreshes its own (non-expiring) opaque token still always gets a
        live Keboola session underneath.

        :param token: The opaque access token to look up.
        :return: A `ProxyAccessToken` carrying the (possibly just-refreshed) Keboola access token,
            or `None` if the token doesn't exist, was revoked, or Connection refused to refresh the
            underlying Keboola session (the session is revoked, so the client has to log in again).
        """
        session = await self._session_store.get_by_access_token(token)
        if session is None:
            _log_debug(f'[load_access_token] Unknown or revoked token: {token}')
            return None
        if session.resource is not None and not self.is_own_resource(session.resource):
            # RFC 8707: a token minted for another resource is not replayed here. No resource is accepted as before.
            LOG.warning(
                f'[load_access_token] Token issued for another resource refused: session_id={session.id}, '
                f'resource={_sanitize_for_log(session.resource)!r}'
            )
            return None

        if session.kbc_access_expires_at.timestamp() <= time.time() + 60:
            try:
                token_set = await self._refresh_kbc_tokens(session.id, session.kbc_refresh_token)
            except httpx.HTTPError as e:
                # Don't fail the request over a refresh hiccup -- the (soon-to-expire) credential we
                # already have may still work for the next little while; the *next* lookup retries.
                LOG.warning(
                    f'[load_access_token] Could not refresh near-expiry Keboola session: session_id={session.id}: {e}',
                    exc_info=True,
                )
            else:
                if token_set is None:
                    # Refused for good: the session is revoked (reloading then finds nothing), unless a
                    # concurrent request rotated its credentials first -- then use what is stored now.
                    reloaded = await self._session_store.get_by_access_token(token)
                    if reloaded is None:
                        return None
                    session = reloaded
                else:
                    await self._session_store.rotate_kbc_tokens(
                        session.id,
                        kbc_access_token=token_set.access_token,
                        kbc_refresh_token=token_set.refresh_token,
                        kbc_access_expires_at=datetime.fromtimestamp(token_set.expires_at, tz=timezone.utc),
                    )
                    session = dataclasses.replace(
                        session, kbc_access_token=token_set.access_token, kbc_refresh_token=token_set.refresh_token
                    )
                    LOG.info(
                        f'[load_access_token] Lazily refreshed near-expiry Keboola session: session_id={session.id}'
                    )

        proxy_token = ProxyAccessToken(
            token=token,
            client_id=session.client_id,
            scopes=_scopes_for_session(session.oauth_projectless),
            expires_at=None,  # no client-visible expiry -- see load_access_token docstring
            resource=session.resource,
            kbc_access_token=session.kbc_access_token,
            session_id=session.id,
            scope_project_ids=session.scope_project_ids,
            scope_read_only=session.scope_read_only,
            scope_confirmed=session.scope_confirmed,
            scope_scoped_token=session.scope_scoped_token,
            scope_scoped_expires_at=session.scope_scoped_expires_at,
        )
        _log_debug(f'[load_access_token] token={token}, session_id={session.id}')
        return proxy_token

    async def load_refresh_token(self, client: OAuthClientInformationFull, refresh_token: str) -> RefreshToken | None:
        """
        Loads a refresh token by looking up the opaque token in the Postgres session store.

        :param client: The OAuth client details.
        :param refresh_token: The opaque refresh token to look up.
        :return: A `ProxyRefreshToken`, or `None` if the token doesn't exist or was revoked.
        """
        session = await self._session_store.get_by_refresh_token(refresh_token)
        if session is None:
            _log_debug(f'[load_refresh_token] Unknown or revoked token: {refresh_token}')
            return None

        proxy_token = ProxyRefreshToken(
            token=refresh_token,
            client_id=session.client_id,
            scopes=_scopes_for_session(session.oauth_projectless),
            expires_at=None,
            resource=session.resource,
            kbc_refresh_token=session.kbc_refresh_token,
            session_id=session.id,
        )
        _log_debug(f'[load_refresh_token] token={refresh_token}, session_id={session.id}')
        return proxy_token

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        """
        Refreshes the exchanged Keboola programmatic session directly (PSGO-261
        oauth_session_exchange RFC) — no round-trip to the league OAuth server: that token pair
        was used once, at initial exchange, and is never touched again.

        Also rotates the client-facing opaque access/refresh token pair (OAuth 2.1's refresh-token-
        rotation recommendation) -- the old pair stops resolving to this session immediately after.

        :param client: The OAuth client details.
        :param refresh_token: The refresh token to use for renewing the tokens.
        :param scopes: List of scopes to associate with the new tokens. If not provided, the scopes
          from the original access token will be used. This can be used to reduce the scopes.

        :return: A new OAuthToken containing the access and refresh tokens.

        :raises TokenError: If the session-refresh call indicates an error.
        """
        _log_debug(
            f'[exchange_refresh_token] client_id={_sanitize_client_id_for_log(client.client_id)}, refresh_token={refresh_token}, scopes={scopes}'
        )

        assert isinstance(refresh_token, ProxyRefreshToken), f'Expected ProxyRefreshToken, got {type(refresh_token)}'
        assert refresh_token.session_id is not None
        # A session from before DMD-2469 has no resource and is not bound now: binding it would refuse a client that
        # worked until this release, for a resource this server does not check on such a session anyway.
        if refresh_token.resource is not None:
            self._token_resource(refresh_token.resource, client.client_id)

        # Raised as TokenError (not HTTPException): this method is invoked by the mcp SDK's own
        # /token endpoint handler, which only recognizes TokenError and formats it into a spec-
        # compliant TokenErrorResponse body ({"error": ..., "error_description": ...}) -- an
        # HTTPException here would bubble up uncaught and reach the client as an opaque, non-OAuth
        # shaped error.
        try:
            token_set = await self._refresh_kbc_tokens(refresh_token.session_id, refresh_token.kbc_refresh_token)
        except httpx.HTTPStatusError as e:
            LOG.exception(f'[exchange_refresh_token] Failed to refresh session: status={e.response.status_code}')
            raise TokenError(
                error='invalid_grant', error_description=f'Failed to refresh token: status={e.response.status_code}'
            ) from e
        except httpx.HTTPError as e:
            LOG.exception('[exchange_refresh_token] Could not reach Connection to refresh session')
            raise TokenError(
                error='invalid_grant', error_description=f'Failed to refresh token: could not reach Connection ({e}).'
            ) from e

        if token_set is not None:
            await self._session_store.rotate_kbc_tokens(
                refresh_token.session_id,
                kbc_access_token=token_set.access_token,
                kbc_refresh_token=token_set.refresh_token,
                kbc_access_expires_at=datetime.fromtimestamp(token_set.expires_at, tz=timezone.utc),
            )
        elif await self._session_store.get_by_refresh_token(refresh_token.token) is None:
            raise TokenError(error='invalid_grant', error_description='Failed to refresh token: status=401')
        # else: a concurrent request rotated the Keboola credentials first; they are fresh, so only the
        # opaque pair is renewed below.
        new_access_token, new_refresh_token = await self._session_store.rotate_opaque_tokens(refresh_token.session_id)
        return self._oauth_token(new_access_token, new_refresh_token, scopes or refresh_token.scopes)

    async def _refresh_kbc_tokens(self, session_id: str, kbc_refresh_token: str) -> TokenSet | None:
        """
        Refreshes the Keboola session behind an OAuth session.

        :return: The new tokens, or `None` if Connection refused the refresh token with a 401. That is final
            (a Keboola session has a fixed lifetime, or was revoked): the OAuth session is then revoked so the
            client has to log in again -- unless a concurrent request has already rotated the stored refresh
            token, in which case the session is left alone and the caller re-reads it.
        :raises httpx.HTTPError: any other failure (network, 5xx, 429...); transient, so the session is kept.
        """
        try:
            return await refresh_tokens(self._storage_api_url, refresh_token=kbc_refresh_token)
        except httpx.HTTPStatusError as e:
            if e.response.status_code != httpx.codes.UNAUTHORIZED:
                raise
        if await self._session_store.revoke_if_kbc_refresh_token(session_id, kbc_refresh_token):
            LOG.warning(f'Connection refused the Keboola refresh token (401), session revoked: session_id={session_id}')
        return None

    @staticmethod
    def _oauth_token(access_token: str, refresh_token: str, scopes: list[str]) -> OAuthToken:
        # expires_in=None: these opaque tokens don't carry a client-visible expiry (see
        # load_access_token) -- the server refreshes the underlying Keboola credential
        # transparently, so the client never needs to proactively refresh either.
        return OAuthToken(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type='Bearer',
            expires_in=None,
            scope=' '.join(scopes),
        )

    async def revoke_token(self, token: str, token_type_hint: str | None = None) -> None:
        """
        Revokes a token by deleting its session from the Postgres store (soft-delete via
        `revoked_at`) -- both the access and refresh token immediately stop resolving.

        :param token: The token to be revoked (access or refresh; `token_type_hint` is advisory).
        :param token_type_hint: An optional hint about the type of the token.
        """
        _log_debug(f'[revoke_token] token={token}, token_type_hint={token_type_hint}')
        session = await self._session_store.get_by_access_token(
            token
        ) or await self._session_store.get_by_refresh_token(token)
        if session is not None:
            await self._session_store.revoke(session.id)

    def _read_oauth_tokens(self, data: dict[str, Any], scopes: list[str]) -> tuple[AccessToken, RefreshToken]:
        """
        Reads the access and refresh tokens from the OAuth server response.
        """
        expires_in = int(data['expires_in'])  # seconds
        if expires_in <= 0:
            # Only the keys by default: the response carries the Connection access and refresh tokens.
            LOG.error(
                f'[_read_oauth_tokens] Received already expired token: expires_in={expires_in}, keys={sorted(data)}'
            )
            _log_debug(f'[_read_oauth_tokens] Received already expired token: data={data}')
            raise HTTPException(400, 'The original OAuth access token has already expired.')

        current_time = int(time.time())

        access_token = AccessToken(
            token=data['access_token'],
            client_id=self._oauth_client_id,
            scopes=scopes,
            # this is slightly different from 'expires_at' kept by the OAuth server
            expires_at=current_time + expires_in,
        )
        refresh_token = RefreshToken(
            token=data['refresh_token'],
            client_id=self._oauth_client_id,
            scopes=scopes,
            # The expires_in refers to the access token.
            # There is no way of knowing when the refresh token expires.
            # The Keboola OAuth server issues refresh tokens that expire in 1 month and access tokens that
            # expire in 1 hour.
            # We derive the lifespan of a refresh token from the lifespan of an access token and make it approximately
            # 1 week long under the default circumstances.
            expires_at=current_time + self._ceil_to_hour(min(168 * expires_in, 168 * 3600)),
        )

        return access_token, refresh_token

    async def _exchange_oauth_for_session(self, oauth_access_token: str) -> TokenSet:
        """
        Exchanges a league OAuth access token -- ``claudai projectless`` scope for a whole-stack
        session (Flow A), or ``claudai`` alone for a session pinned to the one project the admin
        picked on Connection's legacy selector (a session persisted before every registered client
        was granted ``projectless``) -- for a Keboola programmatic session via
        ``manage/internal/auth-bridge/exchange-oauth-token``.

        Raised as ``TokenError`` (not ``HTTPException``): this runs inside ``exchange_authorization_code``,
        invoked by the mcp SDK's own ``/token`` endpoint handler, which only recognizes ``TokenError``
        and formats it into a spec-compliant ``TokenErrorResponse`` body. An ``HTTPException`` here
        would bubble up uncaught and reach the client as an opaque, non-OAuth-shaped error.
        """
        kubernetes_token_path = deployed_sa_token_path()
        if not kubernetes_token_path:
            # OAuth login only runs on the deployed server; a missing SA token path means
            # KBC_KUBERNETES_TOKEN_PATH isn't set there, which is a deployment misconfiguration.
            LOG.error('[_exchange_oauth_for_session] KBC_KUBERNETES_TOKEN_PATH is not set; cannot exchange session.')
            raise TokenError(
                error='invalid_request',
                error_description='OAuth login is misconfigured: no Kubernetes ServiceAccount token available.',
            )

        exchanger = OAuthSessionExchanger(
            storage_api_url=self._storage_api_url,
            kubernetes_token_path=kubernetes_token_path,
        )
        try:
            body = await exchanger.exchange(oauth_access_token=oauth_access_token)
        except OAuthTokenExchangeError as e:
            LOG.error(f'[_exchange_oauth_for_session] {e}')
            raise TokenError(error='invalid_grant', error_description=str(e)) from e

        _log_debug(f'[_exchange_oauth_for_session] exchange response: {body}')
        return parse_token_response(body)

    @staticmethod
    def _ceil_to_hour(seconds: int) -> int:
        return math.ceil(seconds / 3600) * 3600

    def _encode(self, data: Mapping[str, Any], *, key: str | None = None) -> str:
        return encode_jwt(data, key or self._jwt_secret)

    def _decode(self, data: str, *, key: str | None = None) -> dict[str, Any]:
        return decode_jwt(data, key or self._jwt_secret)
