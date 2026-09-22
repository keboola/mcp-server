import base64
import dataclasses
import hashlib
import json
import logging
import math
import os
import secrets
import time
from collections import OrderedDict, deque
from collections.abc import Mapping
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, cast
from urllib.parse import urljoin, urlparse

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
from keboola_mcp_server.session_store.repository import SessionStore


# The two possible OAuth scopes this server requests at /oauth/consent for a session (see
# _scope_for/authorize()) -- 'claudai' always, 'projectless' only for a pre-registered client.
# Which one applies is per-session (OAuthSession.oauth_projectless), not fixed for the whole
# flow: a Flow B (dynamically-approved) session's ProxyAccessToken/ProxyRefreshToken must not
# claim 'projectless' when Connection only ever granted 'claudai' for it (Copilot review finding).
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
# table (see connection/src/Core/Migrations/Application/Migrations/PreRegisterClaudeAiOAuthClientMigration*.php).
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
# _check_registration_uncached before any network call. Connection's own per-IP ceiling for this
# route is 600 calls/60s (connection/config/packages/oauth_client_rate_limit.yaml) -- deliberately
# high because its whole legitimate caller base is "the MCP server", seen as a handful of shared
# egress IPs across every user of the stack. The registration cache above already absorbs the
# common case (repeat callers hit the cache, not Connection), so the only way to burn through that
# budget is an attacker sending a distinct, never-cached (client_id, redirect_uri) pair on every
# request (Copilot review finding) -- this bounds that to a fraction of Connection's ceiling per
# MCP server process, so one flooding caller can no longer exhaust the budget shared by every
# other stack user. Deliberately conservative (half of Connection's limit): several replicas can
# share one egress IP, and this only bounds one process, not the fleet -- a real fix needs a
# limiter shared across replicas (e.g. Redis-backed), tracked as a follow-up, not this PR.
_VALIDATE_RATE_LIMIT_MAX_CALLS = 300
_VALIDATE_RATE_LIMIT_WINDOW_SECONDS = 60.0


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


class _ClientRegistration(Enum):
    """Outcome of asking Connection whether a (client_id, redirect_uri) pair is registered."""

    REGISTERED = auto()
    NOT_REGISTERED = auto()
    # Connection could not be reached, or answered with something other than 200/404 -- never
    # treated as either of the above (fail closed, see AI-3792).
    ERROR = auto()


def _connection_client_id(redirect_uri: str) -> str:
    """
    Maps an AI assistant's own redirect_uri to the client_id used when talking to Connection's
    OAuth client registry.

    Connection's oauth2_client.identifier column (and the pending_mcp_client payload's client_id
    field) are capped at 32 characters, but the mcp SDK mints a 36-character uuid4() as client_id
    for every client that dynamically registers via /register -- forwarding that verbatim would
    never fit. Deriving a short, stable id from redirect_uri instead means the same tool
    reconnecting (same callback URL) lands on the same Connection identity and reuses an earlier
    approval, even though the SDK hands it a fresh uuid on every /register call.
    """
    if known := _WELL_KNOWN_CONNECTION_CLIENT_IDS.get(redirect_uri):
        return known
    digest = hashlib.sha256(redirect_uri.encode()).hexdigest()[:24]
    return f'mcp-{digest}'


def _sanitize_client_name(name: str) -> str:
    """Strips control/bidi/zero-width characters Connection's pending_mcp_client decoder would
    otherwise reject outright (which would silently drop the whole approval payload -- see
    PendingMcpClientApprovalListener's catch-and-ignore on a malformed payload), and truncates to
    Connection's 128-character cap. `str.isprintable()` already excludes control (Cc), format
    (Cf -- covers zero-width and bidi-override characters), surrogate, private-use and separator
    categories, which is exactly what Connection's own DECEPTIVE_CHARS_PATTERN targets."""
    return ''.join(ch for ch in name if ch.isprintable())[:128]


def _create_http_client(*, follow_redirects: bool = True, timeout: httpx.Timeout | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(follow_redirects=follow_redirects, timeout=timeout or httpx.Timeout(30.0))


def _scope_for(connection_client_id: str) -> str:
    """
    The OAuth scope `SimpleOAuthProvider.authorize()` requests from Connection for a given
    (already-REGISTERED) client.

    'claudai' satisfies the exchange endpoint's MissingClaudaiScopeException guard; 'projectless'
    makes the exchanged session whole-stack instead of project-pinned.

    'projectless' is requested ONLY for a client Keboola itself vetted and pre-registered (today:
    claude-ai). Connection's own ClientApprovalProcessor deliberately withholds 'projectless' from
    a client approved through the dynamic (Flow B) screen -- that scope mints an unrestricted,
    every-project grant, and a self-service approval (any authenticated user, no elevated role
    required -- see AI-2883 RFC Decisions §7/§8) is not the same level of vetting as a reviewed
    Keboola migration. This server's own broker identity (`SimpleOAuthProvider._oauth_client_id`)
    is what actually requests the scope, though, so without this function it would silently
    request 'projectless' regardless of which underlying client triggered the flow -- laundering
    the unrestricted grant right back in for a client Connection specifically tried to keep it
    from. Falling through to plain project-selection consent for a dynamically-approved client
    matches what Connection's own scopes intended.

    Known residual gap (Copilot review finding, accepted -- no cheap fix without a Connection-side
    change): this checks the *redirect_uri*, not the row's actual provenance on Connection.
    `/oauth/clients/validate` returns a bare 200/404, so this server has no way to distinguish
    "claude-ai registered by Keboola's migration" from "claude-ai registered via a Flow B approval
    that happened to name the real claude.ai callback". In practice this requires the *exact*
    claude.ai redirect_uri to already be in Flow B, which itself requires the pre-registration
    migration to be absent (it runs on RUN_ON_MIGRATE | RUN_ON_INIT, so every stack gets it) --
    narrow, self-inflicted, and still bounded by the same redirect_uri (the code can only ever
    reach claude.ai's own endpoint either way), not attacker-triggerable. Closing it for real needs
    Connection to expose registration provenance/scopes on the validate response; tracked as a
    follow-up, not fixed here.
    """
    is_pre_registered = connection_client_id in _WELL_KNOWN_CONNECTION_CLIENT_IDS.values()
    return 'claudai projectless' if is_pre_registered else 'claudai'


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

    def __init__(self, server_url: str) -> None:
        self._validate_url = urljoin(server_url, '/oauth/clients/validate')
        self._authorize_url = urljoin(server_url, '/oauth/authorize')

        # client_id -> client_name submitted at /register, so a dynamically-registered client's
        # approval screen can show a real name instead of just the derived Connection client_id
        # (/authorize never receives client_name itself -- see the RFC's Problem section). In-
        # process only, bounded -- see RFC Decisions §4 for why this is a deliberate, display-only
        # tradeoff and not a persistent store. The stored value is sanitized+capped at insertion,
        # not just when later read for display -- /register is unauthenticated, so an arbitrarily
        # long raw name per entry would let a caller inflate memory well past what the entry-count
        # cap alone bounds (Copilot review finding).
        self._client_names: OrderedDict[str, str] = OrderedDict()

        # (connection_client_id, redirect_uri) -> (result, expires_at), REGISTERED only. /authorize
        # is unauthenticated, so every hit costs Connection one call to /oauth/clients/validate --
        # which is itself IP-rate-limited, and this server's whole egress IP shares that budget
        # across every user of the stack. Caching REGISTERED results means the common case (the
        # same handful of already-registered clients reconnecting with the same redirect_uri) never
        # leaves this process.
        #
        # NOT_REGISTERED and ERROR are deliberately never cached:
        # - A cached NOT_REGISTERED would still show stale on the very next retry right after an
        #   admin clicks Allow, contradicting the "approve once, then retry" UX this flow depends
        #   on (Copilot review finding) -- and it buys little anyway: an attacker varying
        #   redirect_uri on every call misses this cache regardless (a fresh key each time), so it
        #   was never a real defense against that flood; local throttling below is what handles it.
        # - Caching ERROR would prolong an outage instead of retrying it -- fail-closed still
        #   applies on every uncached call (see AI-3792).
        self._registration_cache: OrderedDict[tuple[str, str], float] = OrderedDict()

        self._validate_rate_limiter = _SlidingWindowRateLimiter(
            _VALIDATE_RATE_LIMIT_MAX_CALLS, _VALIDATE_RATE_LIMIT_WINDOW_SECONDS
        )

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
        cache_key = (connection_client_id, redirect_uri)
        expires_at = self._registration_cache.get(cache_key)
        if expires_at is not None:
            if time.monotonic() < expires_at:
                # Touch on read, not just on write -- otherwise a frequently-reused entry (e.g.
                # Claude.ai's own pair) never gets bumped and can still be the oldest-inserted
                # entry once enough unique, unrelated keys flood in, making it the first evicted
                # despite being the most valuable entry to keep (Copilot review finding).
                self._registration_cache.move_to_end(cache_key)
                return _ClientRegistration.REGISTERED
            del self._registration_cache[cache_key]

        result = await self._check_registration_uncached(connection_client_id, redirect_uri)
        if result is _ClientRegistration.REGISTERED:
            self._registration_cache[cache_key] = time.monotonic() + _REGISTERED_CACHE_TTL_SECONDS
            self._registration_cache.move_to_end(cache_key)
            if len(self._registration_cache) > _MAX_CACHED_REGISTRATIONS:
                self._registration_cache.popitem(last=False)
        return result

    async def _check_registration_uncached(self, connection_client_id: str, redirect_uri: str) -> _ClientRegistration:
        if not self._validate_rate_limiter.try_acquire():
            LOG.warning(
                f'[check_registration] Local rate limit exceeded, not calling Connection: '
                f'connection_client_id={connection_client_id}, redirect_uri={redirect_uri}'
            )
            return _ClientRegistration.ERROR

        try:
            # Explicit, tighter settings for this call, passed to the factory itself rather than
            # overridden per-request on top of its defaults (which would just make the factory's
            # own follow_redirects=True/30s-timeout defaults dead code for this call site). Never
            # follow a redirect here: a misconfigured proxy/gateway between here and Connection
            # that redirects to something returning 200 (a login page, a catch-all landing page,
            # ...) must surface as an unexpected status (-> ERROR below), never get silently
            # interpreted as "client is registered".
            async with _create_http_client(
                follow_redirects=False, timeout=httpx.Timeout(connect=3.0, read=5.0, write=3.0, pool=3.0)
            ) as http_client:
                response = await http_client.post(
                    self._validate_url,
                    json={'client_id': connection_client_id, 'redirect_uri': redirect_uri},
                )
        except (httpx.HTTPError, httpx.InvalidURL) as e:
            LOG.warning(f'[check_registration] Could not reach Connection: {e}', exc_info=True)
            return _ClientRegistration.ERROR

        if response.status_code == 200:
            # Connection's real 200 is always an empty JSON object (`EmptyJsonResponse`,
            # connection/src/Core/Symfony/Response/EmptyJsonResponse.php) -- checking the body
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
                f'connection_client_id={connection_client_id}, text={response.text[:200]!r}'
            )
            return _ClientRegistration.ERROR
        elif response.status_code == 404:
            return _ClientRegistration.NOT_REGISTERED
        else:
            LOG.warning(
                f'[check_registration] Unexpected response from Connection: '
                f'status={response.status_code}, text={response.text}'
            )
            return _ClientRegistration.ERROR

    def pending_approval_url(self, *, connection_client_id: str, redirect_uri: str, client_name: str | None) -> str:
        """
        Builds the URL that sends the browser to Connection's own `/oauth/authorize` carrying a
        `pending_mcp_client` payload, so an authenticated Keboola user can approve this client
        (Connection's `PendingMcpClientApprovalListener` only gates that route -- not
        `/oauth/consent`, which `SimpleOAuthProvider` otherwise talks to directly; see the AI-2883
        RFC Decisions §3-4).

        The resulting authorization code (if the user allows it) is a REAL, live Connection
        authorization code, delivered to `redirect_uri` -- which, until the moment of approval, is
        still just whatever the *caller* of this server's own `/authorize` claimed (RFC Decisions
        §2-3: the Allow click is a real grant on the approving admin's account, not an inert
        registration side effect). It is unredeemable ONLY because `code_challenge` below is a
        high-entropy random value with no known preimage, and Connection's league config requires
        a code challenge for public clients (`require_code_challenge_for_public_clients: true`,
        `connection/config/packages/league_oauth2_server.yaml`) -- so redeeming it needs a SHA-256
        preimage nobody has. This is load-bearing, not a curiosity: it is what stands between "the
        approval only registers a client" and "the approval hands the caller a live grant".
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
            # MANDATORY, not defensive -- see this method's docstring. secrets.token_urlsafe(32) is
            # a random value presented AS IF it were a SHA-256 digest; no code_verifier can exist
            # for it, so Connection's PKCE check (AuthCodeGrant::validateCodeChallenge, league/
            # oauth2-server) can never be satisfied by anyone, including the caller-controlled
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
        # This is NOT the old per-domain trust list (_ALLOWED_DOMAINS) -- nothing below grants
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

        LOG.info(f'[validate_redirect_uri] Accepted redirect_uri (pending Connection check): {stripped_uri}]')
        return redirect_uri

    @staticmethod
    def _strip_redirect_uri(redirect_uri: AnyUrl) -> AnyUrl:
        return AnyUrl.build(scheme=redirect_uri.scheme or '', host=redirect_uri.host or '', port=redirect_uri.port)


class _ExtendedAuthorizationCode(AuthorizationCode):
    oauth_access_token: AccessToken
    oauth_refresh_token: RefreshToken
    # Whether the Connection OAuth scope requested in authorize() (see _scope_for) included
    # 'projectless' for THIS session, carried from the state JWT so exchange_authorization_code
    # can persist it on the session row instead of load_access_token/load_refresh_token later
    # advertising 'projectless' unconditionally for every session (Copilot review finding).
    # Defaults to True (the pre-AI-2883 behaviour) so an in-flight code encoded just before a
    # deploy, whose state JWT predates this field, still decodes.
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


class UntrustedAuthorizeRedirectMiddleware:
    """Blocks `/authorize` from ever redirecting to a host this server didn't intend.

    The mcp SDK's `AuthorizationHandler.handle` (pinned mcp==1.28.1,
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
    known hosts: Connection's own `server_url` (`_oauth_server_auth_url`, `_oauth_server_authorize_url`,
    and `ConnectionClientRegistry`'s own `/oauth/authorize`) or this server's own `mcp_server_url`
    (`_mcp_callback_url`) -- it never redirects straight to the caller-supplied `redirect_uri` (that
    only happens later, from `/oauth/callback`, after the real grant). So allowlisting the
    `/authorize` route's outgoing redirect host to exactly those two closes the gap without
    touching the intentional "any https host" shape check.

    Must be the outermost middleware (listed first in `get_middleware()`) so it inspects the final
    response after every inner layer -- including the SDK's own route handler -- has run.
    """

    def __init__(self, app: ASGIApp, trusted_hosts: frozenset[str]) -> None:
        self._app = app
        self._trusted_hosts = trusted_hosts

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http' or scope.get('path') != '/authorize':
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
                host = urlparse(location).hostname if location else None
                if host is None or host.lower() not in self._trusted_hosts:
                    blocked = True
                    LOG.warning(f'[authorize] Blocked redirect to untrusted host: {location}')
                    response = JSONResponse(
                        {'error': 'invalid_request', 'error_description': 'Invalid authorization request.'},
                        status_code=400,
                    )
                    await response(scope, receive, send)
                    return
            await send(message)

        await self._app(scope, receive, guarded_send)


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
        # Only ever the target for a 'projectless'-scope request -- see _authorize()'s routing and
        # connection/docs/rfc/mcp-projectless-oauth/mcp-projectless-oauth.md ("No projectless scope
        # -> the legacy selector flow, byte-for-byte. Scope present -> consent flow, admin-subject
        # grant."). A non-projectless session (a dynamically-approved client, denied 'projectless'
        # by _scope_for) must go to Connection's real /oauth/authorize instead: /oauth/consent's
        # own Approve action never sets the session key that route's plain project-selector branch
        # needs, so a non-projectless request sent there loops forever between the two (DMD-2180).
        self._oauth_server_authorize_url = urljoin(server_url, '/oauth/authorize')
        self._oauth_server_token_url = urljoin(server_url, '/oauth/token')
        self._oauth_scope = scope
        self._jwt_secret = jwt_secret or secrets.token_hex(32)
        self._client_registry = ConnectionClientRegistry(server_url)
        # The only two hosts `/authorize` may ever legitimately redirect to -- see
        # UntrustedAuthorizeRedirectMiddleware's docstring.
        self._trusted_redirect_hosts = frozenset(
            h.lower() for h in (urlparse(mcp_server_url).hostname, urlparse(server_url).hostname) if h
        )

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
            Middleware(UntrustedAuthorizeRedirectMiddleware, trusted_hosts=self._trusted_redirect_hosts),
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
        LOG.debug(f'Client loaded: client_id={client_id}')
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
        LOG.debug(f'Client registered: client_id={client_info.client_id}, client_name={sanitized_name}')

    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        """
        Creates a URL that redirects to the OAuth server for authorization.

        First checks the requesting client + redirect_uri against Connection's OAuth client
        registry (`POST /oauth/clients/validate`, via `self._client_registry`) -- see the AI-2883
        RFC (feature_spec/oauth_dynamic_client_registration/RFC.md) for the full design and why
        this replaced a hardcoded domain whitelist. An already-registered, well-known pair
        (pre-registered, e.g. Claude.ai) proceeds exactly as before, unchanged, to Connection's
        `/oauth/consent`. A dynamically-approved pair is registered but never gets 'projectless'
        scope (see `_scope_for`), so it is routed instead to Connection's real `/oauth/authorize`
        -- the only route that resolves a non-'projectless' request (DMD-2180; see
        `_oauth_server_authorize_url`'s docstring). An unregistered pair is sent to Connection's
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
            LOG.exception(f'[authorize] Unexpected error building authorization URL: client_id={client.client_id}')
            return construct_redirect_uri(
                self._mcp_callback_url,
                error='temporarily_unavailable',
                error_description='Could not complete the authorization request.',
            )

    async def _authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        redirect_uri_str = str(params.redirect_uri)
        connection_client_id = _connection_client_id(redirect_uri_str)

        registration = await self._client_registry.check_registration(connection_client_id, redirect_uri_str)
        if registration is _ClientRegistration.ERROR:
            LOG.warning(
                f'[authorize] Could not verify client with Connection: client_id={client.client_id}, '
                f'connection_client_id={connection_client_id}, redirect_uri={redirect_uri_str}'
            )
            # Deliberately NOT `raise AuthorizeError(...)` here: the mcp SDK's own handler catches
            # that and redirects to the *caller-supplied* redirect_uri with the error params
            # (AuthorizationHandler.error_response, which reuses the redirect_uri validate_redirect_uri
            # already accepted). Since that hook now accepts any https host (Connection is the real
            # authority, not a domain list -- see validate_redirect_uri's docstring), raising here
            # would make this server 302 to an attacker-chosen host on demand (e.g. by exhausting
            # Connection's rate limit) -- an open redirect. Redirecting to our own callback endpoint
            # instead keeps the browser on this server's own origin; the caller gets no callback at
            # all for this attempt (same shape as Connection's own Deny-gets-no-callback behavior)
            # and must time out and retry, same as any other undeliverable authorize attempt.
            return construct_redirect_uri(
                self._mcp_callback_url,
                error='temporarily_unavailable',
                error_description='Could not verify OAuth client with Connection.',
            )

        if registration is _ClientRegistration.NOT_REGISTERED:
            LOG.info(
                f'[authorize] Unregistered client sent to Connection for approval: client_id={client.client_id}, '
                f'connection_client_id={connection_client_id}, redirect_uri={redirect_uri_str}'
            )
            return self._client_registry.pending_approval_url(
                connection_client_id=connection_client_id,
                redirect_uri=redirect_uri_str,
                client_name=self._client_registry.get_client_name(client.client_id),
            )

        # registration is REGISTERED from here on -- proceed exactly as before this RFC.
        #
        # Create and encode the authorization state.
        # We don't store the authentication states that we create here to avoid having to persist them.
        # Instead, we encode them to JWT and pass them back to the client.
        # The states expire after 5 minutes.
        scopes = cast(list[str], params.scopes or [])
        scope = _scope_for(connection_client_id)
        is_projectless = 'projectless' in scope.split()
        state = {
            'redirect_uri': redirect_uri_str,
            'redirect_uri_provided_explicitly': str(params.redirect_uri_provided_explicitly),
            # the scopes sent by the MCP server's OAuth client (e.g. claude.ai)
            'scopes': scopes,
            'code_challenge': params.code_challenge,
            'state': params.state,
            'client_id': client.client_id,
            'expires_at': time.time() + 5 * 60,  # 5 minutes from now
            # Carried through to exchange_authorization_code so the session it persists records
            # what Connection actually granted for THIS pair, instead of load_access_token /
            # load_refresh_token later advertising 'projectless' for every session regardless
            # (Copilot review finding).
            'projectless': is_projectless,
        }
        state_jwt = self._encode(state)

        LOG.debug(f'[authorize] client_id={client.client_id}, params={params}, state={state}')

        # create the authorization URL
        url_params = {
            'client_id': self._oauth_client_id,
            'response_type': 'code',
            'redirect_uri': self._mcp_callback_url,
            'state': state_jwt,
            'scope': scope,
        }

        # /oauth/consent only handles a 'projectless' request (Connection's own documented
        # contract, see _oauth_server_authorize_url's docstring) -- a non-projectless session
        # (a dynamically-approved client) must go to Connection's real /oauth/authorize instead,
        # which resolves it through the plain project-selector path with no separate consent step.
        auth_url_base = self._oauth_server_auth_url if is_projectless else self._oauth_server_authorize_url
        auth_url = construct_redirect_uri(auth_url_base, **url_params)
        LOG.debug(f'[authorize] client_id={client.client_id}, params={params}, {auth_url}')

        return auth_url

    async def handle_oauth_callback(self, code: str, state: str) -> str:
        """
        Handles the callback from the OAuth server.

        :param code: The authorization code provided by the OAuth server.
        :param state: The state originally generated in the authorize() function.

        :return: The URL that redirects back to the AI assistant OAuth client.
        """
        # Validate the state first to prevent calling OAuth server with invalid authorization code.
        try:
            state_data = self._decode(state)
        except jwt.InvalidTokenError:
            LOG.debug(f'[handle_oauth_callback] Invalid state: {state}', exc_info=True)
            raise HTTPException(400, 'Invalid state parameter')

        if not state_data:
            LOG.debug(f'[handle_oauth_callback] Invalid state: {state_data}')
            raise HTTPException(400, 'Invalid state parameter')

        if state_data['expires_at'] < time.time():
            LOG.debug(f'[handle_oauth_callback] Expired state: {state_data}')
            raise HTTPException(400, 'Invalid state parameter')

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
        auth_code = {
            'code': f'mcp_{secrets.token_hex(16)}',
            'client_id': state_data['client_id'],
            'redirect_uri': redirect_uri,
            'redirect_uri_provided_explicitly': (state_data['redirect_uri_provided_explicitly'] == 'True'),
            'expires_at': int(time.time() + 5 * 60),  # 5 minutes from now
            'scopes': scopes,
            'code_challenge': state_data['code_challenge'],
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
        )
        LOG.debug(f'[handle_oauth_callback] mcp_redirect_uri={mcp_redirect_uri}')

        return mcp_redirect_uri

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
            LOG.debug(f'[load_authorization_code] Invalid authorization_code: {authorization_code}', exc_info=True)
            return None

        auth_code = _ExtendedAuthorizationCode.model_validate(
            auth_code_raw | {'redirect_uri': AnyUrl(auth_code_raw['redirect_uri'])}
        )
        _log_debug(
            f'[load_authorization_code] client_id={client.client_id}, authorization_code={authorization_code}, '
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
            f'[exchange_authorization_code] authorization_code={authorization_code}, client_id={client.client_id}'
        )
        # Check that we get the instance loaded by load_authorization_code() function.
        assert isinstance(authorization_code, _ExtendedAuthorizationCode)

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
            or `None` if the token doesn't exist or was revoked.
        """
        session = await self._session_store.get_by_access_token(token)
        if session is None:
            _log_debug(f'[load_access_token] Unknown or revoked token: {token}')
            return None

        if session.kbc_access_expires_at.timestamp() <= time.time() + 60:
            try:
                token_set = await refresh_tokens(self._storage_api_url, refresh_token=session.kbc_refresh_token)
            except httpx.HTTPError as e:
                # Don't fail the request over a refresh hiccup -- the (soon-to-expire) credential we
                # already have may still work for the next little while; the *next* lookup retries.
                LOG.warning(f'[load_access_token] Could not refresh near-expiry Keboola session: {e}', exc_info=True)
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
                LOG.info(f'[load_access_token] Lazily refreshed near-expiry Keboola session: session_id={session.id}')

        proxy_token = ProxyAccessToken(
            token=token,
            client_id=session.client_id,
            scopes=_scopes_for_session(session.oauth_projectless),
            expires_at=None,  # no client-visible expiry -- see load_access_token docstring
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
            f'[exchange_refresh_token] client_id={client.client_id}, refresh_token={refresh_token}, scopes={scopes}'
        )

        assert isinstance(refresh_token, ProxyRefreshToken), f'Expected ProxyRefreshToken, got {type(refresh_token)}'
        assert refresh_token.session_id is not None

        # Raised as TokenError (not HTTPException): this method is invoked by the mcp SDK's own
        # /token endpoint handler, which only recognizes TokenError and formats it into a spec-
        # compliant TokenErrorResponse body ({"error": ..., "error_description": ...}) -- an
        # HTTPException here would bubble up uncaught and reach the client as an opaque, non-OAuth
        # shaped error.
        try:
            token_set = await refresh_tokens(self._storage_api_url, refresh_token=refresh_token.kbc_refresh_token)
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

        await self._session_store.rotate_kbc_tokens(
            refresh_token.session_id,
            kbc_access_token=token_set.access_token,
            kbc_refresh_token=token_set.refresh_token,
            kbc_access_expires_at=datetime.fromtimestamp(token_set.expires_at, tz=timezone.utc),
        )
        new_access_token, new_refresh_token = await self._session_store.rotate_opaque_tokens(refresh_token.session_id)
        return self._oauth_token(new_access_token, new_refresh_token, scopes or refresh_token.scopes)

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
            LOG.exception(f'[_read_oauth_tokens] Received already expired token: data={data}')
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
        Exchanges a league OAuth access token (``claudai projectless`` scope) for a whole-stack
        Keboola programmatic session via ``manage/internal/auth-bridge/exchange-oauth-token``.

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
