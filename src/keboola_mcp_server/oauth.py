import dataclasses
import logging
import math
import os
import re
import secrets
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, cast
from urllib.parse import urljoin

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
from starlette.exceptions import HTTPException

from keboola_mcp_server.auth_login import TokenSet, parse_token_response, refresh_tokens
from keboola_mcp_server.clients.auth_bridge import OAuthSessionExchanger, OAuthTokenExchangeError
from keboola_mcp_server.config import deployed_sa_token_path
from keboola_mcp_server.jwt_utils import decode_jwt, encode_jwt
from keboola_mcp_server.session_store.repository import SessionStore

# The OAuth scope this server always requests at /oauth/consent (see authorize()) -- fixed for
# this flow, so the opaque access/refresh tokens don't need their own scopes column; carried here
# only to satisfy the mcp SDK's AccessToken/RefreshToken (scopes: list[str], required).
_OAUTH_SCOPES = ['claudai', 'projectless']

LOG = logging.getLogger(__name__)
_OAUTH_LOG_ALL = bool(os.getenv('KEBOOLA_MCP_SERVER_OAUTH_LOG_ALL'))
_RE_LOCALHOST = re.compile(r'^(localhost|127\.0\.0\.1|\[::1]|::1)$', re.IGNORECASE)
_ALLOWED_DOMAINS = {
    'https': [
        # Any keboola.com/dev subdomain EXCEPT user-deployable data-app subdomains, which live under a
        # '*.hub.<stack>.keboola.com' host. A free-trial user can deploy a data app whose '/callback' would
        # otherwise capture the OAuth code, so we reject any host that contains a 'hub' DNS label (RISK-76).
        re.compile(r'^(?!(?:.*\.)?hub\.).+\.keboola\.(com|dev)$', re.IGNORECASE),
        re.compile(r'^(.*\.)?chatgpt\.com$', re.IGNORECASE),
        re.compile(r'^(.*\.)?claude\.ai$', re.IGNORECASE),
        re.compile(r'^librechat\.glami-ml\.com$', re.IGNORECASE),  # no subdomains allowed
        re.compile(r'^(.*\.)?make\.com$', re.IGNORECASE),
        re.compile(r'^api\.devin\.ai$', re.IGNORECASE),  # devin.ai API domain
        re.compile(r'^cloud\.onyx\.app$', re.IGNORECASE),  # onyx.app OAuth callback
        re.compile(r'^global\.consent\.azure-apim\.net$', re.IGNORECASE),  # Azure APIM consent domain
        re.compile(r'^n8n\.groupondev\.com$', re.IGNORECASE),
        re.compile(r'^n8n-business\.groupondev\.com$', re.IGNORECASE),
        re.compile(r'^n8n-merchant\.groupondev\.com$', re.IGNORECASE),
        re.compile(r'^n8n-llm-traffic\.groupondev\.com$', re.IGNORECASE),
        re.compile(r'^n8n-finance\.groupondev\.com$', re.IGNORECASE),
        re.compile(r'^n8n-playground\.groupondev\.com$', re.IGNORECASE),
        re.compile(r'^n8n-staging\.groupondev\.com$', re.IGNORECASE),
    ],
    'http': [_RE_LOCALHOST],
    'cursor': [re.compile(r'^(anysphere\.cursor-retrieval|anysphere\.cursor-mcp)$', re.IGNORECASE)],
}


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
        # Ideally, this should verify the redirect_uri against the URI registered by the client.
        # That, however, would require a persistent registry of clients.
        # So, instead we require the clients to send their redirect URI in the authorization request,
        # and we discard all URIs that are not on a whitelist.
        if not redirect_uri:
            LOG.warning('[validate_redirect_uri] No redirect_uri specified.')
            raise InvalidRedirectUriError('The redirect_uri must be specified.')

        stripped_uri = self._strip_redirect_uri(redirect_uri)
        if not redirect_uri.scheme:
            LOG.warning(f'[validate_redirect_uri] No scheme in redirect_uri: {stripped_uri}')
            raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')

        # The custom schemes (e.g. cursor://) require a custom handler registered in a browser.
        # They are used for redirecting a browser to a locally running app.

        if allowed_domains := _ALLOWED_DOMAINS.get(redirect_uri.scheme):
            if not any(p.fullmatch(redirect_uri.host or '') for p in allowed_domains):
                LOG.warning(f'[validate_redirect_uri] Unknown domain in redirect_uri: {stripped_uri}')
                raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')

        else:
            LOG.warning(f'[validate_redirect_uri] Forbidden scheme in redirect_uri: {stripped_uri}')
            raise InvalidRedirectUriError(f'Invalid redirect_uri: {stripped_uri}')

        LOG.info(f'[validate_redirect_uri] Accepted redirect_uri: {stripped_uri}]')
        return redirect_uri

    @staticmethod
    def _strip_redirect_uri(redirect_uri: AnyUrl) -> AnyUrl:
        return AnyUrl.build(scheme=redirect_uri.scheme or '', host=redirect_uri.host or '', port=redirect_uri.port)


class _ExtendedAuthorizationCode(AuthorizationCode):
    oauth_access_token: AccessToken
    oauth_refresh_token: RefreshToken


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
        self._oauth_server_token_url = urljoin(server_url, '/oauth/token')
        self._oauth_scope = scope
        self._jwt_secret = jwt_secret or secrets.token_hex(32)

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
        Registers an OAuth client. This specific implementation is a no-op to avoid having to persist the registered
        clients. It simply logs the client registration details for debugging purposes.

        :param client_info: The full information of the OAuth client to be registered.
        """
        # This is a no-op. We don't register clients, otherwise we would need a persistent registry.
        LOG.debug(f'Client registered: client_id={client_info.client_id}')

    async def authorize(self, client: OAuthClientInformationFull, params: AuthorizationParams) -> str:
        """
        Creates a URL that redirects to the OAuth server for authorization.

        The authorization URL's state parameter is an encrypted JWT that contains all the authorization parameters.
        The state expires after 5 minutes.

        :param client: The OAuth client details.
        :param params: The authorization parameters provided by the client, such as redirect URI, state, scopes, etc.

        :return: The authorization URL that redirects to the OAuth server.
        """
        # Create and encode the authorization state.
        # We don't store the authentication states that we create here to avoid having to persist them.
        # Instead, we encode them to JWT and pass them back to the client.
        # The states expire after 5 minutes.
        scopes = cast(list[str], params.scopes or [])
        state = {
            'redirect_uri': str(params.redirect_uri),
            'redirect_uri_provided_explicitly': str(params.redirect_uri_provided_explicitly),
            # the scopes sent by the MCP server's OAuth client (e.g. claude.ai)
            'scopes': scopes,
            'code_challenge': params.code_challenge,
            'state': params.state,
            'client_id': client.client_id,
            'expires_at': time.time() + 5 * 60,  # 5 minutes from now
        }
        state_jwt = self._encode(state)

        LOG.debug(f'[authorize] client_id={client.client_id}, params={params}, state={state}')

        # create the authorization URL
        url_params = {
            'client_id': self._oauth_client_id,
            'response_type': 'code',
            'redirect_uri': self._mcp_callback_url,
            'state': state_jwt,
            # 'claudai' satisfies the exchange endpoint's MissingClaudaiScopeException guard;
            # 'projectless' makes the exchanged session whole-stack instead of project-pinned.
            'scope': 'claudai projectless',
        }

        auth_url = construct_redirect_uri(self._oauth_server_auth_url, **url_params)
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
        async with self._create_http_client() as http_client:
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
        access_token, refresh_token, _session = await self._session_store.create(
            client_id=client.client_id,
            user_email=None,
            kbc_access_token=token_set.access_token,
            kbc_refresh_token=token_set.refresh_token,
            kbc_access_expires_at=datetime.fromtimestamp(token_set.expires_at, tz=timezone.utc),
        )
        return self._oauth_token(access_token, refresh_token, authorization_code.scopes)

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
            scopes=_OAUTH_SCOPES,
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
            scopes=_OAUTH_SCOPES,
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
            LOG.exception(f'[exchange_refresh_token] Could not reach Connection to refresh session: {e}')
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

    @staticmethod
    def _create_http_client():
        return httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30.0))

    def _encode(self, data: Mapping[str, Any], *, key: str | None = None) -> str:
        return encode_jwt(data, key or self._jwt_secret)

    def _decode(self, data: str, *, key: str | None = None) -> dict[str, Any]:
        return decode_jwt(data, key or self._jwt_secret)
