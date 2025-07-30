import gzip
import json
import logging
import math
import secrets
import time
from http.client import HTTPException
from typing import Any, Mapping, cast
from urllib.parse import urljoin

import httpx
import jwt.api_jws
from fastmcp.server.auth.auth import OAuthProvider
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    RefreshToken,
    construct_redirect_uri,
)
from mcp.server.auth.settings import ClientRegistrationOptions
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull, OAuthToken
from pydantic import AnyHttpUrl, AnyUrl

LOG = logging.getLogger(__name__)


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
        # and we just use that.
        if redirect_uri is not None:
            LOG.debug(f'[validate_redirect_uri] redirect_uri={redirect_uri}]')
            return redirect_uri
        else:
            raise InvalidRedirectUriError('The redirect_uri must be specified.')


class _ExtendedAuthorizationCode(AuthorizationCode):
    oauth_access_token: AccessToken
    oauth_refresh_token: RefreshToken


class ProxyAccessToken(AccessToken):
    delegate: AccessToken
    # This token is created by the MCP server and used for calling AI Service and Jobs Queue,
    # which do not support 'Authorization: Bearer <access-token>' header yet.
    sapi_token: str


class ProxyRefreshToken(RefreshToken):
    delegate: RefreshToken


class SimpleOAuthProvider(OAuthProvider):

    def __init__(
            self, *,
            storage_api_url: str,
            mcp_server_url: str,
            callback_endpoint: str,
            client_id: str, client_secret: str, server_url: str, scope: str,
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
        :param jwt_secret: The secret key for encoding and decoding JWT tokens.
        """
        super().__init__(
            issuer_url=mcp_server_url,
            client_registration_options=ClientRegistrationOptions(enabled=True),
        )

        self._sapi_tokens_url = urljoin(storage_api_url, '/v2/storage/tokens')
        self._mcp_callback_url = urljoin(mcp_server_url, callback_endpoint)
        self._oauth_client_id = client_id
        self._oauth_client_secret = client_secret
        self._oauth_server_auth_url = urljoin(server_url, '/oauth/authorize')
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

    async def authorize(
            self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
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

        # create the authorization URL
        url_params = {
            'client_id': self._oauth_client_id,
            'response_type': 'code',
            'redirect_uri': self._mcp_callback_url,
            'state': state_jwt,
            # send no scopes to Keboola OAuth server and let it use its own default scope
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
            LOG.debug(f'[handle_oauth_callback] Invalid state: {state_data}', exc_info=True)
            raise HTTPException(400, 'Invalid state parameter')

        if state_data['expires_at'] < time.time():
            LOG.debug(f'[handle_oauth_callback] Expired state: {state_data}', exc_info=True)
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
                LOG.error('[handle_oauth_callback] Failed to exchange code for token, '
                          f'OAuth server response: status={response.status_code}, text={response.text}')
                raise HTTPException(400, 'Failed to exchange code for token: '
                                         f'status={response.status_code}, text={response.text}')

            data = response.json()
            LOG.debug(f'[handle_oauth_callback] OAuth server response: {data}')

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
            redirect_uri_base=redirect_uri, code=auth_code_jwt, state=state_data['state'],
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
        LOG.debug(f'[load_authorization_code] client_id={client.client_id}, authorization_code={authorization_code}, '
                  f'auth_code={auth_code}')

        # Log the expired authorization code.
        # The mcp library itself performs the check and returns a proper response, but no logs.
        now = time.time()
        if auth_code.expires_at and auth_code.expires_at < now:
            LOG.info(f'[load_authorization_code] Expired authorization code: '
                     f'auth_code.expires_at={auth_code.expires_at}, now={now}')

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
        LOG.debug(f'[exchange_authorization_code] authorization_code={authorization_code}, '
                  f'client_id={client.client_id}')
        # Check that we get the instance loaded by load_authorization_code() function.
        assert isinstance(authorization_code, _ExtendedAuthorizationCode)

        expires_in = max(0, int(authorization_code.oauth_access_token.expires_at - time.time()))  # seconds
        sapi_token = await self._create_sapi_token(
            oauth_access_token=authorization_code.oauth_access_token.token,
            expires_in=self._ceil_to_hour(expires_in * 2)  # twice as much as the access token's time out
        )

        # wrap the access_token from the OAuth into our own access_token
        access_token = ProxyAccessToken(
            token=f'mcp_{secrets.token_hex(32)}',
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=authorization_code.oauth_access_token.expires_at,
            delegate=authorization_code.oauth_access_token,
            sapi_token=sapi_token
        )
        access_token_jwt = self._encode(access_token.model_dump())

        # wrap the refresh_token from the OAuth into our own refresh_token
        refresh_token = ProxyRefreshToken(
            token=f'mcp_{secrets.token_hex(32)}',
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=authorization_code.oauth_refresh_token.expires_at,
            delegate=authorization_code.oauth_refresh_token,
        )
        refresh_token_jwt = self._encode(refresh_token.model_dump())

        oauth_token = OAuthToken(
            access_token=access_token_jwt,
            refresh_token=refresh_token_jwt,
            token_type='Bearer',
            expires_in=expires_in,
            scope=' '.join(access_token.scopes),
        )

        LOG.debug(f'[exchange_authorization_code] access_token={access_token}, refresh_token={refresh_token},'
                  f'oauth_token={oauth_token}')

        return oauth_token

    async def load_access_token(self, token: str) -> AccessToken | None:
        """
        Loads and validates an access token.
        The method decrypts a JWT access token, validates its content, and returns a `ProxyAccessToken` object
        if the token is valid and not expired. Returns `None` if the token is invalid or expired.

        :param token: The JWT access token to be loaded and validated.
        :return: A `ProxyAccessToken` instance if the token is valid and not expired, otherwise `None`.
        """
        try:
            access_token_raw = self._decode(token)
        except jwt.InvalidTokenError:
            LOG.debug(f'[load_access_token] Invalid token: {token}', exc_info=True)
            return None

        proxy_token = ProxyAccessToken.model_validate(access_token_raw)
        LOG.debug(f'[load_access_token] token={token}, proxy_token={proxy_token}')

        # Log the expired authorization code.
        # The mcp library itself performs the check and returns a proper response, but no logs.
        now = time.time()
        if proxy_token.expires_at and proxy_token.expires_at < now:
            LOG.info(f'[load_access_token] Expired access token: proxy_token.expires_at={proxy_token.expires_at}, '
                     f'now={now}')

        return proxy_token

    async def load_refresh_token(
            self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        """
        Loads and validates a refresh token.
        The method decrypts a JWT refresh token, validates its content, and returns a `RefreshToken` object
        if the token is valid and not expired. Returns `None` if the token is invalid or expired.

        :param client: The OAuth client details.
        :param refresh_token: A string representing the refresh token in JWT format.
        :return: A `ProxyRefreshToken` instance if the token is valid and not expired, otherwise `None`.
        """
        try:
            refresh_token_raw = self._decode(refresh_token)
        except jwt.InvalidTokenError:
            LOG.debug(f'[load_refresh_token] Invalid token: {refresh_token}', exc_info=True)
            return None

        proxy_token = ProxyRefreshToken.model_validate(refresh_token_raw)
        LOG.debug(f'[load_refresh_token] token={refresh_token}, proxy_token={proxy_token}')

        # Log the expired authorization code.
        # The mcp library itself performs the check and returns a proper response, but no logs.
        now = time.time()
        if proxy_token.expires_at and proxy_token.expires_at < now:
            LOG.info(f'[load_refresh_token] Expired refresh token: proxy_token.expires_at={proxy_token.expires_at}, '
                     f'now={now}')

        return proxy_token

    async def exchange_refresh_token(
            self,
            client: OAuthClientInformationFull,
            refresh_token: RefreshToken,
            scopes: list[str],
    ) -> OAuthToken:
        """
        Swaps the refresh token for a new access and refresh tokens from the OAuth server. The function also creates
        a new Storage API token for accessing the AI Service and Jobs Queue APIs.

        :param client: The OAuth client details.
        :param refresh_token: The refresh token to use for renewing the tokens.
        :param scopes: List of scopes to associate with the new tokens. If not provided, the scopes
          from the original access token will be used. This can be used to reduce the scopes.

        :return: A new OAuthToken containing the access and refresh tokens.

        :raises HTTPException: If the OAuth server response indicates an error.
        """
        LOG.debug(f'[exchange_refresh_token] client_id={client.client_id}, refresh_token={refresh_token}, '
                  f'scopes={scopes}')

        assert isinstance(refresh_token, ProxyRefreshToken), f'Expected ProxyRefreshToken, got {type(refresh_token)}'

        # get new access and refresh tokens from the OAuth server
        async with self._create_http_client() as http_client:
            response = await http_client.post(
                self._oauth_server_token_url,
                data={
                    'client_id': self._oauth_client_id,
                    'client_secret': self._oauth_client_secret,
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token.delegate.token,
                },
                headers={'Accept': 'application/json'},
            )

            if response.status_code != 200:
                LOG.exception('[exchange_refresh_token] Failed to refresh token, '
                              f'OAuth server response: status={response.status_code}, text={response.text}')
                raise HTTPException(400, 'Failed to refresh token: '
                                         f'status={response.status_code}, text={response.text}')

            data = response.json()
            LOG.debug(f'[exchange_refresh_token] OAuth server response: {data}')

            if 'error' in data:
                LOG.exception(f'[exchange_refresh_token] Error when refreshing token: data={data}')
                raise HTTPException(400, data.get('error_description', data['error']))

        oauth_access_token, oauth_refresh_token = self._read_oauth_tokens(data, scopes or refresh_token.scopes)
        expires_in = max(0, int(oauth_access_token.expires_at - time.time()))  # seconds
        sapi_token = await self._create_sapi_token(
            oauth_access_token=oauth_access_token.token,
            expires_in=self._ceil_to_hour(expires_in * 2)  # twice as much as the access token's time out
        )

        # wrap the access_token from the OAuth into our own access_token
        access_token = ProxyAccessToken(
            token=f'mcp_{secrets.token_hex(32)}',
            client_id=client.client_id,
            scopes=oauth_access_token.scopes,
            expires_at=oauth_access_token.expires_at,
            delegate=oauth_access_token,
            sapi_token=sapi_token
        )
        access_token_jwt = self._encode(access_token.model_dump())

        # wrap the refresh_token from the OAuth into our own refresh_token
        refresh_token = ProxyRefreshToken(
            token=f'mcp_{secrets.token_hex(32)}',
            client_id=client.client_id,
            scopes=oauth_refresh_token.scopes,
            expires_at=oauth_refresh_token.expires_at,
            delegate=oauth_refresh_token,
        )
        refresh_token_jwt = self._encode(refresh_token.model_dump())

        oauth_token = OAuthToken(
            access_token=access_token_jwt,
            refresh_token=refresh_token_jwt,
            token_type='Bearer',
            expires_in=max(0, int(access_token.expires_at - time.time())),
            scope=' '.join(access_token.scopes),
        )

        LOG.debug(f'[exchange_refresh_token] access_token={access_token}, refresh_token={refresh_token}, '
                  f'oauth_token={oauth_token}')

        return oauth_token

    async def revoke_token(
            self, token: str, token_type_hint: str | None = None
    ) -> None:
        """
        Revokes a token.

        This is a no-op function as the tokens are not stored and so there is no way to revoke tokens that have already
        been issued.

        :param token: The token to be revoked.
        :param token_type_hint: An optional hint about the type of the token.
        """
        LOG.debug(f'[revoke_token] token={token}, token_type_hint={token_type_hint}')
        # This is no-op as we don't store the tokens.

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

    async def _create_sapi_token(self, oauth_access_token: str, expires_in: int) -> str:
        """
        Creates a new Storage API token for accessing AI and Jobs Queue services that do not support bearer tokens yet.
        """
        async with self._create_http_client() as http_client:
            response = await http_client.post(
                self._sapi_tokens_url,
                json={
                    'description': 'Created by the MCP server.',
                    'expiresIn': expires_in,
                    'canReadAllFileUploads': True,
                    'canManageBuckets': True,
                },
                headers={
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {oauth_access_token}',
                },
            )

            if response.status_code != 200:
                LOG.error('[_create_sapi_token] Failed to create Storage API token, '
                          f'Storage API response: status={response.status_code}, text={response.text}')
                raise HTTPException(
                    response.status_code,
                    f'Failed to create Storage API token: status={response.status_code}, text={response.text}')

            data = response.json()
            LOG.debug(f'[_create_sapi_token] Storage API response: {data}')

            return data['token']

    @staticmethod
    def _ceil_to_hour(seconds: int) -> int:
        return math.ceil(seconds / 3600) * 3600

    @staticmethod
    def _create_http_client():
        return httpx.AsyncClient(follow_redirects=True, timeout=httpx.Timeout(30.0))

    def _encode(self, data: Mapping[str, Any], *, key: str | None = None) -> str:
        json_str = json.dumps(data)
        json_bytes = json_str.encode('utf-8')
        json_gzip = gzip.compress(json_bytes)
        json_encrypted = jwt.api_jws.encode(json_gzip, key or self._jwt_secret)
        return json_encrypted

    def _decode(self, data: str, *, key: str | None = None) -> dict[str, Any]:
        json_gzip = jwt.api_jws.decode(data, key or self._jwt_secret, algorithms=['HS256'])
        json_bytes = gzip.decompress(json_gzip)
        json_str = json_bytes.decode('utf-8')
        json_obj = json.loads(json_str)
        return json_obj
