import logging
import secrets
import time
from http.client import HTTPException
from typing import cast
from urllib.parse import urljoin

import jwt
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
    construct_redirect_uri,
)
from mcp.shared._httpx_utils import create_mcp_http_client
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


class ProxyAccessToken(AccessToken):
    delegate: AccessToken
    # This token is created by the MCP server and used for calling AI Service and Jobs Queue,
    # which do not support 'Authorization: Bearer <access-token>' header yet.
    sapi_token: str


class SimpleOAuthProvider(OAuthAuthorizationServerProvider):

    MCP_SERVER_SCOPE: str = 'mcp'

    def __init__(
            self, *,
            storage_api_url: str,
            mcp_callback_url: str,
            client_id: str, client_secret: str, server_url: str, scope: str,
            jwt_secret: str | None = None,
    ) -> None:
        """
        Creates OAuth provider implementation.

        :param storage_api_url: The URL of the Storage API service.
        :param mcp_callback_url: The URL where the OAuth server redirects to after the user authorizes.
        :param client_id: The client ID registered with the OAuth server.
        :param client_secret: The client secret registered with the OAuth server
        :param server_url: The URL of the OAuth server that the MCP server should authenticate to.
        :param scope: The scope of access to request from the OAuth server.
        :param jwt_secret: The secret key for encoding and decoding JWT tokens.
        """
        self._sapi_tokens_url = urljoin(storage_api_url, '/v2/storage/tokens')
        self._mcp_callback_url = mcp_callback_url
        self._oauth_client_id = client_id
        self._oauth_client_secret = client_secret
        self._oauth_server_auth_url = urljoin(server_url, '/oauth/authorize')
        self._oauth_server_token_url = urljoin(server_url, '/oauth/token')
        self._oauth_scope = scope
        self._jwt_secret = jwt_secret or secrets.token_hex(32)

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        client = _OAuthClientInformationFull(
            # Use a fake redirect URI. Normally, we would retrieve the client from a persistent registry
            # and return the registered redirect URI.
            redirect_uris=[AnyHttpUrl('http://foo')],
            client_id=client_id,
        )
        LOG.debug(f'Client loaded: client_id={client_id}')
        return client

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        # This is a no-op. We don't register clients otherwise we would need a persistent registry.
        LOG.debug(f'Client registered: client_id={client_info.client_id}')

    async def authorize(
            self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        """Generates an authorization URL."""
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
        state_jwt = jwt.encode(state, self._jwt_secret)

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
        Handle the callback from the OAuth server.

        :param code: The authorization code for the MCP OAuth client.
        :param state: The state generated in the authorize() function.
        :return: The URL that redirects back to the AI assistant OAuth client.
        """
        # Validate the state first to prevent calling OAuth server with invalid authorization code.
        try:
            state_data = jwt.decode(state, self._jwt_secret, algorithms=['HS256'])
        except jwt.InvalidTokenError:
            LOG.debug(f'[handle_oauth_callback] Invalid state: {state}', exc_info=True)
            raise HTTPException(400, 'Invalid state parameter')

        if not state_data:
            LOG.debug(f'[handle_oauth_callback] Invalid state: {state_data}', exc_info=True)
            raise HTTPException(400, 'Invalid state parameter')

        if state_data['expires_at'] < time.time():
            LOG.debug(f'[handle_oauth_callback] Expired state: {state_data}', exc_info=True)
            raise HTTPException(400, 'Invalid state parameter')

        # Exchange the authorization code for the access token with OAuth server.
        # TODO: Don't use create_mcp_http_client from a private module.
        async with create_mcp_http_client() as http_client:
            response = await http_client.post(
                self._oauth_server_token_url,
                data={
                    'client_id': self._oauth_client_id,
                    'client_secret': self._oauth_client_secret,
                    'code': code,
                    'grant_type': 'authorization_code',
                    # FYI: Some tutorials use the redirect_uri here, but it does not seem to be required.
                    # The Keboola OAuth server requires it, but GitHub OAuth server does not.
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

        expires_in = int(data['expires_in'])  # seconds
        if expires_in <= 0:
            LOG.error(f'[handle_oauth_callback] Received already expired token: data={data}')
            raise HTTPException(400, 'Received already expired token.')

        redirect_uri = cast(str, state_data['redirect_uri'])
        scopes = cast(list[str], state_data['scopes'])
        access_token = AccessToken(
            token=data['access_token'],
            client_id=self._oauth_client_id,
            scopes=scopes,
            # this is slightly different from 'expires_at' kept by the OAuth server
            expires_at=int(time.time() + expires_in),
        )

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
        }
        auth_code_jwt = jwt.encode(auth_code, self._jwt_secret)

        mcp_redirect_uri = construct_redirect_uri(
            redirect_uri_base=redirect_uri, code=auth_code_jwt, state=state_data['state'],
        )
        LOG.debug(f'[handle_oauth_callback] mcp_redirect_uri={mcp_redirect_uri}')

        return mcp_redirect_uri

    async def load_authorization_code(
            self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        """Load an authorization code."""
        try:
            auth_code = jwt.decode(authorization_code, self._jwt_secret, algorithms=['HS256'])
        except jwt.InvalidTokenError:
            LOG.debug(f'[load_authorization_code] Invalid authorization_code: {authorization_code}', exc_info=True)
            return None

        LOG.debug(f'[load_authorization_code] client_id={client.client_id}, authorization_code={authorization_code}, '
                  f'auth_code={auth_code}')
        return _ExtendedAuthorizationCode.model_validate(
            auth_code | {'redirect_uri': AnyUrl(auth_code['redirect_uri'])}
        )

    async def exchange_authorization_code(
            self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        """Exchange authorization code for tokens."""
        LOG.debug(f'[exchange_authorization_code] authorization_code={authorization_code}, '
                  f'client_id={client.client_id}')
        # Check that we get the instance loaded by load_authorization_code() function.
        assert isinstance(authorization_code, _ExtendedAuthorizationCode)

        # Create new Storage API token for accessing AI and Jobs Queue services that do not support bearer tokens yet.
        # TODO: Don't use create_mcp_http_client from a private module.
        async with create_mcp_http_client() as http_client:
            response = await http_client.post(
                self._sapi_tokens_url,
                json={
                    'description': 'Created by the MCP server.',
                    'expiresIn': 60 * 60 * 24,  # 1 day
                },
                headers={
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {authorization_code.oauth_access_token.token}',
                },
            )

            if response.status_code != 200:
                LOG.error('[exchange_authorization_code] Failed create Storage API token, '
                          f'Storage API response: status={response.status_code}, text={response.text}')
                raise HTTPException(500, 'Failed to create Storage API token: '
                                         f'status={response.status_code}, text={response.text}')

            data = response.json()
            LOG.debug(f'[exchange_authorization_code] Storage API response: {data}')

            sapi_token = data['token']

        # Store MCP token
        access_token = ProxyAccessToken(
            token=f'mcp_{secrets.token_hex(32)}',
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=authorization_code.oauth_access_token.expires_at,
            delegate=authorization_code.oauth_access_token,
            sapi_token=sapi_token
        )
        access_token_jwt = jwt.encode(access_token.model_dump(), self._jwt_secret)

        oauth_token = OAuthToken(
            access_token=access_token_jwt,
            token_type='bearer',
            expires_in=max(0, int(access_token.expires_at - time.time())),
            scope=' '.join(authorization_code.scopes),
        )

        LOG.debug(f'[exchange_authorization_code] access_token={access_token}, oauth_token={oauth_token}')

        return oauth_token

    async def load_access_token(self, token: str) -> AccessToken | None:
        """Load and validate an access token."""
        try:
            access_token_raw = jwt.decode(token, self._jwt_secret, algorithms=['HS256'])
        except jwt.InvalidTokenError:
            LOG.debug(f'[load_access_token] Invalid token: {token}', exc_info=True)
            return None

        access_token = ProxyAccessToken.model_validate(access_token_raw)
        LOG.debug(f'[load_access_token] token={token}, access_token={access_token}')

        # Check if expired
        now = time.time()
        if access_token.expires_at and access_token.expires_at < now:
            LOG.info(f'[load_access_token] Expired token: access_token.expires_at={access_token.expires_at}, '
                     f'now={now}')
            return None

        return access_token

    async def load_refresh_token(
            self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        """Load a refresh token - not supported."""
        LOG.debug(f'[load_refresh_token] client_id={client.client_id}, refresh_token={refresh_token}')
        return None

    async def exchange_refresh_token(
            self,
            client: OAuthClientInformationFull,
            refresh_token: RefreshToken,
            scopes: list[str],
    ) -> OAuthToken:
        """Exchange refresh token"""
        LOG.debug(f'[exchange_refresh_token] client_id={client.client_id}, refresh_token={refresh_token}, '
                  f'scopes={scopes}')
        raise NotImplementedError('Not supported')

    async def revoke_token(
            self, token: str, token_type_hint: str | None = None
    ) -> None:
        """Revoke a token."""
        LOG.debug(f'[revoke_token] token={token}, token_type_hint={token_type_hint}')
        # This is no-op as we don't store the tokens.
