import time
from typing import Any, Mapping
from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp.server.auth.provider import AccessToken, AuthorizationParams, RefreshToken
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull
from pydantic import AnyHttpUrl, AnyUrl

from keboola_mcp_server.oauth import (
    ClientValidationResult,
    SimpleOAuthProvider,
    _ExtendedAuthorizationCode,
    _OAuthClientInformationFull,
)

JWT_KEY = 'secret'


class TestSimpleOAuthProvider:

    @pytest.fixture
    def oauth_provider(self) -> SimpleOAuthProvider:
        return SimpleOAuthProvider(
            storage_api_url='https://sapi',
            mcp_server_url='https://mcp',
            callback_endpoint='/callback',
            client_id='mcp-server-id',
            client_secret='mcp-server-secret',
            server_url='https://oauth',
            scope='scope',
            jwt_secret=JWT_KEY,
        )

    @staticmethod
    def authorization_code(*, scopes: list[str] | None = None, expires_at: float | None = None) -> Mapping[str, Any]:
        auth_code = _ExtendedAuthorizationCode(
            code='foo',
            scopes=scopes or [],
            expires_at=expires_at or time.time() + 5 * 60,  # 5 minutes from now
            client_id='foo-client-id',
            code_challenge='foo-code-challenge',
            redirect_uri=AnyUrl('foo://bar'),
            redirect_uri_provided_explicitly=True,
            oauth_access_token=AccessToken(token='oauth-access-token', client_id='mcp-server', scopes=['foo']),
            oauth_refresh_token=RefreshToken(token='oauth-refresh-token', client_id='mcp-server', scopes=['foo']),
        )
        auth_code_raw = auth_code.model_dump()
        auth_code_raw['redirect_uri'] = str(auth_code_raw['redirect_uri'])  # AnyUrl is not JSON serializable
        return auth_code_raw

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('auth_code', 'key', 'expected'),
        [
            # valid, no scopes
            (code := authorization_code(), JWT_KEY, _ExtendedAuthorizationCode.model_validate(code)),
            # valid, scopes
            (
                code := authorization_code(scopes=['foo', 'bar']),
                JWT_KEY,
                _ExtendedAuthorizationCode.model_validate(code),
            ),
            # expired, no scopes
            (code := authorization_code(expires_at=1), JWT_KEY, _ExtendedAuthorizationCode.model_validate(code)),
            # wrong encryption key
            (code := authorization_code(), '!@#$%^&', None),
        ],
    )
    async def test_load_authorization_code(
        self,
        auth_code: Mapping[str, Any],
        key: str,
        expected: _ExtendedAuthorizationCode,
        oauth_provider: SimpleOAuthProvider,
    ):
        client_info = OAuthClientInformationFull(client_id='foo-client-id', redirect_uris=[AnyUrl('foo://bar')])
        auth_code_str = oauth_provider._encode(auth_code, key=key)
        loaded_auth_code = await oauth_provider.load_authorization_code(client_info, auth_code_str)
        assert loaded_auth_code == expected

    @pytest.mark.parametrize(
        ('raw_at', 'raw_rt', 'scopes', 'at_expires_in', 'rt_expires_in'),
        [
            ('foo', 'bar', ['email'], 3600, 168 * 3600),
            ('foo', 'bar', ['user', 'email'], 3600, 168 * 3600),
            ('foo', 'bar', [], 3600, 168 * 3600),
            ('foo', 'bar', [], 1, 3600),  # 168 * 1 second rounded up to the nearest hour -> 3600
            ('foo', 'bar', [], 7200, 168 * 3600),
        ],
    )
    def test_read_oauth_tokens(
        self,
        raw_at: str,
        raw_rt: str,
        scopes: list[str],
        at_expires_in: int,
        rt_expires_in: int,
        oauth_provider: SimpleOAuthProvider,
    ):
        access_token, refresh_token = oauth_provider._read_oauth_tokens(
            data={'access_token': raw_at, 'refresh_token': raw_rt, 'expires_in': at_expires_in}, scopes=scopes
        )

        assert access_token.token == raw_at
        assert access_token.scopes == scopes
        assert 0 <= at_expires_in - (access_token.expires_at - time.time()) < 1

        assert refresh_token.token == raw_rt
        assert refresh_token.scopes == scopes
        assert 0 <= rt_expires_in - (refresh_token.expires_at - time.time()) < 1

    @pytest.mark.parametrize(
        ('uri', 'valid'),
        [
            # Any http/https/custom scheme is accepted — per-client redirect URI validation
            # against Connection-registered URIs happens in SimpleOAuthProvider.authorize().
            (AnyUrl('http://localhost:8080/callback'), True),
            (AnyUrl('http://example.com/callback'), True),  # domain check moved to Connection
            (AnyUrl('https://claude.ai/api/mcp/auth_callback'), True),
            (AnyUrl('https://foo.bar.com/callback'), True),  # domain check moved to Connection
            (AnyUrl('cursor://anysphere.cursor-mcp/oauth/callback'), True),
            (AnyUrl('vscode://callback'), True),  # custom IDE schemes now allowed
            (AnyUrl('ftp://foo.bar.com'), True),  # non-dangerous scheme allowed
            # Dangerous scripting schemes are still rejected.
            (AnyUrl('javascript://alert(1)'), False),
            (AnyUrl('data://text/html,<script>alert(1)</script>'), False),
            (AnyUrl('vbscript://foo'), False),
            # Missing redirect_uri is rejected.
            (None, False),
        ],
    )
    def test_validate_redirect_uri(self, uri: AnyUrl | None, valid: bool):
        info = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo')
        if valid:
            actual = info.validate_redirect_uri(uri)
            assert actual == uri
        else:
            with pytest.raises(InvalidRedirectUriError):
                info.validate_redirect_uri(uri)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status_code', 'response_body', 'expected_status', 'expected_redirect_uris'),
        [
            # Connection returns approved with registered URIs
            (
                200,
                {'status': 'approved', 'redirect_uris': ['https://claude.ai/cb']},
                'approved',
                ['https://claude.ai/cb'],
            ),
            # Connection returns pending (unknown client)
            (200, {'status': 'pending'}, 'pending', []),
            # Connection rejects the client (401)
            (401, {}, 'rejected', []),
            # Connection rejects the client (403)
            (403, {}, 'rejected', []),
            # Unexpected status code — fail open
            (500, {}, 'approved', []),
        ],
    )
    async def test__validate_client(
        self,
        status_code: int,
        response_body: dict,
        expected_status: str,
        expected_redirect_uris: list[str],
        oauth_provider: SimpleOAuthProvider,
    ):
        mock_response = MagicMock()
        mock_response.status_code = status_code
        mock_response.json.return_value = response_body
        mock_response.text = str(response_body)

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_response)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        original_create = oauth_provider._create_http_client
        oauth_provider._create_http_client = lambda: mock_http  # type: ignore[method-assign]
        try:
            result = await oauth_provider._validate_client(
                client_id='test-client',
                redirect_uri='https://example.com/cb',
            )
        finally:
            oauth_provider._create_http_client = original_create  # type: ignore[method-assign]

        assert result.status == expected_status
        assert result.redirect_uris == expected_redirect_uris

    @pytest.mark.asyncio
    async def test__validate_client_connection_error(self, oauth_provider: SimpleOAuthProvider):
        """Connection unreachable → fail open (approved)."""
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(side_effect=Exception('connection refused'))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        oauth_provider._create_http_client = lambda: mock_http  # type: ignore[method-assign]
        result = await oauth_provider._validate_client(client_id='x', redirect_uri='https://x.com/cb')

        assert result.status == 'approved'

    def _make_auth_params(self, redirect_uri: str = 'https://claude.ai/cb') -> AuthorizationParams:
        return AuthorizationParams(
            state='state-xyz',
            scopes=['email'],
            redirect_uri=AnyUrl(redirect_uri),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            code_challenge_method='S256',
            resource=None,
        )

    @pytest.mark.asyncio
    async def test_authorize_approved_client(self, oauth_provider: SimpleOAuthProvider, mocker):
        """Flow A: pre-registered client — redirect_uri validated against Connection's list."""
        mocker.patch.object(
            oauth_provider,
            '_validate_client',
            return_value=ClientValidationResult(status='approved', redirect_uris=['https://claude.ai/cb']),
        )
        client = OAuthClientInformationFull(client_id='claude.ai', redirect_uris=[AnyHttpUrl('http://foo')])
        url = await oauth_provider.authorize(client, self._make_auth_params())

        assert 'pending_client_id' not in url
        assert 'state=' in url

    @pytest.mark.asyncio
    async def test_authorize_approved_client_wrong_redirect_uri(self, oauth_provider: SimpleOAuthProvider, mocker):
        """Flow A: redirect_uri not in Connection's registered list → InvalidRedirectUriError."""
        mocker.patch.object(
            oauth_provider,
            '_validate_client',
            return_value=ClientValidationResult(status='approved', redirect_uris=['https://other.example.com/cb']),
        )
        client = OAuthClientInformationFull(client_id='claude.ai', redirect_uris=[AnyHttpUrl('http://foo')])
        with pytest.raises(InvalidRedirectUriError):
            await oauth_provider.authorize(client, self._make_auth_params('https://claude.ai/cb'))

    @pytest.mark.asyncio
    async def test_authorize_pending_client(self, oauth_provider: SimpleOAuthProvider, mocker):
        """Flow B: unknown client — pending_client_* params appended to Connection auth URL."""
        mocker.patch.object(
            oauth_provider,
            '_validate_client',
            return_value=ClientValidationResult(status='pending'),
        )
        client = OAuthClientInformationFull(
            client_id='my-new-app',
            redirect_uris=[AnyHttpUrl('http://foo')],
            client_name='My New App',
        )
        url = await oauth_provider.authorize(client, self._make_auth_params('https://my-app.example.com/cb'))

        assert 'pending_client_id=my-new-app' in url
        assert 'pending_redirect_uri=' in url
        assert 'pending_client_name=My+New+App' in url

    @pytest.mark.asyncio
    async def test_authorize_rejected_client(self, oauth_provider: SimpleOAuthProvider, mocker):
        """Rejected client → HTTPException 403."""
        from http.client import HTTPException as StdHTTPException

        mocker.patch.object(
            oauth_provider,
            '_validate_client',
            return_value=ClientValidationResult(status='rejected'),
        )
        client = OAuthClientInformationFull(client_id='evil-app', redirect_uris=[AnyHttpUrl('http://foo')])
        with pytest.raises(StdHTTPException) as exc_info:
            await oauth_provider.authorize(client, self._make_auth_params())
        assert exc_info.value.args[0] == 403
