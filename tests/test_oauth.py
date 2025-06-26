import time
from typing import Any, Mapping

import jwt
import pytest
from mcp.server.auth.provider import AccessToken, RefreshToken
from mcp.shared.auth import OAuthClientInformationFull
from pydantic import AnyUrl

from keboola_mcp_server.oauth import SimpleOAuthProvider, _ExtendedAuthorizationCode

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
    def authorization_code(
            *, scopes: list[str] | None = None, expires_at: float | None = None
    ) -> Mapping[str, Any]:
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
    @pytest.mark.parametrize(('auth_code', 'key', 'expected'), [
        # valid, no scopes
        (code := authorization_code(), JWT_KEY, _ExtendedAuthorizationCode.model_validate(code)),
        # valid, scopes
        (code := authorization_code(scopes=['foo', 'bar']), JWT_KEY, _ExtendedAuthorizationCode.model_validate(code)),
        # expired, no scopes
        (code := authorization_code(expires_at=1), JWT_KEY, _ExtendedAuthorizationCode.model_validate(code)),
        # wrong encryption key
        (code := authorization_code(), '!@#$%^&', None),
    ])
    async def test_load_authorization_code(
            self, auth_code: Mapping[str, Any], key: str, expected: _ExtendedAuthorizationCode,
            oauth_provider: SimpleOAuthProvider
    ):
        client_info = OAuthClientInformationFull(client_id='foo-client-id', redirect_uris=[AnyUrl('foo://bar')])
        auth_code_str = jwt.encode(auth_code, key=key)
        loaded_auth_code = await oauth_provider.load_authorization_code(client_info, auth_code_str)
        assert loaded_auth_code == expected
