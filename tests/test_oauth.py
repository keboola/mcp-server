import time
from typing import Any, Mapping

import pytest
from mcp.server.auth.provider import AccessToken, RefreshToken
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull
from pydantic import AnyHttpUrl, AnyUrl

from keboola_mcp_server.oauth import SimpleOAuthProvider, _ExtendedAuthorizationCode, _OAuthClientInformationFull

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
            (AnyUrl('http://localhost:8080/foo'), True),
            (AnyUrl('http://localhost:20388/oauth/callback'), True),
            (AnyUrl('http://127.0.0.1:1234/bar'), True),
            (AnyUrl('http://127.0.0.1:54750/auth/callback'), True),
            (AnyUrl('https://foo.keboola.com/bar/baz'), True),
            (AnyUrl('https://bar.keboola.dev/baz'), True),
            (AnyUrl('https://chatgpt.com'), True),
            (AnyUrl('https://foo.chatgpt.com/bar'), True),
            (AnyUrl('https://chatgpt.com/connector_platform_oauth_redirect'), True),
            (AnyUrl('https://claude.ai'), True),
            (AnyUrl('https://foo.claude.ai/bar'), True),
            (AnyUrl('https://claude.ai/api/mcp/auth_callback'), True),
            (AnyUrl('https://librechat.glami-ml.com'), True),
            (AnyUrl('https://librechat.glami-ml.com/api/mcp/keboola/oauth/callback'), True),
            (AnyUrl('https://foo.librechat.glami-ml.com/bar'), False),  # no subdomains allowed
            (AnyUrl('https://make.com'), True),
            (AnyUrl('https://foo.make.com/bar'), True),
            (AnyUrl('https://www.make.com/oauth/cb/mcp'), True),
            (AnyUrl('https://cloud.onyx.app'), True),
            (AnyUrl('https://cloud.onyx.app/mcp/oauth/callback'), True),
            (AnyUrl('https://foo.cloud.onyx.app/bar'), False),  # no subdomains allowed
            (AnyUrl('https://global.consent.azure-apim.net'), True),
            (AnyUrl('https://global.consent.azure-apim.net/oauth/callback'), True),
            (AnyUrl('https://foo.global.consent.azure-apim.net/bar'), False),  # no subdomains allowed
            (AnyUrl('cursor://anysphere.cursor-retrieval/oauth/user-keboola-Data_warehouse/callback'), True),
            (None, False),
            (AnyUrl('https://foo.bar.com/callback'), False),
            (AnyUrl('ftp://foo.bar.com'), False),
        ],
    )
    def test_validate_redirect_uri(self, uri: AnyUrl, valid: bool):
        info = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo')
        if valid:
            actual = info.validate_redirect_uri(uri)
            assert actual == uri
        else:
            with pytest.raises(InvalidRedirectUriError):
                info.validate_redirect_uri(uri)
