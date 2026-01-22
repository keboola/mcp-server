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
            # === HTTP scheme - localhost only ===
            (AnyUrl('http://localhost:8080/foo'), True),
            (AnyUrl('http://localhost:20388/oauth/callback'), True),
            (AnyUrl('http://localhost/callback'), True),
            (AnyUrl('http://127.0.0.1:1234/bar'), True),
            (AnyUrl('http://127.0.0.1:54750/auth/callback'), True),
            (AnyUrl('http://127.0.0.1/callback'), True),
            # IPv6 localhost
            (AnyUrl('http://[::1]:8080/callback'), True),
            (AnyUrl('http://[::1]/callback'), True),
            # HTTP to non-localhost should be rejected
            (AnyUrl('http://example.com/callback'), False),
            (AnyUrl('http://keboola.com/callback'), False),
            (AnyUrl('http://192.168.1.1/callback'), False),
            # === HTTPS scheme - whitelisted domains ===
            # Keboola domains (requires subdomain)
            (AnyUrl('https://foo.keboola.com/bar/baz'), True),
            (AnyUrl('https://bar.keboola.dev/baz'), True),
            (AnyUrl('https://connection.keboola.com/oauth/callback'), True),
            (AnyUrl('https://keboola.com/callback'), False),  # requires subdomain
            (AnyUrl('https://keboola.dev/callback'), False),  # requires subdomain
            # ChatGPT (subdomain optional)
            (AnyUrl('https://chatgpt.com'), True),
            (AnyUrl('https://foo.chatgpt.com/bar'), True),
            (AnyUrl('https://chatgpt.com/connector_platform_oauth_redirect'), True),
            # Claude (subdomain optional)
            (AnyUrl('https://claude.ai'), True),
            (AnyUrl('https://foo.claude.ai/bar'), True),
            (AnyUrl('https://claude.ai/api/mcp/auth_callback'), True),
            # LibreChat (no subdomains allowed)
            (AnyUrl('https://librechat.glami-ml.com'), True),
            (AnyUrl('https://librechat.glami-ml.com/api/mcp/keboola/oauth/callback'), True),
            (AnyUrl('https://foo.librechat.glami-ml.com/bar'), False),  # no subdomains allowed
            # Make.com (subdomain optional)
            (AnyUrl('https://make.com'), True),
            (AnyUrl('https://foo.make.com/bar'), True),
            (AnyUrl('https://www.make.com/oauth/cb/mcp'), True),
            # Devin (exact domain only)
            (AnyUrl('https://api.devin.ai/callback'), True),
            (AnyUrl('https://api.devin.ai'), True),
            (AnyUrl('https://devin.ai/callback'), False),  # must be api.devin.ai
            (AnyUrl('https://foo.api.devin.ai/callback'), False),  # no subdomains
            # Onyx (no subdomains allowed)
            (AnyUrl('https://cloud.onyx.app'), True),
            (AnyUrl('https://cloud.onyx.app/mcp/oauth/callback'), True),
            (AnyUrl('https://foo.cloud.onyx.app/bar'), False),  # no subdomains allowed
            (AnyUrl('https://onyx.app/callback'), False),  # must be cloud.onyx.app
            # Azure APIM (no subdomains allowed)
            (AnyUrl('https://global.consent.azure-apim.net'), True),
            (AnyUrl('https://global.consent.azure-apim.net/oauth/callback'), True),
            (AnyUrl('https://foo.global.consent.azure-apim.net/bar'), False),  # no subdomains allowed
            # Unknown HTTPS domains should be rejected
            (AnyUrl('https://foo.bar.com/callback'), False),
            (AnyUrl('https://evil.com/callback'), False),
            (AnyUrl('https://fakechatgpt.com/callback'), False),
            (AnyUrl('https://evilclaude.ai/callback'), False),
            # === Cursor scheme - specific hosts only ===
            (AnyUrl('cursor://anysphere.cursor-retrieval/oauth/user-keboola-Data_warehouse/callback'), True),
            (AnyUrl('cursor://anysphere.cursor-mcp/oauth/callback'), True),
            (AnyUrl('cursor://anysphere.cursor-mcp/some/path'), True),
            # Cursor with unknown hosts should be rejected
            (AnyUrl('cursor://evil.com/callback'), False),
            (AnyUrl('cursor://localhost/callback'), False),
            (AnyUrl('cursor://anysphere.cursor-other/callback'), False),
            # === Unknown/forbidden schemes should be rejected ===
            (AnyUrl('ftp://foo.bar.com'), False),
            (AnyUrl('file:///etc/passwd'), False),
            (AnyUrl('javascript://alert(1)'), False),
            (AnyUrl('data://text/html,<script>alert(1)</script>'), False),
            # Custom schemes that are NOT whitelisted should be rejected
            (AnyUrl('vscode://localhost/callback'), False),
            (AnyUrl('jetbrains://localhost/callback'), False),
            (AnyUrl('zed://localhost/callback'), False),
            (AnyUrl('myapp://localhost/callback'), False),
            (AnyUrl('evil://localhost/callback'), False),
            # === Edge cases ===
            (None, False),  # no redirect_uri
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
