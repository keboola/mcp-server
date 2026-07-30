import dataclasses
import secrets
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any, Mapping
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from mcp.server.auth.provider import AccessToken, AuthorizationParams, RefreshToken, TokenError
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull
from pydantic import AnyHttpUrl, AnyUrl

from keboola_mcp_server.clients.auth_bridge import OAuthTokenExchangeError
from keboola_mcp_server.oauth import (
    ProxyRefreshToken,
    SimpleOAuthProvider,
    _ExtendedAuthorizationCode,
    _OAuthClientInformationFull,
)
from keboola_mcp_server.session_store.repository import OAuthSession

JWT_KEY = 'secret'


class FakeSessionStore:
    """In-memory `SessionStore` (no real Postgres) for exercising `SimpleOAuthProvider` in isolation."""

    def __init__(self) -> None:
        self._sessions: dict[str, OAuthSession] = {}
        self._access_tokens: dict[str, str] = {}
        self._refresh_tokens: dict[str, str] = {}
        self._next_id = 0

    def _new_token_pair(self, session_id: str) -> tuple[str, str]:
        access_token = f'at_{session_id}_{secrets.token_hex(4)}'
        refresh_token = f'rt_{session_id}_{secrets.token_hex(4)}'
        self._access_tokens[access_token] = session_id
        self._refresh_tokens[refresh_token] = session_id
        return access_token, refresh_token

    async def create(
        self, *, client_id, user_email, kbc_access_token, kbc_refresh_token, kbc_access_expires_at
    ) -> tuple[str, str, OAuthSession]:
        self._next_id += 1
        session_id = str(self._next_id)
        session = OAuthSession(
            id=session_id,
            client_id=client_id,
            user_email=user_email,
            kbc_access_token=kbc_access_token,
            kbc_refresh_token=kbc_refresh_token,
            kbc_access_expires_at=kbc_access_expires_at,
            scope_project_ids=None,
            scope_read_only=False,
            scope_confirmed=False,
            scope_scoped_token=None,
            scope_scoped_expires_at=None,
        )
        self._sessions[session_id] = session
        access_token, refresh_token = self._new_token_pair(session_id)
        return access_token, refresh_token, session

    async def get_by_access_token(self, access_token: str) -> OAuthSession | None:
        session_id = self._access_tokens.get(access_token)
        return self._sessions.get(session_id) if session_id else None

    async def get_by_refresh_token(self, refresh_token: str) -> OAuthSession | None:
        session_id = self._refresh_tokens.get(refresh_token)
        return self._sessions.get(session_id) if session_id else None

    async def rotate_kbc_tokens(
        self, session_id: str, *, kbc_access_token: str, kbc_refresh_token: str, kbc_access_expires_at: datetime
    ) -> None:
        session = self._sessions[session_id]
        self._sessions[session_id] = dataclasses.replace(
            session,
            kbc_access_token=kbc_access_token,
            kbc_refresh_token=kbc_refresh_token,
            kbc_access_expires_at=kbc_access_expires_at,
        )

    async def rotate_opaque_tokens(self, session_id: str) -> tuple[str, str]:
        self._access_tokens = {k: v for k, v in self._access_tokens.items() if v != session_id}
        self._refresh_tokens = {k: v for k, v in self._refresh_tokens.items() if v != session_id}
        return self._new_token_pair(session_id)

    async def update_scope(
        self, session_id: str, *, project_ids, read_only, confirmed, scoped_token, scoped_expires_at
    ) -> None:
        session = self._sessions[session_id]
        self._sessions[session_id] = dataclasses.replace(
            session,
            scope_project_ids=project_ids,
            scope_read_only=read_only,
            scope_confirmed=confirmed,
            scope_scoped_token=scoped_token,
            scope_scoped_expires_at=scoped_expires_at,
        )

    async def revoke(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)


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
            session_store=FakeSessionStore(),
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
            # Data-app 'hub' subdomains are user-deployable and must be rejected (RISK-76)
            (AnyUrl('https://my-app.hub.keboola.com/callback'), False),
            (AnyUrl('https://my-app.hub.north-europe.azure.keboola.com/callback'), False),
            (AnyUrl('https://my-app.hub.keboola.dev/callback'), False),
            (AnyUrl('https://hub.keboola.com/callback'), False),  # the hub root itself
            (AnyUrl('https://my-app.hub.us-east4.gcp.keboola.com/callback'), False),
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
            # n8n at Groupon (no subdomains allowed)
            (AnyUrl('https://n8n.groupondev.com'), True),
            (AnyUrl('https://n8n.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://n8n-business.groupondev.com'), True),
            (AnyUrl('https://n8n-business.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://n8n-merchant.groupondev.com'), True),
            (AnyUrl('https://n8n-merchant.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://n8n-llm-traffic.groupondev.com'), True),
            (AnyUrl('https://n8n-llm-traffic.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://n8n-finance.groupondev.com'), True),
            (AnyUrl('https://n8n-finance.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://n8n-playground.groupondev.com'), True),
            (AnyUrl('https://n8n-playground.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://n8n-staging.groupondev.com'), True),
            (AnyUrl('https://n8n-staging.groupondev.com/rest/oauth2-credential/callback'), True),
            (AnyUrl('https://foo.n8n-playground.groupondev.com/bar'), False),  # no subdomains allowed
            (AnyUrl('https://n8n-unknown.groupondev.com'), False),  # not whitelisted
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

    @pytest.mark.asyncio
    async def test_authorize_redirects_to_consent_with_claudai_projectless_scope(
        self, oauth_provider: SimpleOAuthProvider
    ):
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('http://foo/callback'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )
        auth_url = await oauth_provider.authorize(client, params)

        parsed = urlparse(auth_url)
        assert parsed.path == '/oauth/consent'
        query = parse_qs(parsed.query)
        assert query['scope'] == ['claudai projectless']

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_exchanges_for_session(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')
        captured: dict[str, Any] = {}

        class _FakeExchanger:
            def __init__(self, **kwargs):
                captured['init_kwargs'] = kwargs

            async def exchange(self, *, oauth_access_token: str):
                captured['oauth_access_token'] = oauth_access_token
                return {'accessToken': 'kbc_at_new', 'refreshToken': 'kbc_rt_new', 'expiresIn': 3600}

        monkeypatch.setattr(oauth_module, 'OAuthSessionExchanger', _FakeExchanger)

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(self.authorization_code())

        oauth_token = await oauth_provider.exchange_authorization_code(client, auth_code)

        assert captured['oauth_access_token'] == 'oauth-access-token'
        loaded = await oauth_provider.load_access_token(oauth_token.access_token)
        assert loaded is not None
        assert loaded.kbc_access_token == 'kbc_at_new'
        # The refresh token is carried on ProxyRefreshToken only, not duplicated onto the (more
        # frequently sent/handled) access token.
        assert not hasattr(loaded, 'kbc_refresh_token')
        loaded_refresh = await oauth_provider.load_refresh_token(client, oauth_token.refresh_token)
        assert loaded_refresh is not None
        assert loaded_refresh.kbc_refresh_token == 'kbc_rt_new'
        # Neither opaque token carries a client-visible expiry (oauth_session_persistence RFC): the
        # server refreshes the underlying Keboola credential transparently on lookup, so there's no
        # forced-relogin window tied to the (1h) Keboola access token's lifetime.
        assert loaded.expires_at is None
        assert loaded_refresh.expires_at is None

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_maps_exchange_error(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')

        class _FailingExchanger:
            def __init__(self, **kwargs):
                pass

            async def exchange(self, *, oauth_access_token: str):
                raise OAuthTokenExchangeError('rejected', status_code=int(HTTPStatus.FORBIDDEN))

        monkeypatch.setattr(oauth_module, 'OAuthSessionExchanger', _FailingExchanger)

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(self.authorization_code())

        # Raised as TokenError (not HTTPException): the mcp SDK's /token handler only recognizes
        # TokenError and turns it into a spec-compliant TokenErrorResponse body.
        with pytest.raises(TokenError) as exc:
            await oauth_provider.exchange_authorization_code(client, auth_code)
        assert exc.value.error == 'invalid_grant'

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_missing_sa_token_path(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: None)

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(self.authorization_code())

        with pytest.raises(TokenError) as exc:
            await oauth_provider.exchange_authorization_code(client, auth_code)
        assert exc.value.error == 'invalid_request'

    @pytest.mark.asyncio
    async def test_exchange_refresh_token_calls_refresh_tokens_directly(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.auth_login import TokenSet

        captured: dict[str, Any] = {}

        async def _fake_refresh_tokens(storage_api_url: str, *, refresh_token: str, transport=None):
            captured['storage_api_url'] = storage_api_url
            captured['refresh_token'] = refresh_token
            return TokenSet(
                access_token='kbc_at_rotated', refresh_token='kbc_rt_rotated', expires_at=time.time() + 3600
            )

        monkeypatch.setattr(oauth_module, 'refresh_tokens', _fake_refresh_tokens)
        # If exchange_refresh_token ever called Connection's league OAuth server, this transport
        # would raise, proving the refresh is fully decoupled from it (RFC Decision §4).
        oauth_provider._create_http_client = lambda: (_ for _ in ()).throw(  # type: ignore[method-assign]
            AssertionError('exchange_refresh_token must not call the league OAuth server')
        )

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        _at, _rt, session = await oauth_provider._session_store.create(
            client_id='foo-client-id',
            user_email=None,
            kbc_access_token='kbc_at_old',
            kbc_refresh_token='kbc_rt_old',
            kbc_access_expires_at=datetime.now(timezone.utc),
        )
        refresh_token = ProxyRefreshToken(
            token='mcp_old',
            client_id='foo-client-id',
            scopes=['claudai', 'projectless'],
            expires_at=None,
            kbc_refresh_token='kbc_rt_old',
            session_id=session.id,
        )

        oauth_token = await oauth_provider.exchange_refresh_token(client, refresh_token, [])

        assert captured['refresh_token'] == 'kbc_rt_old'
        loaded = await oauth_provider.load_access_token(oauth_token.access_token)
        assert loaded is not None
        assert loaded.kbc_access_token == 'kbc_at_rotated'
        loaded_refresh = await oauth_provider.load_refresh_token(client, oauth_token.refresh_token)
        assert loaded_refresh is not None
        assert loaded_refresh.kbc_refresh_token == 'kbc_rt_rotated'

    @pytest.mark.asyncio
    async def test_exchange_refresh_token_maps_network_error_to_token_error(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module

        async def _failing_refresh_tokens(storage_api_url: str, *, refresh_token: str, transport=None):
            raise httpx.ConnectError('boom')

        monkeypatch.setattr(oauth_module, 'refresh_tokens', _failing_refresh_tokens)

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        _at, _rt, session = await oauth_provider._session_store.create(
            client_id='foo-client-id',
            user_email=None,
            kbc_access_token='kbc_at_old',
            kbc_refresh_token='kbc_rt_old',
            kbc_access_expires_at=datetime.now(timezone.utc),
        )
        refresh_token = ProxyRefreshToken(
            token='mcp_old',
            client_id='foo-client-id',
            scopes=['claudai', 'projectless'],
            expires_at=None,
            kbc_refresh_token='kbc_rt_old',
            session_id=session.id,
        )

        # A network failure talking to Connection must surface as a clean TokenError, not
        # propagate as a raw httpx error (which the mcp SDK's /token handler can't format).
        with pytest.raises(TokenError) as exc:
            await oauth_provider.exchange_refresh_token(client, refresh_token, [])
        assert exc.value.error == 'invalid_grant'

    @pytest.mark.asyncio
    async def test_load_access_token_refreshes_near_expiry_session_transparently(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.auth_login import TokenSet

        access_token, _rt, session = await oauth_provider._session_store.create(
            client_id='foo-client-id',
            user_email=None,
            kbc_access_token='kbc_at_stale',
            kbc_refresh_token='kbc_rt_stale',
            kbc_access_expires_at=datetime.now(timezone.utc),  # already at/past expiry
        )

        async def _fake_refresh_tokens(storage_api_url: str, *, refresh_token: str, transport=None):
            assert refresh_token == 'kbc_rt_stale'
            return TokenSet(access_token='kbc_at_fresh', refresh_token='kbc_rt_fresh', expires_at=time.time() + 3600)

        monkeypatch.setattr(oauth_module, 'refresh_tokens', _fake_refresh_tokens)

        loaded = await oauth_provider.load_access_token(access_token)

        assert loaded is not None
        assert loaded.kbc_access_token == 'kbc_at_fresh'
        # The refresh is persisted, not just returned once -- a second lookup sees it too.
        stored = await oauth_provider._session_store.get_by_access_token(access_token)
        assert stored is not None
        assert stored.kbc_access_token == 'kbc_at_fresh'
        assert stored.kbc_refresh_token == 'kbc_rt_fresh'

    @pytest.mark.asyncio
    async def test_load_access_token_tolerates_refresh_failure(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        # A refresh hiccup must not break the current request -- the (soon-to-expire) credential
        # already on the session may still work; the next lookup retries the refresh.
        from keboola_mcp_server import oauth as oauth_module

        access_token, _rt, _session = await oauth_provider._session_store.create(
            client_id='foo-client-id',
            user_email=None,
            kbc_access_token='kbc_at_stale',
            kbc_refresh_token='kbc_rt_stale',
            kbc_access_expires_at=datetime.now(timezone.utc),
        )

        async def _failing_refresh_tokens(storage_api_url: str, *, refresh_token: str, transport=None):
            raise httpx.ConnectError('boom')

        monkeypatch.setattr(oauth_module, 'refresh_tokens', _failing_refresh_tokens)

        loaded = await oauth_provider.load_access_token(access_token)

        assert loaded is not None
        assert loaded.kbc_access_token == 'kbc_at_stale'  # unchanged, refresh failed but didn't raise

    @pytest.mark.asyncio
    async def test_load_access_token_unknown_token_returns_none(self, oauth_provider: SimpleOAuthProvider) -> None:
        assert await oauth_provider.load_access_token('never-issued') is None

    @pytest.mark.asyncio
    async def test_revoke_token_invalidates_both_access_and_refresh_token(
        self, oauth_provider: SimpleOAuthProvider
    ) -> None:
        access_token, refresh_token, _session = await oauth_provider._session_store.create(
            client_id='foo-client-id',
            user_email=None,
            kbc_access_token='kbc_at_x',
            kbc_refresh_token='kbc_rt_x',
            kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

        await oauth_provider.revoke_token(access_token)

        assert await oauth_provider.load_access_token(access_token) is None
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        assert await oauth_provider.load_refresh_token(client, refresh_token) is None

    @pytest.mark.asyncio
    async def test_revoke_token_unknown_token_is_a_noop(self, oauth_provider: SimpleOAuthProvider) -> None:
        await oauth_provider.revoke_token('never-issued')  # must not raise
