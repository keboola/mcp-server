import base64
import dataclasses
import json
import logging
import secrets
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Any
from unittest import mock
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from mcp.server.auth.provider import AccessToken, AuthorizationParams, RefreshToken, TokenError
from mcp.shared.auth import InvalidRedirectUriError, OAuthClientInformationFull
from pydantic import AnyHttpUrl, AnyUrl

from keboola_mcp_server.auth_login import Introspection, ProjectAccess, ScopedToken
from keboola_mcp_server.clients.auth_bridge import OAuthTokenExchangeError
from keboola_mcp_server.oauth import (
    DatabaseUnavailableMiddleware,
    ProxyRefreshToken,
    SimpleOAuthProvider,
    _ExtendedAuthorizationCode,
    _OAuthClientInformationFull,
)
from keboola_mcp_server.session_store.repository import OAuthSession

JWT_KEY = 'secret'


def _project(project_id: int) -> ProjectAccess:
    return ProjectAccess(id=project_id, name=None, role=None)


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


class TestDatabaseUnavailableMiddleware:
    """A Postgres outage during bearer-token verification must surface as a clean, retryable 503
    -- not Starlette's bare "Internal Server Error" default. See DatabaseUnavailableMiddleware's
    docstring: a non-AuthenticationError raised inside AuthenticationBackend.authenticate() (this
    server's load_access_token) bypasses AuthenticationMiddleware's own exception handling, and
    FastMCP's Starlette app registers no exception_handlers at all -- this middleware is the only
    layer that can turn it into a response.
    """

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

    def test_get_middleware_prepends_database_unavailable_middleware(self, oauth_provider: SimpleOAuthProvider) -> None:
        middleware = oauth_provider.get_middleware()
        assert middleware[0].cls is DatabaseUnavailableMiddleware
        # The base class's AuthenticationMiddleware/AuthContextMiddleware must still follow --
        # confirms this wraps, rather than replaces, the inherited middleware.
        assert any(m.cls.__name__ == 'AuthenticationMiddleware' for m in middleware[1:])

    @pytest.mark.asyncio
    async def test_translates_database_unavailable_error_to_503(self) -> None:
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.routing import Route
        from starlette.testclient import TestClient

        from keboola_mcp_server.session_store import DatabaseUnavailableError

        async def endpoint(request):
            raise DatabaseUnavailableError('Postgres is unavailable (OSError).')

        app = Starlette(
            routes=[Route('/', endpoint)],
            middleware=[Middleware(DatabaseUnavailableMiddleware)],
        )
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get('/')

        assert response.status_code == 503
        assert response.json() == {'message': 'Service temporarily unavailable: the session database is unreachable.'}

    @pytest.mark.asyncio
    async def test_lets_other_errors_and_websockets_through_unchanged(self) -> None:
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.routing import Route
        from starlette.testclient import TestClient

        async def endpoint(request):
            raise ValueError('a genuine bug')

        app = Starlette(routes=[Route('/', endpoint)], middleware=[Middleware(DatabaseUnavailableMiddleware)])
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get('/')

        # Not our concern to handle -- falls through to Starlette's own error handling, unchanged.
        assert response.status_code == 500


class TestConnectionClientIdentity:
    """`_connection_client_id`/`_sanitize_client_name` -- the mapping between the mcp SDK's own
    client bookkeeping and the identity Connection's oauth2_client registry actually understands
    (see AI-2883 RFC, Resolution Strategy §4)."""

    def test_well_known_redirect_uri_maps_to_pre_registered_client_id(self):
        from keboola_mcp_server.oauth import _connection_client_id

        assert _connection_client_id('https://claude.ai/api/mcp/auth_callback') == 'claude-ai'

    def test_unknown_redirect_uri_derives_a_stable_short_id(self):
        from keboola_mcp_server.oauth import _connection_client_id

        first = _connection_client_id('https://my.tool/oauth/callback')
        second = _connection_client_id('https://my.tool/oauth/callback')
        different = _connection_client_id('https://other.tool/oauth/callback')

        assert first == second  # stable across the SDK minting a fresh uuid on every /register
        assert first != different
        assert len(first) <= 32  # Connection's oauth2_client.identifier / pending_mcp_client cap

    def test_derived_id_fits_even_for_a_very_long_redirect_uri(self):
        from keboola_mcp_server.oauth import _connection_client_id

        assert len(_connection_client_id('https://example.com/' + 'a' * 2000)) <= 32

    @pytest.mark.parametrize(
        ('name', 'expected'),
        [
            ('My Custom Tool', 'My Custom Tool'),
            ('a' * 200, 'a' * 128),  # Connection's client_name cap
            ('Evil​Name', 'EvilName'),  # zero-width space stripped
            ('Evil‮Name', 'EvilName'),  # bidi override stripped
            ('Evil\x00Name', 'EvilName'),  # control character stripped
        ],
    )
    def test_sanitize_client_name(self, name: str, expected: str):
        from keboola_mcp_server.oauth import _sanitize_client_name

        assert _sanitize_client_name(name) == expected


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
            # This hook only rejects what could never legitimately reach Connection's client
            # registry -- the real trust decision (is client_id + this exact redirect_uri
            # registered?) now happens against Connection in SimpleOAuthProvider.authorize(), not
            # here (see AI-2883 RFC). Any scheme+host shape not explicitly dangerous is accepted
            # by this sync hook and left to that async check.
            (AnyUrl('https://claude.ai/api/mcp/auth_callback'), True),
            (AnyUrl('https://anything.example.com/callback'), True),  # unknown host: fine here, Connection decides
            (AnyUrl('http://localhost:8080/callback'), True),
            (AnyUrl('http://127.0.0.1:54750/callback'), True),
            (AnyUrl('cursor://anysphere.cursor-mcp/callback'), True),
            (AnyUrl('cursor://some-other-host/callback'), True),  # likewise left to Connection
            (
                AnyUrl('myapp://localhost/callback'),
                True,
            ),  # unrecognized custom scheme: not dangerous, left to Connection
            # Scripting schemes are rejected outright -- Connection could never register these anyway.
            (AnyUrl('javascript://alert(1)'), False),
            (AnyUrl('data://text/html,<script>alert(1)</script>'), False),
            (AnyUrl('vbscript://msgbox(1)'), False),
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

    @staticmethod
    def _stub_client_registration(monkeypatch: pytest.MonkeyPatch, status) -> None:
        from keboola_mcp_server import oauth as oauth_module

        async def _fake(self, connection_client_id: str, redirect_uri: str):
            return status

        monkeypatch.setattr(oauth_module.SimpleOAuthProvider, '_check_client_registration', _fake)

    @pytest.mark.asyncio
    async def test_authorize_redirects_to_consent_with_claudai_projectless_scope(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.REGISTERED)
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
    async def test_authorize_unregistered_client_redirects_to_connection_approval(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.NOT_REGISTERED)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        await oauth_provider.register_client(client.model_copy(update={'client_name': 'My Custom Tool'}))
        params = AuthorizationParams(
            redirect_uri=AnyUrl('https://my.tool/oauth/callback'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )

        auth_url = await oauth_provider.authorize(client, params)

        parsed = urlparse(auth_url)
        # Must be Connection's own /oauth/authorize -- PendingMcpClientApprovalListener only gates
        # that route, never /oauth/consent (RFC Decisions §3-4).
        assert parsed.path == '/oauth/authorize'
        query = parse_qs(parsed.query)
        assert query['response_type'] == ['code']
        assert 'code_challenge' in query
        assert query['code_challenge_method'] == ['S256']

        # The outer client_id/redirect_uri must match the payload exactly -- Connection's listener
        # requires the query's client_id to equal the payload's.
        from keboola_mcp_server.oauth import _connection_client_id

        connection_client_id = query['client_id'][0]
        assert connection_client_id == _connection_client_id('https://my.tool/oauth/callback')
        assert query['redirect_uri'] == ['https://my.tool/oauth/callback']

        decoded = json.loads(base64.urlsafe_b64decode(query['pending_mcp_client'][0]))
        assert decoded == {
            'client_id': connection_client_id,
            'client_name': 'My Custom Tool',
            'redirect_uri': 'https://my.tool/oauth/callback',
        }

    @pytest.mark.asyncio
    async def test_authorize_unregistered_client_without_name_falls_back_to_connection_client_id(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server.oauth import _ClientRegistration, _connection_client_id

        self._stub_client_registration(monkeypatch, _ClientRegistration.NOT_REGISTERED)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='never-registered')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('https://another.tool/cb'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )

        auth_url = await oauth_provider.authorize(client, params)

        decoded = json.loads(base64.urlsafe_b64decode(parse_qs(urlparse(auth_url).query)['pending_mcp_client'][0]))
        assert decoded['client_name'] == _connection_client_id('https://another.tool/cb')

    @pytest.mark.asyncio
    async def test_authorize_raises_when_connection_check_errors(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from mcp.server.auth.provider import AuthorizeError

        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.ERROR)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('http://foo/callback'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )

        with pytest.raises(AuthorizeError) as exc_info:
            await oauth_provider.authorize(client, params)
        assert exc_info.value.error == 'temporarily_unavailable'

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status_code', 'body', 'expected'),
        [
            (200, '{}', 'REGISTERED'),
            (404, '', 'NOT_REGISTERED'),
            (429, '', 'ERROR'),
            (500, 'boom', 'ERROR'),
            (400, '{"error": "bad"}', 'ERROR'),
        ],
    )
    async def test_check_client_registration_maps_connection_response(
        self,
        oauth_provider: SimpleOAuthProvider,
        monkeypatch: pytest.MonkeyPatch,
        status_code: int,
        body: str,
        expected: str,
    ):
        from keboola_mcp_server.oauth import _ClientRegistration

        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured['url'] = str(request.url)
            captured['json'] = json.loads(request.content)
            return httpx.Response(status_code, text=body)

        monkeypatch.setattr(
            oauth_provider,
            '_create_http_client',
            lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        result = await oauth_provider._check_client_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')

        assert result is getattr(_ClientRegistration, expected)
        assert captured['url'] == 'https://oauth/oauth/clients/validate'
        assert captured['json'] == {'client_id': 'claude-ai', 'redirect_uri': 'https://claude.ai/api/mcp/auth_callback'}

    @pytest.mark.asyncio
    async def test_check_client_registration_fails_closed_on_network_error(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server.oauth import _ClientRegistration

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError('connection refused', request=request)

        monkeypatch.setattr(
            oauth_provider,
            '_create_http_client',
            lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        result = await oauth_provider._check_client_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')

        assert result is _ClientRegistration.ERROR

    @staticmethod
    def _stub_exchanger(monkeypatch: pytest.MonkeyPatch, captured: dict[str, Any]) -> None:
        from keboola_mcp_server import oauth as oauth_module

        class _FakeExchanger:
            def __init__(self, **kwargs):
                captured['init_kwargs'] = kwargs

            async def exchange(self, *, oauth_access_token: str):
                captured['oauth_access_token'] = oauth_access_token
                return {'accessToken': 'kbc_at_new', 'refreshToken': 'kbc_rt_new', 'expiresIn': 3600}

        monkeypatch.setattr(oauth_module, 'OAuthSessionExchanger', _FakeExchanger)

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_exchanges_for_session(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')
        captured: dict[str, Any] = {}
        self._stub_exchanger(monkeypatch, captured)
        # Two reachable projects: the consent screen already made this the user's deliberate choice
        # (whether "all projects" or a picked subset), so the session should still auto-confirm --
        # see test_exchange_authorization_code_auto_confirms_multiple_projects. Also proves
        # introspection failures here are non-fatal to login -- see
        # test_exchange_authorization_code_introspection_failure_is_non_fatal.
        monkeypatch.setattr(
            oauth_module,
            'introspect_token',
            mock.AsyncMock(
                return_value=Introspection(
                    user_id=1, user_email=None, user_name=None, projects=[_project(1), _project(2)]
                )
            ),
        )
        monkeypatch.setattr(
            oauth_module, 'exchange_scoped_token', mock.AsyncMock(side_effect=httpx.ConnectError('unreachable'))
        )

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
        # Still confirmed even though the scoped-token exchange itself failed (best-effort fallback).
        assert loaded.scope_confirmed is True
        assert loaded.scope_project_ids == [1, 2]
        assert loaded.scope_scoped_token is None

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_auto_confirms_single_project(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        # No real scoping choice to make with only one reachable project -- see the "Security
        # hardening" RFC increment: mirrors the same auto-confirm the local `login` flow does.
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')
        captured: dict[str, Any] = {}
        self._stub_exchanger(monkeypatch, captured)
        monkeypatch.setattr(
            oauth_module,
            'introspect_token',
            mock.AsyncMock(
                return_value=Introspection(user_id=1, user_email=None, user_name=None, projects=[_project(42)])
            ),
        )
        monkeypatch.setattr(
            oauth_module,
            'exchange_scoped_token',
            mock.AsyncMock(
                return_value=ScopedToken(
                    access_token='kbc_at_scoped', expires_at=time.time() + 3600, project_ids=[42], read_only=False
                )
            ),
        )

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(self.authorization_code())
        oauth_token = await oauth_provider.exchange_authorization_code(client, auth_code)

        loaded = await oauth_provider.load_access_token(oauth_token.access_token)
        assert loaded is not None
        assert loaded.scope_confirmed is True
        assert loaded.scope_project_ids == [42]
        assert loaded.scope_read_only is False
        assert loaded.scope_scoped_token == 'kbc_at_scoped'

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_auto_confirms_multiple_projects(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        # The league consent screen already lets the user pick "all projects" or freeze access to a
        # specific subset -- either way that's the user's deliberate scoping choice, made before this
        # code path ever runs, so a session reaching more than one project must still auto-confirm
        # instead of making the agent ask the user again via set_project_scope.
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')
        captured: dict[str, Any] = {}
        self._stub_exchanger(monkeypatch, captured)
        monkeypatch.setattr(
            oauth_module,
            'introspect_token',
            mock.AsyncMock(
                return_value=Introspection(
                    user_id=1, user_email=None, user_name=None, projects=[_project(1), _project(2)]
                )
            ),
        )
        monkeypatch.setattr(
            oauth_module,
            'exchange_scoped_token',
            mock.AsyncMock(
                return_value=ScopedToken(
                    access_token='kbc_at_scoped', expires_at=time.time() + 3600, project_ids=[1, 2], read_only=False
                )
            ),
        )

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(self.authorization_code())
        oauth_token = await oauth_provider.exchange_authorization_code(client, auth_code)

        loaded = await oauth_provider.load_access_token(oauth_token.access_token)
        assert loaded is not None
        assert loaded.scope_confirmed is True
        assert loaded.scope_project_ids == [1, 2]
        assert loaded.scope_read_only is False
        assert loaded.scope_scoped_token == 'kbc_at_scoped'

    @pytest.mark.asyncio
    async def test_exchange_authorization_code_introspection_failure_is_non_fatal(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        # Login must still succeed even if the best-effort auto-confirm can't run at all.
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')
        captured: dict[str, Any] = {}
        self._stub_exchanger(monkeypatch, captured)
        monkeypatch.setattr(
            oauth_module, 'introspect_token', mock.AsyncMock(side_effect=httpx.ConnectError('unreachable'))
        )

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(self.authorization_code())
        oauth_token = await oauth_provider.exchange_authorization_code(client, auth_code)

        loaded = await oauth_provider.load_access_token(oauth_token.access_token)
        assert loaded is not None
        assert loaded.scope_confirmed is False

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
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
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

        with caplog.at_level(logging.INFO):
            loaded = await oauth_provider.load_access_token(access_token)

        assert loaded is not None
        assert loaded.kbc_access_token == 'kbc_at_fresh'
        # The refresh is persisted, not just returned once -- a second lookup sees it too.
        stored = await oauth_provider._session_store.get_by_access_token(access_token)
        assert stored is not None
        assert stored.kbc_access_token == 'kbc_at_fresh'
        assert stored.kbc_refresh_token == 'kbc_rt_fresh'
        # Observable in logs (session id only, no token values) -- previously silent on success.
        refresh_logs = [r for r in caplog.records if 'Lazily refreshed near-expiry' in r.message]
        assert len(refresh_logs) == 1
        assert session.id in refresh_logs[0].message
        assert 'kbc_at_fresh' not in refresh_logs[0].message
        assert 'kbc_rt_fresh' not in refresh_logs[0].message

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
