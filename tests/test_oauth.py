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
        self,
        *,
        client_id,
        user_email,
        kbc_access_token,
        kbc_refresh_token,
        kbc_access_expires_at,
        oauth_projectless: bool = True,
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
            oauth_projectless=oauth_projectless,
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
            ('Evil\u200bName', 'EvilName'),  # zero-width space stripped
            ('Evil\u202eName', 'EvilName'),  # bidi override stripped
            ('Evil\x00Name', 'EvilName'),  # control character stripped
        ],
    )
    def test_sanitize_client_name(self, name: str, expected: str):
        from keboola_mcp_server.oauth import _sanitize_client_name

        assert _sanitize_client_name(name) == expected


class TestConnectionClientRegistry:
    """The in-process client-name cache (`ConnectionClientRegistry`) -- display-only, bounded
    (AI-2883 RFC Decisions §4)."""

    def test_remembers_and_returns_client_name(self):
        from keboola_mcp_server.oauth import ConnectionClientRegistry

        registry = ConnectionClientRegistry('https://oauth')
        registry.remember_client_name('client-a', 'My Tool')

        assert registry.get_client_name('client-a') == 'My Tool'
        assert registry.get_client_name('unknown-client') is None
        assert registry.get_client_name(None) is None

    def test_ignores_missing_client_id_or_name(self):
        from keboola_mcp_server.oauth import ConnectionClientRegistry

        registry = ConnectionClientRegistry('https://oauth')
        registry.remember_client_name(None, 'My Tool')
        registry.remember_client_name('client-a', '')

        assert registry.get_client_name('client-a') is None

    def test_sanitizes_and_caps_name_length_at_insertion_not_just_at_display(self):
        """/register is unauthenticated -- an arbitrarily long raw name stored per entry would let
        a caller inflate memory well past what the entry-count cap alone bounds (Copilot review
        finding). The cap must apply when the name is stored, not only when later read for the
        approval-screen payload."""
        from keboola_mcp_server.oauth import ConnectionClientRegistry

        registry = ConnectionClientRegistry('https://oauth')
        registry.remember_client_name('client-a', 'a' * 1_000_000)
        registry.remember_client_name('client-b', '\n\t\r')  # sanitizes to '' -- must not be stored

        assert registry.get_client_name('client-a') == 'a' * 128
        assert registry.get_client_name('client-b') is None

    def test_evicts_oldest_entry_once_over_capacity(self, monkeypatch: pytest.MonkeyPatch):
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import ConnectionClientRegistry

        monkeypatch.setattr(oauth_module, '_MAX_CACHED_CLIENT_NAMES', 2)
        registry = ConnectionClientRegistry('https://oauth')

        registry.remember_client_name('client-1', 'Tool 1')
        registry.remember_client_name('client-2', 'Tool 2')
        registry.remember_client_name('client-3', 'Tool 3')  # evicts client-1 (oldest)

        assert registry.get_client_name('client-1') is None
        assert registry.get_client_name('client-2') == 'Tool 2'
        assert registry.get_client_name('client-3') == 'Tool 3'


class TestSlidingWindowRateLimiter:
    def test_allows_up_to_max_calls_then_blocks(self):
        from keboola_mcp_server.oauth import _SlidingWindowRateLimiter

        limiter = _SlidingWindowRateLimiter(max_calls=3, window_seconds=60.0)

        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False  # 4th call within the window is refused

    def test_recovers_once_the_window_elapses(self, monkeypatch: pytest.MonkeyPatch):
        import time

        from keboola_mcp_server.oauth import _SlidingWindowRateLimiter

        limiter = _SlidingWindowRateLimiter(max_calls=1, window_seconds=60.0)
        now = 1_000.0
        monkeypatch.setattr(time, 'monotonic', lambda: now)

        assert limiter.try_acquire() is True
        assert limiter.try_acquire() is False

        now += 60.01  # past the window: the earlier call falls out of the sliding window
        assert limiter.try_acquire() is True


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
    def authorization_code(
        *, scopes: list[str] | None = None, expires_at: float | None = None, oauth_projectless: bool = True
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
            oauth_projectless=oauth_projectless,
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
            # This hook only checks *shape* -- the real trust decision (is client_id + this exact
            # redirect_uri registered?) happens against Connection in SimpleOAuthProvider.authorize(),
            # not here (see AI-2883 RFC). The shape allowed here is exactly what Connection will
            # ever register: https (any host -- Connection decides), cursor:// restricted to
            # Connection's own fixed host allowlist, or http:// restricted to loopback (RFC 8252).
            # This is NOT the old per-domain trust list -- an unknown https host is still accepted
            # here and left to Connection -- but a shape Connection could never register is rejected
            # outright, so it can't be used as an open-redirect target if something later in
            # authorize() fails unexpectedly (see authorize()'s ERROR-branch docstring).
            (AnyUrl('https://claude.ai/api/mcp/auth_callback'), True),
            (AnyUrl('https://anything.example.com/callback'), True),  # unknown host: fine here, Connection decides
            (AnyUrl('http://localhost:8080/callback'), True),
            (AnyUrl('http://127.0.0.1:54750/callback'), True),
            (AnyUrl('http://[::1]:8080/callback'), True),
            (AnyUrl('cursor://anysphere.cursor-mcp/callback'), True),
            (AnyUrl('cursor://anysphere.cursor-retrieval/callback'), True),
            (AnyUrl('cursor://some-other-host/callback'), False),  # not in the fixed cursor host allowlist
            (AnyUrl('http://evil.example/callback'), False),  # non-loopback http is not a shape Connection allows
            (AnyUrl('myapp://localhost/callback'), False),  # unrecognized custom scheme
            (AnyUrl('file:///etc/passwd'), False),
            (AnyUrl('intent://x/#Intent;scheme=http;end'), False),
            (AnyUrl('mailto:a@b.com'), False),
            (AnyUrl('javascript://alert(1)'), False),
            (AnyUrl('data://text/html,<script>alert(1)</script>'), False),
            (AnyUrl('vbscript://msgbox(1)'), False),
            (AnyUrl('https://user:pass@evil.example/cb'), False),  # userinfo
            (AnyUrl('https://claude.ai@evil.example/cb'), False),  # userinfo dressed up as a trusted host
            (AnyUrl('https://evil.example/cb#frag'), False),  # fragment
            # An empty-but-present fragment parses to '' (falsy), not None -- a truthiness check
            # would silently accept this (Copilot review finding); it must still be rejected.
            (AnyUrl('https://evil.example/cb#'), False),  # empty fragment
            (AnyUrl('https://evil.example/' + 'a' * 2048), False),  # over the 2048-char cap
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

        monkeypatch.setattr(oauth_module.ConnectionClientRegistry, 'check_registration', _fake)

    @pytest.mark.asyncio
    async def test_authorize_pre_registered_client_gets_projectless_scope(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """Only a client Keboola itself vetted and pre-registered (today: claude-ai) gets the
        unrestricted whole-stack 'projectless' grant."""
        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.REGISTERED)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('https://claude.ai/api/mcp/auth_callback'),
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
    async def test_authorize_dynamically_approved_client_does_not_get_projectless_scope(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """A client that became REGISTERED via Connection's dynamic-approval screen (Flow B) must
        NOT get 'projectless' -- Connection's own ClientApprovalProcessor deliberately withholds it
        from a self-service approval (any authenticated user, no elevated role required), but this
        server's broker identity always has 'projectless' on ITS OWN registration. Without this
        distinction, every dynamically-approved client would silently inherit an unrestricted,
        every-project grant regardless of Connection's intent -- see AI-2883 RFC security review.
        """
        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.REGISTERED)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('https://my-self-service-tool.example/cb'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )
        auth_url = await oauth_provider.authorize(client, params)

        parsed = urlparse(auth_url)
        assert parsed.path == '/oauth/consent'
        query = parse_qs(parsed.query)
        assert query['scope'] == ['claudai']

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
    @pytest.mark.parametrize(
        'registered_name',
        [
            None,  # never called register_client() at all
            '\n\t\r',  # called it, but with a name that sanitizes to '' -- must still fall back
        ],
    )
    async def test_authorize_unregistered_client_without_name_falls_back_to_connection_client_id(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch, registered_name: str | None
    ):
        from keboola_mcp_server.oauth import _ClientRegistration, _connection_client_id

        self._stub_client_registration(monkeypatch, _ClientRegistration.NOT_REGISTERED)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='never-registered')
        if registered_name is not None:
            await oauth_provider.register_client(client.model_copy(update={'client_name': registered_name}))
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
    async def test_authorize_redirects_to_own_callback_when_connection_check_errors(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """Must NOT raise AuthorizeError: the mcp SDK's own handler would catch that and redirect
        to the caller-supplied redirect_uri (now host-unrestricted) with the error params -- an
        open redirect once Connection can be made to error on demand (e.g. by exhausting its rate
        limit). Redirecting to this server's own /oauth/callback keeps the browser on our origin."""
        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.ERROR)
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('https://attacker.example/steal'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )

        auth_url = await oauth_provider.authorize(client, params)

        parsed = urlparse(auth_url)
        assert f'{parsed.scheme}://{parsed.netloc}{parsed.path}' == 'https://mcp/callback'
        query = parse_qs(parsed.query)
        assert query['error'] == ['temporarily_unavailable']

    @pytest.mark.asyncio
    async def test_authorize_redirects_to_own_callback_on_unexpected_exception(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """Anything unexpected raised past the registration check must ALSO redirect to this
        server's own /oauth/callback, not escape authorize() -- if it did, the mcp SDK's generic
        `except Exception` handler in AuthorizationHandler.handle would redirect to the
        caller-supplied redirect_uri instead (already validated non-fatally by
        validate_redirect_uri, which now accepts any https host): an open redirect triggerable by
        anything that makes _authorize() raise after registration succeeds (Copilot review
        finding)."""
        from keboola_mcp_server.oauth import _ClientRegistration

        self._stub_client_registration(monkeypatch, _ClientRegistration.REGISTERED)
        monkeypatch.setattr(oauth_provider, '_encode', mock.Mock(side_effect=RuntimeError('encoding blew up')))
        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        params = AuthorizationParams(
            redirect_uri=AnyUrl('https://attacker.example/steal'),
            redirect_uri_provided_explicitly=True,
            code_challenge='challenge',
            state='client-state',
            scopes=None,
        )

        auth_url = await oauth_provider.authorize(client, params)

        parsed = urlparse(auth_url)
        assert f'{parsed.scheme}://{parsed.netloc}{parsed.path}' == 'https://mcp/callback'
        query = parse_qs(parsed.query)
        assert query['error'] == ['temporarily_unavailable']

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status_code', 'body', 'expected'),
        [
            (200, '{}', 'REGISTERED'),
            # A 200 with an unexpected body (a misconfigured proxy, an SSO/captive-portal page, a
            # WAF challenge answering at this exact URL without ever reaching Connection) must not
            # be trusted as REGISTERED just because the status code matches (Copilot review
            # finding) -- Connection's real 200 is always the empty JSON object '{}'.
            (200, '<html>not connection</html>', 'ERROR'),
            (200, '{"unexpected": true}', 'ERROR'),
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
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import _ClientRegistration

        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured['url'] = str(request.url)
            captured['json'] = json.loads(request.content)
            return httpx.Response(status_code, text=body)

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        result = await oauth_provider._client_registry.check_registration(
            'claude-ai', 'https://claude.ai/api/mcp/auth_callback'
        )

        assert result is getattr(_ClientRegistration, expected)
        assert captured['url'] == 'https://oauth/oauth/clients/validate'
        assert captured['json'] == {'client_id': 'claude-ai', 'redirect_uri': 'https://claude.ai/api/mcp/auth_callback'}

    @pytest.mark.asyncio
    async def test_check_client_registration_fails_closed_on_network_error(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import _ClientRegistration

        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError('connection refused', request=request)

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        result = await oauth_provider._client_registry.check_registration(
            'claude-ai', 'https://claude.ai/api/mcp/auth_callback'
        )

        assert result is _ClientRegistration.ERROR

    @pytest.mark.asyncio
    async def test_check_client_registration_never_follows_a_redirect(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """A redirect from Connection's own URL to *anything* that answers 200 (a misconfigured
        proxy/gateway, a login page, a catch-all landing page) must never be silently followed and
        read as "client is registered" -- that would turn an infra misconfiguration into a false
        REGISTERED for a security-critical check. Regression test for a real finding."""
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import _ClientRegistration

        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if str(request.url) == 'https://oauth/oauth/clients/validate':
                return httpx.Response(302, headers={'Location': 'https://oauth/some-landing-page'})
            # Only reached if the client (incorrectly) chases the redirect.
            return httpx.Response(200, json={})

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        result = await oauth_provider._client_registry.check_registration(
            'claude-ai', 'https://claude.ai/api/mcp/auth_callback'
        )

        assert result is _ClientRegistration.ERROR
        assert call_count == 1  # never chased the redirect to the second URL

    @pytest.mark.asyncio
    async def test_check_client_registration_caches_positive_results(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """A REGISTERED verdict is cached so /authorize spam for an already-registered client
        can't 1:1 amplify into Connection's own (IP-shared) rate limit on /oauth/clients/validate."""
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import _ClientRegistration

        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json={})

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        registry = oauth_provider._client_registry
        first = await registry.check_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')
        second = await registry.check_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')

        assert first is _ClientRegistration.REGISTERED
        assert second is _ClientRegistration.REGISTERED
        assert call_count == 1  # second call was served from cache, no second HTTP request

    @pytest.mark.asyncio
    async def test_check_client_registration_never_caches_not_registered(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """NOT_REGISTERED must never be cached: an admin clicking Allow expects the *next* retry
        to work immediately, not wait out a stale negative cache entry (Copilot review finding)."""
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import _ClientRegistration

        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(404)

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        registry = oauth_provider._client_registry
        first = await registry.check_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')
        second = await registry.check_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')

        assert first is _ClientRegistration.NOT_REGISTERED
        assert second is _ClientRegistration.NOT_REGISTERED
        assert call_count == 2  # neither call was served from a cache

    @pytest.mark.asyncio
    async def test_check_client_registration_touches_cache_entry_on_hit(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """A cache hit must refresh the entry's LRU position, not just a write -- otherwise a
        frequently-reused entry (e.g. Claude.ai's own pair) stays the oldest-inserted entry and is
        the first evicted once enough unrelated keys flood in, despite being the most valuable
        entry to keep (Copilot review finding)."""
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import ConnectionClientRegistry

        monkeypatch.setattr(oauth_module, '_MAX_CACHED_REGISTRATIONS', 2)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={})

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        registry = ConnectionClientRegistry('https://oauth')
        await registry.check_registration('client-1', 'https://a.example/cb')
        await registry.check_registration('client-2', 'https://b.example/cb')
        # Touch client-1 again -- without touch-on-read this does nothing to its LRU position.
        await registry.check_registration('client-1', 'https://a.example/cb')
        # A third, unrelated key pushes the cache over capacity (2): the oldest-inserted entry
        # gets evicted. With touch-on-read, that's client-2 (never re-touched); without it, the
        # eviction order is purely insertion order and client-1 would be evicted instead.
        await registry.check_registration('client-3', 'https://c.example/cb')

        assert 'client-1' in [k[0] for k in registry._registration_cache]
        assert 'client-2' not in [k[0] for k in registry._registration_cache]

    @pytest.mark.asyncio
    async def test_check_client_registration_never_caches_error(
        self, oauth_provider: SimpleOAuthProvider, monkeypatch: pytest.MonkeyPatch
    ):
        """Caching a transient failure would prolong an outage instead of retrying it -- fail-closed
        must keep re-checking Connection on every call, not just the first."""
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import _ClientRegistration

        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(500)

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        registry = oauth_provider._client_registry
        first = await registry.check_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')
        second = await registry.check_registration('claude-ai', 'https://claude.ai/api/mcp/auth_callback')

        assert first is _ClientRegistration.ERROR
        assert second is _ClientRegistration.ERROR
        assert call_count == 2  # neither call was served from a cache

    @pytest.mark.asyncio
    async def test_check_client_registration_local_rate_limit_blocks_without_calling_connection(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """A caller that varies redirect_uri on every request always misses the registration
        cache (a fresh key each time), so the cache alone is not a defense against flooding
        Connection's shared, IP-keyed rate limit on /oauth/clients/validate (Copilot review
        finding). The local rate limiter must catch what the cache cannot: once its budget is
        spent, no further HTTP calls reach Connection at all, regardless of how the key varies."""
        from keboola_mcp_server import oauth as oauth_module
        from keboola_mcp_server.oauth import ConnectionClientRegistry, _ClientRegistration

        # Patched before construction: the limiter's budget is captured in __init__, not read
        # live from the module constant on every call.
        monkeypatch.setattr(oauth_module, '_VALIDATE_RATE_LIMIT_MAX_CALLS', 2)

        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(404)

        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda **_kwargs: httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        )

        registry = ConnectionClientRegistry('https://oauth')
        # Three distinct, never-cached (client_id, redirect_uri) pairs -- simulating an attacker
        # who varies the pair every time specifically to defeat the registration cache.
        first = await registry.check_registration('client-1', 'https://a.example/cb')
        second = await registry.check_registration('client-2', 'https://b.example/cb')
        third = await registry.check_registration('client-3', 'https://c.example/cb')

        assert first is _ClientRegistration.NOT_REGISTERED
        assert second is _ClientRegistration.NOT_REGISTERED
        assert third is _ClientRegistration.ERROR  # refused locally, not by Connection
        assert call_count == 2  # the 3rd check never reached the network

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
    @pytest.mark.parametrize(
        ('oauth_projectless', 'expected_scopes'),
        [
            (True, ['claudai', 'projectless']),
            (False, ['claudai']),
        ],
    )
    async def test_exchange_authorization_code_persists_the_actual_granted_scope(
        self,
        oauth_provider: SimpleOAuthProvider,
        monkeypatch: pytest.MonkeyPatch,
        oauth_projectless: bool,
        expected_scopes: list[str],
    ):
        """load_access_token/load_refresh_token must advertise the scope THIS session's Connection
        grant actually had (carried via _ExtendedAuthorizationCode.oauth_projectless from
        authorize()'s state), not a fixed 'claudai projectless' for every session regardless --
        otherwise a Flow B (dynamically-approved) session could claim the unrestricted whole-stack
        grant that Connection specifically withheld from it (Copilot review finding)."""
        from keboola_mcp_server import oauth as oauth_module

        monkeypatch.setattr(oauth_module, 'deployed_sa_token_path', lambda: '/tmp/sa-token')
        captured: dict[str, Any] = {}
        self._stub_exchanger(monkeypatch, captured)
        monkeypatch.setattr(
            oauth_module, 'introspect_token', mock.AsyncMock(side_effect=httpx.ConnectError('unreachable'))
        )

        client = _OAuthClientInformationFull(redirect_uris=[AnyHttpUrl('http://foo')], client_id='foo-client-id')
        auth_code = _ExtendedAuthorizationCode.model_validate(
            self.authorization_code(oauth_projectless=oauth_projectless)
        )
        oauth_token = await oauth_provider.exchange_authorization_code(client, auth_code)

        loaded = await oauth_provider.load_access_token(oauth_token.access_token)
        assert loaded is not None
        assert loaded.scopes == expected_scopes
        loaded_refresh = await oauth_provider.load_refresh_token(client, oauth_token.refresh_token)
        assert loaded_refresh is not None
        assert loaded_refresh.scopes == expected_scopes

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
        monkeypatch.setattr(
            oauth_module,
            '_create_http_client',
            lambda: (_ for _ in ()).throw(
                AssertionError('exchange_refresh_token must not call the league OAuth server')
            ),
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
