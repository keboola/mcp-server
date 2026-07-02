"""Tests for the local browser PKCE login + credential store (PSGO-261, Part B)."""

import base64
import hashlib
import json
import stat
import time
from pathlib import Path

import httpx
import pytest

from keboola_mcp_server import auth_login
from keboola_mcp_server.auth_login import (
    TokenSet,
    create_pat,
    elevate_session,
    ensure_access_token,
    exchange_code,
    exchange_scoped_token,
    get_access_token,
    introspect_token,
    lease_pat,
    load_tokens,
    refresh_tokens,
    save_tokens,
)

STACK = 'https://connection.keboola.com'


@pytest.fixture
def creds_file(tmp_path: Path, monkeypatch) -> Path:
    path = tmp_path / 'creds' / 'credentials.json'
    monkeypatch.setattr(auth_login, '_CREDENTIALS_PATH', path)
    return path


def _token_response(handler_status: int = 200):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            handler_status,
            json={
                'accessToken': 'kbc_at_new',
                'refreshToken': 'kbc_rt_new',
                'tokenType': 'Bearer',
                'expiresIn': 3600,
                'sessionId': 'sess-1',
            },
        )

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_exchange_code_parses_token_set() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['url'] = str(request.url)
        return httpx.Response(200, json={'accessToken': 'kbc_at_x', 'refreshToken': 'kbc_rt_x', 'expiresIn': 3600})

    tokens = await exchange_code(
        STACK,
        code='c',
        state='s',
        code_verifier='v',
        redirect_uri='http://127.0.0.1:1/callback',
        transport=httpx.MockTransport(handler),
    )
    assert tokens.access_token == 'kbc_at_x'
    assert tokens.refresh_token == 'kbc_rt_x'
    assert tokens.expires_at > time.time()
    assert captured['url'] == 'https://connection.keboola.com/v1/auth/pkce/token'


@pytest.mark.asyncio
async def test_refresh_tokens_rotates_pair() -> None:
    tokens = await refresh_tokens(STACK, refresh_token='kbc_rt_old', transport=_token_response())
    assert tokens.access_token == 'kbc_at_new'
    assert tokens.refresh_token == 'kbc_rt_new'


def test_save_and_load_round_trip_mode_600(creds_file: Path) -> None:
    ts = TokenSet(access_token='kbc_at_1', refresh_token='kbc_rt_1', expires_at=time.time() + 3600, session_id='s')
    save_tokens(STACK, ts)

    assert stat.S_IMODE(creds_file.stat().st_mode) == 0o600
    loaded = load_tokens(STACK)
    assert loaded == ts
    # A different stack has no credentials.
    assert load_tokens('https://connection.other.keboola.com') is None


def test_is_near_expiry() -> None:
    assert TokenSet('a', 'r', expires_at=time.time() + 10).is_near_expiry is True
    assert TokenSet('a', 'r', expires_at=time.time() + 3600).is_near_expiry is False


@pytest.mark.asyncio
async def test_get_access_token_without_credentials_raises(creds_file: Path) -> None:
    with pytest.raises(RuntimeError, match='Run "keboola-mcp-server login'):
        await get_access_token(STACK)


@pytest.mark.asyncio
async def test_get_access_token_returns_valid_token_without_refresh(creds_file: Path) -> None:
    ts = TokenSet('kbc_at_valid', 'kbc_rt_1', expires_at=time.time() + 3600)
    save_tokens(STACK, ts)
    # Transport would 500 if called — proves no refresh happens for a fresh token.
    token = await get_access_token(STACK, transport=_token_response(500))
    assert token == 'kbc_at_valid'


@pytest.mark.asyncio
async def test_get_access_token_refreshes_near_expiry_and_persists(creds_file: Path) -> None:
    save_tokens(STACK, TokenSet('kbc_at_old', 'kbc_rt_old', expires_at=time.time() + 5))
    token = await get_access_token(STACK, transport=_token_response())
    assert token == 'kbc_at_new'
    # Rotated pair persisted.
    assert load_tokens(STACK).refresh_token == 'kbc_rt_new'


@pytest.mark.asyncio
async def test_get_access_token_dead_token_forgets_and_raises(creds_file: Path) -> None:
    save_tokens(STACK, TokenSet('kbc_at_old', 'kbc_rt_dead', expires_at=time.time() + 5))
    with pytest.raises(RuntimeError, match='has expired'):
        await get_access_token(STACK, transport=_token_response(401))
    # Stale credentials dropped so the next start triggers a fresh login.
    assert load_tokens(STACK) is None


@pytest.mark.asyncio
async def test_ensure_access_token_returns_stored_without_login(creds_file: Path, monkeypatch) -> None:
    save_tokens(STACK, TokenSet('kbc_at_valid', 'kbc_rt_1', expires_at=time.time() + 3600))

    async def _must_not_login(*_a, **_k):
        raise AssertionError('perform_login must not run when a valid session is stored')

    monkeypatch.setattr(auth_login, 'perform_login', _must_not_login)
    token = await ensure_access_token(STACK, transport=_token_response(500))
    assert token == 'kbc_at_valid'


@pytest.mark.asyncio
async def test_ensure_access_token_logs_in_when_no_session(creds_file: Path, monkeypatch) -> None:
    # No stored session → ensure_access_token runs the browser login, then returns the fresh token.
    calls: list[str] = []

    async def _fake_login(storage_api_url: str, **_k):
        calls.append(storage_api_url)
        save_tokens(storage_api_url, TokenSet('kbc_at_fresh', 'kbc_rt_fresh', expires_at=time.time() + 3600))

    monkeypatch.setattr(auth_login, 'perform_login', _fake_login)
    token = await ensure_access_token(STACK, transport=_token_response(500))
    assert token == 'kbc_at_fresh'
    assert calls == [STACK]


@pytest.mark.asyncio
async def test_ensure_access_token_non_interactive_raises_without_login(creds_file: Path, monkeypatch) -> None:
    # No TTY (e.g. launched by an MCP client): must NOT attempt a browser login (it would corrupt
    # the stdio protocol / hang the handshake); raise the clear guidance instead.
    async def _must_not_login(*_a, **_k):
        raise AssertionError('perform_login must not run when interactive login is disallowed')

    monkeypatch.setattr(auth_login, 'perform_login', _must_not_login)
    with pytest.raises(RuntimeError, match='Run "keboola-mcp-server login'):
        await ensure_access_token(STACK, allow_interactive=False)


def test_pkce_challenge_is_sha256_of_verifier() -> None:
    verifier = auth_login._b64url(b'0123456789abcdef0123456789abcdef0123456789ab')
    expected = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode('ascii')).digest()).decode().rstrip('=')
    assert auth_login._b64url(hashlib.sha256(verifier.encode('ascii')).digest()) == expected


def test_invalid_stack_url_rejected() -> None:
    with pytest.raises(ValueError, match='Invalid Keboola Storage API URL'):
        auth_login._base_url('https://example.com')


# --- introspection + scoped exchange (PSGO-261 increment 2) ---


@pytest.mark.asyncio
async def test_introspect_token_parses_projects() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['url'] = str(request.url)
        captured['auth'] = request.headers['Authorization']
        return httpx.Response(
            200,
            json={
                'user': {'id': 60, 'email': 'm@k.com', 'name': 'M'},
                'projects': [
                    {'id': 18, 'name': 'A', 'role': 'admin'},
                    {'id': 83, 'name': 'B', 'role': 'admin'},
                ],
            },
        )

    intro = await introspect_token(STACK, subject_token='kbc_at_x', transport=httpx.MockTransport(handler))

    assert captured['url'] == 'https://connection.keboola.com/v1/auth/token/introspect'
    assert captured['auth'] == 'Bearer kbc_at_x'
    assert intro.user_email == 'm@k.com'
    assert [(p.id, p.name, p.role) for p in intro.projects] == [(18, 'A', 'admin'), (83, 'B', 'admin')]


@pytest.mark.asyncio
async def test_exchange_scoped_token_sends_scope_and_parses() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['url'] = str(request.url)
        captured['auth'] = request.headers['Authorization']
        captured['body'] = json.loads(request.content)
        return httpx.Response(201, json={'accessToken': 'kbc_at_scoped', 'expiresIn': 3600, 'readOnly': True})

    scoped = await exchange_scoped_token(
        STACK,
        subject_token='kbc_at_parent',
        project_ids=[18, 83],
        read_only=True,
        transport=httpx.MockTransport(handler),
    )

    assert captured['url'] == 'https://connection.keboola.com/v1/auth/pat/exchange'
    assert captured['auth'] == 'Bearer kbc_at_parent'
    # the exchange API requires project ids as strings
    assert captured['body'] == {'expiresIn': None, 'scope': {'projects': ['18', '83'], 'readOnly': True}}
    assert scoped.access_token == 'kbc_at_scoped'
    assert scoped.read_only is True
    assert scoped.project_ids == [18, 83]
    assert scoped.expires_at > time.time()
    assert not scoped.is_near_expiry


# --- sudo elevation + PAT creation (PSGO-261) ---


@pytest.mark.asyncio
async def test_elevate_session_sends_totp_and_returns_token() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['url'] = str(request.url)
        captured['auth'] = request.headers['Authorization']
        captured['body'] = json.loads(request.content)
        return httpx.Response(200, json={'token': 'kbc_sudo_1'})

    token = await elevate_session(
        STACK, subject_token='kbc_at_x', totp_code='123456', transport=httpx.MockTransport(handler)
    )
    assert captured['url'] == 'https://connection.keboola.com/v1/auth/sudo'
    assert captured['auth'] == 'Bearer kbc_at_x'
    assert captured['body'] == {'totpCode': '123456'}  # recoveryCode/type omitted
    assert token == 'kbc_sudo_1'


@pytest.mark.asyncio
async def test_elevate_session_requires_exactly_one_code() -> None:
    with pytest.raises(ValueError, match='exactly one'):
        await elevate_session(STACK, subject_token='kbc_at_x', totp_code='1', recovery_code='2')
    with pytest.raises(ValueError, match='exactly one'):
        await elevate_session(STACK, subject_token='kbc_at_x')


@pytest.mark.asyncio
async def test_create_pat_sends_projects_and_parses_token() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['url'] = str(request.url)
        captured['auth'] = request.headers['Authorization']
        captured['body'] = json.loads(request.content)
        return httpx.Response(201, json={'token': 'kbc_pat_new'})

    pat = await create_pat(
        STACK,
        subject_token='kbc_sudo_1',
        project_ids=[18, 83],
        name='demo',
        expires_in=2592000,
        transport=httpx.MockTransport(handler),
    )
    assert captured['url'] == 'https://connection.keboola.com/v1/auth/pat'
    assert captured['auth'] == 'Bearer kbc_sudo_1'
    # project ids serialized as strings and nested under scope, like the exchange endpoint
    assert captured['body'] == {'name': 'demo', 'expiresIn': 2592000, 'scope': {'projects': ['18', '83']}}
    assert pat == 'kbc_pat_new'


@pytest.mark.asyncio
async def test_lease_pat_introspects_then_sudo_then_creates() -> None:
    # One routing transport across the three endpoints the flow hits, asserting the sudo token is
    # what authorizes PAT creation and that all introspected projects are included.
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        seen.append(path)
        if path.endswith('/token/introspect'):
            return httpx.Response(200, json={'projects': [{'id': 18}, {'id': 83}, {'id': 95}]})
        if path.endswith('/auth/sudo'):
            assert json.loads(request.content) == {'recoveryCode': 'rec-9'}
            return httpx.Response(200, json={'token': 'kbc_sudo_1'})
        if path.endswith('/auth/pat'):
            assert request.headers['Authorization'] == 'Bearer kbc_sudo_1'
            assert json.loads(request.content)['scope']['projects'] == ['18', '83', '95']
            return httpx.Response(201, json={'token': 'kbc_pat_leased'})
        raise AssertionError(f'unexpected path {path}')

    pat = await lease_pat(
        STACK, subject_token='kbc_at_parent', recovery_code='rec-9', transport=httpx.MockTransport(handler)
    )
    assert pat == 'kbc_pat_leased'
    assert [p.split('/')[-1] for p in seen] == ['introspect', 'sudo', 'pat']
