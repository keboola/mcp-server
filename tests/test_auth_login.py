"""Tests for the local browser PKCE login + credential store (PSGO-261, Part B)."""

import asyncio
import base64
import hashlib
import json
import logging
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
    forget_tokens,
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


def test_forget_tokens_one_stack_and_all(creds_file: Path) -> None:
    save_tokens(STACK, TokenSet('kbc_at_a', 'kbc_rt_a', expires_at=time.time() + 3600))
    other = 'https://connection.other.keboola.com'
    save_tokens(other, TokenSet('kbc_at_b', 'kbc_rt_b', expires_at=time.time() + 3600))

    # forget one stack leaves the other intact
    assert forget_tokens(STACK) is True
    assert load_tokens(STACK) is None
    assert load_tokens(other) is not None
    assert forget_tokens(STACK) is False  # already gone

    # forget all clears everything
    assert forget_tokens(None) is True
    assert load_tokens(other) is None


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


@pytest.mark.asyncio
async def test_lease_pat_uses_explicit_project_ids_without_introspecting() -> None:
    # An explicit choice (e.g. from `login`'s scoping prompt) must be used as-is -- lease_pat
    # must not silently widen it back to every accessible project.
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        seen.append(path)
        if path.endswith('/token/introspect'):
            raise AssertionError('must not introspect when project_ids is given explicitly')
        if path.endswith('/auth/sudo'):
            return httpx.Response(200, json={'token': 'kbc_sudo_1'})
        if path.endswith('/auth/pat'):
            assert json.loads(request.content)['scope']['projects'] == ['18']
            return httpx.Response(201, json={'token': 'kbc_pat_leased'})
        raise AssertionError(f'unexpected path {path}')

    pat = await lease_pat(
        STACK,
        subject_token='kbc_at_parent',
        project_ids=[18],
        recovery_code='rec-9',
        transport=httpx.MockTransport(handler),
    )
    assert pat == 'kbc_pat_leased'
    assert seen == ['/v1/auth/sudo', '/v1/auth/pat']


# --- error redaction (Security hardening RFC increment) ---


@pytest.mark.asyncio
async def test_elevate_session_error_is_redacted(caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text='sensitive-detail-should-not-surface')

    with (
        caplog.at_level(logging.DEBUG, logger='keboola_mcp_server.auth_login'),
        pytest.raises(RuntimeError) as exc_info,
    ):
        await elevate_session(STACK, subject_token='kbc_at_x', totp_code='1', transport=httpx.MockTransport(handler))
    assert 'sensitive-detail-should-not-surface' not in str(exc_info.value)
    assert 'sensitive-detail-should-not-surface' in caplog.text


@pytest.mark.asyncio
async def test_create_pat_error_is_redacted(caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text='sensitive-detail-should-not-surface')

    with (
        caplog.at_level(logging.DEBUG, logger='keboola_mcp_server.auth_login'),
        pytest.raises(RuntimeError) as exc_info,
    ):
        await create_pat(
            STACK,
            subject_token='kbc_sudo_1',
            project_ids=[18],
            name='demo',
            transport=httpx.MockTransport(handler),
        )
    assert 'sensitive-detail-should-not-surface' not in str(exc_info.value)
    assert 'sensitive-detail-should-not-surface' in caplog.text


# --- per-profile credential keying (Security hardening RFC increment) ---


def test_different_profiles_same_stack_dont_collide(creds_file: Path) -> None:
    save_tokens(STACK, TokenSet('kbc_at_desktop', 'kbc_rt_d', expires_at=time.time() + 3600), profile='desktop')
    save_tokens(STACK, TokenSet('kbc_at_terminal', 'kbc_rt_t', expires_at=time.time() + 3600), profile='terminal')

    assert load_tokens(STACK, profile='desktop').access_token == 'kbc_at_desktop'
    assert load_tokens(STACK, profile='terminal').access_token == 'kbc_at_terminal'
    # No profile given resolves to the 'default' profile, distinct from either named one.
    assert load_tokens(STACK) is None


def test_profile_env_var_is_the_default_when_none_given(creds_file: Path, monkeypatch) -> None:
    monkeypatch.setenv('KBC_LOGIN_PROFILE', 'desktop')
    save_tokens(STACK, TokenSet('kbc_at_desktop', 'kbc_rt', expires_at=time.time() + 3600), profile='desktop')

    assert load_tokens(STACK).access_token == 'kbc_at_desktop'


def test_forget_one_profile_leaves_other_profiles_of_same_stack(creds_file: Path) -> None:
    save_tokens(STACK, TokenSet('a', 'r', expires_at=time.time() + 3600), profile='desktop')
    save_tokens(STACK, TokenSet('b', 'r', expires_at=time.time() + 3600), profile='terminal')

    assert forget_tokens(STACK, profile='desktop') is True
    assert load_tokens(STACK, profile='desktop') is None
    assert load_tokens(STACK, profile='terminal') is not None


@pytest.mark.asyncio
async def test_get_access_token_preserves_scope_across_refresh(creds_file: Path) -> None:
    save_tokens(
        STACK,
        TokenSet('kbc_at_old', 'kbc_rt_old', expires_at=time.time() + 5, project_ids=[18, 83], read_only=True),
    )
    await get_access_token(STACK, transport=_token_response())

    tokens = load_tokens(STACK)
    assert tokens.access_token == 'kbc_at_new'
    assert tokens.project_ids == [18, 83]
    assert tokens.read_only is True


@pytest.mark.asyncio
async def test_concurrent_get_access_token_refreshes_once(creds_file: Path) -> None:
    # Two callers racing a near-expiry refresh for the SAME (stack, profile) must only hit the
    # network once -- the second one, after acquiring the lock, sees the already-refreshed token.
    save_tokens(STACK, TokenSet('kbc_at_old', 'kbc_rt_old', expires_at=time.time() + 5))
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(
            200,
            json={'accessToken': 'kbc_at_new', 'refreshToken': 'kbc_rt_new', 'expiresIn': 3600, 'sessionId': 's'},
        )

    transport = httpx.MockTransport(handler)
    results = await asyncio.gather(
        get_access_token(STACK, transport=transport),
        get_access_token(STACK, transport=transport),
    )
    assert results == ['kbc_at_new', 'kbc_at_new']
    assert call_count == 1


@pytest.mark.asyncio
async def test_get_access_token_dead_refresh_does_not_clobber_newer_entry(creds_file: Path, monkeypatch) -> None:
    # If the stored refresh token changed (another caller already rotated it) between our read
    # and our failed refresh attempt, don't drop the newer entry.
    save_tokens(STACK, TokenSet('kbc_at_old', 'kbc_rt_dead', expires_at=time.time() + 5))

    async def fake_refresh(*_a, **_k):
        # Simulate another process/caller rotating the token concurrently, then our own
        # (now-stale) refresh attempt failing against the auth server.
        save_tokens(STACK, TokenSet('kbc_at_newer', 'kbc_rt_newer', expires_at=time.time() + 3600))
        raise httpx.HTTPStatusError('dead', request=httpx.Request('POST', STACK), response=httpx.Response(401))

    monkeypatch.setattr(auth_login, 'refresh_tokens', fake_refresh)
    with pytest.raises(RuntimeError, match='has expired'):
        await get_access_token(STACK)

    # The newer entry (written by the "other caller") must survive, not be forgotten.
    assert load_tokens(STACK).access_token == 'kbc_at_newer'


# --- agent provisioning (DMD-1939) ---

_PROVISION_BODY = {
    'project': {'id': 4321, 'name': 'Agent project', 'backend': 'snowflake'},
    'accessToken': 'kbc_at_sess-9_secret',
    'refreshToken': 'kbc_rt_sess-9_secret',
    'tokenType': 'Bearer',
    'accessTokenExpiresIn': 3600,
    'sessionId': 'sess-9',
    'backendInitDispatchedAsync': True,
    'claimToken': 'kbc_apc_claim_secret',
    'claimId': 'claim',
    'confirmUrl': 'https://connection.keboola.com/agent-project/confirm?token=kbc_apc_claim_secret',
}


@pytest.mark.asyncio
async def test_provision_agent_project_sends_request_and_parses_session() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['url'] = str(request.url)
        captured['body'] = json.loads(request.content)
        return httpx.Response(200, json=_PROVISION_BODY)

    provisioned = await auth_login.provision_agent_project(
        STACK,
        client_id='claude-code',
        project_name='My project',
        backend='bigquery',
        transport=httpx.MockTransport(handler),
    )

    assert captured['url'] == f'{STACK}/manage/programmatic-projects'
    assert captured['body'] == {'clientId': 'claude-code', 'projectName': 'My project', 'backend': 'bigquery'}
    assert provisioned.project_id == 4321
    assert provisioned.project_name == 'Agent project'
    assert provisioned.backend == 'snowflake'
    assert provisioned.confirm_url == _PROVISION_BODY['confirmUrl']
    assert provisioned.backend_init_dispatched_async is True
    assert provisioned.tokens.access_token == 'kbc_at_sess-9_secret'
    assert provisioned.tokens.refresh_token == 'kbc_rt_sess-9_secret'
    assert provisioned.tokens.session_id == 'sess-9'
    # `accessTokenExpiresIn` is the endpoint's spelling of `expiresIn`.
    assert not provisioned.tokens.is_near_expiry


@pytest.mark.asyncio
async def test_provision_agent_project_omits_optional_fields() -> None:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['body'] = json.loads(request.content)
        return httpx.Response(200, json=_PROVISION_BODY)

    await auth_login.provision_agent_project(STACK, client_id='claude-code', transport=httpx.MockTransport(handler))

    assert captured['body'] == {'clientId': 'claude-code'}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('status', 'headers', 'expected_type', 'expected_message'),
    [
        (404, {}, auth_login.AgentProvisioningUnavailableError, 'not available'),
        (429, {'Retry-After': '60'}, RuntimeError, 'retry in 60 second'),
        (429, {}, RuntimeError, 'retry later'),
    ],
)
async def test_provision_agent_project_maps_errors(
    status: int, headers: dict, expected_type: type, expected_message: str
) -> None:
    transport = httpx.MockTransport(lambda _rq: httpx.Response(status, json={}, headers=headers))

    with pytest.raises(expected_type, match=expected_message):
        await auth_login.provision_agent_project(STACK, client_id='claude-code', transport=transport)


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        ('claude-code', 'claude-code'),
        ('Claude Code', 'Claude-Code'),
        ('cursor-ide/1.2', 'cursor-ide-1.2'),
        ('!!!', 'keboola-mcp-server'),
        (None, 'keboola-mcp-server'),
        ('x' * 100, 'x' * 64),
    ],
)
def test_sanitize_client_id(raw: str | None, expected: str) -> None:
    assert auth_login.sanitize_client_id(raw) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('stored', 'rejected', 'expected_removed'),
    [
        ('kbc_at_dead', 'kbc_at_dead', True),
        ('kbc_at_dead', 'Bearer kbc_at_dead', True),
        # Rotated by a concurrent refresh, or replaced by a fresh `login`, since the rejected call.
        ('kbc_at_rotated', 'kbc_at_dead', False),
        # A token supplied per request (an HTTP header) was never the stored session.
        ('kbc_at_mine', 'kbc_at_from_a_header', False),
    ],
    ids=['same_token', 'same_token_with_scheme', 'rotated_meanwhile', 'per_request_token'],
)
async def test_forget_rejected_access_token_compares_before_deleting(
    creds_file: Path, stored: str, rejected: str, expected_removed: bool
) -> None:
    save_tokens(STACK, TokenSet(access_token=stored, refresh_token='kbc_rt_x', expires_at=time.time() + 3600))

    removed = await auth_login.forget_rejected_access_token(STACK, rejected)

    assert removed is expected_removed
    remaining = load_tokens(STACK)
    if expected_removed:
        assert remaining is None
    else:
        assert remaining is not None and remaining.access_token == stored


@pytest.mark.asyncio
async def test_forget_rejected_access_token_without_a_stored_session(creds_file: Path) -> None:
    assert await auth_login.forget_rejected_access_token(STACK, 'kbc_at_dead') is False
