import json

import httpx
import pytest

from integtests.service_login import login, project_token, totp_code

# RFC 6238 appendix B seed ("12345678901234567890") and its published 6-digit codes
SEED = 'GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ'


@pytest.mark.parametrize(('now', 'expected'), [(59, '287082'), (1111111109, '081804'), (2000000000, '279037')])
def test_totp_code_matches_rfc_6238_vectors(now: int, expected: str) -> None:
    assert totp_code(SEED, now=now) == expected
    assert totp_code(f'{SEED[:4].lower()} {SEED[4:]}', now=now) == expected  # separators and case are ignored


@pytest.mark.parametrize(
    ('mfa_required', 'expected_paths'),
    [
        (False, ['v1/auth/login']),
        (True, ['v1/auth/login', 'v1/auth/mfa']),
    ],
)
def test_login_returns_the_access_token(mfa_required: bool, expected_paths: list[str]) -> None:
    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path.lstrip('/')
        seen.append((path, json.loads(request.content)))
        if path == 'v1/auth/login' and mfa_required:
            return httpx.Response(200, json={'mfaRequired': True, 'mfaToken': 'mfa-1'})
        return httpx.Response(200, json={'accessToken': 'kbc_at_session', 'refreshToken': 'kbc_rt_x'})

    token = login(
        'https://connection.keboola.test/', 'svc@example.com', 'pw', SEED, transport=httpx.MockTransport(handler)
    )

    assert token == 'kbc_at_session'
    assert [path for path, _ in seen] == expected_paths
    assert seen[0][1] == {'grantType': 'password', 'email': 'svc@example.com', 'password': 'pw'}
    if mfa_required:
        assert seen[1][1]['mfaToken'] == 'mfa-1'
        assert seen[1][1]['type'] == 'totp'
        assert len(seen[1][1]['code']) == 6


def test_login_failure_does_not_surface_the_response_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={'error': 'bad password hunter2 for svc@example.com'})

    with pytest.raises(RuntimeError, match=r'POST /v1/auth/login failed \(401\)') as exc_info:
        login(
            'https://connection.keboola.test',
            'svc@example.com',
            'hunter2',
            SEED,
            transport=httpx.MockTransport(handler),
        )

    assert 'hunter2' not in str(exc_info.value)


def test_login_refuses_a_cleartext_url() -> None:
    with pytest.raises(RuntimeError, match='https'):
        login('http://connection.keboola.test', 'svc@example.com', 'pw', SEED)


@pytest.mark.parametrize(('status', 'attempts'), [(401, 2), (429, 1)])
def test_login_retries_only_a_rejected_mfa_code(monkeypatch: pytest.MonkeyPatch, status: int, attempts: int) -> None:
    monkeypatch.setattr('integtests.service_login.time.sleep', lambda _: None)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path.lstrip('/')
        calls.append(path)
        if path == 'v1/auth/login':
            return httpx.Response(200, json={'mfaRequired': True, 'mfaToken': 'mfa-1'})
        return httpx.Response(status, json={})

    with pytest.raises(RuntimeError, match=f'failed \\({status}\\)'):
        login('https://connection.keboola.test', 'svc@example.com', 'pw', SEED, transport=httpx.MockTransport(handler))

    assert calls.count('v1/auth/mfa') == attempts


def test_login_retries_the_mfa_step_once_with_the_next_window_code(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr('integtests.service_login.time.sleep', slept.append)
    mfa_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path.lstrip('/')
        if path == 'v1/auth/login':
            return httpx.Response(200, json={'mfaRequired': True, 'mfaToken': 'mfa-1'})
        mfa_calls.append(json.loads(request.content)['code'])
        if len(mfa_calls) == 1:
            return httpx.Response(401, json={'error': 'code already used'})
        return httpx.Response(200, json={'accessToken': 'kbc_at_session'})

    token = login(
        'https://connection.keboola.test', 'svc@example.com', 'pw', SEED, transport=httpx.MockTransport(handler)
    )

    assert token == 'kbc_at_session'
    assert len(mfa_calls) == 2
    assert len(slept) == 1
    assert 1 <= slept[0] <= 31


def test_project_token_exchanges_the_session_token_for_one_project() -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen['path'] = request.url.path
        seen['auth'] = request.headers['Authorization']
        seen['body'] = json.loads(request.content)
        return httpx.Response(200, json={'accessToken': 'kbc_pat_child', 'expiresIn': 10800})

    token = project_token(
        'https://connection.keboola.test', 'kbc_at_session', 2728, transport=httpx.MockTransport(handler)
    )

    assert token == 'kbc_pat_child'
    assert seen['path'] == '/v1/auth/pat/exchange'
    assert seen['auth'] == 'Bearer kbc_at_session'
    assert seen['body']['scope'] == {'projects': ['2728']}  # the API rejects integer ids
    assert seen['body']['expiresIn'] > 0
