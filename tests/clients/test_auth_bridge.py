"""Tests for the auth-bridge programmatic-token exchange (PSGO-261)."""

from http import HTTPStatus
from pathlib import Path

import httpx
import pytest

from keboola_mcp_server.clients.auth_bridge import (
    OAuthSessionExchanger,
    OAuthTokenExchangeError,
    StorageTokenExchangeError,
    StorageTokenResolver,
    is_programmatic_token,
)

STORAGE_API_URL = 'https://connection.keboola.com'


@pytest.mark.parametrize(
    ('token', 'expected'),
    [
        ('kbc_at_019ef801_abc', True),
        ('kbc_pat_019ef801_abc', True),
        ('Bearer kbc_at_019ef801_abc', True),
        ('bearer kbc_pat_019ef801_abc', True),
        ('123-legacy-storage-token', False),
        ('kbc_rt_019ef801_abc', False),  # refresh token is not a Storage-token bearer
        ('', False),
        (None, False),
    ],
)
def test_is_programmatic_token(token: str | None, expected: bool) -> None:
    assert is_programmatic_token(token) is expected


@pytest.fixture
def sa_token_file(tmp_path: Path) -> Path:
    path = tmp_path / 'sa-token'
    path.write_text('  sa-jwt-value\n')  # surrounding whitespace must be stripped
    return path


def _resolver(sa_token_file: Path, handler) -> StorageTokenResolver:
    return StorageTokenResolver(
        storage_api_url=STORAGE_API_URL,
        kubernetes_token_path=str(sa_token_file),
        transport=httpx.MockTransport(handler),
    )


@pytest.mark.asyncio
async def test_resolve_success_sends_expected_request(sa_token_file: Path) -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['request'] = request
        return httpx.Response(HTTPStatus.OK, json={'storageToken': 'legacy-token', 'projectId': 42})

    resolver = _resolver(sa_token_file, handler)
    token = await resolver.resolve(subject_token='Bearer kbc_pat_abc', project_id=42)

    assert token == 'legacy-token'
    rq = captured['request']
    assert rq.url.path == '/manage/internal/auth-bridge/resolve-storage-token'
    assert rq.headers['X-Kubernetes-Authorization'] == 'Bearer sa-jwt-value'
    # Subject token is normalized to a single Bearer scheme regardless of inbound form.
    assert rq.headers['X-Subject-Token'] == 'Bearer kbc_pat_abc'


@pytest.mark.asyncio
async def test_resolve_reuses_one_client_across_calls(sa_token_file: Path) -> None:
    # PSGO-280 (P1): a resolver is held for the life of the server and reused across an
    # N-project fan-out, so it must pool one connection rather than handshaking per resolve.
    seen_clients = []

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(HTTPStatus.OK, json={'storageToken': 'legacy-token'})

    resolver = _resolver(sa_token_file, handler)

    for _ in range(3):
        seen_clients.append(resolver._client())
        await resolver.resolve(subject_token='kbc_at_abc', project_id=1)

    assert len({id(c) for c in seen_clients}) == 1


@pytest.mark.asyncio
async def test_resolve_creates_a_new_client_after_aclose(sa_token_file: Path) -> None:
    resolver = _resolver(sa_token_file, lambda rq: httpx.Response(HTTPStatus.OK, json={'storageToken': 'x'}))
    first = resolver._client()

    await resolver.aclose()

    assert resolver._client() is not first


@pytest.mark.parametrize('status', [HTTPStatus.BAD_REQUEST, HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN])
@pytest.mark.asyncio
async def test_resolve_passes_through_client_errors(sa_token_file: Path, status: HTTPStatus) -> None:
    resolver = _resolver(sa_token_file, lambda rq: httpx.Response(status, json={'error': 'nope'}))
    with pytest.raises(StorageTokenExchangeError) as exc:
        await resolver.resolve(subject_token='kbc_at_abc', project_id=1)
    assert exc.value.status_code == int(status)


@pytest.mark.parametrize('status', [HTTPStatus.INTERNAL_SERVER_ERROR, HTTPStatus.BAD_GATEWAY, HTTPStatus.NOT_FOUND])
@pytest.mark.asyncio
async def test_resolve_maps_other_statuses_to_502(sa_token_file: Path, status: HTTPStatus) -> None:
    resolver = _resolver(sa_token_file, lambda rq: httpx.Response(status))
    with pytest.raises(StorageTokenExchangeError) as exc:
        await resolver.resolve(subject_token='kbc_at_abc', project_id=1)
    assert exc.value.status_code == int(HTTPStatus.BAD_GATEWAY)


@pytest.mark.asyncio
async def test_resolve_maps_network_error_to_502(sa_token_file: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('boom', request=request)

    resolver = _resolver(sa_token_file, handler)
    with pytest.raises(StorageTokenExchangeError) as exc:
        await resolver.resolve(subject_token='kbc_at_abc', project_id=1)
    assert exc.value.status_code == int(HTTPStatus.BAD_GATEWAY)
    # No token material leaks into the message.
    assert 'kbc_at_abc' not in str(exc.value)


@pytest.mark.asyncio
async def test_resolve_missing_storage_token_maps_to_502(sa_token_file: Path) -> None:
    resolver = _resolver(sa_token_file, lambda rq: httpx.Response(HTTPStatus.OK, json={'projectId': 1}))
    with pytest.raises(StorageTokenExchangeError) as exc:
        await resolver.resolve(subject_token='kbc_at_abc', project_id=1)
    assert exc.value.status_code == int(HTTPStatus.BAD_GATEWAY)


@pytest.mark.asyncio
async def test_resolve_empty_sa_token_file_fails_loudly(tmp_path: Path) -> None:
    empty = tmp_path / 'empty'
    empty.write_text('   ')
    resolver = StorageTokenResolver(
        storage_api_url=STORAGE_API_URL,
        kubernetes_token_path=str(empty),
        transport=httpx.MockTransport(lambda rq: httpx.Response(HTTPStatus.OK, json={'storageToken': 'x'})),
    )
    with pytest.raises(ValueError, match='empty'):
        await resolver.resolve(subject_token='kbc_at_abc', project_id=1)


def test_invalid_storage_api_url_rejected(sa_token_file: Path) -> None:
    with pytest.raises(ValueError, match='Invalid Keboola Storage API URL'):
        StorageTokenResolver(storage_api_url='https://example.com', kubernetes_token_path=str(sa_token_file))


def _exchanger(sa_token_file: Path, handler) -> OAuthSessionExchanger:
    return OAuthSessionExchanger(
        storage_api_url=STORAGE_API_URL,
        kubernetes_token_path=str(sa_token_file),
        transport=httpx.MockTransport(handler),
    )


def test_invalid_storage_api_url_rejected_for_oauth_exchanger(sa_token_file: Path) -> None:
    with pytest.raises(ValueError, match='Invalid Keboola Storage API URL'):
        OAuthSessionExchanger(storage_api_url='https://example.com', kubernetes_token_path=str(sa_token_file))


@pytest.mark.asyncio
async def test_exchange_success_sends_expected_request(sa_token_file: Path) -> None:
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['request'] = request
        return httpx.Response(
            HTTPStatus.OK,
            json={'accessToken': 'kbc_at_new', 'refreshToken': 'kbc_rt_new', 'expiresIn': 3600, 'sessionId': 's1'},
        )

    exchanger = _exchanger(sa_token_file, handler)
    body = await exchanger.exchange(oauth_access_token='Bearer league-oauth-token')

    assert body == {'accessToken': 'kbc_at_new', 'refreshToken': 'kbc_rt_new', 'expiresIn': 3600, 'sessionId': 's1'}
    rq = captured['request']
    assert rq.url.path == '/manage/internal/auth-bridge/exchange-oauth-token'
    assert rq.headers['X-Kubernetes-Authorization'] == 'Bearer sa-jwt-value'
    # X-KBC-ManageApiToken is a distinct, mutually-exclusive authenticator -- must never be sent
    # alongside X-Kubernetes-Authorization (confirmed against Connection's source).
    assert 'X-KBC-ManageApiToken' not in rq.headers
    # Subject token is normalized to a single Bearer scheme regardless of inbound form.
    assert rq.headers['X-Subject-Token'] == 'Bearer league-oauth-token'


@pytest.mark.asyncio
async def test_exchange_empty_sa_token_file_fails_loudly(tmp_path: Path) -> None:
    empty = tmp_path / 'empty'
    empty.write_text('   ')
    exchanger = OAuthSessionExchanger(
        storage_api_url=STORAGE_API_URL,
        kubernetes_token_path=str(empty),
        transport=httpx.MockTransport(lambda rq: httpx.Response(HTTPStatus.OK, json={'accessToken': 'x'})),
    )
    with pytest.raises(ValueError, match='empty'):
        await exchanger.exchange(oauth_access_token='league-oauth-token')


@pytest.mark.parametrize('status', [HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN])
@pytest.mark.asyncio
async def test_exchange_passes_through_client_errors(sa_token_file: Path, status: HTTPStatus) -> None:
    exchanger = _exchanger(sa_token_file, lambda rq: httpx.Response(status, json={'error': 'nope'}))
    with pytest.raises(OAuthTokenExchangeError) as exc:
        await exchanger.exchange(oauth_access_token='league-oauth-token')
    assert exc.value.status_code == int(status)


@pytest.mark.parametrize('status', [HTTPStatus.INTERNAL_SERVER_ERROR, HTTPStatus.BAD_GATEWAY, HTTPStatus.NOT_FOUND])
@pytest.mark.asyncio
async def test_exchange_maps_other_statuses_to_502(sa_token_file: Path, status: HTTPStatus) -> None:
    exchanger = _exchanger(sa_token_file, lambda rq: httpx.Response(status))
    with pytest.raises(OAuthTokenExchangeError) as exc:
        await exchanger.exchange(oauth_access_token='league-oauth-token')
    assert exc.value.status_code == int(HTTPStatus.BAD_GATEWAY)


@pytest.mark.asyncio
async def test_exchange_maps_network_error_to_502(sa_token_file: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('boom', request=request)

    exchanger = _exchanger(sa_token_file, handler)
    with pytest.raises(OAuthTokenExchangeError) as exc:
        await exchanger.exchange(oauth_access_token='league-oauth-token')
    assert exc.value.status_code == int(HTTPStatus.BAD_GATEWAY)
    assert 'league-oauth-token' not in str(exc.value)


@pytest.mark.parametrize(
    'body',
    [
        {'projectId': 1},  # missing both tokens
        {'accessToken': 'kbc_at_new'},  # missing refreshToken
        {'refreshToken': 'kbc_rt_new'},  # missing accessToken
    ],
)
@pytest.mark.asyncio
async def test_exchange_incomplete_response_maps_to_502(sa_token_file: Path, body: dict) -> None:
    exchanger = _exchanger(sa_token_file, lambda rq: httpx.Response(HTTPStatus.OK, json=body))
    with pytest.raises(OAuthTokenExchangeError) as exc:
        await exchanger.exchange(oauth_access_token='league-oauth-token')
    assert exc.value.status_code == int(HTTPStatus.BAD_GATEWAY)
