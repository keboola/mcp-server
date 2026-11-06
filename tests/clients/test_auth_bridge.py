"""Tests for the auth-bridge programmatic-token exchange (PSGO-261)."""

import json
from http import HTTPStatus
from pathlib import Path

import httpx
import pytest

from keboola_mcp_server.clients.auth_bridge import (
    OAuthSessionExchanger,
    OAuthTokenExchangeError,
)

STORAGE_API_URL = 'https://connection.keboola.com'


@pytest.fixture
def sa_token_file(tmp_path: Path) -> Path:
    path = tmp_path / 'sa-token'
    path.write_text('  sa-jwt-value\n')  # surrounding whitespace must be stripped
    return path


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
    assert json.loads(rq.content) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('app_client_id', 'reported_client_name', 'expected_body'),
    [
        ('local-mcp-client', 'Codex', {'app_client_id': 'local-mcp-client', 'reported_client_name': 'Codex'}),
        # the pair or nothing: Connection stores a name only with the row it is for
        ('local-mcp-client', None, {}),
        (None, 'Codex', {}),
    ],
)
async def test_exchange_sends_the_reported_client_name_in_the_body(
    sa_token_file: Path, app_client_id: str | None, reported_client_name: str | None, expected_body: dict
) -> None:
    """The wire format of Connection's `exchange-oauth-token` body (DMD-2599/DMD-2600)."""
    captured: dict[str, httpx.Request] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured['request'] = request
        return httpx.Response(HTTPStatus.OK, json={'accessToken': 'a', 'refreshToken': 'r', 'expiresIn': 3600})

    exchanger = _exchanger(sa_token_file, handler)
    await exchanger.exchange(
        oauth_access_token='league-oauth-token', app_client_id=app_client_id, reported_client_name=reported_client_name
    )

    assert json.loads(captured['request'].content) == expected_body


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
async def test_exchange_passes_through_client_errors(
    sa_token_file: Path, status: HTTPStatus, caplog: pytest.LogCaptureFixture
) -> None:
    """Same log-level split as StorageTokenResolver.resolve(): a pass-through status is the
    caller's own token being rejected, not this server's fault."""
    exchanger = _exchanger(sa_token_file, lambda rq: httpx.Response(status, json={'error': 'nope'}))
    with caplog.at_level('WARNING'), pytest.raises(OAuthTokenExchangeError) as exc:
        await exchanger.exchange(oauth_access_token='league-oauth-token')
    assert exc.value.status_code == int(status)
    assert not any(r.levelname == 'ERROR' for r in caplog.records)


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
