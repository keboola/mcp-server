import hashlib
import re
from datetime import datetime, timedelta, timezone

import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.storage import AsyncStorageClient
from keboola_mcp_server.search_index import verify as verify_mod
from keboola_mcp_server.search_index.types import VerifiedSession
from keboola_mcp_server.search_index.verify import token_hash, verify_and_cache

_VERIFY_RESPONSE = {'owner': {'id': '1234', 'name': 'Test Project', 'features': []}}


@pytest.fixture(autouse=True)
def _clear_verify_cache():
    verify_mod._clear_cache()
    yield
    verify_mod._clear_cache()


@pytest.fixture
def storage_client(mocker: MockerFixture) -> AsyncStorageClient:
    client = mocker.AsyncMock(AsyncStorageClient)
    client.verify_token.return_value = _VERIFY_RESPONSE
    return client


def test_token_hash_is_deterministic_lowercase_16():
    h1 = token_hash('my-token')
    h2 = token_hash('my-token')
    assert h1 == h2
    assert re.fullmatch(r'[a-f0-9]{16}', h1)
    assert h1 == hashlib.sha256(b'my-token').hexdigest()[:16]


def test_token_hash_differs_per_token():
    assert token_hash('token-a') != token_hash('token-b')


@pytest.mark.asyncio
async def test_verify_and_cache_calls_api_once_per_token(storage_client):
    a = await verify_and_cache(storage_client, 'tkn')
    b = await verify_and_cache(storage_client, 'tkn')
    assert a is b
    storage_client.verify_token.assert_awaited_once()


@pytest.mark.asyncio
async def test_verify_and_cache_returns_well_formed_session(storage_client):
    session = await verify_and_cache(storage_client, 'tkn')
    assert isinstance(session, VerifiedSession)
    assert session.project_id == '1234'
    assert session.token_hash == token_hash('tkn')


@pytest.mark.asyncio
async def test_verify_and_cache_different_tokens_hit_api_each(storage_client):
    await verify_and_cache(storage_client, 'token-a')
    await verify_and_cache(storage_client, 'token-b')
    assert storage_client.verify_token.await_count == 2


@pytest.mark.asyncio
async def test_verify_and_cache_re_verifies_after_ttl(storage_client):
    first = await verify_and_cache(storage_client, 'tkn', ttl_seconds=60)
    aged = VerifiedSession(
        project_id=first.project_id,
        token_hash=first.token_hash,
        verified_at=datetime.now(timezone.utc) - timedelta(seconds=120),
    )
    verify_mod._cache[first.token_hash] = aged

    second = await verify_and_cache(storage_client, 'tkn', ttl_seconds=60)
    assert second is not aged
    assert storage_client.verify_token.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'bad_response',
    [
        {},
        {'owner': None},
        {'owner': {}},
        {'owner': {'id': None}},
        {'owner': 'not-a-dict'},
    ],
)
async def test_verify_and_cache_rejects_missing_owner_id(storage_client, bad_response):
    storage_client.verify_token.return_value = bad_response
    with pytest.raises(ValueError, match='owner.id'):
        await verify_and_cache(storage_client, 'tkn')
    assert token_hash('tkn') not in verify_mod._cache


@pytest.mark.asyncio
async def test_verify_and_cache_does_not_swallow_api_failure(storage_client):
    storage_client.verify_token.side_effect = RuntimeError('network down')
    with pytest.raises(RuntimeError, match='network down'):
        await verify_and_cache(storage_client, 'tkn')
    assert token_hash('tkn') not in verify_mod._cache
