from datetime import datetime, timedelta, timezone

import pytest

from tests.session_store.conftest import requires_postgres

pytestmark = [pytest.mark.asyncio, requires_postgres]


async def test_create_and_get_by_access_token(store) -> None:
    expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
    access_token, refresh_token, session = await store.create(
        client_id='claude.ai',
        user_email='m@k.com',
        kbc_access_token='kbc_at_secret',
        kbc_refresh_token='kbc_rt_secret',
        kbc_access_expires_at=expires_at,
    )

    assert access_token
    assert refresh_token
    assert access_token != refresh_token
    assert session.client_id == 'claude.ai'
    assert session.kbc_access_token == 'kbc_at_secret'
    assert session.kbc_refresh_token == 'kbc_rt_secret'
    assert session.scope_confirmed is False
    assert session.scope_project_ids is None

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.id == session.id
    assert fetched.kbc_access_token == 'kbc_at_secret'


async def test_get_by_access_token_unknown_returns_none(store) -> None:
    assert await store.get_by_access_token('does-not-exist') is None


async def test_get_by_refresh_token(store) -> None:
    _, refresh_token, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    fetched = await store.get_by_refresh_token(refresh_token)
    assert fetched is not None
    assert fetched.id == session.id


async def test_rotate_kbc_tokens_replaces_credentials(store) -> None:
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_old',
        kbc_refresh_token='kbc_rt_old',
        kbc_access_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    new_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
    await store.rotate_kbc_tokens(
        session.id, kbc_access_token='kbc_at_new', kbc_refresh_token='kbc_rt_new', kbc_access_expires_at=new_expiry
    )

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.kbc_access_token == 'kbc_at_new'
    assert fetched.kbc_refresh_token == 'kbc_rt_new'


async def test_update_scope_confirms_project_selection(store) -> None:
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    scoped_expiry = datetime.now(timezone.utc) + timedelta(minutes=30)
    await store.update_scope(
        session.id,
        project_ids=[18, 83],
        read_only=True,
        confirmed=True,
        scoped_token='kbc_pat_scoped',
        scoped_expires_at=scoped_expiry,
    )

    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.scope_project_ids == [18, 83]
    assert fetched.scope_read_only is True
    assert fetched.scope_confirmed is True
    assert fetched.scope_scoped_token == 'kbc_pat_scoped'


async def test_update_scope_without_scoped_token(store) -> None:
    # The whole-stack fallback path (resolver exchange unavailable) confirms scope with no minted
    # token -- must not choke on a None scoped_token.
    access_token, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    await store.update_scope(
        session.id, project_ids=[18], read_only=False, confirmed=True, scoped_token=None, scoped_expires_at=None
    )
    fetched = await store.get_by_access_token(access_token)
    assert fetched is not None
    assert fetched.scope_scoped_token is None


async def test_revoke_makes_session_unreachable(store) -> None:
    access_token, refresh_token, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_x',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    await store.revoke(session.id)

    assert await store.get_by_access_token(access_token) is None
    assert await store.get_by_refresh_token(refresh_token) is None


async def test_credentials_are_encrypted_at_rest(store) -> None:
    # Read the raw row directly -- the plaintext secret must never appear in storage.
    _, _, session = await store.create(
        client_id='claude.ai',
        user_email=None,
        kbc_access_token='kbc_at_should_not_appear_in_plaintext',
        kbc_refresh_token='kbc_rt_x',
        kbc_access_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    raw = await store._pool.fetchrow('SELECT kbc_access_token_enc FROM oauth_sessions WHERE id = $1', session.id)
    assert b'kbc_at_should_not_appear_in_plaintext' not in raw['kbc_access_token_enc']
